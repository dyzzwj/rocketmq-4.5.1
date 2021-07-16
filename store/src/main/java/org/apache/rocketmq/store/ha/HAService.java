/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;

public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * master维护的连接数
     */
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    /**
     * master维护的连接信息
     */
    private final List<HAConnection> connectionList = new LinkedList<>();

    /**
     * master节点接受slave节点连接
     */
    private final AcceptSocketService acceptSocketService;

    //Broker存储实现。
    private final DefaultMessageStore defaultMessageStore;
    //同步等待实现
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    //该Master所有Slave中同步最大的偏移量
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    //判断主从同步复制是否完成
    private final GroupTransferService groupTransferService;

    /**
     * slave对master节点连接、读写数据 线程对象
     */
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        //master节点接受slave节点连接
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        //slave对master节点连接、读写数据 线程对象
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            /**
             *  设置slave给master上报commitLog最新的offset
             *  GroupTransferService.doWaitTransfer会用到push2SlaveMaxOffset
             */
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     *
     * master节点接受slave节点连接
     */
    class AcceptSocketService extends ServiceThread {
        //Broker服务监听套接字(本地IP+端口号)
        private final SocketAddress socketAddressListen;
        //服务端Socket通道，基于NIO
        private ServerSocketChannel serverSocketChannel;
        //事件选择器，基于NIO。
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            //打开通道
            this.serverSocketChannel = ServerSocketChannel.open();

            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            //绑定地址
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            //设置为非阻塞
            this.serverSocketChannel.configureBlocking(false);
            //注册接受连接事件
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         * master节点接受slave节点连接
         *
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //选择就绪事件 最多等待1s
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        //遍历就绪事件
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                //接受连接
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 每有一个Slave连接进来，都会构建一个HAConnection对象，并持有对应的Channel
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        /**
                                         *  readSocketService：处理Slave上传的进度及其他相关操作
                                         *  writeSocketService：
                                         */
                                        conn.start();
                                        //将连接保存到connectionList中
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     *
     *   同步主从
     *
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        /**
                         * 等待slave上传进度
                         * 重试 5次，每次条件不符合都等待Slave上传同步结果
                         */
                        for (int i = 0; !transferOK && i < 5; i++) {
                            //等待
                            this.notifyTransferObject.waitForRunning(1000);
                            //判断同步到的offset跟req的offset
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }
                        // 唤醒请求，并设置是否Slave同步成功
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    /**
                     * 等待
                     * GroupTransferService.putRequest会唤醒
                     */
                    this.waitForRunning(10);
                    //
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }


    /**
     * slave对master节点连接、读写数据 线程对象
     */
    class HAClient extends ServiceThread {
        //socket读缓冲区最大大小
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /**
         * master地址 ip:port
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        /**
         *  向Master汇报Slave最大Offset  Slave向Master发起主从同步的拉取偏移量，固定8个字节
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;
        private Selector selector;
        //上一次写入时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * // Slave向Master汇报Offset，反馈Slave当前的复制进度，当前slave commitlog文件最大偏移量
         */
        private long currentReportedOffset = 0;
        // 粘包拆包使用的 本次已处理读缓存区的指针
        private int dispatchPostion = 0;
        // 从Master接收数据Buffer
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        //读缓存区备份，与BufferRead进行交换
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPostion;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPostion);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPostion = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            /**
             * 如果读取到的字节数大于0，重置读取到0字节的次数，并更新最后一次写入时间戳（lastWriteTimestamp），然后调用dispatchReadRequest()转发该请求，处理消息的解析、入库。
             * 如果连续3次从网络通道读取到0个字节，则结束本次读，返回true。
             * 如果读取到的字节数小于0或发生IO异常，则返回false。
             */
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        readSizeZeroTimes = 0;
                        /**
                         * 处理请求
                         */
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 读取Master传输的CommitLog数据，并返回是否异常
         * 如果读取到数据，写入CommitLog
         * 异常原因：
         *   1. Master传输来的数据offset 不等于 Slave的CommitLog数据最大offset
         *   2. 上报到Master进度失败
         *
         * @return 是否异常
         */
        private boolean dispatchReadRequest() {

            //头部长度 大小为12个字节，包括消息的物理偏移量与消息的长度，长度字节必须首先探测，
            // 否则无法判断byteBufferRead缓存区中是否包含一条完整的消息。
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            //readSocketPos：记录当前byteBufferRead的当前指针。
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                //读取到请求
                if (diff >= msgHeaderSize) {
                    /**
                     * 先探测byteBufferRead缓冲区中是否包含一条消息的头部，如果包含头部，则读取物理偏移量与消息长度，
                     * 然后再探测是否包含一条完整的消息，如果不包含，则需要将byteBufferRead中的数据备份，以便更多数据到达再处理。
                     */
                    // 读取masterPhyOffset、bodySize。使用dispatchPostion的原因是：处理数据“粘包”导致数据读取不完整。
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        // 校验 Master传输来的数据offset 是否和 Slave的CommitLog数据最大offset 是否相同。
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    /**
                     * 如果byteBufferRead中包含一则消息头部，则读取物理偏移量与消息的长度，然后获取Slave当前消息文件的最大物理偏移量，
                     * 如果slave的最大物理偏移量与master给的偏移量不相等，则返回false，
                     * 从后面的处理逻辑来看，返回false,将会关闭与master的连接，在Slave本次周期内将不会再参与主从同步了
                     *  粘包和拆包的处理 如果diff 小于MSG_HEADER_SIZE + bodySize的值，那么代表发生拆包，等待下一次读取数据
                     */
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        /**
                         * dispatchPosition：表示byteBufferRead中已转发的指针。设置byteBufferRead的position指针为dispatchPosition+msgHeaderSize,
                         * 然后读取bodySize个字节内容到byte[]字节数组中，并调用DefaultMessageStore#appendToCommitLog方法将消息内容追加到消息内存映射文件中，
                         * 然后唤醒ReputMessageService实时将消息转发给消息消费队列与索引文件，更新dispatchPosition，并向服务端及时反馈当前已存储进度。
                         * 将所读消息存入内存映射文件后重新向服务端发送slave最新的偏移量。
                         */
                        /**
                         * 将master返回的数据写入到commitLog并唤醒重做日志线程
                         */
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);
                        //设置处理到的位置
                        this.byteBufferRead.position(readSocketPos);
                        this.dispatchPostion += msgHeaderSize + bodySize;
                        //将同步进度上报给master进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }
                        //继续循环
                        continue;
                    }
                }

                //空间写满 重新分配空间
                if (!this.byteBufferRead.hasRemaining()) {
                    /**
                     * 如果byteBufferRead中未包含一条完整的消息的处理逻辑，具体看一下reallocateByteBuffer的实现。
                     *
                     * 其核心思想是将readByteBuffer中剩余的有效数据先复制到readByteBufferBak,然后交换readByteBuffer与readByteBufferBak。
                     */
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            //获取本地commitLog的最大offset(已经同步到的offset)
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }


        /**
         * 在Broker启动时，如果其角色为SLAVE时，将读取Broker配置文件中的haMasterAddress属性更新HAClient的masterAddrees,
         * 如果角色为SLAVE但haMasterAddress为空，启动不会报错，但不会执行主从复制，该方法最终返回是否成功连接上Master。
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                /**
                 *  master的masterAddress为空 所以直接跳过
                 */
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {

                    //有没有连接到master  会更新currentReportedOffset
                    if (this.connectMaster()) {

                        //若满足上报间隔   默认5s
                        if (this.isTimeToReportOffset()) {
                            /**
                             * 如果需要向Master反馈当前拉取偏移量，则向Master发送一个8字节的请求，请求包中包含的数据为当前Broker消息文件的最大偏移量。
                             *  上报给master当前slave的本地的commitLog已经同步的物理位置
                             */
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        this.selector.select(1000);



                        /**
                         *
                         *  处理读事件
                         * 读取Master传输的CommitLog数据，并返回是否异常
                         * 如果读取到数据，写入CommitLog
                         * 异常原因：
                         *   1. Master传输来的数据offset 不等于 Slave的CommitLog数据最大offset
                         *   2. 上报到Master进度失败
                         *
                         * @return 是否异常
                         */
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }
                        //master过久未返回数据 关闭连接
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
