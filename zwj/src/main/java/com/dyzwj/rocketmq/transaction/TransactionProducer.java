package com.dyzwj.rocketmq.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName TransactionProducer.java
 * @Description TODO
 * @createTime 2020年03月14日 11:22:00
 */
public class TransactionProducer {


    public static void main(String[] args) throws MQClientException, InterruptedException {
        //@1 定义TransactionListener
        TransactionListener transactionListener = null;
        //@2 使用事务发送Producer
        TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
        //@3 定义线程池
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        //设置线程池
        producer.setExecutorService(executorService);
        //设置监听器
        producer.setTransactionListener(transactionListener);
        producer.setNamesrvAddr("127.0.0.1:9876");
        //@4 发送者启动
        producer.start();

        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            try {
                Message msg =
                        new Message("TopicTest1234", tags[i % tags.length], "KEY" + i,
                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                //@5 消息发送
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);

                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        //发送者关闭
        producer.shutdown();
    }
}
