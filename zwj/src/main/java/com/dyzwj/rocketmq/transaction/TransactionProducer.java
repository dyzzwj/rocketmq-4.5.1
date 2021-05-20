package com.dyzwj.rocketmq.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName TransactionProducer.java
 * @Description TODO
 * @createTime 2020年03月14日 11:22:00
 */
public class TransactionProducer {

    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("transaction1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        producer.setTransactionListener(new TransactionListener() {
            //执行本地事务
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                return null;
            }


            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                return null;
            }
        });
    }
}
