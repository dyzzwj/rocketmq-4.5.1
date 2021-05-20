package com.dyzwj.rocketmq.filter.sql;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName SqlConsumer.java
 * @Description TODO
 * @createTime 2020年03月14日 10:35:00
 */
public class SqlConsumer {

    public static void main(String[] args) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("sql-1");
        consumer.setNamesrvAddr("localhost:9876;localhost:9877");
        consumer.subscribe("sql", MessageSelector.bySql("i>5"));

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : list) {
                    System.out.println("consumeThread=" + Thread.currentThread().getName() + ", tag=" + msg.getTags() + ", content:" + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        System.out.println("消费者启动");
        consumer.start();

    }
}
