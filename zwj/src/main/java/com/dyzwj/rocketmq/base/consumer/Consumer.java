package com.dyzwj.rocketmq.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName Consumer.java
 * @Description 消费者
 * @createTime 2020年03月01日 11:48:00
 */

/**
 * 消费者模式：负载均衡模式（默认） 广播模式
 */
public class Consumer {

    public static void main(String[] args) throws Exception {

        //1、创建消费者 指定消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-1-0");

        //2、指定nameserver
        consumer.setNamesrvAddr("localhost:9876;localhost:9877");
        //3、订阅topic、tag
        consumer.subscribe("base","tag1");

        //设置消费者得消费模式 - 广播模式
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        //4、设置回调函数 处理消息

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach(msg -> {
                    System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //5、启动消费者

        consumer.start();

        System.out.println("消费者启动");
    }
}
