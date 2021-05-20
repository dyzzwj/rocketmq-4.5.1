package com.dyzwj.rocketmq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName OrderConsumer.java
 * @Description 顺序消费者
 * @createTime 2020年03月04日 11:14:00
 */

public class OrderConsumer {

    public static void main(String[] args) throws Exception {
        //1、创建消费者，指定消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-1");

        //2、指定nameserver
        consumer.setNamesrvAddr("localhost:9876;localhost:9877");

        //3、订阅topic tag
        consumer.subscribe("order","TagA || TagC || TagD");

        //4、设置回调函数 处理消息
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                list.forEach( msg -> {
                    // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                    System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        TimeUnit.SECONDS.sleep(2);

        //5、启动消费者
        consumer.start();
        System.out.println("消费者启动...");

    }




}
