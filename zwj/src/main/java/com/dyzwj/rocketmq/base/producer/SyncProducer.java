package com.dyzwj.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName SyncProducer.java
 * @Description 发送同步消息
 * @createTime 2020年02月29日 14:19:00
 */
public class SyncProducer {


    public static void main(String[] args) throws Exception {

        //1、创建生产者 并指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("producer-1");

        //2、指定nameserver
        producer.setNamesrvAddr("localhost:9876;localhost:9877");

        //3、启动生产者
        producer.start();

        for (int i = 0; i < 10; i++) {
            //4、创建消息，指定topic、tag、消息内容
            Message message = new Message("base","tag1",("Hello RocketMQ" + i).getBytes());
            //5、同步发送消息
            SendResult sendResult = producer.send(message);

            System.out.println(sendResult);
            TimeUnit.SECONDS.sleep(1);
        }
        //6、关闭生产者
        producer.shutdown();
    }

}
