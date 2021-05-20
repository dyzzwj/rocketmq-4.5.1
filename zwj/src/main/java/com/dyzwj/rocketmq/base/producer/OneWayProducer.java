package com.dyzwj.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName OneWayProducer.java
 * @Description 单向消息
 * @createTime 2020年03月04日 10:59:00
 */


public class OneWayProducer {

    public static void main(String[] args) throws Exception {
        //1、创建生产者 并指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("oneway-1");
        //2、指定nameserver
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        //3、启动生产者
        producer.start();
        for (int i = 0; i < 10; i++) {
            //4、创建消息，指定topic、tag、消息体
            Message message = new Message("oneway","tag1",("Hello OneWay" + i).getBytes());
            //5、发送单向消息
            //单向发送消息。没有任何返回结果
            producer.sendOneway(message);
            TimeUnit.SECONDS.sleep(1);
        }

        producer.shutdown();

    }

}
