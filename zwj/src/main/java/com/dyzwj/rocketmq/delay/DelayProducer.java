package com.dyzwj.rocketmq.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName DelayProducer.java
 * @Description 延迟消息
 * @createTime 2020年03月09日 10:49:00
 */
public class DelayProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("delay-1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();

        for (int i = 0; i < 10; i++) {
           Message message = new Message("delay","tag1",("Hello Delay" + 1).getBytes());
           //设置延迟级别
           message.setDelayTimeLevel(2);
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
            TimeUnit.SECONDS.sleep(1);
        }

        producer.shutdown();


    }


}
