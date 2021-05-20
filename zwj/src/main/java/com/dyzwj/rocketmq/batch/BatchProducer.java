package com.dyzwj.rocketmq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName BatchProducer.java
 * @Description TODO
 * @createTime 2020年03月14日 10:22:00
 */
public class BatchProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("batch-1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            Message message = new Message("batch","tag1",("Hello Batch" + i).getBytes());
            messages.add(message);
        }
        //发送批量消息
        SendResult sendResult = producer.send(messages);
        System.out.println(sendResult);
        producer.shutdown();
    }


}
