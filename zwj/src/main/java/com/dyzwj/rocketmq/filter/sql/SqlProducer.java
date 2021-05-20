package com.dyzwj.rocketmq.filter.sql;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName SqlProducer.java
 * @Description TODO
 * @createTime 2020年03月14日 10:35:00
 */
public class SqlProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("sql1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("sql","tag1",("Hello Sql" + i).getBytes());
            message.putUserProperty("i", String.valueOf(i));
            messages.add(message);
        }

        SendResult sendResult = producer.send(messages);
        System.out.println(sendResult);
        producer.shutdown();
    }
}
