package com.dyzwj.rocketmq.filter.tag;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName TagProducer.java
 * @Description TODO
 * @createTime 2020年03月14日 10:34:00
 */
public class TagProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("tag1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();

        String[] tags = new String[]{"tag1","tag2","tag3"};

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("tag",tags[i%tags.length],("Hello Tag" + i).getBytes());
            messages.add(message);
        }

        SendResult sendResult = producer.send(messages);
        System.out.println(sendResult);
        producer.shutdown();
    }

}
