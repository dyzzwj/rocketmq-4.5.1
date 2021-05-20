package com.dyzwj.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName AsyncProducer.java
 * @Description 异步发送
 * @createTime 2020年02月29日 14:47:00
 */
public class AsyncProducer {

    public static void main(String[] args) throws Exception {
        //1、创建生产者 并指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("producer-2");

        //2、指定nameserver
        producer.setNamesrvAddr("localhost:9876;localhost:9877");

        //3、启动生产者
        producer.start();
        for (int i = 0; i < 10; i++) {
            //4、创建消息 指定 topic、tag、消息体
            Message message = new Message("base","tag2",("Async " + i).getBytes());
            final int index = i;
            //5、异步发送

            //sendCallback接受异步返回结果的回调
            producer.send(message, new SendCallback() {

                //发送成功的回调
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                    System.out.println(index);
                }

                //发送失败的回调
                @Override
                public void onException(Throwable throwable) {
                    System.out.println(throwable);
                    System.out.println(index);
                }
            });


            TimeUnit.SECONDS.sleep(1);
        }


        //6、关闭生产者
        producer.shutdown();
    }

}
