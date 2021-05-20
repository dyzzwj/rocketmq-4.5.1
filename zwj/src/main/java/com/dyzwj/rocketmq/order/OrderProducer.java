package com.dyzwj.rocketmq.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName OrderProducer.java
 * @Description 顺序生产者
 * @createTime 2020年03月04日 11:14:00
 */
public class OrderProducer {

    public static void main(String[] args) throws Exception {
        //1、创建生产者 指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("order-1");
        //2、指定nameserver地址
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        //3、启动生产者
        producer.start();

        String[] tags = new String[]{"TagA", "TagC", "TagD"};

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String dateStr = LocalDateTime.now().format(dateTimeFormatter);

        List<OrderStep> orderSteps = OrderStep.buildOrders();

        for (int i = 0; i < orderSteps.size(); i++) {
            String body = dateStr + " Hello RocketMQ " + orderSteps.get(i);
            //4、创建消息 指定topic、tag、消息体
            Message msg = new Message("order", tags[i % tags.length], "KEY" + i, body.getBytes());

            //5、发送消息 指定 队列选择器 此处我们将订单号相同的发送到同一队列
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    long orderId = (long) o;
                    long index = orderId % list.size();
                    return list.get((int) index);
                }
            }, orderSteps.get(i).getOrderId());
            System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    body));

        }
        //6、关闭消费者
        producer.shutdown();
    }
}
