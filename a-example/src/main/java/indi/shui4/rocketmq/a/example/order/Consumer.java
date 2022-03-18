package indi.shui4.rocketmq.a.example.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author shui4
 * @date 2022/3/17
 * @since 1.0.0
 */
public class Consumer {
  public static void main(String[] args) {
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderConsumer");
    consumer.setNamesrvAddr("localhost:9876");
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    consumer.setMessageModel(MessageModel.CLUSTERING);
    try {
      consumer.subscribe("test", "*");
    } catch (MQClientException e) {
      e.printStackTrace();
    }

    consumer.setConsumeThreadMin(1);
    consumer.setConsumeThreadMax(1);

    consumer.setPullBatchSize(1);
    consumer.setConsumeMessageBatchMaxSize(1);

    // MessageListenerOrderly 为每个
    // ConsumerQueue加锁，消费消息前，需要先获得到这个消息对应的ConsumerQueue所对应的锁，这样保证了同一时间，同一个Consumer
    // Queue的消息不被并发消费，但不同Consumer Queue的消息可以并发处理

    consumer.registerMessageListener(
        (MessageListenerOrderly) (msgs, context) -> {
           System.out.println(msgs);
          return ConsumeOrderlyStatus.SUCCESS;
        });
    try {
      consumer.start();
    } catch (MQClientException e) {
      e.printStackTrace();
    }
  }
}
