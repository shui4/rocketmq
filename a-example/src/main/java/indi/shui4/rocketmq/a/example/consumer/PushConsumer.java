package indi.shui4.rocketmq.a.example.consumer;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.Scanner;

/**
 * @author shui4
 * @date 2022/3/16
 * @since 1.0.0
 */
public class PushConsumer {
  public static void main(String[] args) throws MQClientException {
    Log log = LogFactory.get(PushConsumer.class);
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testConsumer");
    consumer.setNamesrvAddr("localhost:9876");
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    consumer.setMessageModel(MessageModel.CLUSTERING);
    consumer.subscribe("TopicTest", "*");
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
      log.info("threadId:{}， msg:{}", Thread
          .currentThread()
          .getId(), msgs.toString());
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    consumer.start();
    // 获取订阅消息队列
    // 在 DefaultMQPushConsumer模式下，即使 namesrv地址填错也不会出现异常，如果需要，执行以下代码
    consumer.fetchSubscribeMessageQueues("TopicTest");
    new Scanner(System.in).nextLine();
    consumer.shutdown();
    
  }
}
