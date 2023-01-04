package indi.shui4.rocketmq.a.example.acl;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ACLConsumer {

  public static void main(String[] args) throws MQClientException {
    SessionCredentials credentials = new SessionCredentials("rocketmq", "12345678");
    DefaultMQPushConsumer consumer =
        new DefaultMQPushConsumer(
            "test", new AclClientRPCHook(credentials), new AllocateMessageQueueAveragely());
    consumer.subscribe("TopicTest3", "*");
    consumer.setNamesrvAddr("127.0.0.1:9876");
    consumer.registerMessageListener(
        new MessageListenerConcurrently() {
          @Override
          public ConsumeConcurrentlyStatus consumeMessage(
              List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt msg : msgs) {
              try {
                System.out.println(new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
              } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
              }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
        });
    consumer.start();
  }
}