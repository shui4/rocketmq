package indi.shui4.rocketmq.a.example.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author shui4
 * @date 2022/3/16
 * @since 1.0.0
 */
public class PullConsumer {
  
  
  private static final Map<MessageQueue, Long> OFFSET_TABLES = new HashMap<>();
  
  public static void main(String[] args) {
    try {
      DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("testPullConsumer");
      Set<MessageQueue> msgSet = consumer.fetchSubscribeMessageQueues("TopicTest");
      for (final MessageQueue mq : msgSet) {
        long offset = consumer.fetchConsumeOffset(mq, true);
        System.out.printf("Consumer from the Queue:" + mq + "%n");
        SINGLE_MQ:
        while (true) {
          PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
          System.out.printf("%s%n", pullResult);
          putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
          switch (pullResult.getPullStatus()) {
            case FOUND:
            case NO_MATCHED_MSG:
            case OFFSET_ILLEGAL:
              break;
            case NO_NEW_MSG:
              break SINGLE_MQ;
            default:
              throw new IllegalStateException("Unexpected value: " + pullResult.getPullStatus());
          }
        }
      }
      
      
    } catch (MQClientException | MQBrokerException | RemotingException | InterruptedException e) {
      e.printStackTrace();
    }
    
    
  }
  
  private static long getMessageQueueOffset(MessageQueue mq) {
    Long offset = OFFSET_TABLES.get(mq);
    if (offset != null) {
      return offset;
    }
    return 0;
  }
  
  private static void putMessageQueueOffset(MessageQueue mq, long offset) {
    OFFSET_TABLES.put(mq, offset);
  }
  
}
