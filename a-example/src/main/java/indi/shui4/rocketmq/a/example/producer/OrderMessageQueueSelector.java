package indi.shui4.rocketmq.a.example.producer;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @author shui4
 * @date 2022/3/16
 * @since 1.0.0
 */
public class OrderMessageQueueSelector implements MessageQueueSelector {
  @Override
  public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
    int id = Integer.parseInt(arg.toString());
    int idMainIndex = id / 100;
    int size = mqs.size();
    int index = idMainIndex % size;
    return mqs.get(index);
  }
}
