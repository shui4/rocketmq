package indi.shui4.rocketmq.a.example.order;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author shui4
 * @date 2022/3/17
 * @since 1.0.0
 */
public class Producer {
  public static void main(String[] args) {
    DefaultMQProducer producer = new DefaultMQProducer("orderProducer");
    try {
      Log log = LogFactory.get(Producer.class);
      producer.setNamesrvAddr("localhost:9876");
      producer.setInstanceName("instance");
      producer.setRetryTimesWhenSendFailed(3);
      producer.setSendMsgTimeout((int) TimeUnit.SECONDS.toMillis(5));
      producer.start();
      new Scanner(System.in).nextLine();
      for (int i = 0; i < 100; i++) {
        SendResult sendResult =
            producer.send(
                new Message("test", ("Hello" + i).getBytes(UTF_8)),
                new MessageQueueSelector() {
                  @Override
                  public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get((Integer) arg % mqs.size());
                  }
                },
                i);
        log.info(sendResult.toString());
      }
    } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      producer.shutdown();
      System.exit(0);
    }
  }
}
