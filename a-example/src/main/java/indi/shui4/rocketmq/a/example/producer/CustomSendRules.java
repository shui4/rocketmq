package indi.shui4.rocketmq.a.example.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 自定义发送规则
 *
 * @author shui4
 * @date 2022/3/16
 * @since 1.0.0
 */
public class CustomSendRules {
  public static void main(String[] args)
      throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
    DefaultMQProducer producer = new DefaultMQProducer("testProducer");
    producer.setInstanceName("instance1");
    producer.setNamesrvAddr("localhost:9876");
    producer.setRetryTimesWhenSendFailed(3);
    producer.start();
    new Scanner(System.in).nextLine();
    Message msg = new Message("test", "hello".getBytes(UTF_8));
    for (int i = 0; i < 100; i++) {
      SendResult send = producer.send(msg, new OrderMessageQueueSelector(), 1);
      System.out.println(send);
    }
    producer.shutdown();
    System.exit(0);
  }
}
