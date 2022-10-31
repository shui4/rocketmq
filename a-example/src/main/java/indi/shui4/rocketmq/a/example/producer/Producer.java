package indi.shui4.rocketmq.a.example.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.Scanner;

public class Producer {
  public static void main(String[] args) throws Exception {
    // Instantiate with a producer group name.
    DefaultMQProducer producer = new DefaultMQProducer("test");
    // 开起故障延迟机制
    producer.setSendLatencyFaultEnable(true);
    // 当一个Jvm需要启动多个Producer的时候，通过设置不同的InstanceName来区分，不设置的话系统使用默认名称 “Default”
    producer.setInstanceName("instance");
    // 设置发送重试次数，当网络出现异常的时候，这个次数影响消息的重复投递次数。想保不丢失消息，可以设置多重复几次
    producer.setRetryTimesWhenSendFailed(3);
    // Specify name server addresses.
    producer.setNamesrvAddr("localhost:9876");
    // Launch the instance.
    producer.start();
    Scanner scanner = new Scanner(System.in);
    while (true) {
      int i1 = scanner.nextInt();
      if (i1 == 0) {
        break;
      }
      try {
        for (int i = 0; i < 10000; i++) {
          try {
            Message msg =
                new Message(
                    "TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i)
                        .getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */);
            // 设置消息延迟
            //            msg.setDelayTimeLevel(3);
            // 同步发送
            //            SendResult sendResult = producer.send(msg);
            // 状态含义
            //            sendResult.getSendStatus();
            //          System.out.printf("%s%n", sendResult);

            // 异步发送
            producer.send(
                msg,
                new SendCallback() {
                  @Override
                  public void onSuccess(SendResult sendResult) {
                    System.out.printf("%s%n", sendResult);
                    //              sendResult.getSendStatus();
                  }

                  @Override
                  public void onException(Throwable e) {
                    e.printStackTrace();
                  }
                });
          } catch (UnsupportedEncodingException
              | MQClientException
              | RemotingException
              | InterruptedException e) {
            e.printStackTrace();
            Thread.sleep(10000);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    producer.shutdown();
    System.exit(0);
  }
}
