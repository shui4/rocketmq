package indi.shui4.rocketmq.a.example.acl;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

public class ACLProducer {
  public static void main(String[] args)
      throws MQClientException, MQBrokerException, RemotingException, InterruptedException,
          UnsupportedEncodingException {
    SessionCredentials credentials = new SessionCredentials("rocketmq", "12345678");
    DefaultMQProducer producer =
        new DefaultMQProducer(
            "test", new AclClientRPCHook(credentials));
    producer.setNamesrvAddr("127.0.0.1:9876");
    producer.start();
    for (int i = 0; i < 1; i++) {
      try {
        Message msg =
            new Message(
                "TopicTest3",
                "TagA",
                ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
        producer.send(msg);
      } catch (Exception e) {
        e.printStackTrace();
        TimeUnit.SECONDS.sleep(1);
      }
    }
    producer.shutdown();
  }
}
