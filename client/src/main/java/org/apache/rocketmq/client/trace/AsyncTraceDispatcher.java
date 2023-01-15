/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.trace;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.client.trace.TraceConstants.TRACE_INSTANCE_NAME;

/**
 * 异步轨迹调度器
 *
 * @author shui4
 */
public class AsyncTraceDispatcher implements TraceDispatcher {

  /** log */
  private static final InternalLogger log = ClientLogger.getLog();

  /** COUNTER */
  private static final AtomicInteger COUNTER = new AtomicInteger();

  /**
   * 异步转发队列长度，默认为 2048，当前版本不能修改。
   *
   * <p> 表示异步线程池能够积 压的消息轨迹数量
   */
  private final int queueSize;

  /** 批量消息条数，消息轨迹一次消息发送请求包含的数据条数，默认为 100，当前版本不能修改 */
  private final int batchSize;

  /** 消息轨迹一次发送的最大消息大小，默认为 128KB，当前版本不能修改 */
  private final int maxMsgSize;

  /**
   * 用来发送消息轨迹的消息发送者。
   *
   * <p> 发送消息轨迹的 Producer，通过 getAndCreateTraceProducer() 方法创建，其所属的消息发送者组名为
   * _INNER_TRACE_PRODUCER。在实践中可以通过该组名查看启用了消息轨 迹的客户端信息
   */
  private final DefaultMQProducer traceProducer;

  /**
   * 线程池，用来异步执行消息发送。
   *
   * <p> 用于发送到 Broker 服务的异步线程池，核心 线程数默认为 10，最大线程池为 20，队列堆积长度为 2048，线程名称 为 MQTraceSendThread_
   */
  private final ThreadPoolExecutor traceExecutor;
  /**
   * 消息轨迹 TraceContext 队列，用来存放待发送到服务端的消息。
   *
   * <p>traceContext 积压队列，客户端（消息 发送者、消息消费者）在收到处理结果后，将消息轨迹提交到这个队 列中并立即返回
   */
  private final ArrayBlockingQueue<TraceContext> traceContextQueue;
  /**
   * 记录丢弃的消息个数。
   *
   * <p> 整个运行过程中丢弃的消息轨迹数据，这里要 说明一点，如果消息 TPS 发送过大，异步转发线程处理不过来就会主动 丢弃消息轨迹数据
   */
  // The last discard number of log
  private AtomicLong discardCount;
  /** 工作线程，主要负责从追加队列中获取一批待发送的消息轨迹数据，将其提交到线程池中执行 */
  private Thread worker;
  /**
   * 线程池内部队列，默认长度为 1024。
   *
   * <p> 提交到 Broker 线程池中的队列
   */
  private ArrayBlockingQueue<Runnable> appenderQueue;

  /** shutDownHook */
  private volatile Thread shutDownHook;

  /** stopped */
  private volatile boolean stopped = false;

  /** hostProducer */
  private DefaultMQProducerImpl hostProducer;

  /** 消费者信息，记录消息消费时的轨迹信息 */
  private DefaultMQPushConsumerImpl hostConsumer;

  /** sendWhichQueue */
  private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

  /** dispatcherId */
  private String dispatcherId = UUID.randomUUID().toString();

  /**
   * 用于跟踪消息轨迹的 topic 名称。
   *
   * <p> 用于接收消息轨迹的 topic，默认为 RMQ_SYS_TRANS_HALF_TOPIC
   */
  private String traceTopicName;

  /** isStarted */
  private AtomicBoolean isStarted = new AtomicBoolean(false);

  /** accessChannel */
  private AccessChannel accessChannel = AccessChannel.LOCAL;

  /** group */
  private String group;

  /** type */
  private Type type;

  /**
   * AsyncTraceDispatcher
   *
   * @param group ignore
   * @param type ignore
   * @param traceTopicName ignore
   * @param rpcHook ignore
   */
  public AsyncTraceDispatcher(String group, Type type, String traceTopicName, RPCHook rpcHook) {
    // queueSize is greater than or equal to the n power of 2 of value
    this.queueSize = 2048;
    this.batchSize = 100;
    this.maxMsgSize = 128000;
    this.discardCount = new AtomicLong(0L);
    this.traceContextQueue = new ArrayBlockingQueue<TraceContext>(1024);
    this.group = group;
    this.type = type;

    this.appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);
    if (!UtilAll.isBlank(traceTopicName)) {
      this.traceTopicName = traceTopicName;
    } else {
      this.traceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
    }
    this.traceExecutor =
        new ThreadPoolExecutor( //
            10, //
            20, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_"));
    traceProducer = getAndCreateTraceProducer(rpcHook);
  }

  /**
   * getAccessChannel
   *
   * @return ignore
   */
  public AccessChannel getAccessChannel() {
    return accessChannel;
  }

  /**
   * setAccessChannel
   *
   * @param accessChannel ignore
   */
  public void setAccessChannel(AccessChannel accessChannel) {
    this.accessChannel = accessChannel;
  }

  /**
   * getTraceTopicName
   *
   * @return ignore
   */
  public String getTraceTopicName() {
    return traceTopicName;
  }

  /**
   * setTraceTopicName
   *
   * @param traceTopicName ignore
   */
  public void setTraceTopicName(String traceTopicName) {
    this.traceTopicName = traceTopicName;
  }

  /**
   * getTraceProducer
   *
   * @return ignore
   */
  public DefaultMQProducer getTraceProducer() {
    return traceProducer;
  }

  /**
   * getHostProducer
   *
   * @return ignore
   */
  public DefaultMQProducerImpl getHostProducer() {
    return hostProducer;
  }

  /**
   * setHostProducer
   *
   * @param hostProducer ignore
   */
  public void setHostProducer(DefaultMQProducerImpl hostProducer) {
    this.hostProducer = hostProducer;
  }

  /**
   * getHostConsumer
   *
   * @return ignore
   */
  public DefaultMQPushConsumerImpl getHostConsumer() {
    return hostConsumer;
  }

  /**
   * setHostConsumer
   *
   * @param hostConsumer ignore
   */
  public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
    this.hostConsumer = hostConsumer;
  }

  public void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException {
    // 使用 CAS 机制避免 start() 方法重复执行，然后启动一个后台线程，其执行逻辑被封装在 AsyncRunnable 中
    if (isStarted.compareAndSet(false, true)) {
      traceProducer.setNamesrvAddr(nameSrvAddr);
      traceProducer.setInstanceName(TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
      traceProducer.start();
    }
    this.accessChannel = accessChannel;
    this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
    this.worker.setDaemon(true);
    this.worker.start();
    this.registerShutDownHook();
  }

  /**
   * getAndCreateTraceProducer
   *
   * @param rpcHook ignore
   * @return ignore
   */
  private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook) {
    DefaultMQProducer traceProducerInstance = this.traceProducer;
    if (traceProducerInstance == null) {
      traceProducerInstance = new DefaultMQProducer(rpcHook);
      traceProducerInstance.setProducerGroup(genGroupNameForTrace());
      traceProducerInstance.setSendMsgTimeout(5000);
      traceProducerInstance.setVipChannelEnabled(false);
      // The max size of message is 128K
      traceProducerInstance.setMaxMessageSize(maxMsgSize - 10 * 1000);
    }
    return traceProducerInstance;
  }

  /**
   * genGroupNameForTrace
   *
   * @return ignore
   */
  private String genGroupNameForTrace() {
    return TraceConstants.GROUP_NAME_PREFIX
        + "-"
        + this.group
        + "-"
        + this.type
        + "-"
        + COUNTER.incrementAndGet();
  }

  @Override
  public boolean append(final Object ctx) {
    boolean result = traceContextQueue.offer((TraceContext) ctx);
    if (!result) {
      log.info("buffer full" + discardCount.incrementAndGet() + ",context is" + ctx);
    }
    return result;
  }

  @Override
  public void flush() {
    // The maximum waiting time for refresh,avoid being written all the time, resulting in failure
    // to return.
    long end = System.currentTimeMillis() + 500;
    while (System.currentTimeMillis() <= end) {
      synchronized (traceContextQueue) {
        if (traceContextQueue.size() == 0 && appenderQueue.size()== 0) {
          break;
        }
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        break;
      }
    }
    log.info("------end trace send" + traceContextQueue.size() + " " + appenderQueue.size());
  }

  @Override
  public void shutdown() {
    this.stopped = true;
    flush();
    this.traceExecutor.shutdown();
    if (isStarted.get()) {
      traceProducer.shutdown();
    }
    this.removeShutdownHook();
  }

  /** registerShutDownHook */
  public void registerShutDownHook() {
    if (shutDownHook == null) {
      shutDownHook =
          new Thread(
              new Runnable() {
                private volatile boolean hasShutdown = false;

                @Override
                public void run() {
                  synchronized (this) {
                    if (!this.hasShutdown) {
                      flush();
                    }
                  }
                }
              },
              "ShutdownHookMQTrace");
      Runtime.getRuntime().addShutdownHook(shutDownHook);
    }
  }

  /** removeShutdownHook */
  public void removeShutdownHook() {
    if (shutDownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutDownHook);
      } catch (IllegalStateException e) {
        // ignore - VM is already shutting down
      }
    }
  }

  /**
   * AsyncRunnable
   *
   * @author shui4
   */
  class AsyncRunnable implements Runnable {
    /** stopped */
    private boolean stopped;

    @Override
    public void run() {
      while (!stopped) {
        List<TraceContext> contexts = new ArrayList<TraceContext>(batchSize);
        synchronized (traceContextQueue) {
          // region 从 traceContextQueue 中最多收集 100 条消息
          for (int i = 0; i < batchSize; i++) {
            TraceContext context = null;
            try {
              // get trace data element from blocking Queue - traceContextQueue
              context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
            if (context != null) {
              contexts.add(context);
            } else {
              break;
            }
          }
          // endregion
          if (contexts.size() > 0) {
            // 发送轨迹消息的逻辑在 AsyncAppenderRequest 中
            AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
            traceExecutor.submit(request);
          } else if (AsyncTraceDispatcher.this.stopped) {
            this.stopped = true;
          }
        }
      }
    }
  }

  /**
   * AsyncAppenderRequest
   *
   * @author shui4
   */
  class AsyncAppenderRequest implements Runnable {
    /** contextList */
    List<TraceContext> contextList;

    /**
     * AsyncAppenderRequest
     *
     * @param contextList ignore
     */
    public AsyncAppenderRequest(final List<TraceContext> contextList) {
      if (contextList != null) {
        this.contextList = contextList;
      } else {
        this.contextList = new ArrayList<TraceContext>(1);
      }
    }

    @Override
    public void run() {
      sendTraceData(contextList);
    }

    /**
     * sendTraceData
     *
     * @param contextList ignore
     */
    public void sendTraceData(List<TraceContext> contextList) {
      // key：原主题 + 所属 regionId
      Map<String, List<TraceTransferBean>> transBeanMap =
          new HashMap<String, List<TraceTransferBean>>();
      for (TraceContext context : contextList) {
        if (context.getTraceBeans().isEmpty()) {
          continue;
        }
        // 原始消息实体内容对应的主题值
        String topic = context.getTraceBeans().get(0).getTopic();
        String regionId = context.getRegionId();
        // 使用原始消息实体的主题作为键
        String key = topic;
        if (!StringUtils.isBlank(regionId)) {
          key = key + TraceConstants.CONTENT_SPLITOR + regionId;
        }
        List<TraceTransferBean> transBeanList = transBeanMap.get(key);
        if (transBeanList == null) {
          transBeanList = new ArrayList<TraceTransferBean>();
          transBeanMap.put(key, transBeanList);
        }
        // 按照消息轨迹的存储协议对消息轨迹内容进行编码，当前版本
        // 使用的是字符串追加模式，实现比较简单，对扩展不太友好
        TraceTransferBean traceData = TraceDataEncoder.encoderFromContextBean(context);
        transBeanList.add(traceData);
      }
      for (Map.Entry<String, List<TraceTransferBean>> entry : transBeanMap.entrySet()) {
        String[] key = entry.getKey().split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
        String dataTopic = entry.getKey();
        String regionId = null;
        if (key.length > 1) {
          dataTopic = key[0];
          regionId = key[1];
        }
        // 按照 topic 分批调用 flushData() 方法将消息发送到
        // Broker 中，完成消息轨迹数据的存储
        flushData(entry.getValue(), dataTopic, regionId);
      }
    }

    /**
     * Batch sending data actually
     *
     * @param transBeanList ignore
     * @param dataTopic ignore
     * @param regionId ignore
     */
    private void flushData(
        List<TraceTransferBean> transBeanList, String dataTopic, String regionId) {
      if (transBeanList.size() == 0) {
        return;
      }
      // Temporary buffer
      StringBuilder buffer = new StringBuilder(1024);
      int count = 0;
      Set<String> keySet = new HashSet<String>();

      for (TraceTransferBean bean : transBeanList) {
        // Keyset of message trace includes msgId of or original message
        keySet.addAll(bean.getTransKey());
        buffer.append(bean.getTransData());
        count++;
        // Ensure that the size of the package should not exceed the upper limit.
        if (buffer.length() >= traceProducer.getMaxMessageSize()) {
          sendTraceDataByMQ(keySet, buffer.toString(), dataTopic, regionId);
          // Clear temporary buffer after finishing
          buffer.delete(0, buffer.length());
          keySet.clear();
          count = 0;
        }
      }
      if (count > 0) {
        sendTraceDataByMQ(keySet, buffer.toString(), dataTopic, regionId);
      }
      transBeanList.clear();
    }

    /**
     * Send message trace data
     *
     * @param keySet the keyset in this batch(including msgId in original message not offsetMsgId)
     * @param data the message trace data in this batch
     * @param dataTopic ignore
     * @param regionId ignore
     */
    private void sendTraceDataByMQ(
        Set<String> keySet, final String data, String dataTopic, String regionId) {
      String traceTopic = traceTopicName;
      if (AccessChannel.CLOUD == accessChannel) {
        traceTopic = TraceConstants.TRACE_TOPIC_PREFIX + regionId;
      }
      final Message message = new Message(traceTopic, data.getBytes());
      // Keyset of message trace includes msgId of or original message
      message.setKeys(keySet);
      try {
        Set<String> traceBrokerSet =
            tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), traceTopic);
        SendCallback callback =
            new SendCallback() {
              @Override
              public void onSuccess(SendResult sendResult) {}

              @Override
              public void onException(Throwable e) {
                log.error("send trace data failed, the traceData is {}", data, e);
              }
            };
        if (traceBrokerSet.isEmpty()) {
          // No cross set
          traceProducer.send(message, callback, 5000);
        } else {
          traceProducer.send(
              message,
              new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                  Set<String> brokerSet = (Set<String>) arg;
                  List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                  for (MessageQueue queue : mqs) {
                    if (brokerSet.contains(queue.getBrokerName())) {
                      filterMqs.add(queue);
                    }
                  }
                  int index = sendWhichQueue.incrementAndGet();
                  int pos = Math.abs(index) % filterMqs.size();
                  if (pos < 0) {
                    pos = 0;
                  }
                  return filterMqs.get(pos);
                }
              },
              traceBrokerSet,
              callback);
        }

      } catch (Exception e) {
        log.error("send trace data failed, the traceData is {}", data, e);
      }
    }

    /**
     * tryGetMessageQueueBrokerSet
     *
     * @param producer ignore
     * @param topic ignore
     * @return ignore
     */
    private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
      Set<String> brokerSet = new HashSet<String>();
      TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
      if (null == topicPublishInfo || !topicPublishInfo.ok()) {
        producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
        producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
        topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
      }
      if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
        for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
          brokerSet.add(queue.getBrokerName());
        }
      }
      return brokerSet;
    }
  }
}