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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.*;
import java.util.concurrent.*;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
  private static final InternalLogger log = ClientLogger.getLog();
  /**
   * 定时删除过期消息线程池。为了揭示消息消费的完整过程，从服务器拉取到消息后，回调 PullCallBack 方法，先将消息放入 ProccessQueue 中，
   * 然后把消息提交到消费线程池中执行，也就是调用 ConsumeMessageService#submitConsumeRequest 开始进入消息消费的世界
   */
  private final ScheduledExecutorService cleanExpireMsgExecutors;
  /** 消息消费线程池 */
  private final ThreadPoolExecutor consumeExecutor;
  /** 消息消费任务队列 */
  private final BlockingQueue<Runnable> consumeRequestQueue;
  /** 消费组 */
  private final String consumerGroup;
  /** 消费者对象 */
  private final DefaultMQPushConsumer defaultMQPushConsumer;

  /** 消息推模式实现类 */
  private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

  /** 并发消息业务事件类。 */
  private final MessageListenerConcurrently messageListener;

  /** 添加消费任务到 consumeExecutor 延迟调度器 */
  private final ScheduledExecutorService scheduledExecutorService;

  public ConsumeMessageConcurrentlyService(
      DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
      MessageListenerConcurrently messageListener) {
    this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    this.messageListener = messageListener;

    this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
    this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
    this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

    String consumeThreadPrefix = null;
    if (consumerGroup.length() > 100) {
      consumeThreadPrefix =
          new StringBuilder("ConsumeMessageThread_")
              .append(consumerGroup.substring(0, 100))
              .append("_")
              .toString();
    } else {
      consumeThreadPrefix =
          new StringBuilder("ConsumeMessageThread_").append(consumerGroup).append("_").toString();
    }
    // 默认线程池大小：20~20
    this.consumeExecutor =
        new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl(consumeThreadPrefix));
    this.scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    // 清理过期消息定时器
    this.cleanExpireMsgExecutors =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
  }

  public void processConsumeResult(
      final ConsumeConcurrentlyStatus status,
      final ConsumeConcurrentlyContext context,
      final ConsumeRequest consumeRequest) {
    int ackIndex = context.getAckIndex();

    if (consumeRequest.getMsgs().isEmpty()) return;
    // 根据消息监听器返回的结果计算 ackIndex，如果返回
    // CONSUME_SUCCESS，则将 ackIndex 设置为 msgs.size()-1，如果返回
    // RECONSUME_LATER，则将 ackIndex 设置为 -1，这是为下文发送 msg
    // back（ACK）消息做的准备
    switch (status) {
      case CONSUME_SUCCESS:
        if (ackIndex >= consumeRequest.getMsgs().size()) {
          ackIndex = consumeRequest.getMsgs().size() - 1;
        }
        int ok = ackIndex + 1;
        int failed = consumeRequest.getMsgs().size() - ok;
        this.getConsumerStatsManager()
            .incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
        this.getConsumerStatsManager()
            .incConsumeFailedTPS(
                consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
        break;
      case RECONSUME_LATER:
        ackIndex = -1;
        this.getConsumerStatsManager()
            .incConsumeFailedTPS(
                consumerGroup,
                consumeRequest.getMessageQueue().getTopic(),
                consumeRequest.getMsgs().size());
        break;
      default:
        break;
    }

    switch (this.defaultMQPushConsumer.getMessageModel()) {
        // 如果是广播模式，业务方会返回 RECONSUME_LATER，消息
        // 并不会被重新消费，而是以警告级别输出到日志文件中。
      case BROADCASTING:
        for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
          MessageExt msg = consumeRequest.getMsgs().get(i);
          log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
        }
        break;
        // 如果是集群模式，消息消费成功，因为 ackIndex=consumeRequest.getMsgs().size()-1 ，所以 i=ackIndex+1 等于
        // consumeRequest.getMsgs().size() ，并不会执行 sendMessageBack。
        // 只有在业务方返回 RECONSUME_LATER 时，该批消息都需要发送 ACK 消息，如果消息发送失败，
        // 则直接将本批 ACK 消费发送失败的消息再次封装为 ConsumeRequest，然后延迟 5s 重新消费
      case CLUSTERING:
        List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
        for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
          MessageExt msg = consumeRequest.getMsgs().get(i);
          boolean result = this.sendMessageBack(msg, context);
          if (!result) {
            msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
            msgBackFailed.add(msg);
          }
        }

        if (!msgBackFailed.isEmpty()) {
          consumeRequest.getMsgs().removeAll(msgBackFailed);

          this.submitConsumeRequestLater(
              msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
        }
        break;
      default:
        break;
    }
    // 从 ProcessQueue 中移除这批消息，这里返回的偏移量
    // 是移除该批消息后最小的偏移量。然后用该偏移量更新消息消费进
    // 度，以便消费者重启后能从上一次的消费进度开始消费，避免消息重
    // 复消费。值得注意的是，当消息监听器返回 RECONSUME_LATER 时，消息
    // 消费进度也会向前推进，并用 ProcessQueue 中最小的队列偏移量调用
    // 消息消费进度存储器 OffsetStore 更新消费进度。这是因为当返回
    // RECONSUME_LATER 时，RocketMQ 会创建一条与原消息属性相同的消息，
    // 拥有一个唯一的新 msgId，并存储原消息 ID，该消息会存入 CommitLog
    // 文件，与原消息没有任何关联，所以该消息也会进入 ConsuemeQueue，
    // 并拥有一个全新的队列偏移量
    long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
    if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
      this.defaultMQPushConsumerImpl
          .getOffsetStore()
          .updateOffset(consumeRequest.getMessageQueue(), offset, true);
    }
  }

  public ConsumerStatsManager getConsumerStatsManager() {
    return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
  }

  public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
    int delayLevel = context.getDelayLevelWhenNextConsume();

    // Wrap topic with namespace before sending back message.
    msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
    try {
      this.defaultMQPushConsumerImpl.sendMessageBack(
          msg, delayLevel, context.getMessageQueue().getBrokerName());
      return true;
    } catch (Exception e) {
      log.error(
          "sendMessageBack exception, group:" + this.consumerGroup + "msg:" + msg.toString(), e);
    }

    return false;
  }

  private void submitConsumeRequestLater(
      final List<MessageExt> msgs,
      final ProcessQueue processQueue,
      final MessageQueue messageQueue) {

    this.scheduledExecutorService.schedule(
        new Runnable() {

          @Override
          public void run() {
            ConsumeMessageConcurrentlyService.this.submitConsumeRequest(
                msgs, processQueue, messageQueue, true);
          }
        },
        5000,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void submitConsumeRequest(
      final List<MessageExt> msgs,
      final ProcessQueue processQueue,
      final MessageQueue messageQueue,
      final boolean dispatchToConsume) {
    // consumeMessageBatchMaxSize 表示消息批次，也就是一次消息消费任务 ConsumeRequest 中包含的消息条数，默认为 1
    final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
    // msgs.size() 默认最多为 32 条消息，受 DefaultMQPushConsumer.pullBatchSize 属性控制，
    // 如果 msgs.size() 小于 consumeMessage BatchMaxSize
    if (msgs.size() <= consumeBatchSize) {
      // 则直接将拉取到的消息放入 ConsumeRequest
      ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
      try {
        // 提交到消息消费者线程池中
        this.consumeExecutor.submit(consumeRequest);
      } catch (RejectedExecutionException e) {
        // 如果提交过程中出现拒绝提交异常，则延迟 5s 再提交
        // 由于消费者线程池使用的任务队列 LinkedBlockingQueue 为无界队列，故不会出现拒绝提交异常
        this.submitConsumeRequestLater(consumeRequest);
      }
    }
    // 如果拉取的消息条数大于 consumeMessageBatchMaxSize，则对拉取消息进行分页，每页
    // consumeMessageBatchMaxSize 条消息，创建多个 ConsumeRequest 任务并提交到消费线程池
    else {
      for (int total = 0; total < msgs.size(); ) {
        List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
        for (int i = 0; i < consumeBatchSize; i++, total++) {
          if (total < msgs.size()) {
            msgThis.add(msgs.get(total));
          } else {
            break;
          }
        }

        ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
        try {
          this.consumeExecutor.submit(consumeRequest);
        } catch (RejectedExecutionException e) {
          for (; total < msgs.size(); total++) {
            msgThis.add(msgs.get(total));
          }

          this.submitConsumeRequestLater(consumeRequest);
        }
      }
    }
  }

  private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {

    this.scheduledExecutorService.schedule(
        new Runnable() {

          @Override
          public void run() {
            ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
          }
        },
        5000,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void start() {
    // 清理超时消息，默认 15 分钟
    this.cleanExpireMsgExecutors.scheduleAtFixedRate(
        new Runnable() {

          @Override
          public void run() {
            try {
              cleanExpireMsg();
            } catch (Throwable e) {
              log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
            }
          }
        },
        this.defaultMQPushConsumer.getConsumeTimeout(),
        this.defaultMQPushConsumer.getConsumeTimeout(),
        TimeUnit.MINUTES);
  }

  private void cleanExpireMsg() {
    Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
        this.defaultMQPushConsumerImpl
            .getRebalanceImpl()
            .getProcessQueueTable()
            .entrySet()
            .iterator();
    while (it.hasNext()) {
      Map.Entry<MessageQueue, ProcessQueue> next = it.next();
      ProcessQueue pq = next.getValue();
      pq.cleanExpiredMsg(this.defaultMQPushConsumer);
    }
  }

  @Override
  public void shutdown(long awaitTerminateMillis) {
    this.scheduledExecutorService.shutdown();
    ThreadUtils.shutdownGracefully(
        this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
    this.cleanExpireMsgExecutors.shutdown();
  }

  @Override
  public void updateCorePoolSize(int corePoolSize) {
    if (corePoolSize > 0
        && corePoolSize <= Short.MAX_VALUE
        && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
      this.consumeExecutor.setCorePoolSize(corePoolSize);
    }
  }

  @Override
  public void incCorePoolSize() {}

  @Override
  public void decCorePoolSize() {}

  @Override
  public int getCorePoolSize() {
    return this.consumeExecutor.getCorePoolSize();
  }

  @Override
  public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
    ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
    result.setOrder(false);
    result.setAutoCommit(true);

    List<MessageExt> msgs = new ArrayList<MessageExt>();
    msgs.add(msg);
    MessageQueue mq = new MessageQueue();
    mq.setBrokerName(brokerName);
    mq.setTopic(msg.getTopic());
    mq.setQueueId(msg.getQueueId());

    ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

    this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

    final long beginTime = System.currentTimeMillis();

    log.info("consumeMessageDirectly receive new message: {}", msg);

    try {
      ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
      if (status != null) {
        switch (status) {
          case CONSUME_SUCCESS:
            result.setConsumeResult(CMResult.CR_SUCCESS);
            break;
          case RECONSUME_LATER:
            result.setConsumeResult(CMResult.CR_LATER);
            break;
          default:
            break;
        }
      } else {
        result.setConsumeResult(CMResult.CR_RETURN_NULL);
      }
    } catch (Throwable e) {
      result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
      result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

      log.warn(
          String.format(
              "consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
              RemotingHelper.exceptionSimpleDesc(e),
              ConsumeMessageConcurrentlyService.this.consumerGroup,
              msgs,
              mq),
          e);
    }

    result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

    log.info("consumeMessageDirectly Result: {}", result);

    return result;
  }

  class ConsumeRequest implements Runnable {
    private final MessageQueue messageQueue;
    private final List<MessageExt> msgs;
    private final ProcessQueue processQueue;

    public ConsumeRequest(
        List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
      this.msgs = msgs;
      this.processQueue = processQueue;
      this.messageQueue = messageQueue;
    }

    public MessageQueue getMessageQueue() {
      return messageQueue;
    }

    public List<MessageExt> getMsgs() {
      return msgs;
    }

    public ProcessQueue getProcessQueue() {
      return processQueue;
    }

    @Override
    public void run() {
      // 进入具体的消息消费队列时，会先检查 processQueue 的 dropped ，如果设置为 true ，则停止该队列的消费。在进行消息重新负
      // 载时，如果该消息队列被分配给消费组内的其他消费者，需要将 droped 设置为 true ，阻止消费者继续消费不属于自己的消息队列
      if (this.processQueue.isDropped()) {
        log.info(
            "the message queue not be able to consume, because it's dropped. group={} {}",
            ConsumeMessageConcurrentlyService.this.consumerGroup,
            this.messageQueue);
        return;
      }

      MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
      ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
      ConsumeConcurrentlyStatus status = null;
      defaultMQPushConsumerImpl.resetRetryAndNamespace(
          msgs, defaultMQPushConsumer.getConsumerGroup());

      ConsumeMessageContext consumeMessageContext = null;
      if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
        consumeMessageContext = new ConsumeMessageContext();
        consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
        consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
        consumeMessageContext.setProps(new HashMap<String, String>());
        consumeMessageContext.setMq(messageQueue);
        consumeMessageContext.setMsgList(msgs);
        consumeMessageContext.setSuccess(false);
        // 执行消息消费钩子函数
        ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(
            consumeMessageContext);
      }

      long beginTimestamp = System.currentTimeMillis();
      boolean hasException = false;
      ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
      try {
        if (msgs != null && !msgs.isEmpty()) {
          for (MessageExt msg : msgs) {
            MessageAccessor.setConsumeStartTimeStamp(
                msg, String.valueOf(System.currentTimeMillis()));
          }
        }
        // 执行具体的消息消费，调用应用程序消息监听器的 consumeMessage 方法，进入具体的消息消费业务逻辑，返回该批消息的消费结果，
        // 即 CONSUME_SUCCESS （消费成功）或 RECONSUME_LATER （需要重新消费）
        status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
      } catch (Throwable e) {
        log.warn(
            String.format(
                "consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                messageQueue),
            e);
        hasException = true;
      }
      long consumeRT = System.currentTimeMillis() - beginTimestamp;
      if (null == status) {
        if (hasException) {
          returnType = ConsumeReturnType.EXCEPTION;
        } else {
          returnType = ConsumeReturnType.RETURNNULL;
        }
      } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
        returnType = ConsumeReturnType.TIME_OUT;
      } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
        returnType = ConsumeReturnType.FAILED;
      } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
        returnType = ConsumeReturnType.SUCCESS;
      }

      if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
        consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
      }

      if (null == status) {
        log.warn(
            "consumeMessage return null, Group: {} Msgs: {} MQ: {}",
            ConsumeMessageConcurrentlyService.this.consumerGroup,
            msgs,
            messageQueue);
        status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
      }

      if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
        consumeMessageContext.setStatus(status.toString());
        consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
        // 执行消息消费钩子函数
        ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(
            consumeMessageContext);
      }

      ConsumeMessageConcurrentlyService.this
          .getConsumerStatsManager()
          .incConsumeRT(
              ConsumeMessageConcurrentlyService.this.consumerGroup,
              messageQueue.getTopic(),
              consumeRT);
      // 执行业务消息消费后，在处理结果前再次验证一次 ProcessQueue 的 isDroped 状态值。如果状态值为 true，将不对结果进
      // 行任何处理。也就是说，在消息消费进入第四步时，如果因新的消费者加入或原先的消费者出现宕机，导致原先分配给消费者的队列在负
      // 载之后分配给了别的消费者，那么消息会被重复消费
      if (!processQueue.isDropped()) {
        ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
      } else {
        log.warn(
            "processQueue is dropped without process consume result. messageQueue={}, msgs={}",
            messageQueue,
            msgs);
      }
    }
  }
}