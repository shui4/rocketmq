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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * RocketMQ并没有真正实现推模式，而是消费者主动向消息服务器 拉取消息，RocketMQ推模式是循环向消息服务端发送消息拉取请求，
 * 如果消息消费者向RocketMQ发送消息拉取时，消息并未到达消费队 列，且未启用长轮询机制，则会在服务端等待shortPollingTimeMills
 * 时间后（挂起），再去判断消息是否已到达消息队列。如果消息未到 达，则提示消息拉取客户端PULL_NOT_FOUND（消息不存在），如果开
 * 启长轮询模式，RocketMQ一方面会每5s轮询检查一次消息是否可达，<br>
 * 同时一有新消息到达后，立即通知挂起线程再次验证新消息是否是自 己感兴趣的，如果是则从CommitLog文件提取消息返回给消息拉取客户 端，否则挂起超时，超时时间由消息拉取方在消息拉取时封装在请求
 * 参数中，推模式默认为15s，拉模式通过 DefaultMQPullConsumer#setBrokerSuspendMaxTimeMillis进行设置。
 * RocketMQ通过在Broker端配置longPollingEnable为true来开启长轮询 模式。消息拉取时服务端从CommitLog文件中未找到消息的处理逻辑
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
  private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
  private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
  /** Flow control interval */
  private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
  /** Delay some time when suspend pull service */
  private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;

  private final ArrayList<ConsumeMessageHook> consumeMessageHookList =
      new ArrayList<ConsumeMessageHook>();
  private final long consumerStartTimestamp = System.currentTimeMillis();
  private final DefaultMQPushConsumer defaultMQPushConsumer;
  private final ArrayList<FilterMessageHook> filterMessageHookList =
      new ArrayList<FilterMessageHook>();
  private final InternalLogger log = ClientLogger.getLog();
  private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
  private final RPCHook rpcHook;
  private ConsumeMessageService consumeMessageService;
  private boolean consumeOrderly = false;
  private MQClientInstance mQClientFactory;
  private MessageListener messageListenerInner;
  private OffsetStore offsetStore;
  private volatile boolean pause = false;
  private PullAPIWrapper pullAPIWrapper;
  /** 发生异常时延迟一段时间 */
  private long pullTimeDelayMillsWhenException = 3000;

  private long queueFlowControlTimes = 0;
  private long queueMaxSpanFlowControlTimes = 0;
  private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

  public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
    this.defaultMQPushConsumer = defaultMQPushConsumer;
    this.rpcHook = rpcHook;
    this.pullTimeDelayMillsWhenException =
        defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
  }

  public void adjustThreadPool() {
    long computeAccTotal = this.computeAccumulationTotal();
    long adjustThreadPoolNumsThreshold =
        this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

    long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

    long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

    if (computeAccTotal >= incThreshold) {
      this.consumeMessageService.incCorePoolSize();
    }

    if (computeAccTotal < decThreshold) {
      this.consumeMessageService.decCorePoolSize();
    }
  }

  private long computeAccumulationTotal() {
    long msgAccTotal = 0;
    ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable =
        this.rebalanceImpl.getProcessQueueTable();
    Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
    while (it.hasNext()) {
      Entry<MessageQueue, ProcessQueue> next = it.next();
      ProcessQueue value = next.getValue();
      msgAccTotal += value.getMsgAccCnt();
    }

    return msgAccTotal;
  }

  public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
    createTopic(key, newTopic, queueNum, 0);
  }

  public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
      throws MQClientException {
    this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
  }

  public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
    return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
  }

  public void executeHookAfter(final ConsumeMessageContext context) {
    if (!this.consumeMessageHookList.isEmpty()) {
      for (ConsumeMessageHook hook : this.consumeMessageHookList) {
        try {
          hook.consumeMessageAfter(context);
        } catch (Throwable e) {
        }
      }
    }
  }

  public void executeHookBefore(final ConsumeMessageContext context) {
    if (!this.consumeMessageHookList.isEmpty()) {
      for (ConsumeMessageHook hook : this.consumeMessageHookList) {
        try {
          hook.consumeMessageBefore(context);
        } catch (Throwable e) {
        }
      }
    }
  }

  public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
    Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
    if (null == result) {
      this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
      result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
    }

    if (null == result) {
      throw new MQClientException("The topic[" + topic + "] not exist", null);
    }

    return parseSubscribeMessageQueues(result);
  }

  public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
    Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
    for (MessageQueue queue : messageQueueList) {
      String userTopic =
          NamespaceUtil.withoutNamespace(
              queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
      resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
    }

    return resultQueues;
  }

  public ConsumeMessageService getConsumeMessageService() {
    return consumeMessageService;
  }

  public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
    this.consumeMessageService = consumeMessageService;
  }

  public DefaultMQPushConsumer getDefaultMQPushConsumer() {
    return defaultMQPushConsumer;
  }

  public OffsetStore getOffsetStore() {
    return offsetStore;
  }

  public void setOffsetStore(OffsetStore offsetStore) {
    this.offsetStore = offsetStore;
  }

  public RebalanceImpl getRebalanceImpl() {
    return rebalanceImpl;
  }

  public ServiceState getServiceState() {
    return serviceState;
  }

  // Don't use this deprecated setter, which will be removed soon.
  @Deprecated
  public synchronized void setServiceState(ServiceState serviceState) {
    this.serviceState = serviceState;
  }

  public MQClientInstance getmQClientFactory() {
    return mQClientFactory;
  }

  public void setmQClientFactory(MQClientInstance mQClientFactory) {
    this.mQClientFactory = mQClientFactory;
  }

  public boolean hasHook() {
    return !this.consumeMessageHookList.isEmpty();
  }

  public long maxOffset(MessageQueue mq) throws MQClientException {
    return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
  }

  public long minOffset(MessageQueue mq) throws MQClientException {
    return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
  }

  public void pullMessage(final PullRequest pullRequest) {
    // 从 PullRequest 中获取 ProcessQueue
    final ProcessQueue processQueue = pullRequest.getProcessQueue();
    // ? 被丢弃
    if (processQueue.isDropped()) {
      log.info("the pull request[{}] is dropped.", pullRequest.toString());
      return;
    }
    // ProcessQueue最后一次处理时间
    pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

    try {
      // 验证当前状态是不是运行的
      this.makeSureStateOK();
    } catch (MQClientException e) {
      log.warn("pullMessage exception, consumer state not ok", e);
      // 如果不能，那就暂时先存起来，稍候再试
      this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
      return;
    }
    // ? 暂停
    // 将 pullRequest 通过定时器重新放入队列（这里将延迟 1s ），再结束
    if (this.isPause()) {
      log.warn(
          "consumer was paused, execute pull request later. instanceName={}, group={}",
          this.defaultMQPushConsumer.getInstanceName(),
          this.defaultMQPushConsumer.getConsumerGroup());
      this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
      return;
    }
    // region 流控
    // 消息总数
    long cachedMessageCount = processQueue.getMsgCount().get();
    // 消息总大小
    long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);
    // ? 消息数量达到阈值
    // 放弃本次拉取任务
    if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
      // 该队列的下一次拉取任务将在 50ms 后才加入拉取任务队列
      this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
      // 每触发 1000 次流控后输出提示语
      if ((queueFlowControlTimes++ % 1000) == 0) {
        log.warn(
            "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
            this.defaultMQPushConsumer.getPullThresholdForQueue(),
            processQueue.getMsgTreeMap().firstKey(),
            processQueue.getMsgTreeMap().lastKey(),
            cachedMessageCount,
            cachedMessageSizeInMiB,
            pullRequest,
            queueFlowControlTimes);
      }
      return;
    }
    // ? 消息总大小达到阈值
    if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
      this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
      if ((queueFlowControlTimes++ % 1000) == 0) {
        log.warn(
            "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
            this.defaultMQPushConsumer.getPullThresholdSizeForQueue(),
            processQueue.getMsgTreeMap().firstKey(),
            processQueue.getMsgTreeMap().lastKey(),
            cachedMessageCount,
            cachedMessageSizeInMiB,
            pullRequest,
            queueFlowControlTimes);
      }
      return;
    }
    // endregion

    // region 根据MessageListener的类型进行特定处理
    // MessageListenerConcurrently
    if (!this.consumeOrderly) {
      // ? ProcessQueue 中队列最大偏移量与最小偏离量的间距超过 consumeConcurrentlyMaxSpan
      // 这里主要的考量是担心因为一条消息堵塞，使消息进度无法向前推进，可能会造成大量消息重复消费
      if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
        this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
        if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
          log.warn(
              "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
              processQueue.getMsgTreeMap().firstKey(),
              processQueue.getMsgTreeMap().lastKey(),
              processQueue.getMaxSpan(),
              pullRequest,
              queueMaxSpanFlowControlTimes);
        }
        return;
      }
    }
    // 顺序消费：MessageListenerOrderly
    else {
      // 进程队列被上锁？
      if (processQueue.isLocked()) {
        // 以前没有被上锁？
        if (!pullRequest.isPreviouslyLocked()) {
          long offset = -1L;
          try {
            // 计算从哪里拉取异常即偏移量
            offset =
                this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
          } catch (Exception e) {
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
            return;
          }
          boolean brokerBusy = offset < pullRequest.getNextOffset();
          log.info(
              "the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
              pullRequest,
              offset,
              brokerBusy);
          if (brokerBusy) {
            log.info(
                "[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                pullRequest,
                offset);
          }
          // 标记以前被锁定
          pullRequest.setPreviouslyLocked(true);
          // 设置下一个offset
          pullRequest.setNextOffset(offset);
        }
      }
      // 稍候再拉取
      else {
        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        log.info("pull message later because not locked in broker, {}", pullRequest);
        return;
      }
    }
    // endregion

    // 获取订阅中的数据
    final SubscriptionData subscriptionData =
        this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
    // ? 订阅信息为空
    if (null == subscriptionData) {
      // 该队列的下一次拉取任务将延迟 3s 执行
      this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
      log.warn("find the consumer's subscription failed, {}", pullRequest);
      return;
    }

    final long beginTimestamp = System.currentTimeMillis();
    // 从Broker拉取到消息后的回调方法
    PullCallback pullCallback =
        new PullCallback() {
          @Override
          public void onSuccess(PullResult pullResult) {
            if (pullResult != null) {
              pullResult =
                  DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(
                      pullRequest.getMessageQueue(), pullResult, subscriptionData);

              switch (pullResult.getPullStatus()) {
                case FOUND:
                  // 更新PullRequest的下一次拉取偏移量
                  long prevRequestOffset = pullRequest.getNextOffset();
                  pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                  long pullRT = System.currentTimeMillis() - beginTimestamp;
                  DefaultMQPushConsumerImpl.this
                      .getConsumerStatsManager()
                      .incPullRT(
                          pullRequest.getConsumerGroup(),
                          pullRequest.getMessageQueue().getTopic(),
                          pullRT);

                  long firstMsgOffset = Long.MAX_VALUE;
                  // 如果 msgFoundList 为空，则立即将 PullReqeuest 放入 PullMessageService 的 pullRequestQueue
                  // ，以便 PullMessageSerivce 能及时唤醒并再次执行消息拉取

                  // 为什么 PullStatus.msgFoundList 还会为空呢？因为 RocketMQ 根据 TAG 进行消息过滤时，在服务端只是验
                  // 证了 TAG 的哈希码，所以客户端再次对消息进行过滤时，可能会出现
                  // msgFoundList 为空的情况
                  if (pullResult.getMsgFoundList() == null
                      || pullResult.getMsgFoundList().isEmpty()) {
                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                  } else {
                    firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                    DefaultMQPushConsumerImpl.this
                        .getConsumerStatsManager()
                        .incPullTPS(
                            pullRequest.getConsumerGroup(),
                            pullRequest.getMessageQueue().getTopic(),
                            pullResult.getMsgFoundList().size());
                    // 首先将拉取到的消息存入 ProcessQueue
                    boolean dispatchToConsume =
                        processQueue.putMessage(pullResult.getMsgFoundList());
                    // 将拉取到的消息提交到 ConsumeMessageService 中供消费者消费
                    // 该方法是一个异步方法，也就是 PullCallBack 将消息提交到 ConsumeMessageService 中就会立即返回，至于这些消息如何消费，
                    // PullCallBack 不会关注
                    DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                        pullResult.getMsgFoundList(),
                        processQueue,
                        pullRequest.getMessageQueue(),
                        dispatchToConsume);
                    // 将消息提交给消费者线程之后， PullCallBack 将立即返回，可以说本次消息拉取顺利完成。然后查看 pullInterval 参数，如
                    // 果 pullInterval>0 ，则等待 pullInterval 毫秒后将 PullRequest 对象放
                    // 入 PullMessageService 的 pullRequestQueue 中，该消息队列的下次拉
                    // 取即将被激活，达到持续消息拉取，实现准实时拉取消息的效果
                    if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval()
                        > 0) {
                      DefaultMQPushConsumerImpl.this.executePullRequestLater(
                          pullRequest,
                          DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                    } else {
                      DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                    }
                  }

                  if (pullResult.getNextBeginOffset() < prevRequestOffset
                      || firstMsgOffset < prevRequestOffset) {
                    log.warn(
                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                        pullResult.getNextBeginOffset(),
                        firstMsgOffset,
                        prevRequestOffset);
                  }

                  break;
                  // 没有新消息，对应 GetMessageResult.OFFSET_FOUND_NULL 、
                  // GetMessageResult.OFFSET_OVERFLOW_ONE

                  // OFFSET_FOUND_NULL 表示根据 ConsumeQueue 文件的偏移量没有找到
                  // 内容，使用偏移量定位到下一个 ConsumeQueue 文件，其实就是 offset
                  // + （一个 ConsumeQueue 文件包含多少个条目 =MappedFileSize / 20 ）

                  // OFFSET_OVERFLOW_ONE 表示待拉取消息的物理偏移量等于消息队列
                  // 最大的偏移量，如果有新的消息到达，此时会创建一个新的
                  // ConsumeQueue 文件，因为上一个 ConsueQueue 文件的最大偏移量就是下
                  // 一个文件的起始偏移量，所以可以按照该物理偏移量第二次拉取消息
                case NO_NEW_MSG:
                  // 没有匹配消息
                case NO_MATCHED_MSG:
                  pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                  // 则直接使用服务器端校正的偏移量进行下一次消息的拉取
                  DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                  DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                  break;

                  // 如果拉取结果显示偏移量非法，首先将ProcessQueue的dropped设为true，表示丢弃该消费队列，意味着ProcessQueue中拉取的消息将
                  // 停止消费，然后根据服务端下一次校对的偏移量尝试更新消息消费进 度（内存中），然后尝试持久化消息消费进度，并将该消息队列从
                  // RebalacnImpl的处理队列中移除，意味着暂停该消息队列的消息拉取，等待下一次消息队列重新负载

                  // 对应服务端GetMessageResult状态的 NO_MATCHED_LOGIC_QUEUE、NO_MESSAGE_IN_QUEUE
                  // OFFSET_OVERFLOW_BADLY、OFFSET_TOO_SMALL，这些状态服务端偏移量校正基本上使用原偏移量，在客户端更新消息消费进度时只有当消息
                  // 进度比当前消费进度大才会覆盖，以此保证消息进度的准确性
                case OFFSET_ILLEGAL:
                  log.warn(
                      "the pull request offset illegal, {} {}",
                      pullRequest.toString(),
                      pullResult.toString());
                  pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                  pullRequest.getProcessQueue().setDropped(true);
                  DefaultMQPushConsumerImpl.this.executeTaskLater(
                      new Runnable() {

                        @Override
                        public void run() {
                          try {
                            DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(
                                pullRequest.getMessageQueue(), pullRequest.getNextOffset(), false);

                            DefaultMQPushConsumerImpl.this.offsetStore.persist(
                                pullRequest.getMessageQueue());

                            DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(
                                pullRequest.getMessageQueue());

                            log.warn("fix the pull request offset, {}", pullRequest);
                          } catch (Throwable e) {
                            log.error("executeTaskLater Exception", e);
                          }
                        }
                      },
                      10000);
                  break;
                default:
                  break;
              }
            }
          }

          @Override
          public void onException(Throwable e) {
            if (!pullRequest
                .getMessageQueue()
                .getTopic()
                .startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
              log.warn("execute the pull request exception", e);
            }

            DefaultMQPushConsumerImpl.this.executePullRequestLater(
                pullRequest, pullTimeDelayMillsWhenException);
          }
        };

    boolean commitOffsetEnable = false;
    long commitOffsetValue = 0L;
    if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
      commitOffsetValue =
          this.offsetStore.readOffset(
              pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
      if (commitOffsetValue > 0) {
        commitOffsetEnable = true;
      }
    }

    String subExpression = null;
    boolean classFilter = false;
    SubscriptionData sd =
        this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
    if (sd != null) {
      if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
        subExpression = sd.getSubString();
      }

      classFilter = sd.isClassFilterMode();
    }
    // 构建消息拉取系统标记
    int sysFlag =
        PullSysFlag.buildSysFlag(
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null, // subscription
            classFilter // class filter
            );
    try {
      // 与服务端交互
      this.pullAPIWrapper.pullKernelImpl(
          pullRequest.getMessageQueue(),
          subExpression,
          subscriptionData.getExpressionType(),
          subscriptionData.getSubVersion(),
          pullRequest.getNextOffset(),
          this.defaultMQPushConsumer.getPullBatchSize(),
          sysFlag,
          commitOffsetValue,
          BROKER_SUSPEND_MAX_TIME_MILLIS,
          CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
          CommunicationMode.ASYNC,
          pullCallback);
    } catch (Exception e) {
      log.error("pullKernelImpl exception", e);
      this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
    }
  }

  private void makeSureStateOK() throws MQClientException {
    if (this.serviceState != ServiceState.RUNNING) {
      throw new MQClientException(
          "The consumer service state not OK, "
              + this.serviceState
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
          null);
    }
  }

  private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
    this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
  }

  public boolean isPause() {
    return pause;
  }

  public void setPause(boolean pause) {
    this.pause = pause;
  }

  public ConsumerStatsManager getConsumerStatsManager() {
    return this.mQClientFactory.getConsumerStatsManager();
  }

  public void executePullRequestImmediately(final PullRequest pullRequest) {
    this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
  }

  private void correctTagsOffset(final PullRequest pullRequest) {
    if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
      this.offsetStore.updateOffset(
          pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
    }
  }

  public void executeTaskLater(final Runnable r, final long timeDelay) {
    this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
  }

  public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
      throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
    List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
    TopicRouteData routeData =
        this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
    for (BrokerData brokerData : routeData.getBrokerDatas()) {
      String addr = brokerData.selectBrokerAddr();
      queueTimeSpan.addAll(
          this.mQClientFactory
              .getMQClientAPIImpl()
              .queryConsumeTimeSpan(addr, topic, groupName(), 3000));
    }

    return queueTimeSpan;
  }

  @Override
  public String groupName() {
    return this.defaultMQPushConsumer.getConsumerGroup();
  }

  @Override
  public MessageModel messageModel() {
    return this.defaultMQPushConsumer.getMessageModel();
  }

  @Override
  public ConsumeType consumeType() {
    return ConsumeType.CONSUME_PASSIVELY;
  }

  @Override
  public ConsumeFromWhere consumeFromWhere() {
    return this.defaultMQPushConsumer.getConsumeFromWhere();
  }

  @Override
  public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
    Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
    if (subTable != null) {
      if (subTable.containsKey(topic)) {
        this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
      }
    }
  }

  public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
    return this.rebalanceImpl.getSubscriptionInner();
  }

  @Override
  public boolean isSubscribeTopicNeedUpdate(String topic) {
    Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
    if (subTable != null) {
      if (subTable.containsKey(topic)) {
        return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
      }
    }

    return false;
  }

  @Override
  public ConsumerRunningInfo consumerRunningInfo() {
    ConsumerRunningInfo info = new ConsumerRunningInfo();

    Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

    prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
    prop.put(
        ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE,
        String.valueOf(this.consumeMessageService.getCorePoolSize()));
    prop.put(
        ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP,
        String.valueOf(this.consumerStartTimestamp));

    info.setProperties(prop);

    Set<SubscriptionData> subSet = this.subscriptions();
    info.getSubscriptionSet().addAll(subSet);

    Iterator<Entry<MessageQueue, ProcessQueue>> it =
        this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
    while (it.hasNext()) {
      Entry<MessageQueue, ProcessQueue> next = it.next();
      MessageQueue mq = next.getKey();
      ProcessQueue pq = next.getValue();

      ProcessQueueInfo pqinfo = new ProcessQueueInfo();
      pqinfo.setCommitOffset(
          this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
      pq.fillProcessQueueInfo(pqinfo);
      info.getMqTable().put(mq, pqinfo);
    }

    for (SubscriptionData sd : subSet) {
      ConsumeStatus consumeStatus =
          this.mQClientFactory
              .getConsumerStatsManager()
              .consumeStatus(this.groupName(), sd.getTopic());
      info.getStatusTable().put(sd.getTopic(), consumeStatus);
    }

    return info;
  }

  @Override
  public Set<SubscriptionData> subscriptions() {
    Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

    subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

    return subSet;
  }

  public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
      throws MQClientException, InterruptedException {
    return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
  }

  public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
      throws MQClientException, InterruptedException {
    return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
  }

  public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
    this.consumeMessageHookList.add(hook);
    log.info("register consumeMessageHook Hook, {}", hook.hookName());
  }

  public void registerFilterMessageHook(final FilterMessageHook hook) {
    this.filterMessageHookList.add(hook);
    log.info("register FilterMessageHook Hook, {}", hook.hookName());
  }

  public void registerMessageListener(MessageListener messageListener) {
    this.messageListenerInner = messageListener;
  }

  public void resetOffsetByTimeStamp(long timeStamp)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
    for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
      Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
      Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
      if (mqs != null) {
        for (MessageQueue mq : mqs) {
          long offset = searchOffset(mq, timeStamp);
          offsetTable.put(mq, offset);
        }
        this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
      }
    }
  }

  public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
    return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
  }

  public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
    final String groupTopic = MixAll.getRetryTopic(consumerGroup);
    for (MessageExt msg : msgs) {
      String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
      if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
        msg.setTopic(retryTopic);
      }

      if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
        msg.setTopic(
            NamespaceUtil.withoutNamespace(
                msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
      }
    }
  }

  public void resume() {
    this.pause = false;
    doRebalance();
    log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
  }

  @Override
  public void doRebalance() {
    if (!this.pause) {
      this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
    }
  }

  public boolean isConsumeOrderly() {
    return consumeOrderly;
  }

  public void setConsumeOrderly(boolean consumeOrderly) {
    this.consumeOrderly = consumeOrderly;
  }

  public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
    try {
      String brokerAddr =
          (null != brokerName)
              ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
              : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
      this.mQClientFactory
          .getMQClientAPIImpl()
          .consumerSendMessageBack(
              brokerAddr,
              msg,
              this.defaultMQPushConsumer.getConsumerGroup(),
              delayLevel,
              5000,
              getMaxReconsumeTimes());
    } catch (Exception e) {
      log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

      Message newMsg =
          new Message(
              MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

      String originMsgId = MessageAccessor.getOriginMessageId(msg);
      MessageAccessor.setOriginMessageId(
          newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

      newMsg.setFlag(msg.getFlag());
      MessageAccessor.setProperties(newMsg, msg.getProperties());
      MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
      MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
      MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
      MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
      newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

      this.mQClientFactory.getDefaultMQProducer().send(newMsg);
    } finally {
      msg.setTopic(
          NamespaceUtil.withoutNamespace(
              msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
    }
  }

  private int getMaxReconsumeTimes() {
    // default reconsume times: 16
    if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
      return 16;
    } else {
      return this.defaultMQPushConsumer.getMaxReconsumeTimes();
    }
  }

  public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
    this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
  }

  public void shutdown() {
    shutdown(0);
  }

  public synchronized void shutdown(long awaitTerminateMillis) {
    switch (this.serviceState) {
      case CREATE_JUST:
        break;
      case RUNNING:
        this.consumeMessageService.shutdown(awaitTerminateMillis);
        this.persistConsumerOffset();
        this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
        this.mQClientFactory.shutdown();
        log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
        this.rebalanceImpl.destroy();
        this.serviceState = ServiceState.SHUTDOWN_ALREADY;
        break;
      case SHUTDOWN_ALREADY:
        break;
      default:
        break;
    }
  }

  @Override
  public void persistConsumerOffset() {
    try {
      this.makeSureStateOK();
      Set<MessageQueue> mqs = new HashSet<MessageQueue>();
      Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
      mqs.addAll(allocateMq);

      this.offsetStore.persistAll(mqs);
    } catch (Exception e) {
      log.error(
          "group: "
              + this.defaultMQPushConsumer.getConsumerGroup()
              + " persistConsumerOffset exception",
          e);
    }
  }

  public synchronized void start() throws MQClientException {
    // 根据服务状态进行处理
    switch (this.serviceState) {
        // 服务刚刚创建，未启动
      case CREATE_JUST:
        {
          log.info(
              "the consumer [{}] start beginning. messageModel={}, isUnitMode={}",
              this.defaultMQPushConsumer.getConsumerGroup(),
              this.defaultMQPushConsumer.getMessageModel(),
              this.defaultMQPushConsumer.isUnitMode());
          this.serviceState = ServiceState.START_FAILED;
          // 检查配置属性
          this.checkConfig();
          // 订阅信息和 ( 重试前缀 + 消费组的订阅 ) 信息加入 RebalanceImpl

          this.copySubscription();

          if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
            this.defaultMQPushConsumer.changeInstanceNameToPID();
          }

          this.mQClientFactory =
              MQClientManager.getInstance()
                  .getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

          // region 初始化RebalanceImpl（消息重新负载实现类）
          this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
          this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
          this.rebalanceImpl.setAllocateMessageQueueStrategy(
              this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
          this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
          // endregion

          this.pullAPIWrapper =
              new PullAPIWrapper(
                  mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
          this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

          // region 初始化OffsetStore（消息进度）
          if (this.defaultMQPushConsumer.getOffsetStore() != null) {
            this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
          } else {
            // 根据不同的消费方式，OffSetStore存在不同的类型
            switch (this.defaultMQPushConsumer.getMessageModel()) {
                // 广播
              case BROADCASTING:
                // Offset存到本地（消费者端）
                this.offsetStore =
                    new LocalFileOffsetStore(
                        this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                break;
                // 集群
              case CLUSTERING:
                // Offset存在Broker机器上
                this.offsetStore =
                    new RemoteBrokerOffsetStore(
                        this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                break;
              default:
                break;
            }
            this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
          }
          this.offsetStore.load();
          // endregion

          // region 初始化 consumeMessageService，并启动
          // ? 顺序消费
          if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
            this.consumeOrderly = true;
            this.consumeMessageService =
                new ConsumeMessageOrderlyService(
                    this, (MessageListenerOrderly) this.getMessageListenerInner());
          }
          // ? 并发消费
          else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
            this.consumeOrderly = false;
            this.consumeMessageService =
                new ConsumeMessageConcurrentlyService(
                    this, (MessageListenerConcurrently) this.getMessageListenerInner());
          }
          this.consumeMessageService.start();
          // endregion
          // 向 MQClientInstance 注册消费者并启动 MQClientInstance ， JVM 中的所有消费者、生产者持有同一个 MQClientInstance ，
          // MQClientInstance 只会启动一次
          boolean registerOK =
              mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
          // ? 注册失败，重复注册、未设置 group 造成失败
          if (!registerOK) {
            this.serviceState = ServiceState.CREATE_JUST;
            this.consumeMessageService.shutdown(
                defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
            throw new MQClientException(
                "The consumer group["
                    + this.defaultMQPushConsumer.getConsumerGroup()
                    + "] has been created before, specify another name please."
                    + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                null);
          }
          // 启动 MQClientInstance
          mQClientFactory.start();
          log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
          this.serviceState = ServiceState.RUNNING;
          break;
        }
        // 运行中
      case RUNNING:
        // 服务启动失败
      case START_FAILED:
        // 服务关闭
      case SHUTDOWN_ALREADY:
        throw new MQClientException(
            "The PushConsumer service state not OK, maybe started once, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
            null);
      default:
        break;
    }

    this.updateTopicSubscribeInfoWhenSubscriptionChanged();
    this.mQClientFactory.checkClientInBroker();
    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
    this.mQClientFactory.rebalanceImmediately();
  }

  private void checkConfig() throws MQClientException {
    Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

    if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
      throw new MQClientException(
          "consumerGroup is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
    }

    if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
      throw new MQClientException(
          "consumerGroup can not equal "
              + MixAll.DEFAULT_CONSUMER_GROUP
              + ", please specify another one."
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    if (null == this.defaultMQPushConsumer.getMessageModel()) {
      throw new MQClientException(
          "messageModel is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
    }

    if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
      throw new MQClientException(
          "consumeFromWhere is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
    }

    Date dt =
        UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
    if (null == dt) {
      throw new MQClientException(
          "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
              + this.defaultMQPushConsumer.getConsumeTimestamp()
              + " "
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // allocateMessageQueueStrategy
    if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
      throw new MQClientException(
          "allocateMessageQueueStrategy is null"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // subscription
    if (null == this.defaultMQPushConsumer.getSubscription()) {
      throw new MQClientException(
          "subscription is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
    }

    // messageListener
    if (null == this.defaultMQPushConsumer.getMessageListener()) {
      throw new MQClientException(
          "messageListener is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
    }

    boolean orderly =
        this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
    boolean concurrently =
        this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
    if (!orderly && !concurrently) {
      throw new MQClientException(
          "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // consumeThreadMin
    if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
        || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
      throw new MQClientException(
          "consumeThreadMin Out of range [1, 1000]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // consumeThreadMax
    if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1
        || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
      throw new MQClientException(
          "consumeThreadMax Out of range [1, 1000]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // consumeThreadMin can't be larger than consumeThreadMax
    if (this.defaultMQPushConsumer.getConsumeThreadMin()
        > this.defaultMQPushConsumer.getConsumeThreadMax()) {
      throw new MQClientException(
          "consumeThreadMin ("
              + this.defaultMQPushConsumer.getConsumeThreadMin()
              + ") "
              + "is larger than consumeThreadMax ("
              + this.defaultMQPushConsumer.getConsumeThreadMax()
              + ")",
          null);
    }

    // consumeConcurrentlyMaxSpan
    if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
        || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
      throw new MQClientException(
          "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // pullThresholdForQueue
    if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1
        || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
      throw new MQClientException(
          "pullThresholdForQueue Out of range [1, 65535]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // pullThresholdForTopic
    if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
      if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1
          || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
        throw new MQClientException(
            "pullThresholdForTopic Out of range [1, 6553500]"
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
            null);
      }
    }

    // pullThresholdSizeForQueue
    if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1
        || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
      throw new MQClientException(
          "pullThresholdSizeForQueue Out of range [1, 1024]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
      // pullThresholdSizeForTopic
      if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1
          || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
        throw new MQClientException(
            "pullThresholdSizeForTopic Out of range [1, 102400]"
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
            null);
      }
    }

    // pullInterval
    if (this.defaultMQPushConsumer.getPullInterval() < 0
        || this.defaultMQPushConsumer.getPullInterval() > 65535) {
      throw new MQClientException(
          "pullInterval Out of range [0, 65535]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // consumeMessageBatchMaxSize
    if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
        || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
      throw new MQClientException(
          "consumeMessageBatchMaxSize Out of range [1, 1024]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }

    // pullBatchSize
    if (this.defaultMQPushConsumer.getPullBatchSize() < 1
        || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
      throw new MQClientException(
          "pullBatchSize Out of range [1, 1024]"
              + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
          null);
    }
  }

  private void copySubscription() throws MQClientException {
    try {
      Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
      if (sub != null) {
        for (final Map.Entry<String, String> entry : sub.entrySet()) {
          final String topic = entry.getKey();
          final String subString = entry.getValue();
          // 构建主题订阅信息 SubscriptionData
          SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
          // 加入 RebalanceImpl 的订阅消息中
          this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
        }
      }

      if (null == this.messageListenerInner) {
        this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
      }

      switch (this.defaultMQPushConsumer.getMessageModel()) {
        case BROADCASTING:
          break;
        case CLUSTERING:
          // 消息重试是以消费组为单位，而不是主题，消息重试主题名为 %RETRY%+消费组名。
          // 消费者在启动时会自动订阅该主题，参与该主题的消息队列负载
          final String retryTopic =
              MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
          SubscriptionData subscriptionData =
              FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
          this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
          break;
        default:
          break;
      }
    } catch (Exception e) {
      throw new MQClientException("subscription exception", e);
    }
  }

  @Override
  public boolean isUnitMode() {
    return this.defaultMQPushConsumer.isUnitMode();
  }

  public MessageListener getMessageListenerInner() {
    return messageListenerInner;
  }

  private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
    Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
    if (subTable != null) {
      for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
        final String topic = entry.getKey();
        this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
      }
    }
  }

  public void subscribe(String topic, String fullClassName, String filterClassSource)
      throws MQClientException {
    try {
      SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, "*");
      subscriptionData.setSubString(fullClassName);
      subscriptionData.setClassFilterMode(true);
      subscriptionData.setFilterClassSource(filterClassSource);
      this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
      if (this.mQClientFactory != null) {
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
      }

    } catch (Exception e) {
      throw new MQClientException("subscription exception", e);
    }
  }

  public void subscribe(final String topic, final MessageSelector messageSelector)
      throws MQClientException {
    try {
      if (messageSelector == null) {
        subscribe(topic, SubscriptionData.SUB_ALL);
        return;
      }

      SubscriptionData subscriptionData =
          FilterAPI.build(
              topic, messageSelector.getExpression(), messageSelector.getExpressionType());

      this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
      if (this.mQClientFactory != null) {
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
      }
    } catch (Exception e) {
      throw new MQClientException("subscription exception", e);
    }
  }

  public void subscribe(String topic, String subExpression) throws MQClientException {
    try {
      SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
      this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
      if (this.mQClientFactory != null) {
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
      }
    } catch (Exception e) {
      throw new MQClientException("subscription exception", e);
    }
  }

  public void suspend() {
    this.pause = true;
    log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
  }

  public void unsubscribe(String topic) {
    this.rebalanceImpl.getSubscriptionInner().remove(topic);
  }

  public void updateConsumeOffset(MessageQueue mq, long offset) {
    this.offsetStore.updateOffset(mq, offset, false);
  }

  public void updateCorePoolSize(int corePoolSize) {
    this.consumeMessageService.updateCorePoolSize(corePoolSize);
  }

  public MessageExt viewMessage(String msgId)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
    return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
  }
}
