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

import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PullAPIWrapper {
  private final InternalLogger log = ClientLogger.getLog();
  private final MQClientInstance mQClientFactory;
  private final String consumerGroup;
  private final boolean unitMode;
  /**
   * BrokerId 缓存表。
   *
   * <p>这里的信息维护发生在：{@link PullMessageService}根据 {@link PullRequest} 请求从主服务器拉取消息后，会 返回下一次建议拉取的
   * brokerId，消息消费者线程在收到消息后，会 根据主服务器的建议拉取 brokerId 来更新它，* 消息消费者线程更新它
   */
  private ConcurrentMap<MessageQueue, AtomicLong /* brokerId */> pullFromWhichNodeTable =
      new ConcurrentHashMap<MessageQueue, AtomicLong>(32);

  private volatile boolean connectBrokerByUser = false;
  private volatile long defaultBrokerId = MixAll.MASTER_ID;
  private Random random = new Random(System.currentTimeMillis());
  private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

  public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
    this.mQClientFactory = mQClientFactory;
    this.consumerGroup = consumerGroup;
    this.unitMode = unitMode;
  }

  public PullResult processPullResult(
      final MessageQueue mq, final PullResult pullResult, final SubscriptionData subscriptionData) {
    // 根据响应结果解码成 PullResultExt 对象
    PullResultExt pullResultExt = (PullResultExt) pullResult;

    this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
    // 消息拉取线程 PullMessageService 默认会使用异步方式从服务器 拉取消息，消息消费端会通过 PullAPIWrapper 从响应结果解析拉取到 的消息。如果消息过滤模式为
    // TAG，并且订阅 TAG 集合不为空，则对消 息的标志进行判断，如果集合中包含消息的 TAG，则返回给消费者消 费，否则跳过
    if (PullStatus.FOUND == pullResult.getPullStatus()) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
      List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

      List<MessageExt> msgListFilterAgain = msgList;
      if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
        msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
        for (MessageExt msg : msgList) {
          if (msg.getTags() != null) {
            if (subscriptionData.getTagsSet().contains(msg.getTags())) {
              msgListFilterAgain.add(msg);
            }
          }
        }
      }

      if (this.hasHook()) {
        FilterMessageContext filterMessageContext = new FilterMessageContext();
        filterMessageContext.setUnitMode(unitMode);
        filterMessageContext.setMsgList(msgListFilterAgain);
        this.executeHook(filterMessageContext);
      }

      for (MessageExt msg : msgListFilterAgain) {
        String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (Boolean.parseBoolean(traFlag)) {
          msg.setTransactionId(
              msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        }
        MessageAccessor.putProperty(
            msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
        MessageAccessor.putProperty(
            msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
        msg.setBrokerName(mq.getBrokerName());
      }

      pullResultExt.setMsgFoundList(msgListFilterAgain);
    }

    pullResultExt.setMessageBinary(null);

    return pullResult;
  }

  public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
    AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
    if (null == suggest) {
      this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
    } else {
      suggest.set(brokerId);
    }
  }

  public boolean hasHook() {
    return !this.filterMessageHookList.isEmpty();
  }

  public void executeHook(final FilterMessageContext context) {
    if (!this.filterMessageHookList.isEmpty()) {
      for (FilterMessageHook hook : this.filterMessageHookList) {
        try {
          hook.filterMessage(context);
        } catch (Throwable e) {
          log.error("execute hook error. hookName={}", hook.hookName());
        }
      }
    }
  }

  /**
   * 与服务端交互 拉取
   *
   * @param mq 从哪个消息消费队列拉取消息
   * @param subExpression 消息过滤表达式
   * @param expressionType 消息表达式类型，分为 TAG、 SQL92
   * @param subVersion ignore
   * @param offset 消息拉取偏移量
   * @param maxNums 本次拉取最大消息条数，默认 32 条
   * @param sysFlag 拉取系统标记
   * @param commitOffset 当前 MessageQueue 的消费进度（内存中）
   * @param brokerSuspendMaxTimeMillis 消息拉取过程中允许 Broker 挂起的时间，默认 15s
   * @param timeoutMillis 消息拉取超时时间
   * @param communicationMode 消息拉取模式，默认为异步拉取
   * @param pullCallback 从 Broker 拉取到消息后的回调方法
   * @return {@link PullResult}
   * @throws MQClientException ignore
   * @throws RemotingException ignore
   * @throws MQBrokerException ignore
   * @throws InterruptedException ignore
   */
  public PullResult pullKernelImpl(
      final MessageQueue mq,
      final String subExpression,
      final String expressionType,
      final long subVersion,
      final long offset,
      final int maxNums,
      final int sysFlag,
      final long commitOffset,
      final long brokerSuspendMaxTimeMillis,
      final long timeoutMillis,
      final CommunicationMode communicationMode,
      final PullCallback pullCallback)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    // 根据 brokerName、BrokerId 从 MQClientInstance 中获取 Broker 地址，在整个 RocketMQ
    // Broker 的部署结构中，相同名称的 Broker 构成主从结构，其 BrokerId 会不一样，在每次拉取消息后，给出一个建议，下次是从主节点还是从节点拉取
    FindBrokerResult findBrokerResult =
        this.mQClientFactory.findBrokerAddressInSubscribe(
            mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
    if (null == findBrokerResult) {
      this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
      findBrokerResult =
          this.mQClientFactory.findBrokerAddressInSubscribe(
              mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
    }

    if (findBrokerResult != null) {
      {
        // check version
        if (!ExpressionType.isTagType(expressionType)
            && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
          throw new MQClientException(
              "The broker["
                  + mq.getBrokerName()
                  + ","
                  + findBrokerResult.getBrokerVersion()
                  + "] does not upgrade to support for filter message by"
                  + expressionType,
              null);
        }
      }
      int sysFlagInner = sysFlag;

      if (findBrokerResult.isSlave()) {
        sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
      }
      PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
      requestHeader.setTopic(mq.getTopic());
      requestHeader.setConsumerGroup(this.consumerGroup);
      requestHeader.setQueueId(mq.getQueueId());
      // 消费者在向 Broker 发送拉取消息请求时，会先将客户端存储的消费进度提交到 Broker 端
      requestHeader.setQueueOffset(offset);
      requestHeader.setMaxMsgNums(maxNums);
      requestHeader.setSysFlag(sysFlagInner);
      requestHeader.setCommitOffset(commitOffset);
      // 设置暂停超时毫秒
      requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
      requestHeader.setSubscription(subExpression);
      requestHeader.setSubVersion(subVersion);
      requestHeader.setExpressionType(expressionType);

      String brokerAddr = findBrokerResult.getBrokerAddr();
      // ? 消息过滤模式为类过滤
      // 根据主题名称、 broker 地址找到注册在 Broker 上的 FilterServer 地址，从 FilterServer 上拉取消息，否则从 Broker 上拉取消息
      if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
        brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
      }
      // 异步向 Broker 拉取消息 RequestCode#PULL_MESSAGE
      PullResult pullResult =
          this.mQClientFactory
              .getMQClientAPIImpl()
              .pullMessage(
                  brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback);

      return pullResult;
    }

    throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
  }

  public long recalculatePullFromWhichNode(final MessageQueue mq) {
    if (this.isConnectBrokerByUser()) {
      return this.defaultBrokerId;
    }
    // 获取该消息消费队列的 brokerId，
    // 如果找到，则返回，否则返回 brokerName 的主节点
    AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
    if (suggest != null) {
      return suggest.get();
    }

    return MixAll.MASTER_ID;
  }

  private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
      throws MQClientException {
    ConcurrentMap<String, TopicRouteData> topicRouteTable =
        this.mQClientFactory.getTopicRouteTable();
    if (topicRouteTable != null) {
      TopicRouteData topicRouteData = topicRouteTable.get(topic);
      List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

      if (list != null && !list.isEmpty()) {
        return list.get(randomNum() % list.size());
      }
    }

    throw new MQClientException(
        "Find Filter Server Failed, Broker Addr:" + brokerAddr + "topic:" + topic, null);
  }

  public boolean isConnectBrokerByUser() {
    return connectBrokerByUser;
  }

  public void setConnectBrokerByUser(boolean connectBrokerByUser) {
    this.connectBrokerByUser = connectBrokerByUser;
  }

  public int randomNum() {
    int value = random.nextInt();
    if (value < 0) {
      value = Math.abs(value);
      if (value < 0) value = 0;
    }
    return value;
  }

  public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
    this.filterMessageHookList = filterMessageHookList;
  }

  public long getDefaultBrokerId() {
    return defaultBrokerId;
  }

  public void setDefaultBrokerId(long defaultBrokerId) {
    this.defaultBrokerId = defaultBrokerId;
  }
}
