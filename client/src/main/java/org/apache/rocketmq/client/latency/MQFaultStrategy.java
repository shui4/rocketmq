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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl.FaultItem;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类 <br>
 * 根据 {@link FaultItem#getCurrentLatency()}
 * 本次下那些发送的延迟时间，从latencyMax尾部向前找到第一个比currentLatency小的索引index，如果没有找到，则返回0，然后根据这个索引从 {@link
 * #getNotAvailableDuration()} 数组中取出对应的时间，在这个时长内，Broker将设置为不可用。
 */
public class MQFaultStrategy {
  private static final InternalLogger log = ClientLogger.getLog();
  private final LatencyFaultTolerance<String> latencyFaultTolerance =
      new LatencyFaultToleranceImpl();
  /**
   * false（默认）不启用Broker故障延迟机制。 只会在本次消息发送的重试过程中规避该Broker（equals+continue）<br>
   * true：启用Broker故障延迟机制。一种悲观的做法，当消息发送者遇到一次消息发送失败后，就会悲观地认为Broker不可用，在接下来的一段时间内就不再其发消息，当然死马当活马医除外<br>
   */
  private boolean sendLatencyFaultEnable = false;

  private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
  private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

  public long[] getNotAvailableDuration() {
    return notAvailableDuration;
  }

  public void setNotAvailableDuration(final long[] notAvailableDuration) {
    this.notAvailableDuration = notAvailableDuration;
  }

  public long[] getLatencyMax() {
    return latencyMax;
  }

  public void setLatencyMax(final long[] latencyMax) {
    this.latencyMax = latencyMax;
  }

  public boolean isSendLatencyFaultEnable() {
    return sendLatencyFaultEnable;
  }

  public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
    this.sendLatencyFaultEnable = sendLatencyFaultEnable;
  }
  //   // 代码清单3-15
  public MessageQueue selectOneMessageQueue(
      final TopicPublishInfo tpInfo, final String lastBrokerName) {
    // ? 启用Broker故障延迟机制
    if (this.sendLatencyFaultEnable) {
      try {
        // 自增
        // 轮询获取一个消息队列
        int index = tpInfo.getSendWhichQueue().incrementAndGet();
        for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
          // 与当前路由表中消息队列的个数取模
          int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
          if (pos < 0) pos = 0;
          MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
          // 验证消息队列是否可用
          if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) return mq;
        }
        // 在都不可用的情况，随机获取一个Broker
        final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
        int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
        if (writeQueueNums > 0) {
          final MessageQueue mq = tpInfo.selectOneMessageQueue();
          if (notBestBroker != null) {
            mq.setBrokerName(notBestBroker);
            mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
          }
          return mq;
        } else {
          // 延迟容错信息中移除（因为写队列数量小于0，该信息保留就没有任何意义）
          latencyFaultTolerance.remove(notBestBroker);
        }
      } catch (Exception e) {
        log.error("Error occurred when selecting message queue", e);
      }
      // 死马当活马医，那还是得获取一个 尝试一下吧？
      return tpInfo.selectOneMessageQueue();
    }

    return tpInfo.selectOneMessageQueue(lastBrokerName);
  }
  //  代码清单3-17
  public void updateFaultItem(
      final String brokerName, final long currentLatency, boolean isolation) {
    if (this.sendLatencyFaultEnable) {
      // isolation：true 30秒
      // computeNotAvailableDuration的作用是计算因本次消息发送故障需要规避的时长，也就是接下来多长的时间内，该Broker将不参与消息发送队列负载。
      // 具体算法是，从latencyMax数组尾部开始寻找，找到第一个比currentLatency小的下标，然后 updateFaultItem 从
      // notAvailableDuration数组中获取需要规避的时长，
      long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
      this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
    }
  }

  private long computeNotAvailableDuration(final long currentLatency) {
    for (int i = latencyMax.length - 1; i >= 0; i--) {
      if (currentLatency >= latencyMax[i]) return this.notAvailableDuration[i];
    }

    return 0;
  }
}
