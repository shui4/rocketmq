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
package org.apache.rocketmq.client.consumer.store;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;
import java.util.Set;

/** OffsetStore 里存储的是当前消费者所消费的消息在队列的偏移量 */
public interface OffsetStore {
  /** 从消息进度存储文件加载消息进度到内存 */
  void load() throws MQClientException;

  /**
   * 更新内存中的消息消费进度
   *
   * @param mq 消息消费队列
   * @param offset 消息消费偏移量
   * @param increaseOnly true 表示 offset 必须大于内存中当前的消费偏移量才更新
   */
  void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

  /**
   * 从磁盘中读取消息队列的消费进度
   *
   * @param mq 消息消费队列
   * @param type 读取方式，可选值包括：
   *     <ul>
   *       <li>READ_FROM_MEMORY，即从内存中读取，
   *       <li>READ_FROM_STORE，即从磁盘中读取，
   *       <li>MEMORY_FIRST_THEN_STORE，即先从内存中读取，再从磁盘中读取
   *     </ul>
   *
   * @return 偏移量
   *     <ul>
   *       <li>-1：表示该消息队列刚创建
   *       <li>>-1：错误的偏移量
   *     </ul>
   */
  long readOffset(final MessageQueue mq, final ReadOffsetType type);

  /**
   * 持久化指定消息队列进度到磁盘
   * <p>一个定时任务会执行该方法 {@link MQClientInstance#startScheduledTask()}</p>
   *
   * @param mqs 消息队列集合
   */
  @SuppressWarnings("JavadocReference")
  void persistAll(final Set<MessageQueue> mqs);

  /** Persist the offset,may be in local storage or remote name server */
  void persist(final MessageQueue mq);

  /**
   * 将消息队列的消息消费进度从内存中移除
   *
   * @param mq mq
   */
  void removeOffset(MessageQueue mq);

  /**
   * 复制该主题下所有消 息队列的消息消费进度
   *
   * @param topic 主题
   * @return ignore
   */
  Map<MessageQueue, Long> cloneOffsetTable(String topic);

  /**
   * 使用集群模式更新存储在 Broker 端的消息消费进度
   *
   * @param mq ignore
   * @param offset ignore
   * @param isOneway 单向？
   * @throws RemotingException 远程异常
   * @throws MQBrokerException mqbroker 异常
   * @throws InterruptedException 中断异常
   * @throws MQClientException mqclient 异常
   */
  void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException;
}