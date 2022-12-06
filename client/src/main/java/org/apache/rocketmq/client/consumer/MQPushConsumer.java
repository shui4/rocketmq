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
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;

/** Push consumer */
public interface MQPushConsumer extends MQConsumer {
  /** 启动消费者 */
  void start() throws MQClientException;

  /** 关闭使用者 */
  void shutdown();

  /** 注册消息侦听器 */
  @Deprecated
  void registerMessageListener(MessageListener messageListener);

  /**
   * 注册并发消息事件监听器
   *
   * @param messageListener 消息侦听器
   */
  void registerMessageListener(final MessageListenerConcurrently messageListener);

  /**
   * 注册顺序消费事件监听器
   *
   * @param messageListener 消息侦听器
   */
  void registerMessageListener(final MessageListenerOrderly messageListener);

  /**
   * 订阅主题
   *
   * @param subExpression 消息过滤表达式， TAG 或 SQL92 表达式
   */
  void subscribe(final String topic, final String subExpression) throws MQClientException;

  /**
   * 订阅主题
   *
   * @param topic 主题
   * @param fullClassName 过滤类全路径名
   * @param filterClassSource 过滤类代码
   */
  @Deprecated
  void subscribe(final String topic, final String fullClassName, final String filterClassSource)
      throws MQClientException;

  /**
   * 使用选择器订阅一些主题。
   *
   * <p>该接口还具有以下功能： {@link #subscribe(String, String)}, 并且，支持其他消息选择，例如 {@link
   * org.apache.rocketmq.common.filter.ExpressionType#SQL92}。
   *
   * <p>
   *
   * <p>选择标签: {@link MessageSelector#byTag(java.lang.String)}
   *
   * <p>
   *
   * <p>选择 SQL92: {@link MessageSelector#bySql(java.lang.String)}
   *
   * @param selector 消息选择器({@link MessageSelector}), 可以为空。
   */
  void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

  /**
   * 取消订阅
   *
   * @param topic 消息主题
   */
  void unsubscribe(final String topic);

  /** 动态更新使用者线程池大小 */
  void updateCorePoolSize(int corePoolSize);

  /** 暂停消费 */
  void suspend();

  /** 恢复消费 */
  void resume();
}
