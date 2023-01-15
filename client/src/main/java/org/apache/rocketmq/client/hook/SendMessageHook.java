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
package org.apache.rocketmq.client.hook;

// 代码清单 3-24

/**
 * 消息发送钩子，用于在消息发送之 前、发送之后执行一定的业务逻辑，是记录消息轨迹的最佳扩展点
 *
 * @author shui4
 */
public interface SendMessageHook {
  /**
   * hookName
   *
   * @return ignore
   */
  String hookName();

  /**
   * sendMessageBefore
   *
   * @param context ignore
   */
  void sendMessageBefore(final SendMessageContext context);

  /**
   * 客户端接收到服务端消息发送响应请求后被调用
   *
   * @param context ignore
   */
  void sendMessageAfter(final SendMessageContext context);
}