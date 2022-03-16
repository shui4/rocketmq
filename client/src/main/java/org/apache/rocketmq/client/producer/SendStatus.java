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
package org.apache.rocketmq.client.producer;

public enum SendStatus {
  /**
   * 成功，发送成功的具体含义，
   * 比如消息是否已经被存储到磁盘？
   * 消息是否被同步到了Slave上？
   * 消息在Slave上是否被写入磁盘？
   * 需要结合所配置的刷盘策略、主从策略来定。这个状态还可以简单理解为，没有发生上面列出的三个问题状态就是SEND_OK
   */
  SEND_OK,
  /**
   * 表示没有在规定时间内完成刷盘（需要Broker的刷盘策略被设置成SYNC_FLUSH才会报这个错误）
   */
  FLUSH_DISK_TIMEOUT,
  /**
   * 在主备方式下，并且Broker被设置成SYNC_MASTER，但是没有找到配置成Slave的Broker
   */
  FLUSH_SLAVE_TIMEOUT,
  /**
   * 在主备方式下，并且Broker被设置成SYNC_MASTER，没有在设定时间内完成主从同步
   */
  SLAVE_NOT_AVAILABLE,
}
