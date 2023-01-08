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
package org.apache.rocketmq.store;

import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

@SuppressWarnings("JavadocReference")
public class StoreUtil {
  /**
   * RocketMQ所在服务器的总内存 大小。{@link MessageStoreConfig#accessMessageInMemoryMaxRatio} 表示RocketMQ所能使用的最大
   * 内存比例，超过该比例，消息将被置换出内存。memory表示RocketMQ 消息常驻内存的大小，超过该大小，RocketMQ会将旧的消息置换回磁 盘
   */
  public static final long TOTAL_PHYSICAL_MEMORY_SIZE = getTotalPhysicalMemorySize();

  @SuppressWarnings("restriction")
  public static long getTotalPhysicalMemorySize() {
    long physicalTotal = 1024 * 1024 * 1024 * 24L;
    OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
    if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
      physicalTotal =
          ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
    }

    return physicalTotal;
  }
}
