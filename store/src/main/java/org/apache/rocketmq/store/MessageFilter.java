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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 消息过滤器
 *
 * @author shui4
 */
public interface MessageFilter {
  /**
   * 根据 {@link ConsumeQueue} 判断消息是否匹配
   *
   * @param tagsCode 消息标志的哈希码
   * @param cqExtUnit {@link ConsumeQueue} 条目扩展属性
   * @return ignore
   */
  boolean isMatchedByConsumeQueue(final Long tagsCode, final ConsumeQueueExt.CqExtUnit cqExtUnit);

  /**
   * 根据存储在 {@link CommitLog} 文件中的 内容判断消息是否匹配。
   *
   * <p>该方法主要 是为 SQL92 表达式模式服务的，根据消息属性实现类似于数据库 SQL where 条件的过滤方式
   *
   * @param msgBuffer 消息内容，如果为空，该方法返回 <code>true</code>
   * @param properties 消息属性，主要用于 <code>SQL92</code> 过滤模式
   * @return ignore
   */
  boolean isMatchedByCommitLog(final ByteBuffer msgBuffer, final Map<String, String> properties);
}