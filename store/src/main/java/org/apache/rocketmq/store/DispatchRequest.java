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

import java.util.Map;

/**
 * 调度请求
 *
 * @author shui4
 */
public class DispatchRequest {
  /** 消息主题名称 */
  private final String topic;
  /** 消息队列id */
  private final int queueId;
  /** 消息物理偏移量 */
  private final long commitLogOffset;
  /** 消息长度 */
  private int msgSize;
  /** 消息过滤tag哈希码 */
  private final long tagsCode;
  /** 消息存储时间戳 */
  private final long storeTimestamp;
  /** 消息队列偏移量 */
  private final long consumeQueueOffset;
  /** 消息索引key。多个索引key用空格隔开 */
  private final String keys;
  /** 是否成功解析到完整的消息 */
  private final boolean success;
  /** 消息唯一键 */
  private final String uniqKey;
  /** 消息系统标记 */
  private final int sysFlag;
  /** 消息预处理事务偏移量 */
  private final long preparedTransactionOffset;
  /** 消息属性 */
  private final Map<String, String> propertiesMap;
  /** 位图 */
  private byte[] bitMap;

  /** 缓冲区大小 */
  private int bufferSize =
      -1; // the buffer size maybe larger than the msg size if the message is wrapped by something

  /**
   * 调度请求
   *
   * @param topic 话题
   * @param queueId 队列id
   * @param commitLogOffset 提交日志偏移量
   * @param msgSize 消息大小
   * @param tagsCode 标签代码
   * @param storeTimestamp 存储时间戳
   * @param consumeQueueOffset 使用队列偏移
   * @param keys 钥匙
   * @param uniqKey uniq键
   * @param sysFlag sys标志
   * @param preparedTransactionOffset 准备交易抵销
   * @param propertiesMap 属性映射
   */
  public DispatchRequest(
      final String topic,
      final int queueId,
      final long commitLogOffset,
      final int msgSize,
      final long tagsCode,
      final long storeTimestamp,
      final long consumeQueueOffset,
      final String keys,
      final String uniqKey,
      final int sysFlag,
      final long preparedTransactionOffset,
      final Map<String, String> propertiesMap) {
    this.topic = topic;
    this.queueId = queueId;
    this.commitLogOffset = commitLogOffset;
    this.msgSize = msgSize;
    this.tagsCode = tagsCode;
    this.storeTimestamp = storeTimestamp;
    this.consumeQueueOffset = consumeQueueOffset;
    this.keys = keys;
    this.uniqKey = uniqKey;

    this.sysFlag = sysFlag;
    this.preparedTransactionOffset = preparedTransactionOffset;
    this.success = true;
    this.propertiesMap = propertiesMap;
  }

  /**
   * 调度请求
   *
   * @param size 大小
   */
  public DispatchRequest(int size) {
    this.topic = "";
    this.queueId = 0;
    this.commitLogOffset = 0;
    this.msgSize = size;
    this.tagsCode = 0;
    this.storeTimestamp = 0;
    this.consumeQueueOffset = 0;
    this.keys = "";
    this.uniqKey = null;
    this.sysFlag = 0;
    this.preparedTransactionOffset = 0;
    this.success = false;
    this.propertiesMap = null;
  }

  /**
   * 调度请求
   *
   * @param size 大小
   * @param success 成功
   */
  public DispatchRequest(int size, boolean success) {
    this.topic = "";
    this.queueId = 0;
    this.commitLogOffset = 0;
    this.msgSize = size;
    this.tagsCode = 0;
    this.storeTimestamp = 0;
    this.consumeQueueOffset = 0;
    this.keys = "";
    this.uniqKey = null;
    this.sysFlag = 0;
    this.preparedTransactionOffset = 0;
    this.success = success;
    this.propertiesMap = null;
  }

  /**
   * 获取主题
   *
   * @return {@link String}
   */
  public String getTopic() {
    return topic;
  }

  /**
   * 获取队列id
   *
   * @return int
   */
  public int getQueueId() {
    return queueId;
  }

  /**
   * 获取提交日志偏移量
   *
   * @return long
   */
  public long getCommitLogOffset() {
    return commitLogOffset;
  }

  /**
   * 获取消息大小
   *
   * @return int
   */
  public int getMsgSize() {
    return msgSize;
  }

  /**
   * 获取存储时间戳
   *
   * @return long
   */
  public long getStoreTimestamp() {
    return storeTimestamp;
  }

  /**
   * 获取消费队列偏移量
   *
   * @return long
   */
  public long getConsumeQueueOffset() {
    return consumeQueueOffset;
  }

  /**
   * 获取密钥
   *
   * @return {@link String}
   */
  public String getKeys() {
    return keys;
  }

  /**
   * 获取标签代码
   *
   * @return long
   */
  public long getTagsCode() {
    return tagsCode;
  }

  /**
   * 获取sys标志
   *
   * @return int
   */
  public int getSysFlag() {
    return sysFlag;
  }

  /**
   * 获取准备好交易记录抵销
   *
   * @return long
   */
  public long getPreparedTransactionOffset() {
    return preparedTransactionOffset;
  }

  /**
   * 就是成功
   *
   * @return boolean
   */
  public boolean isSuccess() {
    return success;
  }

  /**
   * 获取uniq密钥
   *
   * @return {@link String}
   */
  public String getUniqKey() {
    return uniqKey;
  }

  /**
   * 获取属性映射
   *
   * @return {@link Map}<{@link String}, {@link String}>
   */
  public Map<String, String> getPropertiesMap() {
    return propertiesMap;
  }

  /**
   * 获取位图
   *
   * @return {@link byte[]}
   */
  public byte[] getBitMap() {
    return bitMap;
  }

  /**
   * 设置位图
   *
   * @param bitMap 位图
   */
  public void setBitMap(byte[] bitMap) {
    this.bitMap = bitMap;
  }

  /**
   * 设置消息大小
   *
   * @param msgSize 消息大小
   */
  public void setMsgSize(int msgSize) {
    this.msgSize = msgSize;
  }

  /**
   * 获取缓冲区大小
   *
   * @return int
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * 设置缓冲区大小
   *
   * @param bufferSize 缓冲区大小
   */
  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }
}
