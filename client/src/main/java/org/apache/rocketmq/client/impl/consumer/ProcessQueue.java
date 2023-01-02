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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link ProcessQueue} 是 {@link MessageQueue} 在消费端的重现、快照。 PullMessageService 从消息服务器默认每次拉取 32
 * 条消息，按消息队 列偏移量的顺序存放在 ProcessQueue 中，PullMessageService 将消息 提交到消费者消费线程池，消息成功消费后，再从 ProcessQueue 中移除
 */
public class ProcessQueue {
  public static final long REBALANCE_LOCK_MAX_LIVE_TIME =
      Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
  public static final long REBALANCE_LOCK_INTERVAL =
      Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
  private static final long PULL_MAX_IDLE_TIME =
      Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
  private final InternalLogger log = ClientLogger.getLog();
  /** 读写锁，控制多线程并发修改 {@link #msgTreeMap} 、 msgTreeMapTemp */
  private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
  /**
   * 消息存储容器，键为消息在 ConsumeQueue 中的偏移量
   *
   * @see Long 偏移量
   * @see MessageExt 消息
   */
  private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
  /** ProcessQueue 中总消息数 */
  private final AtomicLong msgCount = new AtomicLong();

  private final AtomicLong msgSize = new AtomicLong();

  private final Lock consumeLock = new ReentrantLock();
  /**
   * msgTreeMap 的一个子集，仅在有序使用时使用 <br>
   * 消息临时存储容器，键为消息在 ConsumeQueue 中的偏移量。该结构用于处理顺序消息，消息消费线程。 从 ProcessQueue 的 {@link #msgTreeMap}
   * 中取出消息前，先将消息临时存储在这里
   */
  private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap =
      new TreeMap<Long, MessageExt>();

  private final AtomicLong tryUnlockTimes = new AtomicLong(0);
  /** 当前 ProcessQueue 中包含的 最大队列偏移量 */
  private volatile long queueOffsetMax = 0L;
  /** 当前 ProccesQueue 是否 被丢弃 */
  private volatile boolean dropped = false;
  /** 上一次开始拉取消息的 时间戳 */
  private volatile long lastPullTimestamp = System.currentTimeMillis();
  /** 上一次消费消息的时 间戳 */
  private volatile long lastConsumeTimestamp = System.currentTimeMillis();

  private volatile boolean locked = false;
  private volatile long lastLockTimestamp = System.currentTimeMillis();
  private volatile boolean consuming = false;
  private volatile long msgAccCnt = 0;

  /**
   * 判断锁是否过期，锁超时 时间默认为 30s，通过系统参数 rocketmq.broker.rebalance.lockMaxLiveTime 进行设置。{@link
   * #REBALANCE_LOCK_MAX_LIVE_TIME}
   */
  public boolean isLockExpired() {
    return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
  }

  /**
   * 判断 PullMessageService 是否空闲，空闲时间默认 120s，通过系统参数 rocketmq.client.pull.pullMaxIdleTime 进行设置。{@link
   * #PULL_MAX_IDLE_TIME}
   */
  public boolean isPullExpired() {
    return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
  }

  /**
   * 移除消费超时的消息，默认超过 15min 未消费的消息 将延迟 3 个延迟级别再消费
   *
   * @param pushConsumer
   */
  public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
    if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
      return;
    }

    int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
    for (int i = 0; i < loop; i++) {
      MessageExt msg = null;
      try {
        this.treeMapLock.readLock().lockInterruptibly();
        try {
          if (!msgTreeMap.isEmpty()) {
            String consumeStartTimeStamp =
                MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
            if (StringUtils.isNotEmpty(consumeStartTimeStamp)
                && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp)
                    > pushConsumer.getConsumeTimeout() * 60 * 1000) {
              msg = msgTreeMap.firstEntry().getValue();
            } else {
              break;
            }
          } else {
            break;
          }
        } finally {
          this.treeMapLock.readLock().unlock();
        }
      } catch (InterruptedException e) {
        log.error("getExpiredMsg exception", e);
      }

      try {

        pushConsumer.sendMessageBack(msg, 3);
        log.info(
            "send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}",
            msg.getTopic(),
            msg.getMsgId(),
            msg.getStoreHost(),
            msg.getQueueId(),
            msg.getQueueOffset());
        try {
          this.treeMapLock.writeLock().lockInterruptibly();
          try {
            if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
              try {
                removeMessage(Collections.singletonList(msg));
              } catch (Exception e) {
                log.error("send expired msg exception", e);
              }
            }
          } finally {
            this.treeMapLock.writeLock().unlock();
          }
        } catch (InterruptedException e) {
          log.error("getExpiredMsg exception", e);
        }
      } catch (Exception e) {
        log.error("send expired msg exception", e);
      }
    }
  }

  /** 移除消息 */
  public long removeMessage(final List<MessageExt> msgs) {
    long result = -1;
    final long now = System.currentTimeMillis();
    try {
      this.treeMapLock.writeLock().lockInterruptibly();
      this.lastConsumeTimestamp = now;
      try {
        if (!msgTreeMap.isEmpty()) {
          result = this.queueOffsetMax + 1;
          int removedCnt = 0;
          for (MessageExt msg : msgs) {
            MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
            if (prev != null) {
              removedCnt--;
              msgSize.addAndGet(0 - msg.getBody().length);
            }
          }
          msgCount.addAndGet(removedCnt);

          if (!msgTreeMap.isEmpty()) {
            result = msgTreeMap.firstKey();
          }
        }
      } finally {
        this.treeMapLock.writeLock().unlock();
      }
    } catch (Throwable t) {
      log.error("removeMessage exception", t);
    }

    return result;
  }

  /** 添加消息， PullMessageService 拉取消息后，调用该方法将消息添加到 ProcessQueue */
  public boolean putMessage(final List<MessageExt> msgs) {
    boolean dispatchToConsume = false;
    try {
      this.treeMapLock.writeLock().lockInterruptibly();
      try {
        int validMsgCnt = 0;
        for (MessageExt msg : msgs) {
          MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
          if (null == old) {
            validMsgCnt++;
            this.queueOffsetMax = msg.getQueueOffset();
            msgSize.addAndGet(msg.getBody().length);
          }
        }
        msgCount.addAndGet(validMsgCnt);

        if (!msgTreeMap.isEmpty() && !this.consuming) {
          dispatchToConsume = true;
          this.consuming = true;
        }

        if (!msgs.isEmpty()) {
          MessageExt messageExt = msgs.get(msgs.size() - 1);
          String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
          if (property != null) {
            long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
            if (accTotal > 0) {
              this.msgAccCnt = accTotal;
            }
          }
        }
      } finally {
        this.treeMapLock.writeLock().unlock();
      }
    } catch (InterruptedException e) {
      log.error("putMessage exception", e);
    }

    return dispatchToConsume;
  }

  /** 获取当前消息的最大间隔。 getMaxSpan() 并不能说明 ProceQueue 包含的消息个数，但是能说明当 前处理队列中第一条消息与最后一条消息的偏移量的差距。 */
  public long getMaxSpan() {
    try {
      this.treeMapLock.readLock().lockInterruptibly();
      try {
        if (!this.msgTreeMap.isEmpty()) {
          return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
        }
      } finally {
        this.treeMapLock.readLock().unlock();
      }
    } catch (InterruptedException e) {
      log.error("getMaxSpan exception", e);
    }

    return 0;
  }

  public TreeMap<Long, MessageExt> getMsgTreeMap() {
    return msgTreeMap;
  }

  public AtomicLong getMsgCount() {
    return msgCount;
  }

  public AtomicLong getMsgSize() {
    return msgSize;
  }

  public boolean isDropped() {
    return dropped;
  }

  public void setDropped(boolean dropped) {
    this.dropped = dropped;
  }

  public boolean isLocked() {
    return locked;
  }

  public void setLocked(boolean locked) {
    this.locked = locked;
  }

  /**
   * 将 {@link #consumingMsgOrderlyTreeMap} 中的所有消息重 新放入 {@link #msgTreeMap} 并清除 {@link
   * #consumingMsgOrderlyTreeMap}
   */
  public void rollback() {
    try {
      this.treeMapLock.writeLock().lockInterruptibly();
      try {
        this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
        this.consumingMsgOrderlyTreeMap.clear();
      } finally {
        this.treeMapLock.writeLock().unlock();
      }
    } catch (InterruptedException e) {
      log.error("rollback exception", e);
    }
  }

  /** 将 {@link #consumingMsgOrderlyTreeMap} 中的消息清除，表 示成功处理该批消息。 */
  public long commit() {
    // 提交就是将该批消息从 ProcessQueue 中移除，维护 msgCount（消
    // 息处理队列中的消息条数）并获取消息消费的偏移量 offset，然后将
    // 该批消息从 msgTreeMapTemp 中移除，并返回待保存的消息消费进度
    // （offset+1）。从中可以看出，offset 表示消息消费队列的逻辑偏移
    // 量，类似于数组的下标，代表第 n 个 ConsumeQueue 条目
    try {
      this.treeMapLock.writeLock().lockInterruptibly();
      try {
        Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
        msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
        for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
          msgSize.addAndGet(0 - msg.getBody().length);
        }
        this.consumingMsgOrderlyTreeMap.clear();
        if (offset != null) {
          return offset + 1;
        }
      } finally {
        this.treeMapLock.writeLock().unlock();
      }
    } catch (InterruptedException e) {
      log.error("commit exception", e);
    }

    return -1;
  }

  /** 重新 消费该批消息 */
  public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
    try {
      this.treeMapLock.writeLock().lockInterruptibly();
      try {
        for (MessageExt msg : msgs) {
          this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
          this.msgTreeMap.put(msg.getQueueOffset(), msg);
        }
      } finally {
        this.treeMapLock.writeLock().unlock();
      }
    } catch (InterruptedException e) {
      log.error("makeMessageToCosumeAgain exception", e);
    }
  }

  /**
   * 从 ProcessQueue 中取出 batchSize 条消息
   *
   * @param batchSize 批量大小
   */
  public List<MessageExt> takeMessages(final int batchSize) {
    List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
    final long now = System.currentTimeMillis();
    try {
      this.treeMapLock.writeLock().lockInterruptibly();
      this.lastConsumeTimestamp = now;
      try {
        if (!this.msgTreeMap.isEmpty()) {
          for (int i = 0; i < batchSize; i++) {
            Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
            if (entry != null) {
              result.add(entry.getValue());
              consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
            } else {
              break;
            }
          }
        }

        if (result.isEmpty()) {
          consuming = false;
        }
      } finally {
        this.treeMapLock.writeLock().unlock();
      }
    } catch (InterruptedException e) {
      log.error("take Messages exception", e);
    }

    return result;
  }

  public boolean hasTempMessage() {
    try {
      this.treeMapLock.readLock().lockInterruptibly();
      try {
        return !this.msgTreeMap.isEmpty();
      } finally {
        this.treeMapLock.readLock().unlock();
      }
    } catch (InterruptedException e) {
    }

    return true;
  }

  public void clear() {
    try {
      this.treeMapLock.writeLock().lockInterruptibly();
      try {
        this.msgTreeMap.clear();
        this.consumingMsgOrderlyTreeMap.clear();
        this.msgCount.set(0);
        this.msgSize.set(0);
        this.queueOffsetMax = 0L;
      } finally {
        this.treeMapLock.writeLock().unlock();
      }
    } catch (InterruptedException e) {
      log.error("rollback exception", e);
    }
  }

  public long getLastLockTimestamp() {
    return lastLockTimestamp;
  }

  public void setLastLockTimestamp(long lastLockTimestamp) {
    this.lastLockTimestamp = lastLockTimestamp;
  }

  public Lock getConsumeLock() {
    return consumeLock;
  }

  public long getLastPullTimestamp() {
    return lastPullTimestamp;
  }

  public void setLastPullTimestamp(long lastPullTimestamp) {
    this.lastPullTimestamp = lastPullTimestamp;
  }

  public long getMsgAccCnt() {
    return msgAccCnt;
  }

  public void setMsgAccCnt(long msgAccCnt) {
    this.msgAccCnt = msgAccCnt;
  }

  public long getTryUnlockTimes() {
    return this.tryUnlockTimes.get();
  }

  public void incTryUnlockTimes() {
    this.tryUnlockTimes.incrementAndGet();
  }

  public void fillProcessQueueInfo(final ProcessQueueInfo info) {
    try {
      this.treeMapLock.readLock().lockInterruptibly();

      if (!this.msgTreeMap.isEmpty()) {
        info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
        info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
        info.setCachedMsgCount(this.msgTreeMap.size());
        info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
      }

      if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
        info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
        info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
        info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
      }

      info.setLocked(this.locked);
      info.setTryUnlockTimes(this.tryUnlockTimes.get());
      info.setLastLockTimestamp(this.lastLockTimestamp);

      info.setDroped(this.dropped);
      info.setLastPullTimestamp(this.lastPullTimestamp);
      info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
    } catch (Exception e) {
    } finally {
      this.treeMapLock.readLock().unlock();
    }
  }

  public long getLastConsumeTimestamp() {
    return lastConsumeTimestamp;
  }

  public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
    this.lastConsumeTimestamp = lastConsumeTimestamp;
  }
}