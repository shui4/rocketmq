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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/** 消费队列 */
public class ConsumeQueue {
  /** 日志 */
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

  /** cq存储单元大小 */
  public static final int CQ_STORE_UNIT_SIZE = 20;
  /** 错误日志 */
  private static final InternalLogger LOG_ERROR =
      InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

  /** 默认消息存储 */
  private final DefaultMessageStore defaultMessageStore;

  /** 映射文件队列 */
  private final MappedFileQueue mappedFileQueue;
  /** 主题 */
  private final String topic;
  /** 队列id */
  private final int queueId;
  /** 字节缓冲区指数 */
  private final ByteBuffer byteBufferIndex;

  /** 存储路径 */
  private final String storePath;
  /** 映射文件大小 */
  private final int mappedFileSize;
  /** 最大物理抵消 */
  private long maxPhysicOffset = -1;
  /** 最小值逻辑抵消 */
  private volatile long minLogicOffset = 0;
  /** 使用队列ext */
  private ConsumeQueueExt consumeQueueExt = null;

  /**
   * 使用队列
   *
   * @param topic 主题
   * @param queueId 队列id
   * @param storePath 存储路径
   * @param mappedFileSize 映射文件大小
   * @param defaultMessageStore 默认消息存储
   */
  public ConsumeQueue(
      final String topic,
      final int queueId,
      final String storePath,
      final int mappedFileSize,
      final DefaultMessageStore defaultMessageStore) {
    this.storePath = storePath;
    this.mappedFileSize = mappedFileSize;
    this.defaultMessageStore = defaultMessageStore;

    this.topic = topic;
    this.queueId = queueId;

    String queueDir = this.storePath + File.separator + topic + File.separator + queueId;

    this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

    this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

    if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
      this.consumeQueueExt =
          new ConsumeQueueExt(
              topic,
              queueId,
              StorePathConfigHelper.getStorePathConsumeQueueExt(
                  defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
              defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
              defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt());
    }
  }

  /**
   * 负载
   *
   * @return boolean
   */
  public boolean load() {
    boolean result = this.mappedFileQueue.load();
    log.info(
        "load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
    if (isExtReadEnable()) {
      result &= this.consumeQueueExt.load();
    }
    return result;
  }

  /** 恢复 */
  public void recover() {
    final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
    if (!mappedFiles.isEmpty()) {

      int index = mappedFiles.size() - 3;
      if (index < 0) index = 0;

      int mappedFileSizeLogics = this.mappedFileSize;
      MappedFile mappedFile = mappedFiles.get(index);
      ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
      long processOffset = mappedFile.getFileFromOffset();
      long mappedFileOffset = 0;
      long maxExtAddr = 1;
      while (true) {
        for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
          long offset = byteBuffer.getLong();
          int size = byteBuffer.getInt();
          long tagsCode = byteBuffer.getLong();

          if (offset >= 0 && size > 0) {
            mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
            this.maxPhysicOffset = offset + size;
            if (isExtAddr(tagsCode)) {
              maxExtAddr = tagsCode;
            }
          } else {
            log.info(
                "recover current consume queue file over,  "
                    + mappedFile.getFileName()
                    + " "
                    + offset
                    + " "
                    + size
                    + " "
                    + tagsCode);
            break;
          }
        }

        if (mappedFileOffset == mappedFileSizeLogics) {
          index++;
          if (index >= mappedFiles.size()) {

            log.info(
                "recover last consume queue file over, last mapped file "
                    + mappedFile.getFileName());
            break;
          } else {
            mappedFile = mappedFiles.get(index);
            byteBuffer = mappedFile.sliceByteBuffer();
            processOffset = mappedFile.getFileFromOffset();
            mappedFileOffset = 0;
            log.info("recover next consume queue file, " + mappedFile.getFileName());
          }
        } else {
          log.info(
              "recover current consume queue queue over "
                  + mappedFile.getFileName()
                  + " "
                  + (processOffset + mappedFileOffset));
          break;
        }
      }

      processOffset += mappedFileOffset;
      this.mappedFileQueue.setFlushedWhere(processOffset);
      this.mappedFileQueue.setCommittedWhere(processOffset);
      this.mappedFileQueue.truncateDirtyFiles(processOffset);

      if (isExtReadEnable()) {
        this.consumeQueueExt.recover();
        log.info("Truncate consume queue extend file by max {}", maxExtAddr);
        this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
      }
    }
  }

  /**
   * 被时间抵消在队列
   *
   * @param timestamp 时间戳
   * @return long
   */
  public long getOffsetInQueueByTime(final long timestamp) {
    MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
    if (mappedFile != null) {
      long offset = 0;
      int low =
          minLogicOffset > mappedFile.getFileFromOffset()
              ? (int) (minLogicOffset - mappedFile.getFileFromOffset())
              : 0;
      int high = 0;
      int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
      long leftIndexValue = -1L, rightIndexValue = -1L;
      long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
      SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
      if (null != sbr) {
        ByteBuffer byteBuffer = sbr.getByteBuffer();
        high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
        try {
          while (high >= low) {
            midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
            byteBuffer.position(midOffset);
            long phyOffset = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            if (phyOffset < minPhysicOffset) {
              low = midOffset + CQ_STORE_UNIT_SIZE;
              leftOffset = midOffset;
              continue;
            }

            long storeTime =
                this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
            if (storeTime < 0) {
              return 0;
            } else if (storeTime == timestamp) {
              targetOffset = midOffset;
              break;
            } else if (storeTime > timestamp) {
              high = midOffset - CQ_STORE_UNIT_SIZE;
              rightOffset = midOffset;
              rightIndexValue = storeTime;
            } else {
              low = midOffset + CQ_STORE_UNIT_SIZE;
              leftOffset = midOffset;
              leftIndexValue = storeTime;
            }
          }

          if (targetOffset != -1) {

            offset = targetOffset;
          } else {
            if (leftIndexValue == -1) {

              offset = rightOffset;
            } else if (rightIndexValue == -1) {

              offset = leftOffset;
            } else {
              offset =
                  Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp - rightIndexValue)
                      ? rightOffset
                      : leftOffset;
            }
          }

          return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
        } finally {
          sbr.release();
        }
      }
    }
    return 0;
  }

  /**
   * 截断脏逻辑文件
   *
   * @param phyOffet phy offet
   */
  public void truncateDirtyLogicFiles(long phyOffet) {

    int logicFileSize = this.mappedFileSize;

    this.maxPhysicOffset = phyOffet;
    long maxExtAddr = 1;
    while (true) {
      MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
      if (mappedFile != null) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        mappedFile.setWrotePosition(0);
        mappedFile.setCommittedPosition(0);
        mappedFile.setFlushedPosition(0);

        for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
          long offset = byteBuffer.getLong();
          int size = byteBuffer.getInt();
          long tagsCode = byteBuffer.getLong();

          if (0 == i) {
            if (offset >= phyOffet) {
              this.mappedFileQueue.deleteLastMappedFile();
              break;
            } else {
              int pos = i + CQ_STORE_UNIT_SIZE;
              mappedFile.setWrotePosition(pos);
              mappedFile.setCommittedPosition(pos);
              mappedFile.setFlushedPosition(pos);
              this.maxPhysicOffset = offset + size;
              // This maybe not take effect, when not every consume queue has extend file.
              if (isExtAddr(tagsCode)) {
                maxExtAddr = tagsCode;
              }
            }
          } else {

            if (offset >= 0 && size > 0) {

              if (offset >= phyOffet) {
                return;
              }

              int pos = i + CQ_STORE_UNIT_SIZE;
              mappedFile.setWrotePosition(pos);
              mappedFile.setCommittedPosition(pos);
              mappedFile.setFlushedPosition(pos);
              this.maxPhysicOffset = offset + size;
              if (isExtAddr(tagsCode)) {
                maxExtAddr = tagsCode;
              }

              if (pos == logicFileSize) {
                return;
              }
            } else {
              return;
            }
          }
        }
      } else {
        break;
      }
    }

    if (isExtReadEnable()) {
      this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
    }
  }

  /**
   * 得到最后抵消
   *
   * @return long
   */
  public long getLastOffset() {
    long lastOffset = -1;

    int logicFileSize = this.mappedFileSize;

    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
    if (mappedFile != null) {

      int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
      if (position < 0) position = 0;

      ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
      byteBuffer.position(position);
      for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
        long offset = byteBuffer.getLong();
        int size = byteBuffer.getInt();
        byteBuffer.getLong();

        if (offset >= 0 && size > 0) {
          lastOffset = offset + size;
        } else {
          break;
        }
      }
    }

    return lastOffset;
  }

  /**
   * 冲洗
   *
   * @param flushLeastPages 冲洗至少页面
   * @return boolean
   */
  public boolean flush(final int flushLeastPages) {
    boolean result = this.mappedFileQueue.flush(flushLeastPages);
    if (isExtReadEnable()) {
      result = result & this.consumeQueueExt.flush(flushLeastPages);
    }

    return result;
  }

  /**
   * 删除过期文件
   *
   * @param offset 抵消
   * @return int
   */
  public int deleteExpiredFile(long offset) {
    int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
    this.correctMinOffset(offset);
    return cnt;
  }

  /**
   * 正确最小偏移量
   *
   * @param phyMinOffset phy最小偏移量
   */
  public void correctMinOffset(long phyMinOffset) {
    MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
    long minExtAddr = 1;
    if (mappedFile != null) {
      SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
      if (result != null) {
        try {
          for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
            long offsetPy = result.getByteBuffer().getLong();
            result.getByteBuffer().getInt();
            long tagsCode = result.getByteBuffer().getLong();

            if (offsetPy >= phyMinOffset) {
              this.minLogicOffset = mappedFile.getFileFromOffset() + i;
              log.info(
                  "Compute logical min offset: {}, topic: {}, queueId: {}",
                  this.getMinOffsetInQueue(),
                  this.topic,
                  this.queueId);
              // This maybe not take effect, when not every consume queue has extend file.
              if (isExtAddr(tagsCode)) {
                minExtAddr = tagsCode;
              }
              break;
            }
          }
        } catch (Exception e) {
          log.error("Exception thrown when correctMinOffset", e);
        } finally {
          result.release();
        }
      }
    }

    if (isExtReadEnable()) {
      this.consumeQueueExt.truncateByMinAddress(minExtAddr);
    }
  }

  /**
   * 分钟抵消在队列中
   *
   * @return long
   */
  public long getMinOffsetInQueue() {
    return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
  }

  /**
   * 把包装信息位置信息
   *
   * @param request 请求
   * @param multiQueue 多队列
   */
  public void putMessagePositionInfoWrapper(DispatchRequest request, boolean multiQueue) {
    final int maxRetries = 30;
    boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
    for (int i = 0; i < maxRetries && canWrite; i++) {
      long tagsCode = request.getTagsCode();
      if (isExtWriteEnable()) {
        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
        cqExtUnit.setFilterBitMap(request.getBitMap());
        cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
        cqExtUnit.setTagsCode(request.getTagsCode());

        long extAddr = this.consumeQueueExt.put(cqExtUnit);
        if (isExtAddr(extAddr)) {
          tagsCode = extAddr;
        } else {
          log.warn(
              "Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}",
              cqExtUnit,
              topic,
              queueId,
              request.getCommitLogOffset());
        }
      }
      boolean result =
          this.putMessagePositionInfo(
              request.getCommitLogOffset(),
              request.getMsgSize(),
              tagsCode,
              request.getConsumeQueueOffset());
      if (result) {
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE
            || this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
          this.defaultMessageStore
              .getStoreCheckpoint()
              .setPhysicMsgTimestamp(request.getStoreTimestamp());
        }
        this.defaultMessageStore
            .getStoreCheckpoint()
            .setLogicsMsgTimestamp(request.getStoreTimestamp());
        if (multiQueue) {
          multiDispatchLmqQueue(request, maxRetries);
        }
        return;
      } else {
        // XXX: warn and notify me
        log.warn(
            "[BUG]put commit log position info to "
                + topic
                + ":"
                + queueId
                + " "
                + request.getCommitLogOffset()
                + " failed, retry "
                + i
                + " times");

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          log.warn("", e);
        }
      }
    }

    // XXX: warn and notify me
    log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
    this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
  }

  /**
   * 多分派lmq队列
   *
   * @param request 请求
   * @param maxRetries 马克斯重试
   */
  private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
    Map<String, String> prop = request.getPropertiesMap();
    String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
    String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
    String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
    String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
    if (queues.length != queueOffsets.length) {
      log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
      return;
    }
    for (int i = 0; i < queues.length; i++) {
      String queueName = queues[i];
      long queueOffset = Long.parseLong(queueOffsets[i]);
      int queueId = request.getQueueId();
      if (this.defaultMessageStore.getMessageStoreConfig().isEnableLmq()
          && MixAll.isLmq(queueName)) {
        queueId = 0;
      }
      doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);
    }
    return;
  }

  /**
   * 做派遣lmq队列
   *
   * @param request 请求
   * @param maxRetries 马克斯重试
   * @param queueName 队列名称
   * @param queueOffset 队列抵消
   * @param queueId 队列id
   */
  private void doDispatchLmqQueue(
      DispatchRequest request, int maxRetries, String queueName, long queueOffset, int queueId) {
    ConsumeQueue cq = this.defaultMessageStore.findConsumeQueue(queueName, queueId);
    boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
    for (int i = 0; i < maxRetries && canWrite; i++) {
      boolean result =
          cq.putMessagePositionInfo(
              request.getCommitLogOffset(),
              request.getMsgSize(),
              request.getTagsCode(),
              queueOffset);
      if (result) {
        break;
      } else {
        log.warn(
            "[BUG]put commit log position info to "
                + queueName
                + ":"
                + queueId
                + " "
                + request.getCommitLogOffset()
                + " failed, retry "
                + i
                + " times");

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          log.warn("", e);
        }
      }
    }
  }

  /**
   * 把消息位置信息
   *
   * @param offset 抵消
   * @param size 大小
   * @param tagsCode 标签代码
   * @param cqOffset cq抵消
   * @return boolean
   */
  private boolean putMessagePositionInfo(
      final long offset, final int size, final long tagsCode, final long cqOffset) {

    if (offset + size <= this.maxPhysicOffset) {
      log.warn(
          "Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}",
          maxPhysicOffset,
          offset);
      return true;
    }

    this.byteBufferIndex.flip();
    this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
    this.byteBufferIndex.putLong(offset);
    this.byteBufferIndex.putInt(size);
    this.byteBufferIndex.putLong(tagsCode);

    final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
    if (mappedFile != null) {

      if (mappedFile.isFirstCreateInQueue()
          && cqOffset != 0
          && mappedFile.getWrotePosition() == 0) {
        this.minLogicOffset = expectLogicOffset;
        this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
        this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
        this.fillPreBlank(mappedFile, expectLogicOffset);
        log.info(
            "fill pre blank space "
                + mappedFile.getFileName()
                + " "
                + expectLogicOffset
                + " "
                + mappedFile.getWrotePosition());
      }

      if (cqOffset != 0) {
        long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

        if (expectLogicOffset < currentLogicOffset) {
          log.warn(
              "Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
              expectLogicOffset,
              currentLogicOffset,
              this.topic,
              this.queueId,
              expectLogicOffset - currentLogicOffset);
          return true;
        }

        if (expectLogicOffset != currentLogicOffset) {
          LOG_ERROR.warn(
              "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
              expectLogicOffset,
              currentLogicOffset,
              this.topic,
              this.queueId,
              expectLogicOffset - currentLogicOffset);
        }
      }
      this.maxPhysicOffset = offset + size;
      return mappedFile.appendMessage(this.byteBufferIndex.array());
    }
    return false;
  }

  /**
   * 填补之前空白
   *
   * @param mappedFile 映射文件
   * @param untilWhere 之前在哪里
   */
  private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
    byteBuffer.putLong(0L);
    byteBuffer.putInt(Integer.MAX_VALUE);
    byteBuffer.putLong(0L);

    int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
    for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
      mappedFile.appendMessage(byteBuffer.array());
    }
  }

  /**
   * 得到索引缓冲区
   *
   * @param startIndex 开始指数
   * @return {@link SelectMappedBufferResult}
   */
  public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
    int mappedFileSize = this.mappedFileSize;
    long offset = startIndex * CQ_STORE_UNIT_SIZE;
    if (offset >= this.getMinLogicOffset()) {
      MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
      if (mappedFile != null) {
        SelectMappedBufferResult result =
            mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
        return result;
      }
    }
    return null;
  }

  /**
   * 获取ext
   *
   * @param offset 抵消
   * @return {@link ConsumeQueueExt.CqExtUnit}
   */
  public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
    if (isExtReadEnable()) {
      return this.consumeQueueExt.get(offset);
    }
    return null;
  }

  /**
   * 获取ext
   *
   * @param offset 抵消
   * @param cqExtUnit cq ext单位
   * @return boolean
   */
  public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
    if (isExtReadEnable()) {
      return this.consumeQueueExt.get(offset, cqExtUnit);
    }
    return false;
  }

  /**
   * 得到最小值逻辑抵消
   *
   * @return long
   */
  public long getMinLogicOffset() {
    return minLogicOffset;
  }

  /**
   * 设置最小逻辑抵消
   *
   * @param minLogicOffset 最小值逻辑抵消
   */
  public void setMinLogicOffset(long minLogicOffset) {
    this.minLogicOffset = minLogicOffset;
  }

  /**
   * 滚下一个文件
   *
   * @param index 指数
   * @return long
   */
  public long rollNextFile(final long index) {
    int mappedFileSize = this.mappedFileSize;
    int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
    return index + totalUnitsInFile - index % totalUnitsInFile;
  }

  /**
   * 得到话题
   *
   * @return {@link String}
   */
  public String getTopic() {
    return topic;
  }

  /**
   * 获得队列id
   *
   * @return int
   */
  public int getQueueId() {
    return queueId;
  }

  /**
   * 得到最大物理抵消
   *
   * @return long
   */
  public long getMaxPhysicOffset() {
    return maxPhysicOffset;
  }

  /**
   * 设置最大物理抵消
   *
   * @param maxPhysicOffset 最大物理抵消
   */
  public void setMaxPhysicOffset(long maxPhysicOffset) {
    this.maxPhysicOffset = maxPhysicOffset;
  }

  /** 摧毁 */
  public void destroy() {
    this.maxPhysicOffset = -1;
    this.minLogicOffset = 0;
    this.mappedFileQueue.destroy();
    if (isExtReadEnable()) {
      this.consumeQueueExt.destroy();
    }
  }

  /**
   * 得到消息在队列中
   *
   * @return long
   */
  public long getMessageTotalInQueue() {
    return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
  }

  /**
   * 马克斯抵消在队列中
   *
   * @return long
   */
  public long getMaxOffsetInQueue() {
    return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
  }

  /** 自我检查 */
  public void checkSelf() {
    mappedFileQueue.checkSelf();
    if (isExtReadEnable()) {
      this.consumeQueueExt.checkSelf();
    }
  }

  /**
   * ext阅读使
   *
   * @return boolean
   */
  protected boolean isExtReadEnable() {
    return this.consumeQueueExt != null;
  }

  /**
   * ext写启用
   *
   * @return boolean
   */
  protected boolean isExtWriteEnable() {
    return this.consumeQueueExt != null
        && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
  }

  /**
   * 是ext addr Check {@code tagsCode} is address of extend file or tags code. @param tagsCode 标签代码
   *
   * @return boolean
   */
  public boolean isExtAddr(long tagsCode) {
    return ConsumeQueueExt.isExtAddr(tagsCode);
  }
}
