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

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Store all metadata downtime for recovery, data protection reliability */
public class CommitLog {
  // Message's MAGIC CODE daa320a7
  public static final int MESSAGE_MAGIC_CODE = -626843481;
  protected static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
  // End of file empty MAGIC CODE cbd43194
  protected static final int BLANK_MAGIC_CODE = -875286124;
  protected final MappedFileQueue mappedFileQueue;
  protected final DefaultMessageStore defaultMessageStore;
  protected final PutMessageLock putMessageLock;
  private final FlushCommitLogService flushCommitLogService;
  // If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
  private final FlushCommitLogService commitLogService;
  private final AppendMessageCallback appendMessageCallback;
  private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;
  private final MultiDispatch multiDispatch;
  private final FlushDiskWatcher flushDiskWatcher;
  protected HashMap<String /* topic-queueid */, Long /* offset */> topicQueueTable =
      new HashMap<String, Long>(1024);
  protected Map<String /* topic-queueid */, Long /* offset */> lmqTopicQueueTable =
      new ConcurrentHashMap<>(1024);
  protected volatile long confirmOffset = -1L;
  private volatile long beginTimeInLock = 0;
  private volatile Set<String> fullStorePaths = Collections.emptySet();

  public CommitLog(final DefaultMessageStore defaultMessageStore) {
    String storePath = defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog();
    if (storePath.contains(MessageStoreConfig.MULTI_PATH_SPLITTER)) {
      this.mappedFileQueue =
          new MultiPathMappedFileQueue(
              defaultMessageStore.getMessageStoreConfig(),
              defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
              defaultMessageStore.getAllocateMappedFileService(),
              this::getFullStorePaths);
    } else {
      this.mappedFileQueue =
          new MappedFileQueue(
              storePath,
              defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
              defaultMessageStore.getAllocateMappedFileService());
    }

    this.defaultMessageStore = defaultMessageStore;

    if (FlushDiskType.SYNC_FLUSH
        == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
      this.flushCommitLogService = new GroupCommitService();
    } else {
      this.flushCommitLogService = new FlushRealTimeService();
    }

    this.commitLogService = new CommitRealTimeService();

    this.appendMessageCallback =
        new DefaultAppendMessageCallback(
            defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
    putMessageThreadLocal =
        new ThreadLocal<PutMessageThreadLocal>() {
          @Override
          protected PutMessageThreadLocal initialValue() {
            return new PutMessageThreadLocal(
                defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
          }
        };
    this.putMessageLock =
        defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage()
            ? new PutMessageReentrantLock()
            : new PutMessageSpinLock();

    this.multiDispatch = new MultiDispatch(defaultMessageStore, this);

    flushDiskWatcher = new FlushDiskWatcher();
  }

  public Set<String> getFullStorePaths() {
    return fullStorePaths;
  }

  public void setFullStorePaths(Set<String> fullStorePaths) {
    this.fullStorePaths = fullStorePaths;
  }

  public ThreadLocal<PutMessageThreadLocal> getPutMessageThreadLocal() {
    return putMessageThreadLocal;
  }

  public boolean load() {
    boolean result = this.mappedFileQueue.load();
    log.info("load commit log " + (result ? "OK" : "Failed"));
    return result;
  }

  public void start() {
    this.flushCommitLogService.start();

    flushDiskWatcher.setDaemon(true);
    flushDiskWatcher.start();

    if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
      this.commitLogService.start();
    }
  }

  public void shutdown() {
    if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
      this.commitLogService.shutdown();
    }

    this.flushCommitLogService.shutdown();

    flushDiskWatcher.shutdown(true);
  }

  public long flush() {
    this.mappedFileQueue.commit(0);
    this.mappedFileQueue.flush(0);
    return this.mappedFileQueue.getFlushedWhere();
  }

  public long getMaxOffset() {
    return this.mappedFileQueue.getMaxOffset();
  }

  public long remainHowManyDataToCommit() {
    return this.mappedFileQueue.remainHowManyDataToCommit();
  }

  public long remainHowManyDataToFlush() {
    return this.mappedFileQueue.remainHowManyDataToFlush();
  }

  public int deleteExpiredFile(
      final long expiredTime,
      final int deleteFilesInterval,
      final long intervalForcibly,
      final boolean cleanImmediately) {
    return this.mappedFileQueue.deleteExpiredFileByTime(
        expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
  }

  /** Read CommitLog data, use data replication */
  public SelectMappedBufferResult getData(final long offset) {
    return this.getData(offset, offset == 0);
  }

  public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
    int mappedFileSize =
        this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    MappedFile mappedFile =
        this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
    if (mappedFile != null) {
      int pos = (int) (offset % mappedFileSize);
      SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
      return result;
    }

    return null;
  }

  /**
   * 正常退出时，数据恢复，所有内存数据都已flush
   *
   * @param maxPhyOffsetOfConsumeQueue 消耗队列最大phy偏移
   */
  public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
    // 代码清单4-65
    // ? 在进行文件恢复时查找消息是否验证CRC
    boolean checkCRCOnRecover =
        this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
    final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
    if (!mappedFiles.isEmpty()) {
      // Began to recover from the last third file
      // Broker正常停止再重启时，从倒数第3个文件开始恢复，如果不足3个文件，则从第一个文件开始恢复
      int index = mappedFiles.size() - 3;
      if (index < 0) index = 0;
      // 代码清单 4-66
      MappedFile mappedFile = mappedFiles.get(index);
      ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
      // CommitLog文件已确认的物理偏移量
      long processOffset = mappedFile.getFileFromOffset();
      // 当前文件已校验通过的物理偏移量（最后一个文件最后一个消息的偏移量）
      long mappedFileOffset = 0;
      // 遍历 mappedFiles中的CommitLog
      while (true) {
        // 验证消息是否正常
        DispatchRequest dispatchRequest =
            this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
        // 消息大小为0
        int size = dispatchRequest.getMsgSize();
        // Normal data
        // 正常 并且 消息大小大于 0
        if (dispatchRequest.isSuccess() && size > 0) {
          // mappedFileOffset指针向前移动本条消息的长度
          mappedFileOffset += size;
        }
        // 来到文件末尾，切换到下一个文件由于返回0代表遇到了最后一个洞，
        // 这不能包含在截断偏移中
        else if (dispatchRequest.isSuccess() && size == 0) {
          index++;
          if (index >= mappedFiles.size()) {
            // Current branch can not happen
            log.info(
                "recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
            break;
          }
          // 如果还有下一个文件，则重置 processOffset、mappedFileOffset,循环
          else {
            mappedFile = mappedFiles.get(index);
            byteBuffer = mappedFile.sliceByteBuffer();
            processOffset = mappedFile.getFileFromOffset();
            mappedFileOffset = 0;
            log.info("recover next physics file, " + mappedFile.getFileName());
          }
        }
        // Intermediate file read error
        // 如果查找结果为 false ，表明该文件未填满所有消息，则跳出循环，结束遍历文件
        // 会应用前面一个成功消息的指针，后面这个失败的消息将被后续生产者发送的消息覆盖
        else if (!dispatchRequest.isSuccess()) {
          log.info("recover physics file end, " + mappedFile.getFileName());
          break;
        }
      }
      // 最后一个文件+最后一个文件最后一个消息的偏移量
      processOffset += mappedFileOffset;
      // 更新 MappedFileQueue 的 flushedWhere 和 committedPosition 指针
      this.mappedFileQueue.setFlushedWhere(processOffset);
      this.mappedFileQueue.setCommittedWhere(processOffset);
      this.mappedFileQueue.truncateDirtyFiles(processOffset);
      // ? ConsumeQueue 中条目中所说的CommitLog最大偏移量比实际最大偏移量（processOffset）还大
      // * 清除 ConsumeQueue 冗余数据
      if (maxPhyOffsetOfConsumeQueue >= processOffset) {
        log.warn(
            "maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files",
            maxPhyOffsetOfConsumeQueue,
            processOffset);
        // 截断脏逻辑文件，在 processOffset 之后的
        this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
      }
    }
    // * 没有CommitLog文件
    else {
      // Commitlog case files are deleted
      log.warn("The commitlog files are deleted, and delete the consume queue files");
      this.mappedFileQueue.setFlushedWhere(0);
      this.mappedFileQueue.setCommittedWhere(0);
      // 清理 ConsumeQueue表，以及其中的所有脏文件
      this.defaultMessageStore.destroyLogics();
    }
  }

  public DispatchRequest checkMessageAndReturnSize(
      java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
    return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
  }

  /**
   * check the message and returns the message size
   *
   * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
   */
  public DispatchRequest checkMessageAndReturnSize(
      java.nio.ByteBuffer byteBuffer, final boolean checkCRC, final boolean readBody) {
    try {
      // 1 TOTAL SIZE
      int totalSize = byteBuffer.getInt();

      // 2 MAGIC CODE
      int magicCode = byteBuffer.getInt();
      switch (magicCode) {
        case MESSAGE_MAGIC_CODE:
          break;
        case BLANK_MAGIC_CODE:
          return new DispatchRequest(0, true /* success */);
        default:
          log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
          return new DispatchRequest(-1, false /* success */);
      }

      byte[] bytesContent = new byte[totalSize];

      int bodyCRC = byteBuffer.getInt();

      int queueId = byteBuffer.getInt();

      int flag = byteBuffer.getInt();

      long queueOffset = byteBuffer.getLong();

      long physicOffset = byteBuffer.getLong();

      int sysFlag = byteBuffer.getInt();

      long bornTimeStamp = byteBuffer.getLong();

      ByteBuffer byteBuffer1;
      if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
        byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
      } else {
        byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
      }

      long storeTimestamp = byteBuffer.getLong();

      ByteBuffer byteBuffer2;
      if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
        byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
      } else {
        byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
      }

      int reconsumeTimes = byteBuffer.getInt();

      long preparedTransactionOffset = byteBuffer.getLong();

      int bodyLen = byteBuffer.getInt();
      if (bodyLen > 0) {
        if (readBody) {
          byteBuffer.get(bytesContent, 0, bodyLen);

          if (checkCRC) {
            int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
            if (crc != bodyCRC) {
              log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
              return new DispatchRequest(-1, false /* success */);
            }
          }
        } else {
          byteBuffer.position(byteBuffer.position() + bodyLen);
        }
      }

      byte topicLen = byteBuffer.get();
      byteBuffer.get(bytesContent, 0, topicLen);
      String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

      long tagsCode = 0;
      String keys = "";
      String uniqKey = null;

      short propertiesLength = byteBuffer.getShort();
      Map<String, String> propertiesMap = null;
      if (propertiesLength > 0) {
        byteBuffer.get(bytesContent, 0, propertiesLength);
        String properties =
            new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
        propertiesMap = MessageDecoder.string2messageProperties(properties);

        keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

        uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

        String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
        if (tags != null && tags.length() > 0) {
          tagsCode =
              MessageExtBrokerInner.tagsString2tagsCode(
                  MessageExt.parseTopicFilterType(sysFlag), tags);
        }

        // Timing message processing
        {
          String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
          if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
            int delayLevel = Integer.parseInt(t);

            if (delayLevel
                > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
              delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
            }

            if (delayLevel > 0) {
              tagsCode =
                  this.defaultMessageStore
                      .getScheduleMessageService()
                      .computeDeliverTimestamp(delayLevel, storeTimestamp);
            }
          }
        }
      }

      int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
      if (totalSize != readLength) {
        doNothingForDeadCode(reconsumeTimes);
        doNothingForDeadCode(flag);
        doNothingForDeadCode(bornTimeStamp);
        doNothingForDeadCode(byteBuffer1);
        doNothingForDeadCode(byteBuffer2);
        log.error(
            "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
            totalSize,
            readLength,
            bodyLen,
            topicLen,
            propertiesLength);
        return new DispatchRequest(totalSize, false /* success */);
      }

      return new DispatchRequest(
          topic,
          queueId,
          physicOffset,
          totalSize,
          tagsCode,
          storeTimestamp,
          queueOffset,
          keys,
          uniqKey,
          sysFlag,
          preparedTransactionOffset,
          propertiesMap);
    } catch (Exception e) {
    }

    return new DispatchRequest(-1, false /* success */);
  }

  // 代码清单4-6
  // 获取该消息在消息队列的物理偏移量
  protected static int calMsgLength(
      int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
    int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
    final int msgLen =
        4 // TOTALSIZE
            + 4 // MAGICCODE
            + 4 // BODYCRC
            + 4 // QUEUEID
            + 4 // FLAG
            + 8 // QUEUEOFFSET
            + 8 // PHYSICALOFFSET
            + 4 // SYSFLAG
            + 8 // BORNTIMESTAMP
            + bornhostLength // BORNHOST
            + 8 // STORETIMESTAMP
            + storehostAddressLength // STOREHOSTADDRESS
            + 4 // RECONSUMETIMES
            + 8 // Prepared Transaction Offset
            + 4
            + (bodyLength > 0 ? bodyLength : 0) // BODY
            + 1
            + topicLength // TOPIC
            + 2
            + (propertiesLength > 0 ? propertiesLength : 0) // propertiesLength
            + 0;
    return msgLen;
  }

  private void doNothingForDeadCode(final Object obj) {
    if (obj != null) {
      log.debug(String.valueOf(obj.hashCode()));
    }
  }

  public long getConfirmOffset() {
    return this.confirmOffset;
  }

  public void setConfirmOffset(long phyOffset) {
    this.confirmOffset = phyOffset;
  }

  /**
   * Broker 异常停止文件恢复 <br>
   * 异常文件恢复与正常停止文件恢复的 步骤基本相同，主要差别有两个：首先，Broker正常停止默认从倒数 第三个文件开始恢复，而异常停止则需要从最后一个文件倒序推进，
   * 找到第一个消息存储正常的文件；其次，如果CommitLog目录没有消息 文件，在ConsumeQueue目录下存在的文件则需要销毁。
   *
   * @param maxPhyOffsetOfConsumeQueue 消费队列的最大物理偏移
   */
  @Deprecated
  public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
    // 按最小时间戳恢复
    boolean checkCRCOnRecover =
        this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
    final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
    if (!mappedFiles.isEmpty()) {
      int index = mappedFiles.size() - 1;
      MappedFile mappedFile = null;
      // * 遍历 mappedFileQueue 中的 映射文件
      // 寻找开始从哪个文件中恢复，到倒数第一个循环，直到找到可靠的
      for (; index >= 0; index--) {
        mappedFile = mappedFiles.get(index);
        // ? 消息文件是否正常
        // Y 结束循环
        if (this.isMappedFileMatchedRecover(mappedFile)) {
          log.info("recover from this mapped file " + mappedFile.getFileName());
          break;
        }
      }
      // ? index 小于 0
      if (index < 0) {
        index = 0;
        mappedFile = mappedFiles.get(index);
      }

      ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
      long processOffset = mappedFile.getFileFromOffset();
      long mappedFileOffset = 0;
      while (true) {
        DispatchRequest dispatchRequest =
            this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
        int size = dispatchRequest.getMsgSize();

        if (dispatchRequest.isSuccess()) {
          // Normal data
          if (size > 0) {
            mappedFileOffset += size;
            // 转发到 Index 、 ConsumeQueue 文件
            if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
              if (dispatchRequest.getCommitLogOffset()
                  < this.defaultMessageStore.getConfirmOffset()) {
                this.defaultMessageStore.doDispatch(dispatchRequest);
              }
            } else {
              this.defaultMessageStore.doDispatch(dispatchRequest);
            }
          }
          // 来到文件末尾，切换到下一个文件由于返回0代表遇到了最后一个 hole，这不能包含在截断偏移中
          else if (size == 0) {
            index++;
            if (index >= mappedFiles.size()) {
              // The current branch under normal circumstances should
              // not happen
              log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
              break;
            } else {
              mappedFile = mappedFiles.get(index);
              byteBuffer = mappedFile.sliceByteBuffer();
              processOffset = mappedFile.getFileFromOffset();
              mappedFileOffset = 0;
              log.info("recover next physics file, " + mappedFile.getFileName());
            }
          }
        }
        // * 未找到有效文件
        else {
          log.info(
              "recover physics file end, "
                  + mappedFile.getFileName()
                  + " pos="
                  + byteBuffer.position());
          break;
        }
      }

      processOffset += mappedFileOffset;
      this.mappedFileQueue.setFlushedWhere(processOffset);
      this.mappedFileQueue.setCommittedWhere(processOffset);
      // 截断，processOffset为可靠偏移量，把它之后的截断掉
      this.mappedFileQueue.truncateDirtyFiles(processOffset);
      //  ? 消费队列的最大偏移量超过 commitLog 可靠偏移量（ processOffset ）
      // * 清除 ConsumeQueue 冗余数据
      if (maxPhyOffsetOfConsumeQueue >= processOffset) {
        log.warn(
            "maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files",
            maxPhyOffsetOfConsumeQueue,
            processOffset);
        // 截断 ConsumeQueue
        this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
      }
    }
    // ? 未找到有效的MappedFile
    // Commitlog case files are deleted
    // * 设置CommitLog目录的flushedWhere、committedWhere指针都为0，并销毁ConsumeQueue文件
    else {
      log.warn("The commitlog files are deleted, and delete the consume queue files");
      this.mappedFileQueue.setFlushedWhere(0);
      this.mappedFileQueue.setCommittedWhere(0);
      this.defaultMessageStore.destroyLogics();
    }
  }

  private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
    ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

    int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
    // ? 不符合魔术CODE
    if (magicCode != MESSAGE_MAGIC_CODE) {
      return false;
    }

    int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
    long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
    // ? 文件中第一条消息的存储时间等于 0
    if (0 == storeTimestamp) {
      return false;
    }
    // checkpoint 文件中保存了 CommitLog 、 ConsumeQueue 、 Index 的文件刷盘点， RocketMQ 默认选择 CommitLog 文 件与
    // ConsumeQueue 这两个文件的刷盘点中较小值与 CommitLog 文件第一
    // 条消息的时间戳做对比，
    // ?  如果 messageIndexEnable 为 true ，表示 Index 文件的刷盘时间点也参与计算对比文件第一条消息的时间戳与检测点
    if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
        && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
      // ? 文件第一条消息的时间戳小于文件检测点
      // Y 说明该文件的部分消息是可靠的，则从该文件开始恢复
      if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
        log.info(
            "find check timestamp, {} {}",
            storeTimestamp,
            UtilAll.timeMillisToHumanString(storeTimestamp));
        return true;
      }
    } else {
      if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
        log.info(
            "find check timestamp, {} {}",
            storeTimestamp,
            UtilAll.timeMillisToHumanString(storeTimestamp));
        return true;
      }
    }

    return false;
  }

  private void notifyMessageArriving() {}

  public boolean resetOffset(long offset) {
    return this.mappedFileQueue.resetOffset(offset);
  }

  public long getBeginTimeInLock() {
    return beginTimeInLock;
  }

  public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
    // Set the storage time
    // 设置存储时间
    msg.setStoreTimestamp(System.currentTimeMillis());
    // Set the message body BODY CRC (consider the most appropriate setting
    // on the client)
    // 设置消息体 BODY CRC（在客户端考虑最合适的设置） 。这是个啥东西？─https://zhuanlan.zhihu.com/p/38411551
    msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
    // Back to Results
    AppendMessageResult result = null;

    StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

    String topic = msg.getTopic();
    //        int queueId msg.getQueueId();
    // region 事物相关
    final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
    if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
        || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
      // Delay Delivery
      if (msg.getDelayTimeLevel() > 0) {
        if (msg.getDelayTimeLevel()
            > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
          msg.setDelayTimeLevel(
              this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
        }

        topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
        int queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

        // Backup real topic, queueId
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
        MessageAccessor.putProperty(
            msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        msg.setTopic(topic);
        msg.setQueueId(queueId);
      }
    }
    // endregion

    InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
    if (bornSocketAddress.getAddress() instanceof Inet6Address) {
      msg.setBornHostV6Flag();
    }

    InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
    if (storeSocketAddress.getAddress() instanceof Inet6Address) {
      msg.setStoreHostAddressV6Flag();
    }
    // putMessageThreadLocal 根据配置信息中的消息最大值，创建一个ByteBuffer，以及StringBuilder
    PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
    // 对消息进行编码
    PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
    // ? 未成功 完成编码（因属性、消息长度超过限制）
    if (encodeResult != null) {
      return CompletableFuture.completedFuture(encodeResult);
    }
    // 将前面编码的 设置到属性上
    msg.setEncodedBuff(putMessageThreadLocal.getEncoder().encoderBuffer);
    // Put消息上线文
    PutMessageContext putMessageContext =
        new PutMessageContext(generateKey(putMessageThreadLocal.getKeyBuilder(), msg));

    long elapsedTimeInLock = 0;
    MappedFile unlockMappedFile = null;
    // 上锁（自旋锁、可重入锁(默认)）
    putMessageLock.lock(); // spin or ReentrantLock ,depending on store config
    try {
      // 代码清单4-1
      MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
      long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
      this.beginTimeInLock = beginLockTimestamp;
      // 代码清单4-2
      // Here settings are stored timestamp, in order to ensure an orderly
      // global
      // 这里设置保存时间戳，以保证全局有序
      msg.setStoreTimestamp(beginLockTimestamp);
      //  如果为空，则从偏移量0中获取一个文件
      if (null == mappedFile || mappedFile.isFull()) {
        mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
      }
      // 还获取不到，可能是磁盘空间主足或者权限不够
      if (null == mappedFile) {
        log.error(
            "create mapped file1 error, topic: "
                + msg.getTopic()
                + " clientAddr: "
                + msg.getBornHostString());
        return CompletableFuture.completedFuture(
            new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
      }
      // 将消息追加到MappedFile中
      result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
      switch (result.getStatus()) {
          // 追加成功
        case PUT_OK:
          break;
          // 超过文件大小
        case END_OF_FILE:
          unlockMappedFile = mappedFile;
          // Create a new file, re-write the message
          // 新建文件。
          mappedFile = this.mappedFileQueue.getLastMappedFile(0);
          // 创建失败？磁盘空间不足、没有权限创建
          if (null == mappedFile) {
            // XXX: warn and notify me
            log.error(
                "create mapped file2 error, topic: "
                    + msg.getTopic()
                    + " clientAddr: "
                    + msg.getBornHostString());
            return CompletableFuture.completedFuture(
                new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
          }
          result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
          break;
        case MESSAGE_SIZE_EXCEEDED:
        case PROPERTIES_SIZE_EXCEEDED:
          return CompletableFuture.completedFuture(
              new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
        case UNKNOWN_ERROR:
          return CompletableFuture.completedFuture(
              new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
        default:
          return CompletableFuture.completedFuture(
              new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
      }

      elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
    } finally {
      beginTimeInLock = 0;
      putMessageLock.unlock();
    }

    if (elapsedTimeInLock > 500) {
      log.warn(
          "[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
          elapsedTimeInLock,
          msg.getBody().length,
          result);
    }

    if (null != unlockMappedFile
        && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
      this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
    }

    PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

    // Statistics
    storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(1);
    storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());
    // 同步消息？异步消息？
    CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result, msg);
    // 同步复制？异步复制
    CompletableFuture<PutMessageStatus> replicaResultFuture = submitReplicaRequest(result, msg);
    return flushResultFuture.thenCombine(
        replicaResultFuture,
        (flushStatus, replicaStatus) -> {
          if (flushStatus != PutMessageStatus.PUT_OK) {
            putMessageResult.setPutMessageStatus(flushStatus);
          }
          if (replicaStatus != PutMessageStatus.PUT_OK) {
            putMessageResult.setPutMessageStatus(replicaStatus);
          }
          return putMessageResult;
        });
  }

  /** 代码清单4-5 */
  private String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
    keyBuilder.setLength(0);
    keyBuilder.append(messageExt.getTopic());
    keyBuilder.append('-');
    keyBuilder.append(messageExt.getQueueId());
    return keyBuilder.toString();
  }

  public CompletableFuture<PutMessageStatus> submitFlushRequest(
      AppendMessageResult result, MessageExt messageExt) {
    // ? 同步刷新
    if (FlushDiskType.SYNC_FLUSH
        == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
      final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
      if (messageExt.isWaitStoreMsgOK()) {
        GroupCommitRequest request =
            new GroupCommitRequest(
                result.getWroteOffset() + result.getWroteBytes(),
                this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
        flushDiskWatcher.add(request);
        service.putRequest(request);
        return request.future();
      } else {
        service.wakeup();
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
      }
    }
    // 异步刷新
    else {
      if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
        flushCommitLogService.wakeup();
      } else {
        commitLogService.wakeup();
      }
      return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }
  }

  public CompletableFuture<PutMessageStatus> submitReplicaRequest(
      AppendMessageResult result, MessageExt messageExt) {
    // ? 同步复制
    if (BrokerRole.SYNC_MASTER
        == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
      HAService service = this.defaultMessageStore.getHaService();
      if (messageExt.isWaitStoreMsgOK()) {
        if (service.isSlaveOK(result.getWroteBytes() + result.getWroteOffset())) {
          GroupCommitRequest request =
              new GroupCommitRequest(
                  result.getWroteOffset() + result.getWroteBytes(),
                  this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout());
          service.putRequest(request);
          service.getWaitNotifyObject().wakeupAll();
          return request.future();
        } else {
          return CompletableFuture.completedFuture(PutMessageStatus.SLAVE_NOT_AVAILABLE);
        }
      }
    }
    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
  }

  public CompletableFuture<PutMessageResult> asyncPutMessages(
      final MessageExtBatch messageExtBatch) {
    messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
    AppendMessageResult result;

    StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

    final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

    if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
      return CompletableFuture.completedFuture(
          new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
    }
    if (messageExtBatch.getDelayTimeLevel() > 0) {
      return CompletableFuture.completedFuture(
          new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
    }

    InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
    if (bornSocketAddress.getAddress() instanceof Inet6Address) {
      messageExtBatch.setBornHostV6Flag();
    }

    InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
    if (storeSocketAddress.getAddress() instanceof Inet6Address) {
      messageExtBatch.setStoreHostAddressV6Flag();
    }

    long elapsedTimeInLock = 0;
    MappedFile unlockMappedFile = null;
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

    // fine-grained lock instead of the coarse-grained
    PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
    MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

    PutMessageContext putMessageContext =
        new PutMessageContext(generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch));
    messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

    putMessageLock.lock();
    try {
      long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
      this.beginTimeInLock = beginLockTimestamp;

      // Here settings are stored timestamp, in order to ensure an orderly
      // global
      messageExtBatch.setStoreTimestamp(beginLockTimestamp);

      if (null == mappedFile || mappedFile.isFull()) {
        mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
      }
      if (null == mappedFile) {
        log.error(
            "Create mapped file1 error, topic: {} clientAddr: {}",
            messageExtBatch.getTopic(),
            messageExtBatch.getBornHostString());
        return CompletableFuture.completedFuture(
            new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
      }

      result =
          mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
      switch (result.getStatus()) {
        case PUT_OK:
          break;
        case END_OF_FILE:
          unlockMappedFile = mappedFile;
          // Create a new file, re-write the message
          mappedFile = this.mappedFileQueue.getLastMappedFile(0);
          if (null == mappedFile) {
            // XXX: warn and notify me
            log.error(
                "Create mapped file2 error, topic: {} clientAddr: {}",
                messageExtBatch.getTopic(),
                messageExtBatch.getBornHostString());
            return CompletableFuture.completedFuture(
                new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
          }
          result =
              mappedFile.appendMessages(
                  messageExtBatch, this.appendMessageCallback, putMessageContext);
          break;
        case MESSAGE_SIZE_EXCEEDED:
        case PROPERTIES_SIZE_EXCEEDED:
          return CompletableFuture.completedFuture(
              new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
        case UNKNOWN_ERROR:
        default:
          return CompletableFuture.completedFuture(
              new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
      }

      elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
    } finally {
      beginTimeInLock = 0;
      putMessageLock.unlock();
    }

    if (elapsedTimeInLock > 500) {
      log.warn(
          "[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
          elapsedTimeInLock,
          messageExtBatch.getBody().length,
          result);
    }

    if (null != unlockMappedFile
        && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
      this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
    }

    PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

    // Statistics
    storeStatsService
        .getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic())
        .add(result.getMsgNum());
    storeStatsService
        .getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic())
        .add(result.getWroteBytes());

    CompletableFuture<PutMessageStatus> flushOKFuture = submitFlushRequest(result, messageExtBatch);
    CompletableFuture<PutMessageStatus> replicaOKFuture =
        submitReplicaRequest(result, messageExtBatch);
    return flushOKFuture.thenCombine(
        replicaOKFuture,
        (flushStatus, replicaStatus) -> {
          if (flushStatus != PutMessageStatus.PUT_OK) {
            putMessageResult.setPutMessageStatus(flushStatus);
          }
          if (replicaStatus != PutMessageStatus.PUT_OK) {
            putMessageResult.setPutMessageStatus(replicaStatus);
          }
          return putMessageResult;
        });
  }

  /** 如果发生错误，根据接收到的某个消息或偏移存储时间，返回-1 */
  public long pickupStoreTimestamp(final long offset, final int size) {
    if (offset >= this.getMinOffset()) {
      SelectMappedBufferResult result = this.getMessage(offset, size);
      if (null != result) {
        try {
          int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
          int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
          int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
          return result.getByteBuffer().getLong(msgStoreTimePos);
        } finally {
          result.release();
        }
      }
    }

    return -1;
  }

  /**
   * 代码清单4-30
   *
   * @return 最小偏移量
   */
  public long getMinOffset() {
    // 从中获取第一个
    MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
    if (mappedFile != null) {
      // ? 可用
      if (mappedFile.isAvailable()) {
        return mappedFile.getFileFromOffset();
      }
      // 不可用，获取下一个文件的起始偏移量
      else {
        return this.rollNextFile(mappedFile.getFileFromOffset());
      }
    }

    return -1;
  }

  /**
   * @param offset 偏移量
   * @param size 大小 （限制消息的大小）
   * @return 获取消息
   */
  public SelectMappedBufferResult getMessage(final long offset, final int size) {
    int mappedFileSize =
        this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
    if (mappedFile != null) {
      // 在 MappedFile 中的偏移量
      int pos = (int) (offset % mappedFileSize);
      return mappedFile.selectMappedBuffer(pos, size);
    }
    return null;
  }

  /**
   * 代码清单4-31
   *
   * @param offset 上一个文件的偏移量
   * @return 获取下一个文件的偏移量
   */
  public long rollNextFile(final long offset) {
    int mappedFileSize =
        this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    return offset + mappedFileSize - offset % mappedFileSize;
  }

  public HashMap<String, Long> getTopicQueueTable() {
    return topicQueueTable;
  }

  public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
    this.topicQueueTable = topicQueueTable;
  }

  public void destroy() {
    this.mappedFileQueue.destroy();
  }

  public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
    putMessageLock.lock();
    try {
      MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
      if (null == mappedFile) {
        log.error("appendData getLastMappedFile error  " + startOffset);
        return false;
      }

      return mappedFile.appendMessage(data, dataStart, dataLength);
    } finally {
      putMessageLock.unlock();
    }
  }

  public boolean retryDeleteFirstFile(final long intervalForcibly) {
    return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
  }

  public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
    String key = topic + "-" + queueId;
    synchronized (this) {
      this.topicQueueTable.remove(key);
      this.lmqTopicQueueTable.remove(key);
    }

    log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
  }

  public void checkSelf() {
    mappedFileQueue.checkSelf();
  }

  public long lockTimeMills() {
    long diff = 0;
    long begin = this.beginTimeInLock;
    if (begin > 0) {
      diff = this.defaultMessageStore.now() - begin;
    }

    if (diff < 0) {
      diff = 0;
    }

    return diff;
  }

  public Map<String, Long> getLmqTopicQueueTable() {
    return this.lmqTopicQueueTable;
  }

  public static class GroupCommitRequest {
    /** 刷盘点偏移量 */
    private final long nextOffset;

    private final long deadLine;
    private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();

    public GroupCommitRequest(long nextOffset, long timeoutMillis) {
      this.nextOffset = nextOffset;
      this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
    }

    public long getDeadLine() {
      return deadLine;
    }

    public long getNextOffset() {
      return nextOffset;
    }

    public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
      // 完成
      this.flushOKFuture.complete(putMessageStatus);
    }

    public CompletableFuture<PutMessageStatus> future() {
      return flushOKFuture;
    }
  }

  public static class MessageExtEncoder {
    // Store the message content
    private final ByteBuffer encoderBuffer;
    // The maximum length of the message
    private final int maxMessageSize;

    MessageExtEncoder(final int size) {
      this.encoderBuffer = ByteBuffer.allocateDirect(size);
      this.maxMessageSize = size;
    }

    protected PutMessageResult encode(MessageExtBrokerInner msgInner) {
      /** Serialize message */
      final byte[] propertiesData =
          msgInner.getPropertiesString() == null
              ? null
              : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

      final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

      if (propertiesLength > Short.MAX_VALUE) {
        log.warn("putMessage message properties length too long. length={}", propertiesData.length);
        return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
      }

      final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
      final int topicLength = topicData.length;

      final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

      final int msgLen =
          calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);
      // ? 消息超过最大值
      // Exceeds the maximum message
      if (msgLen > this.maxMessageSize) {
        CommitLog.log.warn(
            "message size exceeded, msg total size: "
                + msgLen
                + ", msg body size: "
                + bodyLength
                + ", maxMessageSize: "
                + this.maxMessageSize);
        return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
      }
      // 原本它是根据配置文件中的最大值初始化的，这里会根据实际消息设置它的limit+读模式=缩小ByteBuffer的大小
      // Initialization of storage space
      this.resetByteBuffer(encoderBuffer, msgLen);
      // 1 TOTALSIZE（总大小）
      this.encoderBuffer.putInt(msgLen);
      // 2 MAGICCODE（魔术）
      this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
      // 3 BODYCRC（CRC）https://zhuanlan.zhihu.com/p/38411551
      this.encoderBuffer.putInt(msgInner.getBodyCRC());
      // 4 QUEUEID（队列ID）
      this.encoderBuffer.putInt(msgInner.getQueueId());
      // 5 FLAG(标志)
      this.encoderBuffer.putInt(msgInner.getFlag());
      // 6 QUEUEOFFSET, need update later（队列偏移量）
      this.encoderBuffer.putLong(0);
      // 7 PHYSICALOFFSET, need update later（物理偏移量）
      this.encoderBuffer.putLong(0);
      // 8 SYSFLAG（系统标志）
      this.encoderBuffer.putInt(msgInner.getSysFlag());
      // 9 BORNTIMESTAMP（出生（写）时间戳）
      this.encoderBuffer.putLong(msgInner.getBornTimestamp());
      // 10 BORNHOST（IP）
      socketAddress2ByteBuffer(msgInner.getBornHost(), this.encoderBuffer);
      // 11 STORETIMESTAMP（storeuijmi）
      this.encoderBuffer.putLong(msgInner.getStoreTimestamp());
      // 12 STOREHOSTADDRESS（？）
      socketAddress2ByteBuffer(msgInner.getStoreHost(), this.encoderBuffer);
      // 13 RECONSUMETIMES（重新消费时间）
      this.encoderBuffer.putInt(msgInner.getReconsumeTimes());
      // 14 Prepared Transaction Offset（准备事物偏移量）
      this.encoderBuffer.putLong(msgInner.getPreparedTransactionOffset());
      // 15 BODY（Body大小）
      this.encoderBuffer.putInt(bodyLength);
      if (bodyLength > 0) this.encoderBuffer.put(msgInner.getBody());
      // 16 TOPIC（主题大小）
      this.encoderBuffer.put((byte) topicLength);
      // Topic内容
      this.encoderBuffer.put(topicData);
      // 17 PROPERTIES（属性大小）
      this.encoderBuffer.putShort((short) propertiesLength);
      // ? 有属性──存内容
      if (propertiesLength > 0) this.encoderBuffer.put(propertiesData);
      // 切换为读模式
      encoderBuffer.flip();
      return null;
    }

    private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
      byteBuffer.flip();
      byteBuffer.limit(limit);
    }

    private void socketAddress2ByteBuffer(
        final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
      InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
      InetAddress address = inetSocketAddress.getAddress();
      if (address instanceof Inet4Address) {
        byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
      } else {
        byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
      }
      byteBuffer.putInt(inetSocketAddress.getPort());
    }

    protected ByteBuffer encode(
        final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
      encoderBuffer.clear(); // not thread-safe
      int totalMsgLen = 0;
      ByteBuffer messagesByteBuff = messageExtBatch.wrap();

      int sysFlag = messageExtBatch.getSysFlag();
      int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      int storeHostLength =
          (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
      ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

      // properties from MessageExtBatch
      String batchPropStr =
          MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
      final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
      int batchPropDataLen = batchPropData.length;
      if (batchPropDataLen > Short.MAX_VALUE) {
        CommitLog.log.warn(
            "Properties size of messageExtBatch exceeded, properties size: {}, maxSize: {}.",
            batchPropDataLen,
            Short.MAX_VALUE);
        throw new RuntimeException("Properties size of messageExtBatch exceeded!");
      }
      final short batchPropLen = (short) batchPropDataLen;

      int batchSize = 0;
      while (messagesByteBuff.hasRemaining()) {
        batchSize++;
        // 1 TOTALSIZE
        messagesByteBuff.getInt();
        // 2 MAGICCODE
        messagesByteBuff.getInt();
        // 3 BODYCRC
        messagesByteBuff.getInt();
        // 4 FLAG
        int flag = messagesByteBuff.getInt();
        // 5 BODY
        int bodyLen = messagesByteBuff.getInt();
        int bodyPos = messagesByteBuff.position();
        int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
        messagesByteBuff.position(bodyPos + bodyLen);
        // 6 properties
        short propertiesLen = messagesByteBuff.getShort();
        int propertiesPos = messagesByteBuff.position();
        messagesByteBuff.position(propertiesPos + propertiesLen);
        boolean needAppendLastPropertySeparator =
            propertiesLen > 0
                && batchPropLen > 0
                && messagesByteBuff.get(messagesByteBuff.position() - 1)
                    != MessageDecoder.PROPERTY_SEPARATOR;

        final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

        final int topicLength = topicData.length;

        int totalPropLen =
            needAppendLastPropertySeparator
                ? propertiesLen + batchPropLen + 1
                : propertiesLen + batchPropLen;
        final int msgLen =
            calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, totalPropLen);

        // Exceeds the maximum message
        if (msgLen > this.maxMessageSize) {
          CommitLog.log.warn(
              "message size exceeded, msg total size: "
                  + msgLen
                  + ", msg body size: "
                  + bodyLen
                  + ", maxMessageSize: "
                  + this.maxMessageSize);
          throw new RuntimeException("message size exceeded");
        }

        totalMsgLen += msgLen;
        // Determines whether there is sufficient free space
        if (totalMsgLen > maxMessageSize) {
          throw new RuntimeException("message size exceeded");
        }

        // 1 TOTALSIZE
        this.encoderBuffer.putInt(msgLen);
        // 2 MAGICCODE
        this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
        // 3 BODYCRC
        this.encoderBuffer.putInt(bodyCrc);
        // 4 QUEUEID
        this.encoderBuffer.putInt(messageExtBatch.getQueueId());
        // 5 FLAG
        this.encoderBuffer.putInt(flag);
        // 6 QUEUEOFFSET
        this.encoderBuffer.putLong(0);
        // 7 PHYSICALOFFSET
        this.encoderBuffer.putLong(0);
        // 8 SYSFLAG
        this.encoderBuffer.putInt(messageExtBatch.getSysFlag());
        // 9 BORNTIMESTAMP
        this.encoderBuffer.putLong(messageExtBatch.getBornTimestamp());
        // 10 BORNHOST
        this.resetByteBuffer(bornHostHolder, bornHostLength);
        this.encoderBuffer.put(messageExtBatch.getBornHostBytes(bornHostHolder));
        // 11 STORETIMESTAMP
        this.encoderBuffer.putLong(messageExtBatch.getStoreTimestamp());
        // 12 STOREHOSTADDRESS
        this.resetByteBuffer(storeHostHolder, storeHostLength);
        this.encoderBuffer.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
        // 13 RECONSUMETIMES
        this.encoderBuffer.putInt(messageExtBatch.getReconsumeTimes());
        // 14 Prepared Transaction Offset, batch does not support transaction
        this.encoderBuffer.putLong(0);
        // 15 BODY
        this.encoderBuffer.putInt(bodyLen);
        if (bodyLen > 0) this.encoderBuffer.put(messagesByteBuff.array(), bodyPos, bodyLen);
        // 16 TOPIC
        this.encoderBuffer.put((byte) topicLength);
        this.encoderBuffer.put(topicData);
        // 17 PROPERTIES
        this.encoderBuffer.putShort((short) totalPropLen);
        if (propertiesLen > 0) {
          this.encoderBuffer.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
        }
        if (batchPropLen > 0) {
          if (needAppendLastPropertySeparator) {
            this.encoderBuffer.put((byte) MessageDecoder.PROPERTY_SEPARATOR);
          }
          this.encoderBuffer.put(batchPropData, 0, batchPropLen);
        }
      }
      putMessageContext.setBatchSize(batchSize);
      putMessageContext.setPhyPos(new long[batchSize]);
      encoderBuffer.flip();
      return encoderBuffer;
    }

    public ByteBuffer getEncoderBuffer() {
      return encoderBuffer;
    }
  }

  static class PutMessageThreadLocal {
    private MessageExtEncoder encoder;
    private StringBuilder keyBuilder;

    PutMessageThreadLocal(int size) {
      encoder = new MessageExtEncoder(size);
      keyBuilder = new StringBuilder();
    }

    public MessageExtEncoder getEncoder() {
      return encoder;
    }

    public StringBuilder getKeyBuilder() {
      return keyBuilder;
    }
  }

  static class PutMessageContext {
    private String topicQueueTableKey;
    private long[] phyPos;
    private int batchSize;

    public PutMessageContext(String topicQueueTableKey) {
      this.topicQueueTableKey = topicQueueTableKey;
    }

    public String getTopicQueueTableKey() {
      return topicQueueTableKey;
    }

    public long[] getPhyPos() {
      return phyPos;
    }

    public void setPhyPos(long[] phyPos) {
      this.phyPos = phyPos;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }
  }

  abstract class FlushCommitLogService extends ServiceThread {
    protected static final int RETRY_TIMES_OVER = 10;
  }

  class CommitRealTimeService extends FlushCommitLogService {

    private long lastCommitTimestamp = 0;

    @Override
    public void run() {
      CommitLog.log.info(this.getServiceName() + " service started");
      while (!this.isStopped()) {
        int interval =
            CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

        int commitDataLeastPages =
            CommitLog.this
                .defaultMessageStore
                .getMessageStoreConfig()
                .getCommitCommitLogLeastPages();

        int commitDataThoroughInterval =
            CommitLog.this
                .defaultMessageStore
                .getMessageStoreConfig()
                .getCommitCommitLogThoroughInterval();

        long begin = System.currentTimeMillis();
        if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
          this.lastCommitTimestamp = begin;
          commitDataLeastPages = 0;
        }

        try {
          boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
          long end = System.currentTimeMillis();
          if (!result) {
            this.lastCommitTimestamp = end; // result = false means some data committed.
            // now wake up flush thread.
            flushCommitLogService.wakeup();
          }

          if (end - begin > 500) {
            log.info("Commit data to file costs {} ms", end - begin);
          }
          this.waitForRunning(interval);
        } catch (Throwable e) {
          CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
        }
      }

      boolean result = false;
      for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
        result = CommitLog.this.mappedFileQueue.commit(0);
        CommitLog.log.info(
            this.getServiceName()
                + " service shutdown, retry "
                + (i + 1)
                + " times "
                + (result ? "OK" : "Not OK"));
      }
      CommitLog.log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
      return CommitRealTimeService.class.getSimpleName();
    }
  }

  class FlushRealTimeService extends FlushCommitLogService {
    private long lastFlushTimestamp = 0;
    private long printTimes = 0;

    public void run() {
      CommitLog.log.info(this.getServiceName() + " service started");

      while (!this.isStopped()) {
        boolean flushCommitLogTimed =
            CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

        int interval =
            CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
        int flushPhysicQueueLeastPages =
            CommitLog.this
                .defaultMessageStore
                .getMessageStoreConfig()
                .getFlushCommitLogLeastPages();

        int flushPhysicQueueThoroughInterval =
            CommitLog.this
                .defaultMessageStore
                .getMessageStoreConfig()
                .getFlushCommitLogThoroughInterval();

        boolean printFlushProgress = false;

        // Print flush progress
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
          this.lastFlushTimestamp = currentTimeMillis;
          flushPhysicQueueLeastPages = 0;
          printFlushProgress = (printTimes++ % 10) == 0;
        }

        try {
          if (flushCommitLogTimed) {
            Thread.sleep(interval);
          } else {
            this.waitForRunning(interval);
          }

          if (printFlushProgress) {
            this.printFlushProgress();
          }

          long begin = System.currentTimeMillis();
          CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
          long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
          if (storeTimestamp > 0) {
            CommitLog.this
                .defaultMessageStore
                .getStoreCheckpoint()
                .setPhysicMsgTimestamp(storeTimestamp);
          }
          long past = System.currentTimeMillis() - begin;
          if (past > 500) {
            log.info("Flush data to disk costs {} ms", past);
          }
        } catch (Throwable e) {
          CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
          this.printFlushProgress();
        }
      }

      // Normal shutdown, to ensure that all the flush before exit
      boolean result = false;
      for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
        result = CommitLog.this.mappedFileQueue.flush(0);
        CommitLog.log.info(
            this.getServiceName()
                + " service shutdown, retry "
                + (i + 1)
                + " times "
                + (result ? "OK" : "Not OK"));
      }

      this.printFlushProgress();

      CommitLog.log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
      return FlushRealTimeService.class.getSimpleName();
    }

    private void printFlushProgress() {
      // CommitLog.log.info("how much disk fall behind memory, "
      // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
    }

    @Override
    public long getJointime() {
      return 1000 * 60 * 5;
    }
  }

  /**
   * GroupCommit Service<br>
   * 每 10 秒毫秒定时刷盘
   */
  class GroupCommitService extends FlushCommitLogService {
    private final PutMessageSpinLock lock = new PutMessageSpinLock();
    private volatile LinkedList<GroupCommitRequest> requestsWrite =
        new LinkedList<GroupCommitRequest>();
    private volatile LinkedList<GroupCommitRequest> requestsRead =
        new LinkedList<GroupCommitRequest>();

    public synchronized void putRequest(final GroupCommitRequest request) {
      lock.lock();
      try {
        this.requestsWrite.add(request);
      } finally {
        lock.unlock();
      }
      this.wakeup();
    }

    public void run() {
      CommitLog.log.info(this.getServiceName() + " service started");

      while (!this.isStopped()) {
        try {
          this.waitForRunning(10);
          this.doCommit();
        } catch (Exception e) {
          CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
        }
      }

      // Under normal circumstances shutdown, wait for the arrival of the
      // request, and then flush
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        CommitLog.log.warn(this.getServiceName() + " Exception, ", e);
      }

      synchronized (this) {
        this.swapRequests();
      }

      this.doCommit();

      CommitLog.log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
      return GroupCommitService.class.getSimpleName();
    }

    private void doCommit() {
      if (!this.requestsRead.isEmpty()) {
        for (GroupCommitRequest req : this.requestsRead) {
          // There may be a message in the next file, so a maximum of
          // two times the flush
          boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
          for (int i = 0; i < 2 && !flushOK; i++) {
            CommitLog.this.mappedFileQueue.flush(0);
            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
          }

          req.wakeupCustomer(
              flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
        }

        long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
        if (storeTimestamp > 0) {
          CommitLog.this
              .defaultMessageStore
              .getStoreCheckpoint()
              .setPhysicMsgTimestamp(storeTimestamp);
        }

        this.requestsRead = new LinkedList<>();
      } else {
        // Because of individual messages is set to not sync flush, it
        // will come to this process
        CommitLog.this.mappedFileQueue.flush(0);
      }
    }

    private void swapRequests() {
      lock.lock();
      try {
        LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
        this.requestsWrite = this.requestsRead;
        this.requestsRead = tmp;
      } finally {
        lock.unlock();
      }
    }

    @Override
    protected void onWaitEnd() {
      this.swapRequests();
    }

    @Override
    public long getJointime() {
      return 1000 * 60 * 5;
    }
  }

  class DefaultAppendMessageCallback implements AppendMessageCallback {
    // File at the end of the minimum fixed length empty
    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
    private final ByteBuffer msgIdMemory;
    private final ByteBuffer msgIdV6Memory;
    // Store the message content
    private final ByteBuffer msgStoreItemMemory;
    // The maximum length of the message
    private final int maxMessageSize;

    DefaultAppendMessageCallback(final int size) {
      this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
      this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
      this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
      this.maxMessageSize = size;
    }

    public AppendMessageResult doAppend(
        final long fileFromOffset,
        final ByteBuffer byteBuffer,
        final int maxBlank,
        final MessageExtBrokerInner msgInner,
        PutMessageContext putMessageContext) {
      // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>
      // 代码清单4-4
      // PHY OFFSET
      long wroteOffset = fileFromOffset + byteBuffer.position();

      Supplier<String> msgIdSupplier =
          () -> {
            int sysflag = msgInner.getSysFlag();
            int msgIdLen =
                (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
            ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
            MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
            msgIdBuffer.clear(); // because socketAddress2ByteBuffer flip the buffer
            msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
            return UtilAll.bytes2string(msgIdBuffer.array());
          };

      // Record ConsumeQueue information
      String key = putMessageContext.getTopicQueueTableKey();
      Long queueOffset = CommitLog.this.topicQueueTable.get(key);
      if (null == queueOffset) {
        queueOffset = 0L;
        CommitLog.this.topicQueueTable.put(key, queueOffset);
      }

      boolean multiDispatchWrapResult = CommitLog.this.multiDispatch.wrapMultiDispatch(msgInner);
      if (!multiDispatchWrapResult) {
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
      }

      // Transaction messages that require special handling
      final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
      switch (tranType) {
          // Prepared and Rollback message is not consumed, will not enter the
          // consumer queuec
        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
          queueOffset = 0L;
          break;
        case MessageSysFlag.TRANSACTION_NOT_TYPE:
        case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
        default:
          break;
      }

      ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
      final int msgLen = preEncodeBuffer.getInt(0);
      // 消息大小 超过文件可写大小
      // Determines whether there is sufficient free space
      if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
        this.msgStoreItemMemory.clear();
        // 1 TOTALSIZE（可用最大小）
        this.msgStoreItemMemory.putInt(maxBlank);
        // 2 MAGICCODE（魔术）
        this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
        // 3 The remaining space may be any value
        // Here the length of the specially set maxBlank
        final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
        // 所以最少空闲8个字节
        byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
        return new AppendMessageResult(
            AppendMessageStatus.END_OF_FILE,
            wroteOffset,
            maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
            msgIdSupplier,
            msgInner.getStoreTimestamp(),
            queueOffset,
            CommitLog.this.defaultMessageStore.now() - beginTimeMills);
      }
      // 消息存储
      int pos = 4 + 4 + 4 + 4 + 4;
      // 6 QUEUEOFFSET（队列偏移量）
      preEncodeBuffer.putLong(pos, queueOffset);
      pos += 8;
      // 7 PHYSICALOFFSET
      preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
      int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
      pos += 8 + 4 + 8 + ipLen;
      // refresh store time stamp in lock
      preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());

      final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
      // Write messages to the queue buffer
      byteBuffer.put(preEncodeBuffer);
      msgInner.setEncodedBuff(null);
      AppendMessageResult result =
          new AppendMessageResult(
              AppendMessageStatus.PUT_OK,
              wroteOffset,
              msgLen,
              msgIdSupplier,
              msgInner.getStoreTimestamp(),
              queueOffset,
              CommitLog.this.defaultMessageStore.now() - beginTimeMills);

      switch (tranType) {
        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
          break;
        case MessageSysFlag.TRANSACTION_NOT_TYPE:
        case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
          // The next update ConsumeQueue information
          CommitLog.this.topicQueueTable.put(key, ++queueOffset);
          CommitLog.this.multiDispatch.updateMultiQueueOffset(msgInner);
          break;
        default:
          break;
      }
      return result;
    }

    public AppendMessageResult doAppend(
        final long fileFromOffset,
        final ByteBuffer byteBuffer,
        final int maxBlank,
        final MessageExtBatch messageExtBatch,
        PutMessageContext putMessageContext) {
      byteBuffer.mark();
      // physical offset
      long wroteOffset = fileFromOffset + byteBuffer.position();
      // Record ConsumeQueue information
      String key = putMessageContext.getTopicQueueTableKey();
      Long queueOffset = CommitLog.this.topicQueueTable.get(key);
      if (null == queueOffset) {
        queueOffset = 0L;
        CommitLog.this.topicQueueTable.put(key, queueOffset);
      }
      long beginQueueOffset = queueOffset;
      int totalMsgLen = 0;
      int msgNum = 0;

      final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
      ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

      int sysFlag = messageExtBatch.getSysFlag();
      int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      int storeHostLength =
          (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      Supplier<String> msgIdSupplier =
          () -> {
            int msgIdLen = storeHostLength + 8;
            int batchCount = putMessageContext.getBatchSize();
            long[] phyPosArray = putMessageContext.getPhyPos();
            ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
            MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
            msgIdBuffer.clear(); // because socketAddress2ByteBuffer flip the buffer

            StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
            for (int i = 0; i < phyPosArray.length; i++) {
              msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
              String msgId = UtilAll.bytes2string(msgIdBuffer.array());
              if (i != 0) {
                buffer.append(',');
              }
              buffer.append(msgId);
            }
            return buffer.toString();
          };

      messagesByteBuff.mark();
      int index = 0;
      while (messagesByteBuff.hasRemaining()) {
        // 1 TOTALSIZE
        final int msgPos = messagesByteBuff.position();
        final int msgLen = messagesByteBuff.getInt();
        final int bodyLen = msgLen - 40; // only for log, just estimate it
        // Exceeds the maximum message
        if (msgLen > this.maxMessageSize) {
          CommitLog.log.warn(
              "message size exceeded, msg total size: "
                  + msgLen
                  + ", msg body size: "
                  + bodyLen
                  + ", maxMessageSize: "
                  + this.maxMessageSize);
          return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
        }
        totalMsgLen += msgLen;
        // Determines whether there is sufficient free space
        if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
          this.msgStoreItemMemory.clear();
          // 1 TOTALSIZE
          this.msgStoreItemMemory.putInt(maxBlank);
          // 2 MAGICCODE
          this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
          // 3 The remaining space may be any value
          // ignore previous read
          messagesByteBuff.reset();
          // Here the length of the specially set maxBlank
          byteBuffer.reset(); // ignore the previous appended messages
          byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
          return new AppendMessageResult(
              AppendMessageStatus.END_OF_FILE,
              wroteOffset,
              maxBlank,
              msgIdSupplier,
              messageExtBatch.getStoreTimestamp(),
              beginQueueOffset,
              CommitLog.this.defaultMessageStore.now() - beginTimeMills);
        }
        // move to add queue offset and commitlog offset
        int pos = msgPos + 20;
        messagesByteBuff.putLong(pos, queueOffset);
        pos += 8;
        messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
        // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
        pos += 8 + 4 + 8 + bornHostLength;
        // refresh store time stamp in lock
        messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());

        putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
        queueOffset++;
        msgNum++;
        messagesByteBuff.position(msgPos + msgLen);
      }

      messagesByteBuff.position(0);
      messagesByteBuff.limit(totalMsgLen);
      byteBuffer.put(messagesByteBuff);
      messageExtBatch.setEncodedBuff(null);
      AppendMessageResult result =
          new AppendMessageResult(
              AppendMessageStatus.PUT_OK,
              wroteOffset,
              totalMsgLen,
              msgIdSupplier,
              messageExtBatch.getStoreTimestamp(),
              beginQueueOffset,
              CommitLog.this.defaultMessageStore.now() - beginTimeMills);
      result.setMsgNum(msgNum);
      CommitLog.this.topicQueueTable.put(key, queueOffset);

      return result;
    }

    private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
      byteBuffer.flip();
      byteBuffer.limit(limit);
    }
  }
}
