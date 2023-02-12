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
package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dledger.AppendFuture;
import io.openmessaging.storage.dledger.BatchAppendFuture;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFile;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import io.openmessaging.storage.dledger.store.file.SelectMmapBufferResult;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 存储所有元数据停机时间以实现恢复、数据保护可靠性.
 *
 * <p>DLedgerCommitlog 集成在 CommitLog 类中，主要实现基于 DLedger 的日志存储
 *
 * @author shui4
 */
public class DLedgerCommitLog extends CommitLog {
  /** 基于 Raft 协议实现的集群内 的一个节点，用 DLedgerServer 实例表示 */
  private final DLedgerServer dLedgerServer;

  /** DLedger 的配置信息 */
  private final DLedgerConfig dLedgerConfig;

  /** DLedger 基于文件 映射的存储实现 */
  private final DLedgerMmapFileStore dLedgerFileStore;

  /** DLedger 管理的存储文件集 合，对标 RocketMQ 中的 MappedFileQueue */
  private final MmapFileList dLedgerFileList;

  /** 节点 ID，0 表示主节点, 非 0 表示从节点 */
  // The id identifies the broker role, 0 means master, others means slave
  private final int id;

  /** 消息序列器 */
  private final MessageSerializer messageSerializer;

  /** msgIdBuilder */
  private final StringBuilder msgIdBuilder = new StringBuilder();

  /** 用于记录消息追加的耗时（日志追加所持有锁时间） */
  private volatile long beginTimeInDledgerLock = 0;
  /** 记录旧的 CommitLog 文 件中的最大偏移量，如果访问的偏移量大于它，则访问 Dledger 管理的文件 */
  // This offset separate the old commitlog from dledger commitlog
  private long dividedCommitlogOffset = -1;

  /** 是否正在恢复旧的 CommitLog 文件 */
  private boolean isInrecoveringOldCommitlog = false;

  /**
   * DLedgerCommitLog
   *
   * @param defaultMessageStore ignore
   */
  public DLedgerCommitLog(final DefaultMessageStore defaultMessageStore) {
    // 调用父类，即 CommitLog 的构造函数，加载 ${ROCKETMQ_HOME}/store/
    // comitlog 下的 CommitLog 文件，即开启主从切换后需要兼容之前的消息
    super(defaultMessageStore);
    dLedgerConfig = new DLedgerConfig();
    // 是否强制删除文件，取自 Broker 配置
    // 属性 cleanFileForcibly Enable，默认为 true
    dLedgerConfig.setEnableDiskForceClean(
        defaultMessageStore.getMessageStoreConfig().isCleanFileForciblyEnable());
    // DLedger 存储类型，固定为基于文件的存储模式
    dLedgerConfig.setStoreType(DLedgerConfig.FILE);
    // 节点的 ID 名称，示例配置为 n0，配置要求是第二个字符后必须是数字
    dLedgerConfig.setSelfId(defaultMessageStore.getMessageStoreConfig().getdLegerSelfId());
    // DLegergroup 的名称，即一个复制组的组名称，建议与 Broker 配置属性 brokerName 保持一致
    dLedgerConfig.setGroup(defaultMessageStore.getMessageStoreConfig().getdLegerGroup());
    // DLegergroup 中所有的节点信息，其配置显示为 n0-127.0.0.1:40911; n1-127.0.0.1:40912; n2-127.0.0.1:40913。
    // 多个节点使用分号隔开
    dLedgerConfig.setPeers(defaultMessageStore.getMessageStoreConfig().getdLegerPeers());
    // 设置 DLedger 日志文件的根目录，取自 Borker 配件文件中的 storePath RootDir，即 RocketMQ 的数据存储根路径
    dLedgerConfig.setStoreBaseDir(
        defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
    // 设置 DLedger 单个日志文件的大小，取自 Broker 配置文件中的 mapedFileSizeCommitLog
    dLedgerConfig.setMappedFileSizeForEntryData(
        defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
    // DLedger 日志文件的过期删除时间，取自 Broker 配置文件中的 deleteWhen，默认为凌晨 4 点
    dLedgerConfig.setDeleteWhen(defaultMessageStore.getMessageStoreConfig().getDeleteWhen());
    // DLedger 日志文件保留时长，取自 Broker 配置文件中的 fileReserved Hours，默认为 72h
    dLedgerConfig.setFileReservedHours(
        defaultMessageStore.getMessageStoreConfig().getFileReservedTime() + 1);
    dLedgerConfig.setPreferredLeaderId(
        defaultMessageStore.getMessageStoreConfig().getPreferredLeaderId());
    dLedgerConfig.setEnableBatchPush(
        defaultMessageStore.getMessageStoreConfig().isEnableBatchPush());

    id = Integer.parseInt(dLedgerConfig.getSelfId().substring(1)) + 1;
    // 使用 DLedger 相关的配置创建 DLedgerServer，即每一个 Broker 节点为 Raft 集群中的一个节点，同一个复制组会使用 Raft 协议进行日志复制
    dLedgerServer = new DLedgerServer(dLedgerConfig);
    dLedgerFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
    // 添加消息 Append 事件的处理钩子，主要是完成 CommitLog
    // 文件的物理偏移量在启用主从切换后与未开启主从切换的语义保持一
    // 致性，即如果启用了主从切换机制，消息追加时返回的物理偏移量并
    // 不是 DLedger 日志条目的起始位置，而是其 body 字段的开始位置
    DLedgerMmapFileStore.AppendHook appendHook =
        (entry, buffer, bodyOffset) -> {
          assert bodyOffset == DLedgerEntry.BODY_OFFSET;
          buffer.position(buffer.position() + bodyOffset + MessageDecoder.PHY_POS_POSITION);
          buffer.putLong(entry.getPos() + bodyOffset);
        };
    dLedgerFileStore.addAppendHook(appendHook);
    dLedgerFileList = dLedgerFileStore.getDataFileList();
    this.messageSerializer =
        new MessageSerializer(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
  }

  @Override
  public boolean load() {
    // DLedgerCommitLog 在加载时先调用其父类 CommitLog 文件的 load()
    // 方法，即启用主从切换后依然会加载原 CommitLog 中的文件
    return super.load();
  }

  /** refreshConfig */
  private void refreshConfig() {
    dLedgerConfig.setEnableDiskForceClean(
        defaultMessageStore.getMessageStoreConfig().isCleanFileForciblyEnable());
    dLedgerConfig.setDeleteWhen(defaultMessageStore.getMessageStoreConfig().getDeleteWhen());
    dLedgerConfig.setFileReservedHours(
        defaultMessageStore.getMessageStoreConfig().getFileReservedTime() + 1);
  }

  /** disableDeleteDledger */
  private void disableDeleteDledger() {
    dLedgerConfig.setEnableDiskForceClean(false);
    dLedgerConfig.setFileReservedHours(24 * 365 * 10);
  }

  @Override
  public void start() {
    dLedgerServer.startup();
  }

  @Override
  public void shutdown() {
    dLedgerServer.shutdown();
  }

  @Override
  public long flush() {
    dLedgerFileStore.flush();
    return dLedgerFileList.getFlushedWhere();
  }

  @Override
  public long getMaxOffset() {
    if (dLedgerFileStore.getCommittedPos() > 0) {
      return dLedgerFileStore.getCommittedPos();
    }
    if (dLedgerFileList.getMinOffset() > 0) {
      return dLedgerFileList.getMinOffset();
    }
    return 0;
  }

  @Override
  public long getMinOffset() {
    if (!mappedFileQueue.getMappedFiles().isEmpty()) {
      return mappedFileQueue.getMinOffset();
    }
    return dLedgerFileList.getMinOffset();
  }

  @Override
  public long getConfirmOffset() {
    return this.getMaxOffset();
  }

  @Override
  public void setConfirmOffset(long phyOffset) {
    log.warn("Should not set confirm offset {} for dleger commitlog", phyOffset);
  }

  @Override
  public long remainHowManyDataToCommit() {
    return dLedgerFileList.remainHowManyDataToCommit();
  }

  @Override
  public long remainHowManyDataToFlush() {
    return dLedgerFileList.remainHowManyDataToFlush();
  }

  @Override
  public int deleteExpiredFile(
      final long expiredTime,
      final int deleteFilesInterval,
      final long intervalForcibly,
      final boolean cleanImmediately) {
    if (mappedFileQueue.getMappedFiles().isEmpty()) {
      refreshConfig();
      // To prevent too much log in defaultMessageStore
      return Integer.MAX_VALUE;
    } else {
      disableDeleteDledger();
    }
    int count =
        super.deleteExpiredFile(
            expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    if (count > 0 || mappedFileQueue.getMappedFiles().size()!= 1) {
      return count;
    }
    // the old logic will keep the last file, here to delete it
    MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
    log.info("Try to delete the last old commitlog file {}", mappedFile.getFileName());
    long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
    if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
      while (!mappedFile.destroy(10 * 1000)) {
        DLedgerUtils.sleep(1000);
      }
      mappedFileQueue.getMappedFiles().remove(mappedFile);
    }
    return 1;
  }

  /**
   * convertSbr
   *
   * @param sbr ignore
   * @return ignore
   */
  public SelectMappedBufferResult convertSbr(SelectMmapBufferResult sbr) {
    if (sbr == null) {
      return null;
    } else {
      return new DLedgerSelectMappedBufferResult(sbr);
    }
  }

  /**
   * truncate
   *
   * @param sbr ignore
   * @return ignore
   */
  public SelectMmapBufferResult truncate(SelectMmapBufferResult sbr) {
    long committedPos = dLedgerFileStore.getCommittedPos();
    if (sbr == null || sbr.getStartOffset() == committedPos) {
      return null;
    }
    if (sbr.getStartOffset() + sbr.getSize()<= committedPos) {
      return sbr;
    } else {
      sbr.setSize((int) (committedPos - sbr.getStartOffset()));
      return sbr;
    }
  }

  @Override
  public SelectMappedBufferResult getData(final long offset) {
    if (offset < dividedCommitlogOffset) {
      return super.getData(offset);
    }
    return this.getData(offset, offset == 0);
  }

  @Override
  public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
    if (offset < dividedCommitlogOffset) {
      return super.getData(offset, returnFirstOnNotFound);
    }
    if (offset >= dLedgerFileStore.getCommittedPos()) {
      return null;
    }
    int mappedFileSize = this.dLedgerServer.getdLedgerConfig().getMappedFileSizeForEntryData();
    MmapFile mappedFile =
        this.dLedgerFileList.findMappedFileByOffset(offset, returnFirstOnNotFound);
    if (mappedFile != null) {
      int pos = (int) (offset % mappedFileSize);
      SelectMmapBufferResult sbr = mappedFile.selectMappedBuffer(pos);
      return convertSbr(truncate(sbr));
    }

    return null;
  }

  /**
   * 在 Broker 启动时会加载 CommitLog、ConsumeQueue 等文件，需要恢 复其相关的数据结构，特别是写入、刷盘、提交等指针，具体调用 recover() 方法实现
   *
   * @param maxPhyOffsetOfConsumeQueue ignore
   */
  private void recover(long maxPhyOffsetOfConsumeQueue) {
    // 加载 DLedger 相关的存储文件，并逐一构建对应的 MmapFile。初始化三个重要的指针 wrotePosition、
    // flushedPosition、committedPosition 表示文件的大小
    dLedgerFileStore.load();
    // 如果已存在 DLedger 的数据文件，则只需要恢复 DLedger 相关的数据文件，因为在加载旧的 CommitLog 文件时已经将重要的数据指针设置为最大值
    if (dLedgerFileList.getMappedFiles().size()> 0) {
      // 调用 DLedger 文件存储实现类 DLedgerFileStore 的 recover() 方法，恢复管辖的 MMapFile 对象（一个文件对应一个 MMapFile 实例）的
      // 相关指针，其实现方法与 RocketMQ 的 DefaultMessageStore 恢复过程类似。
      dLedgerFileStore.recover();
      /// 设置 dividedCommitlogOffset 的值为 DLedger 中物理文件的最
      // 小偏移量。消息的物理偏移量如果小于该值，则从 CommitLog 文件中查
      // 找消息，消息的物理偏移量如果大于或等于该值，则从 DLedger 相关的 文件中查找消息
      dividedCommitlogOffset = dLedgerFileList.getFirstMappedFile().getFileFromOffset();
      MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
      // 如果存在旧的 CommitLog 文件，则禁止删除 DLedger 文件，具体做法就是禁止强制删除文件，并将文件的有效存储时间设置为 10 年
      if (mappedFile != null) {
        disableDeleteDledger();
      }
      long maxPhyOffset = dLedgerFileList.getMaxWrotePosition();
      // 如果 ConsumeQueue 中存储的最大物理偏移量大于 DLedger 中最大的物理偏移量，则删除多余的 ConsumeQueue 文件
      // Clear ConsumeQueue redundant data
      if (maxPhyOffsetOfConsumeQueue >= maxPhyOffset) {
        log.warn(
            "[TruncateCQ]maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files",
            maxPhyOffsetOfConsumeQueue,
            maxPhyOffset);
        this.defaultMessageStore.truncateDirtyLogicFiles(maxPhyOffset);
      }
      return;
    }
    // 从该步骤开始，只针对开启主从切换并且是初次启动
    // （并没有生成 DLedger 相关的数据文件）的相关流程，调用 CommitLog 的 recoverNormally 文件恢复旧的 CommitLog 文件.

    // 表示，这是第一次加载混合提交日志，需要恢复旧的提交日志
    isInrecoveringOldCommitlog = true;
    // No need the abnormal recover
    super.recoverNormally(maxPhyOffsetOfConsumeQueue);
    isInrecoveringOldCommitlog = false;
    // 如果不存在旧的 CommitLog 文件，直接结束日志文件的恢复流程
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
    if (mappedFile == null) {
      return;
    }
    // 如果存在旧的 CommitLog 文件，需要将文件剩余部分全部填充数据，即不再接受新的数据写入，使新的数据全部写入 DLedger 的数据文件，关键实现点如下。
    // 1）尝试查找最后一个 CommitLog 文件，如果未找到则结束查找。

    // 2）从最后一个文件的最后写入点（原 CommitLog 文件的待写入位 点），尝试查找写入的魔数，如果存在魔数并等于
    // CommitLog.BLANK_MAGIC_CODE，则无须写入魔数，在升级 DLedger 第一
    // 次启动时，魔数为空，故需要写入魔数。

    // 3）初始化 dividedCommitlogOffset，等于最后一个文件的起始偏
    // 移量加上文件的大小，即该指针指向最后一个文件的结束位置。

    // 4）将最后一个未写满数据的 CommitLog 文件全部写满，其方法为
    // 设置消息体的大小与魔数。

    // 5）设置最后一个文件的 wrotePosition、flushedPosition、
    // committedPosition 为文件的大小，同样意味着最后一个文件已经写
    // 满，下一条消息将写入 DLedger。

    ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
    byteBuffer.position(mappedFile.getWrotePosition());
    boolean needWriteMagicCode = true;
    // 1 TOTAL SIZE
    byteBuffer.getInt(); // size
    int magicCode = byteBuffer.getInt();
    if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
      needWriteMagicCode = false;
    } else {
      log.info("Recover old commitlog found a illegal magic code={}", magicCode);
    }
    dLedgerConfig.setEnableDiskForceClean(false);
    dividedCommitlogOffset = mappedFile.getFileFromOffset()+ mappedFile.getFileSize();
    log.info(
        "Recover old commitlog needWriteMagicCode={} pos={} file={} dividedCommitlogOffset={}",
        needWriteMagicCode,
        mappedFile.getFileFromOffset()+ mappedFile.getWrotePosition(),
        mappedFile.getFileName(),
        dividedCommitlogOffset);
    if (needWriteMagicCode) {
      byteBuffer.position(mappedFile.getWrotePosition());
      byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
      byteBuffer.putInt(BLANK_MAGIC_CODE);
      mappedFile.flush(0);
    }
    mappedFile.setWrotePosition(mappedFile.getFileSize());
    mappedFile.setCommittedPosition(mappedFile.getFileSize());
    mappedFile.setFlushedPosition(mappedFile.getFileSize());
    dLedgerFileList.getLastMappedFile(dividedCommitlogOffset);
    log.info("Will set the initial commitlog offset={} for dledger", dividedCommitlogOffset);
  }

  @Override
  public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
    recover(maxPhyOffsetOfConsumeQueue);
  }

  @Override
  public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
    recover(maxPhyOffsetOfConsumeQueue);
  }

  @Override
  public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC) {
    return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
  }

  @Override
  public DispatchRequest checkMessageAndReturnSize(
      ByteBuffer byteBuffer, final boolean checkCRC, final boolean readBody) {
    if (isInrecoveringOldCommitlog) {
      return super.checkMessageAndReturnSize(byteBuffer, checkCRC, readBody);
    }
    try {
      int bodyOffset = DLedgerEntry.BODY_OFFSET;
      int pos = byteBuffer.position();
      int magic = byteBuffer.getInt();
      // In dledger, this field is size, it must be gt 0, so it could prevent collision
      int magicOld = byteBuffer.getInt();
      if (magicOld == CommitLog.BLANK_MAGIC_CODE || magicOld == CommitLog.MESSAGE_MAGIC_CODE) {
        byteBuffer.position(pos);
        return super.checkMessageAndReturnSize(byteBuffer, checkCRC, readBody);
      }
      if (magic == MmapFileList.BLANK_MAGIC_CODE) {
        return new DispatchRequest(0, true);
      }
      byteBuffer.position(pos + bodyOffset);
      DispatchRequest dispatchRequest =
          super.checkMessageAndReturnSize(byteBuffer, checkCRC, readBody);
      if (dispatchRequest.isSuccess()) {
        dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
      } else if (dispatchRequest.getMsgSize() > 0) {
        dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
      }
      return dispatchRequest;
    } catch (Throwable ignored) {
    }

    return new DispatchRequest(-1, false /* success */);
  }

  @Override
  public boolean resetOffset(long offset) {
    // currently, it seems resetOffset has no use
    return false;
  }

  @Override
  public long getBeginTimeInLock() {
    return beginTimeInDledgerLock;
  }

  /**
   * setMessageInfo
   *
   * @param msg ignore
   * @param tranType ignore
   */
  private void setMessageInfo(MessageExtBrokerInner msg, int tranType) {
    // Set the storage time
    msg.setStoreTimestamp(System.currentTimeMillis());
    // Set the message body BODY CRC (consider the most appropriate setting
    // on the client)
    msg.setBodyCRC(UtilAll.crc32(msg.getBody()));

    // should be consistent with the old version
    if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
        || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
      // Delay Delivery
      if (msg.getDelayTimeLevel() > 0) {
        if (msg.getDelayTimeLevel()
            > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
          msg.setDelayTimeLevel(
              this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
        }

        String topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
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

    InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
    if (bornSocketAddress.getAddress() instanceof Inet6Address) {
      msg.setBornHostV6Flag();
    }

    InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
    if (storeSocketAddress.getAddress() instanceof Inet6Address) {
      msg.setStoreHostAddressV6Flag();
    }
  }

  @Override
  public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {

    StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

    final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

    setMessageInfo(msg, tranType);

    final String finalTopic = msg.getTopic();

    // Back to Results
    AppendMessageResult appendResult;
    AppendFuture<AppendEntryResponse> dledgerFuture;
    EncodeResult encodeResult;

    encodeResult = this.messageSerializer.serialize(msg);
    if (encodeResult.status != AppendMessageStatus.PUT_OK) {
      return CompletableFuture.completedFuture(
          new PutMessageResult(
              PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(encodeResult.status)));
    }
    putMessageLock.lock(); // spin or ReentrantLock ,depending on store config
    long elapsedTimeInLock;
    long queueOffset;
    try {
      beginTimeInDledgerLock = this.defaultMessageStore.getSystemClock().now();
      queueOffset = getQueueOffsetByKey(encodeResult.queueOffsetKey, tranType);
      encodeResult.setQueueOffsetKey(queueOffset, false);
      AppendEntryRequest request = new AppendEntryRequest();
      request.setGroup(dLedgerConfig.getGroup());
      request.setRemoteId(dLedgerServer.getMemberState().getSelfId());
      request.setBody(encodeResult.getData());
      dledgerFuture = (AppendFuture<AppendEntryResponse>) dLedgerServer.handleAppend(request);
      if (dledgerFuture.getPos() == -1) {
        return CompletableFuture.completedFuture(
            new PutMessageResult(
                PutMessageStatus.OS_PAGECACHE_BUSY,
                new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
      }
      long wroteOffset = dledgerFuture.getPos() + DLedgerEntry.BODY_OFFSET;

      int msgIdLength =
          (msg.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0
              ? 4 + 4 + 8
              : 16 + 4 + 8;
      ByteBuffer buffer = ByteBuffer.allocate(msgIdLength);

      String msgId = MessageDecoder.createMessageId(buffer, msg.getStoreHostBytes(), wroteOffset);
      elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDledgerLock;
      appendResult =
          new AppendMessageResult(
              AppendMessageStatus.PUT_OK,
              wroteOffset,
              encodeResult.getData().length,
              msgId,
              System.currentTimeMillis(),
              queueOffset,
              elapsedTimeInLock);
      switch (tranType) {
        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
          break;
        case MessageSysFlag.TRANSACTION_NOT_TYPE:
        case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
          // The next update ConsumeQueue information
          DLedgerCommitLog.this.topicQueueTable.put(encodeResult.queueOffsetKey, queueOffset + 1);
          break;
        default:
          break;
      }
    } catch (Exception e) {
      log.error("Put message error", e);
      return CompletableFuture.completedFuture(
          new PutMessageResult(
              PutMessageStatus.UNKNOWN_ERROR,
              new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
    } finally {
      beginTimeInDledgerLock = 0;
      putMessageLock.unlock();
    }

    if (elapsedTimeInLock > 500) {
      log.warn(
          "[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
          elapsedTimeInLock,
          msg.getBody().length,
          appendResult);
    }

    return dledgerFuture.thenApply(
        appendEntryResponse -> {
          PutMessageStatus putMessageStatus = PutMessageStatus.UNKNOWN_ERROR;
          switch (DLedgerResponseCode.valueOf(appendEntryResponse.getCode())) {
            case SUCCESS:
              putMessageStatus = PutMessageStatus.PUT_OK;
              break;
            case INCONSISTENT_LEADER:
            case NOT_LEADER:
            case LEADER_NOT_READY:
            case DISK_FULL:
              putMessageStatus = PutMessageStatus.SERVICE_NOT_AVAILABLE;
              break;
            case WAIT_QUORUM_ACK_TIMEOUT:
              // Do not return flush_slave_timeout to the client, for the ons client will ignore it.
              putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
              break;
            case LEADER_PENDING_FULL:
              putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
              break;
          }
          PutMessageResult putMessageResult = new PutMessageResult(putMessageStatus, appendResult);
          if (putMessageStatus == PutMessageStatus.PUT_OK) {
            // Statistics
            storeStatsService.getSinglePutMessageTopicTimesTotal(finalTopic).add(1);
            storeStatsService
                .getSinglePutMessageTopicSizeTotal(msg.getTopic())
                .add(appendResult.getWroteBytes());
          }
          return putMessageResult;
        });
  }

  @Override
  public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
    final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

    if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
      return CompletableFuture.completedFuture(
          new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
    }
    if (messageExtBatch.getDelayTimeLevel() > 0) {
      return CompletableFuture.completedFuture(
          new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
    }

    // Set the storage time
    messageExtBatch.setStoreTimestamp(System.currentTimeMillis());

    StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

    InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
    if (bornSocketAddress.getAddress() instanceof Inet6Address) {
      messageExtBatch.setBornHostV6Flag();
    }

    InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
    if (storeSocketAddress.getAddress() instanceof Inet6Address) {
      messageExtBatch.setStoreHostAddressV6Flag();
    }

    // Back to Results
    AppendMessageResult appendResult;
    BatchAppendFuture<AppendEntryResponse> dledgerFuture;
    EncodeResult encodeResult;

    encodeResult = this.messageSerializer.serialize(messageExtBatch);
    if (encodeResult.status != AppendMessageStatus.PUT_OK) {
      return CompletableFuture.completedFuture(
          new PutMessageResult(
              PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(encodeResult.status)));
    }

    putMessageLock.lock(); // spin or ReentrantLock ,depending on store config
    msgIdBuilder.setLength(0);
    long elapsedTimeInLock;
    long queueOffset;
    int msgNum = 0;
    try {
      beginTimeInDledgerLock = this.defaultMessageStore.getSystemClock().now();
      queueOffset = getQueueOffsetByKey(encodeResult.queueOffsetKey, tranType);
      encodeResult.setQueueOffsetKey(queueOffset, true);
      BatchAppendEntryRequest request = new BatchAppendEntryRequest();
      request.setGroup(dLedgerConfig.getGroup());
      request.setRemoteId(dLedgerServer.getMemberState().getSelfId());
      request.setBatchMsgs(encodeResult.batchData);
      AppendFuture<AppendEntryResponse> appendFuture =
          (AppendFuture<AppendEntryResponse>) dLedgerServer.handleAppend(request);
      if (appendFuture.getPos() == -1) {
        log.warn("HandleAppend return false due to error code {}", appendFuture.get().getCode());
        return CompletableFuture.completedFuture(
            new PutMessageResult(
                PutMessageStatus.OS_PAGECACHE_BUSY,
                new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
      }
      dledgerFuture = (BatchAppendFuture<AppendEntryResponse>) appendFuture;

      long wroteOffset = 0;

      int msgIdLength =
          (messageExtBatch.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0
              ? 4 + 4 + 8
              : 16 + 4 + 8;
      ByteBuffer buffer = ByteBuffer.allocate(msgIdLength);

      boolean isFirstOffset = true;
      long firstWroteOffset = 0;
      for (long pos : dledgerFuture.getPositions()) {
        wroteOffset = pos + DLedgerEntry.BODY_OFFSET;
        if (isFirstOffset) {
          firstWroteOffset = wroteOffset;
          isFirstOffset = false;
        }
        String msgId =
            MessageDecoder.createMessageId(
                buffer, messageExtBatch.getStoreHostBytes(), wroteOffset);
        if (msgIdBuilder.length() > 0) {
          msgIdBuilder.append(',').append(msgId);
        } else {
          msgIdBuilder.append(msgId);
        }
        msgNum++;
      }

      elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDledgerLock;
      appendResult =
          new AppendMessageResult(
              AppendMessageStatus.PUT_OK,
              firstWroteOffset,
              encodeResult.totalMsgLen,
              msgIdBuilder.toString(),
              System.currentTimeMillis(),
              queueOffset,
              elapsedTimeInLock);
      appendResult.setMsgNum(msgNum);
      DLedgerCommitLog.this.topicQueueTable.put(encodeResult.queueOffsetKey, queueOffset + msgNum);
    } catch (Exception e) {
      log.error("Put message error", e);
      return CompletableFuture.completedFuture(
          new PutMessageResult(
              PutMessageStatus.UNKNOWN_ERROR,
              new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
    } finally {
      beginTimeInDledgerLock = 0;
      putMessageLock.unlock();
    }

    if (elapsedTimeInLock > 500) {
      log.warn(
          "[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
          elapsedTimeInLock,
          messageExtBatch.getBody().length,
          appendResult);
    }

    return dledgerFuture.thenApply(
        appendEntryResponse -> {
          PutMessageStatus putMessageStatus = PutMessageStatus.UNKNOWN_ERROR;
          switch (DLedgerResponseCode.valueOf(appendEntryResponse.getCode())) {
            case SUCCESS:
              putMessageStatus = PutMessageStatus.PUT_OK;
              break;
            case INCONSISTENT_LEADER:
            case NOT_LEADER:
            case LEADER_NOT_READY:
            case DISK_FULL:
              putMessageStatus = PutMessageStatus.SERVICE_NOT_AVAILABLE;
              break;
            case WAIT_QUORUM_ACK_TIMEOUT:
              // Do not return flush_slave_timeout to the client, for the ons client will ignore it.
              putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
              break;
            case LEADER_PENDING_FULL:
              putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
              break;
          }
          PutMessageResult putMessageResult = new PutMessageResult(putMessageStatus, appendResult);
          if (putMessageStatus == PutMessageStatus.PUT_OK) {
            // Statistics
            storeStatsService
                .getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic())
                .add(appendResult.getMsgNum());
            storeStatsService
                .getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic())
                .add(appendResult.getWroteBytes());
          }
          return putMessageResult;
        });
  }

  @Override
  public SelectMappedBufferResult getMessage(final long offset, final int size) {
    if (offset < dividedCommitlogOffset) {
      return super.getMessage(offset, size);
    }
    int mappedFileSize = this.dLedgerServer.getdLedgerConfig().getMappedFileSizeForEntryData();
    MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, offset == 0);
    if (mappedFile != null) {
      int pos = (int) (offset % mappedFileSize);
      return convertSbr(mappedFile.selectMappedBuffer(pos, size));
    }
    return null;
  }

  @Override
  public long rollNextFile(final long offset) {
    int mappedFileSize =
        this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    return offset + mappedFileSize - offset % mappedFileSize;
  }

  @Override
  public HashMap<String, Long> getTopicQueueTable() {
    return topicQueueTable;
  }

  @Override
  public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
    this.topicQueueTable = topicQueueTable;
  }

  @Override
  public void destroy() {
    super.destroy();
    dLedgerFileList.destroy();
  }

  @Override
  public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
    // the old ha service will invoke method, here to prevent it
    return false;
  }

  @Override
  public void checkSelf() {
    dLedgerFileList.checkSelf();
  }

  @Override
  public long lockTimeMills() {
    long diff = 0;
    long begin = this.beginTimeInDledgerLock;
    if (begin > 0) {
      diff = this.defaultMessageStore.now() - begin;
    }

    if (diff < 0) {
      diff = 0;
    }

    return diff;
  }

  /**
   * getQueueOffsetByKey
   *
   * @param key ignore
   * @param tranType ignore
   * @return ignore
   */
  private long getQueueOffsetByKey(String key, int tranType) {
    Long queueOffset = DLedgerCommitLog.this.topicQueueTable.get(key);
    if (null == queueOffset) {
      queueOffset = 0L;
      DLedgerCommitLog.this.topicQueueTable.put(key, queueOffset);
    }

    // Transaction messages that require special handling
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
    return queueOffset;
  }

  /**
   * getdLedgerServer
   *
   * @return ignore
   */
  public DLedgerServer getdLedgerServer() {
    return dLedgerServer;
  }

  /**
   * getId
   *
   * @return ignore
   */
  public int getId() {
    return id;
  }

  /**
   * getDividedCommitlogOffset
   *
   * @return ignore
   */
  public long getDividedCommitlogOffset() {
    return dividedCommitlogOffset;
  }

  /**
   * DLedgerSelectMappedBufferResult
   *
   * @author shui4
   */
  public static class DLedgerSelectMappedBufferResult extends SelectMappedBufferResult {

    /** sbr */
    private SelectMmapBufferResult sbr;

    /**
     * DLedgerSelectMappedBufferResult
     *
     * @param sbr ignore
     */
    public DLedgerSelectMappedBufferResult(SelectMmapBufferResult sbr) {
      super(sbr.getStartOffset(), sbr.getByteBuffer(), sbr.getSize(), null);
      this.sbr = sbr;
    }

    @Override
    public synchronized void release() {
      super.release();
      if (sbr != null) {
        sbr.release();
      }
    }
  }

  /**
   * EncodeResult
   *
   * @author shui4
   */
  class EncodeResult {
    /** queueOffsetKey */
    private String queueOffsetKey;

    /** data */
    private ByteBuffer data;

    /** batchData */
    private List<byte[]> batchData;

    /** status */
    private AppendMessageStatus status;

    /** totalMsgLen */
    private int totalMsgLen;

    /**
     * EncodeResult
     *
     * @param status ignore
     * @param data ignore
     * @param queueOffsetKey ignore
     */
    public EncodeResult(AppendMessageStatus status, ByteBuffer data, String queueOffsetKey) {
      this.data = data;
      this.status = status;
      this.queueOffsetKey = queueOffsetKey;
    }

    /**
     * EncodeResult
     *
     * @param status ignore
     * @param queueOffsetKey ignore
     * @param batchData ignore
     * @param totalMsgLen ignore
     */
    public EncodeResult(
        AppendMessageStatus status,
        String queueOffsetKey,
        List<byte[]> batchData,
        int totalMsgLen) {
      this.batchData = batchData;
      this.status = status;
      this.queueOffsetKey = queueOffsetKey;
      this.totalMsgLen = totalMsgLen;
    }

    /**
     * setQueueOffsetKey
     *
     * @param offset ignore
     * @param isBatch ignore
     */
    public void setQueueOffsetKey(long offset, boolean isBatch) {
      if (!isBatch) {
        this.data.putLong(MessageDecoder.QUEUE_OFFSET_POSITION, offset);
        return;
      }

      for (byte[] data : batchData) {
        ByteBuffer.wrap(data).putLong(MessageDecoder.QUEUE_OFFSET_POSITION, offset++);
      }
    }

    /**
     * getData
     *
     * @return ignore
     */
    public byte[] getData() {
      return data.array();
    }
  }

  /**
   * MessageSerializer
   *
   * @author shui4
   */
  class MessageSerializer {

    /** maxMessageSize */
    // The maximum length of the message
    private final int maxMessageSize;

    /**
     * MessageSerializer
     *
     * @param size ignore
     */
    MessageSerializer(final int size) {
      this.maxMessageSize = size;
    }

    /**
     * serialize
     *
     * @param msgInner ignore
     * @return ignore
     */
    public EncodeResult serialize(final MessageExtBrokerInner msgInner) {
      // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

      // PHY OFFSET
      long wroteOffset = 0;

      long queueOffset = 0;

      int sysflag = msgInner.getSysFlag();

      int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      int storeHostLength =
          (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
      ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

      String key = msgInner.getTopic()+ "-" + msgInner.getQueueId();

      /** Serialize message */
      final byte[] propertiesData =
          msgInner.getPropertiesString() == null
              ? null
              : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

      final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

      if (propertiesLength > Short.MAX_VALUE) {
        log.warn("putMessage message properties length too long. length={}", propertiesData.length);
        return new EncodeResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED, null, key);
      }

      final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
      final int topicLength = topicData.length;

      final int bodyLength = msgInner.getBody()== null ? 0 : msgInner.getBody().length;

      final int msgLen =
          calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

      ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);

      // Exceeds the maximum message
      if (msgLen > this.maxMessageSize) {
        DLedgerCommitLog.log.warn(
            "message size exceeded, msg total size:"
                + msgLen
                + ", msg body size:"
                + bodyLength
                + ", maxMessageSize:"
                + this.maxMessageSize);
        return new EncodeResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, null, key);
      }
      // Initialization of storage space
      this.resetByteBuffer(msgStoreItemMemory, msgLen);
      // 1 TOTALSIZE
      msgStoreItemMemory.putInt(msgLen);
      // 2 MAGICCODE
      msgStoreItemMemory.putInt(DLedgerCommitLog.MESSAGE_MAGIC_CODE);
      // 3 BODYCRC
      msgStoreItemMemory.putInt(msgInner.getBodyCRC());
      // 4 QUEUEID
      msgStoreItemMemory.putInt(msgInner.getQueueId());
      // 5 FLAG
      msgStoreItemMemory.putInt(msgInner.getFlag());
      // 6 QUEUEOFFSET
      msgStoreItemMemory.putLong(queueOffset);
      // 7 PHYSICALOFFSET
      msgStoreItemMemory.putLong(wroteOffset);
      // 8 SYSFLAG
      msgStoreItemMemory.putInt(msgInner.getSysFlag());
      // 9 BORNTIMESTAMP
      msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
      // 10 BORNHOST
      resetByteBuffer(bornHostHolder, bornHostLength);
      msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
      // 11 STORETIMESTAMP
      msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
      // 12 STOREHOSTADDRESS
      resetByteBuffer(storeHostHolder, storeHostLength);
      msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
      // this.msgBatchMemory.put(msgInner.getStoreHostBytes());
      // 13 RECONSUMETIMES
      msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
      // 14 Prepared Transaction Offset
      msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
      // 15 BODY
      msgStoreItemMemory.putInt(bodyLength);
      if (bodyLength > 0) {
        msgStoreItemMemory.put(msgInner.getBody());
      }
      // 16 TOPIC
      msgStoreItemMemory.put((byte) topicLength);
      msgStoreItemMemory.put(topicData);
      // 17 PROPERTIES
      msgStoreItemMemory.putShort((short) propertiesLength);
      if (propertiesLength > 0) {
        msgStoreItemMemory.put(propertiesData);
      }
      return new EncodeResult(AppendMessageStatus.PUT_OK, msgStoreItemMemory, key);
    }

    /**
     * serialize
     *
     * @param messageExtBatch ignore
     * @return ignore
     */
    public EncodeResult serialize(final MessageExtBatch messageExtBatch) {
      String key = messageExtBatch.getTopic()+ "-" + messageExtBatch.getQueueId();

      int totalMsgLen = 0;
      ByteBuffer messagesByteBuff = messageExtBatch.wrap();
      List<byte[]> batchBody = new LinkedList<>();

      int sysFlag = messageExtBatch.getSysFlag();
      int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      int storeHostLength =
          (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
      ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
      ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

      while (messagesByteBuff.hasRemaining()) {
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

        final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

        final int topicLength = topicData.length;

        final int msgLen =
            calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, propertiesLen);
        ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);

        // Exceeds the maximum message
        if (msgLen > this.maxMessageSize) {
          CommitLog.log.warn(
              "message size exceeded, msg total size:"
                  + msgLen
                  + ", msg body size:"
                  + bodyLen
                  + ", maxMessageSize:"
                  + this.maxMessageSize);
          throw new RuntimeException("message size exceeded");
        }

        totalMsgLen += msgLen;
        // Determines whether there is sufficient free space
        if (totalMsgLen > maxMessageSize) {
          throw new RuntimeException("message size exceeded");
        }

        // Initialization of storage space
        this.resetByteBuffer(msgStoreItemMemory, msgLen);
        // 1 TOTALSIZE
        msgStoreItemMemory.putInt(msgLen);
        // 2 MAGICCODE
        msgStoreItemMemory.putInt(DLedgerCommitLog.MESSAGE_MAGIC_CODE);
        // 3 BODYCRC
        msgStoreItemMemory.putInt(bodyCrc);
        // 4 QUEUEID
        msgStoreItemMemory.putInt(messageExtBatch.getQueueId());
        // 5 FLAG
        msgStoreItemMemory.putInt(flag);
        // 6 QUEUEOFFSET
        msgStoreItemMemory.putLong(0L);
        // 7 PHYSICALOFFSET
        msgStoreItemMemory.putLong(0);
        // 8 SYSFLAG
        msgStoreItemMemory.putInt(messageExtBatch.getSysFlag());
        // 9 BORNTIMESTAMP
        msgStoreItemMemory.putLong(messageExtBatch.getBornTimestamp());
        // 10 BORNHOST
        resetByteBuffer(bornHostHolder, bornHostLength);
        msgStoreItemMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
        // 11 STORETIMESTAMP
        msgStoreItemMemory.putLong(messageExtBatch.getStoreTimestamp());
        // 12 STOREHOSTADDRESS
        resetByteBuffer(storeHostHolder, storeHostLength);
        msgStoreItemMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
        // 13 RECONSUMETIMES
        msgStoreItemMemory.putInt(messageExtBatch.getReconsumeTimes());
        // 14 Prepared Transaction Offset
        msgStoreItemMemory.putLong(0);
        // 15 BODY
        msgStoreItemMemory.putInt(bodyLen);
        if (bodyLen > 0) {
          msgStoreItemMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
        }
        // 16 TOPIC
        msgStoreItemMemory.put((byte) topicLength);
        msgStoreItemMemory.put(topicData);
        // 17 PROPERTIES
        msgStoreItemMemory.putShort(propertiesLen);
        if (propertiesLen > 0) {
          msgStoreItemMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
        }
        byte[] data = new byte[msgLen];
        msgStoreItemMemory.clear();
        msgStoreItemMemory.get(data);
        batchBody.add(data);
      }

      return new EncodeResult(AppendMessageStatus.PUT_OK, key, batchBody, totalMsgLen);
    }

    /**
     * resetByteBuffer
     *
     * @param byteBuffer ignore
     * @param limit ignore
     */
    private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
      byteBuffer.flip();
      byteBuffer.limit(limit);
    }
  }
}