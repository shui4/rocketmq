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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** 文件存储 API，其它模块对于消息存储都是通过它进行 */
public class DefaultMessageStore implements MessageStore {
  /** 日志 */
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

  /** 消息存储配置 */
  private final MessageStoreConfig messageStoreConfig;
  /** 提交日志 */
  // CommitLog
  private final CommitLog commitLog;

  /** 消息队列存储缓存表， 按消息主题分组 */
  private final ConcurrentMap<
          String /* topic */, ConcurrentMap<Integer /* queueId */, ConsumeQueue>>
      consumeQueueTable;

  /** {@link ConsumeQueue} 文件刷盘线程 */
  private final FlushConsumeQueueService flushConsumeQueueService;

  /** 清除 CommitLog 文件服务 */
  private final CleanCommitLogService cleanCommitLogService;

  /** 清除 ConsumeQueue 文件服务 */
  private final CleanConsumeQueueService cleanConsumeQueueService;

  /** Index 文件服务 */
  private final IndexService indexService;

  /** MappedFile 分配服务 */
  private final AllocateMappedFileService allocateMappedFileService;

  /** CommitLog 消息分发，根据 CommitLog 文件构建 ConsumeQueue 、 Index 文件 */
  private final ReputMessageService reputMessageService;

  /** 存储高可用机制 */
  private final HAService haService;

  /** 消息调度服务 */
  private final ScheduleMessageService scheduleMessageService;

  /** 商店统计服务 */
  private final StoreStatsService storeStatsService;

  /** 消息堆内存缓存池 */
  private final TransientStorePool transientStorePool;

  /** 运行标志 */
  private final RunningFlags runningFlags = new RunningFlags();
  /** 系统时钟 */
  private final SystemClock systemClock = new SystemClock();

  /** 计划执行服务 */
  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
  /** 经纪人统计管理器 */
  private final BrokerStatsManager brokerStatsManager;
  /** 在消息拉取长轮询模式下的消息达到监听器 */
  private final MessageArrivingListener messageArrivingListener;

  /** Broker 配置属性 */
  private final BrokerConfig brokerConfig;
  /** lmq 消耗队列编号 */
  private final AtomicInteger lmqConsumeQueueNum = new AtomicInteger(0);
  /** CommitLog 文件转发请求 */
  private final LinkedList<CommitLogDispatcher> dispatcherList;
  /** 磁盘检查计划执行器服务 */
  private final ScheduledExecutorService diskCheckScheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));
  /** 正常停机 */
  boolean shutDownNormal = false;
  /** 停机 */
  private volatile boolean shutdown = true;
  /** 文件刷盘监测点 */
  private StoreCheckpoint storeCheckpoint;
  /** 打印次数 */
  private AtomicLong printTimes = new AtomicLong(0);
  /** 锁定文件 */
  private RandomAccessFile lockFile;
  /** 锁 */
  private FileLock lock;

  /**
   * 默认消息存储
   *
   * @param messageStoreConfig 消息存储配置
   * @param brokerStatsManager 经纪人统计管理器
   * @param messageArrivingListener 消息到达侦听器
   * @param brokerConfig 代理配置
   * @throws IOException IO 异常
   */
  public DefaultMessageStore(
      final MessageStoreConfig messageStoreConfig,
      final BrokerStatsManager brokerStatsManager,
      final MessageArrivingListener messageArrivingListener,
      final BrokerConfig brokerConfig)
      throws IOException {
    this.messageArrivingListener = messageArrivingListener;
    this.brokerConfig = brokerConfig;
    this.messageStoreConfig = messageStoreConfig;
    this.brokerStatsManager = brokerStatsManager;
    this.allocateMappedFileService = new AllocateMappedFileService(this);
    if (messageStoreConfig.isEnableDLegerCommitLog()) {
      this.commitLog = new DLedgerCommitLog(this);
    } else {
      this.commitLog = new CommitLog(this);
    }
    this.consumeQueueTable = new ConcurrentHashMap<>(32);

    this.flushConsumeQueueService = new FlushConsumeQueueService();
    this.cleanCommitLogService = new CleanCommitLogService();
    this.cleanConsumeQueueService = new CleanConsumeQueueService();
    this.storeStatsService = new StoreStatsService();
    this.indexService = new IndexService(this);
    if (!messageStoreConfig.isEnableDLegerCommitLog()) {
      this.haService = new HAService(this);
    } else {
      this.haService = null;
    }
    this.reputMessageService = new ReputMessageService();

    this.scheduleMessageService = new ScheduleMessageService(this);

    this.transientStorePool = new TransientStorePool(messageStoreConfig);

    if (messageStoreConfig.isTransientStorePoolEnable()) {
      this.transientStorePool.init();
    }

    this.allocateMappedFileService.start();

    this.indexService.start();

    this.dispatcherList = new LinkedList<>();
    this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
    this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

    File file =
        new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
    MappedFile.ensureDirOK(file.getParent());
    MappedFile.ensureDirOK(getStorePathPhysic());
    MappedFile.ensureDirOK(getStorePathLogic());
    lockFile = new RandomAccessFile(file, "rw");
  }

  /**
   * 获取存储路径物理
   *
   * @return {@link String}
   */
  public String getStorePathPhysic() {
    String storePathPhysic;
    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
      storePathPhysic =
          ((DLedgerCommitLog) DefaultMessageStore.this.getCommitLog())
              .getdLedgerServer()
              .getdLedgerConfig()
              .getDataStorePath();
    } else {
      storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
    }
    return storePathPhysic;
  }

  /**
   * 获取存储路径逻辑
   *
   * @return {@link String}
   */
  public String getStorePathLogic() {
    return StorePathConfigHelper.getStorePathConsumeQueue(
        this.messageStoreConfig.getStorePathRootDir());
  }

  /**
   * 获取消息存储配置
   *
   * @return {@link MessageStoreConfig}
   */
  public MessageStoreConfig getMessageStoreConfig() {
    return messageStoreConfig;
  }

  /**
   * 获取提交日志
   *
   * @return {@link CommitLog}
   */
  public CommitLog getCommitLog() {
    return commitLog;
  }

  /**
   * ConsumeQueue 截断脏逻辑文件
   *
   * @param phyOffset phy 偏移
   */
  public void truncateDirtyLogicFiles(long phyOffset) {
    ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables =
        DefaultMessageStore.this.consumeQueueTable;

    for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
      for (ConsumeQueue logic : maps.values()) {
        logic.truncateDirtyLogicFiles(phyOffset);
      }
    }
  }

  /**
   * 负载
   *
   * @return boolean
   */
  public boolean load() {
    boolean result = true;

    try {
      // 代码清单 4-58
      // ? ${ROCKET_HOME}/store/abort 不存在
      // 在退出时通过注册 JVM 钩子函数删除 abort 文件，所以存在表示异常退出，可能 CommitLog 与 ConsumeQueue 数据有可能不一致
      boolean lastExitOK = !this.isTempFileExist();
      log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

      // 加载 Commit Log
      result = result && this.commitLog.load();

      // 加载 ConsumeQueue
      result = result && this.loadConsumeQueue();
      // ? 上面都成功
      if (result) {
        // 加载并存储 checkpoint 文件，主要用于记录 CommitLog 文件、ConsumeQueue 文件、Index 文件的刷盘点
        this.storeCheckpoint =
            new StoreCheckpoint(
                StorePathConfigHelper.getStoreCheckpoint(
                    this.messageStoreConfig.getStorePathRootDir()));
        // 加载 Index 文件
        this.indexService.load(lastExitOK);
        // 恢复
        this.recover(lastExitOK);

        log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
        // 代码清单 4-59
        if (null != scheduleMessageService) {
          result = this.scheduleMessageService.load();
        }
      }

    } catch (Exception e) {
      log.error("load exception", e);
      result = false;
    }

    if (!result) {
      this.allocateMappedFileService.shutdown();
    }

    return result;
  }

  /**
   * 临时文件是否存在
   *
   * @return boolean
   */
  private boolean isTempFileExist() {
    String fileName =
        StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
    File file = new File(fileName);
    return file.exists();
  }

  /**
   * 加载 {@link ConsumeQueue}
   *
   * @return boolean
   */
  private boolean loadConsumeQueue() {
    // basedir/store/consumequeue
    File dirLogic =
        new File(
            StorePathConfigHelper.getStorePathConsumeQueue(
                this.messageStoreConfig.getStorePathRootDir()));
    // tree basedir/store/consumequeue
    //  consumequeue
    //      └── TopicTest
    //      └── ...more
    // 获取 Topic 目录
    File[] fileTopicList = dirLogic.listFiles();
    if (fileTopicList != null) {

      for (File fileTopic : fileTopicList) {
        String topic = fileTopic.getName();
        // TopicTest
        // ├── 0
        // ├── 1
        // ├── 2
        // └── ...more
        // 获取 Topic 下的队列目录
        File[] fileQueueIdList = fileTopic.listFiles();
        if (fileQueueIdList != null) {
          for (File fileQueueId : fileQueueIdList) {
            int queueId;
            try {
              queueId = Integer.parseInt(fileQueueId.getName());
            } catch (NumberFormatException e) {
              continue;
            }
            // 创建 ConsumeQueue
            ConsumeQueue logic =
                new ConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueue(
                        this.messageStoreConfig.getStorePathRootDir()),
                    this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                    this);
            // put consumeQueueTable
            this.putConsumeQueue(topic, queueId, logic);
            // 加载 ConsumeQueue
            if (!logic.load()) {
              return false;
            }
          }
        }
      }
    }

    log.info("load logics queue all over, OK");

    return true;
  }

  /**
   * 恢复 <br>
   * 存储启动时所谓的文件恢复主要完成 flushedPosition 、 committedWhere 指针的设置、将消息消费队列最大偏移量加载到内 存，并删除 flushedPosition
   * 之后所有的文件。如果 Broker 异常停止， 在文件恢复过程中， RocketMQ 会将最后一个有效文件中的所有消息重 新转发到 ConsumeQueue 和 Index
   * 文件中，确保不丢失消息，但同时会带 来消息重复的问题。纵观 RocketMQ 的整体设计思想， RocketMQ 保证消息 不丢失但不保证消息不会重复消费，故消息消费业务方需要实现消息
   * 消费的幂等设计
   *
   * @param lastExitOK 最后一个出口正常
   */
  private void recover(final boolean lastExitOK) {
    // 恢复 ConsumeQueue
    long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();
    // Broker 正常停止文件恢复
    if (lastExitOK) {
      this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
    } else {
      this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
    }

    this.recoverTopicQueueTable();
  }

  /**
   * 获取最大 phy 偏移
   *
   * @return long
   */
  @Override
  public long getMaxPhyOffset() {
    return this.commitLog.getMaxOffset();
  }

  /**
   * 放置消费队列
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @param consumeQueue 消费队列
   */
  private void putConsumeQueue(
      final String topic, final int queueId, final ConsumeQueue consumeQueue) {
    ConcurrentMap<Integer /* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
    if (null == map) {
      map = new ConcurrentHashMap<Integer /* queueId */, ConsumeQueue>();
      map.put(queueId, consumeQueue);
      this.consumeQueueTable.put(topic, map);
      if (MixAll.isLmq(topic)) {
        this.lmqConsumeQueueNum.getAndIncrement();
      }
    } else {
      map.put(queueId, consumeQueue);
    }
  }

  /**
   * 恢复消费队列
   *
   * @return long
   */
  private long recoverConsumeQueue() {
    long maxPhysicOffset = -1;
    for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
      for (ConsumeQueue logic : maps.values()) {
        logic.recover();
        if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
          maxPhysicOffset = logic.getMaxPhysicOffset();
        }
      }
    }

    return maxPhysicOffset;
  }

  /** 恢复主题队列表 */
  public void recoverTopicQueueTable() {
    HashMap<String /* topic-queueid */, Long /* offset */> table = new HashMap<String, Long>(1024);
    long minPhyOffset = this.commitLog.getMinOffset();
    for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
      for (ConsumeQueue logic : maps.values()) {
        String key = logic.getTopic() + "-" + logic.getQueueId();
        table.put(key, logic.getMaxOffsetInQueue());
        // 正确的最小偏移量
        logic.correctMinOffset(minPhyOffset);
      }
    }

    this.commitLog.setTopicQueueTable(table);
  }

  public void start() throws Exception {

    lock = lockFile.getChannel().tryLock(0, 1, false);
    if (lock == null || lock.isShared() || !lock.isValid()) {
      throw new RuntimeException("Lock failed,MQ already started");
    }

    lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
    lockFile.getChannel().force(true);
    {
      /**
       * 1. Make sure the fast-forward messages to be truncated during the recovering according to
       * the max physical offset of the commitlog; 2. DLedger committedPos may be missing, so the
       * maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just
       * let it go; 3. Calculate the reput offset according to the consume queue; 4. Make sure the
       * fall-behind messages to be dispatched before starting the commitlog, especially when the
       * broker role are automatically changed.
       */
      long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
      for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
        for (ConsumeQueue logic : maps.values()) {
          if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
            maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
          }
        }
      }
      if (maxPhysicalPosInLogicQueue < 0) {
        maxPhysicalPosInLogicQueue = 0;
      }
      if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
        maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
        /**
         * This happens in following conditions: 1. If someone removes all the consumequeue files or
         * the disk get damaged. 2. Launch a new broker, and copy the commitlog from other brokers.
         *
         * <p>All the conditions has the same in common that the maxPhysicalPosInLogicQueue should
         * be 0. If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
         */
        log.warn(
            "[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}",
            maxPhysicalPosInLogicQueue,
            this.commitLog.getMinOffset());
      }
      log.info(
          "[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
          maxPhysicalPosInLogicQueue,
          this.commitLog.getMinOffset(),
          this.commitLog.getMaxOffset(),
          this.commitLog.getConfirmOffset());
      // 从哪里开始， maxPhysicalPosInLogicQueue 为 CommitLog 第一个文件的文件名称即最小偏移量
      this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);
      // 启动转发服务线程
      this.reputMessageService.start();

      /**
       * 1. Finish dispatching the messages fall behind, then to start other services. 2. DLedger
       * committedPos may be missing, so here just require dispatchBehindBytes <= 0
       */
      while (true) {
        if (dispatchBehindBytes() <= 0) {
          break;
        }
        Thread.sleep(1000);
        log.info(
            "Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}",
            this.reputMessageService.getReputFromOffset(),
            this.getMaxPhyOffset(),
            this.dispatchBehindBytes());
      }
      this.recoverTopicQueueTable();
    }

    if (!messageStoreConfig.isEnableDLegerCommitLog()) {
      this.haService.start();
      this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
    }

    this.flushConsumeQueueService.start();
    this.commitLog.start();
    this.storeStatsService.start();
    // 创建 abort 文件
    this.createTempFile();
    // 添加定时任务
    this.addScheduleTask();
    this.shutdown = false;
  }

  /**
   * 字节后调度
   *
   * @return long
   */
  @Override
  public long dispatchBehindBytes() {
    return this.reputMessageService.behind();
  }

  /**
   * 处理计划消息服务
   *
   * @param brokerRole 经纪人角色
   */
  @Override
  public void handleScheduleMessageService(final BrokerRole brokerRole) {
    if (this.scheduleMessageService != null) {
      if (brokerRole == BrokerRole.SLAVE) {
        this.scheduleMessageService.shutdown();
      } else {
        this.scheduleMessageService.start();
      }
    }
  }

  /**
   * 创建临时文件
   *
   * @throws IOException IO 异常
   */
  private void createTempFile() throws IOException {
    String fileName =
        StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
    File file = new File(fileName);
    MappedFile.ensureDirOK(file.getParent());
    boolean result = file.createNewFile();
    log.info(fileName + (result ? "create OK" : "already exists"));
  }

  /** 添加定时任务 */
  private void addScheduleTask() {
    // 默认 10 秒 this.messageStoreConfig.getCleanResourceInterval(),
    // 定期清理文件
    this.scheduledExecutorService.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            DefaultMessageStore.this.cleanFilesPeriodically();
          }
        },
        1000 * 60,
        this.messageStoreConfig.getCleanResourceInterval(),
        TimeUnit.MILLISECONDS);

    this.scheduledExecutorService.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            DefaultMessageStore.this.checkSelf();
          }
        },
        1,
        10,
        TimeUnit.MINUTES);

    this.scheduledExecutorService.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
              try {
                if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                  long lockTime =
                      System.currentTimeMillis()
                          - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                  if (lockTime > 1000 && lockTime < 10000000) {

                    String stack = UtilAll.jstack();
                    final String fileName =
                        System.getProperty("user.home")
                            + File.separator
                            + "debug/lock/stack-"
                            + DefaultMessageStore.this.commitLog.getBeginTimeInLock()
                            + "-"
                            + lockTime;
                    MixAll.string2FileNotSafe(stack, fileName);
                  }
                }
              } catch (Exception e) {
              }
            }
          }
        },
        1,
        1,
        TimeUnit.SECONDS);

    // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
    // @Override
    // public void run() {
    // DefaultMessageStore.this.cleanExpiredConsumerQueue();
    // }
    // }, 1, 1, TimeUnit.HOURS);
    this.diskCheckScheduledExecutorService.scheduleAtFixedRate(
        new Runnable() {
          public void run() {
            DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();
          }
        },
        1000L,
        10000L,
        TimeUnit.MILLISECONDS);
  }

  /** 定期清理文件 */
  private void cleanFilesPeriodically() {
    // 执行清理 commit log 任务
    this.cleanCommitLogService.run();
    // 执行清理 consume queue 任务
    this.cleanConsumeQueueService.run();
  }

  /** 自我检查 */
  private void checkSelf() {
    this.commitLog.checkSelf();

    Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it =
        this.consumeQueueTable.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
      Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
      while (itNext.hasNext()) {
        Entry<Integer, ConsumeQueue> cq = itNext.next();
        cq.getValue().checkSelf();
      }
    }
  }

  /** 停机 */
  public void shutdown() {
    if (!this.shutdown) {
      this.shutdown = true;

      this.scheduledExecutorService.shutdown();
      this.diskCheckScheduledExecutorService.shutdown();
      try {

        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.error("shutdown Exception,", e);
      }

      if (this.scheduleMessageService != null) {
        this.scheduleMessageService.shutdown();
      }
      if (this.haService != null) {
        this.haService.shutdown();
      }

      this.storeStatsService.shutdown();
      this.indexService.shutdown();
      this.commitLog.shutdown();
      this.reputMessageService.shutdown();
      this.flushConsumeQueueService.shutdown();
      this.allocateMappedFileService.shutdown();
      this.storeCheckpoint.flush();
      this.storeCheckpoint.shutdown();

      if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
        this.deleteFile(
            StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        shutDownNormal = true;
      } else {
        log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
      }
    }

    this.transientStorePool.destroy();

    if (lockFile != null && lock != null) {
      try {
        lock.release();
        lockFile.close();
      } catch (IOException e) {
      }
    }
  }

  /**
   * 删除文件
   *
   * @param fileName 文件名
   */
  private void deleteFile(final String fileName) {
    File file = new File(fileName);
    boolean result = file.delete();
    log.info(fileName + (result ? "delete OK" : "delete Failed"));
  }

  /** 摧毁 */
  public void destroy() {
    this.destroyLogics();
    this.commitLog.destroy();
    this.indexService.destroy();
    this.deleteFile(
        StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
    this.deleteFile(
        StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
  }

  /** 销毁 ConsumeQueue 表 */
  public void destroyLogics() {
    for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
      for (ConsumeQueue logic : maps.values()) {
        logic.destroy();
      }
    }
  }

  /**
   * 放置消息
   *
   * @param msg 消息
   * @return {@link PutMessageResult}
   */
  @Override
  public PutMessageResult putMessage(MessageExtBrokerInner msg) {
    return waitForPutResult(asyncPutMessage(msg));
  }

  /**
   * 等待结果
   *
   * @param putMessageResultFuture 将来放置消息结果
   * @return {@link PutMessageResult}
   */
  private PutMessageResult waitForPutResult(
      CompletableFuture<PutMessageResult> putMessageResultFuture) {
    try {
      int putMessageTimeout =
          Math.max(
                  this.messageStoreConfig.getSyncFlushTimeout(),
                  this.messageStoreConfig.getSlaveTimeout())
              + 5000;
      return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
    } catch (TimeoutException e) {
      log.error(
          "usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and"
              + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc"
              + "process hangs or other unexpected situations.");
      return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
    }
  }

  /**
   * 异步放置消息
   *
   * @param msg 消息
   * @return {@link CompletableFuture}<{@link PutMessageResult}>
   */
  @Override
  public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
    // region 检查状态
    PutMessageStatus checkStoreStatus = this.checkStoreStatus();
    if (checkStoreStatus != PutMessageStatus.PUT_OK) {
      return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
    }

    PutMessageStatus msgCheckStatus = this.checkMessage(msg);
    if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
      return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
    }

    PutMessageStatus lmqMsgCheckStatus = this.checkLmqMessage(msg);
    if (msgCheckStatus == PutMessageStatus.LMQ_CONSUME_QUEUE_NUM_EXCEEDED) {
      return CompletableFuture.completedFuture(new PutMessageResult(lmqMsgCheckStatus, null));
    }
    // endregion

    long beginTime = this.getSystemClock().now();
    CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

    putResultFuture.thenAccept(
        (result) -> {
          // 经过时间
          long elapsedTime = this.getSystemClock().now() - beginTime;
          if (elapsedTime > 500) {
            log.warn(
                "putMessage not in lock elapsed time(ms)={}, bodyLength={}",
                elapsedTime,
                msg.getBody().length);
          }
          // 设置放置消息整个时间最大值
          this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

          if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().add(1);
          }
        });

    return putResultFuture;
  }

  /**
   * 检查存储状态
   *
   * @return {@link PutMessageStatus}
   */
  private PutMessageStatus checkStoreStatus() {
    if (this.shutdown) {
      log.warn("message store has shutdown, so putMessage is forbidden");
      return PutMessageStatus.SERVICE_NOT_AVAILABLE;
    }

    if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
      long value = this.printTimes.getAndIncrement();
      if ((value % 50000) == 0) {
        log.warn("broke role is slave, so putMessage is forbidden");
      }
      return PutMessageStatus.SERVICE_NOT_AVAILABLE;
    }

    if (!this.runningFlags.isWriteable()) {
      long value = this.printTimes.getAndIncrement();
      if ((value % 50000) == 0) {
        log.warn(
            "the message store is not writable. It may be caused by one of the following reasons:"
                + "the broker's disk is full, write to logic queue error, write to index file error, etc");
      }
      return PutMessageStatus.SERVICE_NOT_AVAILABLE;
    } else {
      this.printTimes.set(0);
    }

    if (this.isOSPageCacheBusy()) {
      return PutMessageStatus.OS_PAGECACHE_BUSY;
    }
    return PutMessageStatus.PUT_OK;
  }

  /**
   * 检查消息
   *
   * @param msg 消息
   * @return {@link PutMessageStatus}
   */
  private PutMessageStatus checkMessage(MessageExtBrokerInner msg) {
    if (msg.getTopic().length() > Byte.MAX_VALUE) {
      log.warn("putMessage message topic length too long" + msg.getTopic().length());
      return PutMessageStatus.MESSAGE_ILLEGAL;
    }

    if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
      log.warn(
          "putMessage message properties length too long" + msg.getPropertiesString().length());
      return PutMessageStatus.MESSAGE_ILLEGAL;
    }
    return PutMessageStatus.PUT_OK;
  }

  /**
   * 检查 lmq 消息
   *
   * @param msg 消息
   * @return {@link PutMessageStatus}
   */
  private PutMessageStatus checkLmqMessage(MessageExtBrokerInner msg) {
    if (msg.getProperties() != null
        && StringUtils.isNotBlank(msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))
        && this.isLmqConsumeQueueNumExceeded()) {
      return PutMessageStatus.LMQ_CONSUME_QUEUE_NUM_EXCEEDED;
    }
    return PutMessageStatus.PUT_OK;
  }

  /**
   * 获取系统时钟
   *
   * @return {@link SystemClock}
   */
  public SystemClock getSystemClock() {
    return systemClock;
  }

  /**
   * ospage 缓存忙吗
   *
   * @return boolean
   */
  @Override
  public boolean isOSPageCacheBusy() {
    long begin = this.getCommitLog().getBeginTimeInLock();
    long diff = this.systemClock.now() - begin;

    return diff < 10000000 && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
  }

  /**
   * 是否超过 lmq 消费队列编号
   *
   * @return boolean
   */
  private boolean isLmqConsumeQueueNumExceeded() {
    if (this.getMessageStoreConfig().isEnableLmq()
        && this.getMessageStoreConfig().isEnableMultiDispatch()
        && this.lmqConsumeQueueNum.get() > this.messageStoreConfig.getMaxLmqConsumeQueueNum()) {
      return true;
    }
    return false;
  }

  /**
   * 放置消息
   *
   * @param messageExtBatch 消息扩展批处理
   * @return {@link PutMessageResult}
   */
  @Override
  public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
    return waitForPutResult(asyncPutMessages(messageExtBatch));
  }

  /**
   * 异步放置消息
   *
   * @param messageExtBatch 消息扩展批处理
   * @return {@link CompletableFuture}<{@link PutMessageResult}>
   */
  public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
    PutMessageStatus checkStoreStatus = this.checkStoreStatus();
    if (checkStoreStatus != PutMessageStatus.PUT_OK) {
      return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
    }

    PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
    if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
      return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
    }

    long beginTime = this.getSystemClock().now();
    CompletableFuture<PutMessageResult> resultFuture =
        this.commitLog.asyncPutMessages(messageExtBatch);

    resultFuture.thenAccept(
        (result) -> {
          long elapsedTime = this.getSystemClock().now() - beginTime;
          if (elapsedTime > 500) {
            log.warn(
                "not in lock elapsed time(ms)={}, bodyLength={}",
                elapsedTime,
                messageExtBatch.getBody().length);
          }

          this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

          if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().add(1);
          }
        });

    return resultFuture;
  }

  /**
   * 检查消息
   *
   * @param messageExtBatch 消息扩展批处理
   * @return {@link PutMessageStatus}
   */
  private PutMessageStatus checkMessages(MessageExtBatch messageExtBatch) {
    if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
      log.warn("putMessage message topic length too long" + messageExtBatch.getTopic().length());
      return PutMessageStatus.MESSAGE_ILLEGAL;
    }

    if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
      log.warn("PutMessages body length too long" + messageExtBatch.getBody().length);
      return PutMessageStatus.MESSAGE_ILLEGAL;
    }

    return PutMessageStatus.PUT_OK;
  }

  /**
   * 锁定时间磨机
   *
   * @return long
   */
  @Override
  public long lockTimeMills() {
    return this.commitLog.lockTimeMills();
  }

  @Override
  public GetMessageResult getMessage(
      final String group,
      final String topic,
      final int queueId,
      final long offset,
      final int maxMsgNums,
      final MessageFilter messageFilter) {
    // region 健壮性代码
    if (this.shutdown) {
      log.warn("message store has shutdown, so getMessage is forbidden");
      return null;
    }

    if (!this.runningFlags.isReadable()) {
      log.warn(
          "message store is not readable, so getMessage is forbidden"
              + this.runningFlags.getFlagBits());
      return null;
    }

    if (MixAll.isLmq(topic) && this.isLmqConsumeQueueNumExceeded()) {
      log.warn(
          "message store is not available, broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num");
      return null;
    }
    // endregion

    long beginTime = this.getSystemClock().now();

    GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
    long nextBeginOffset = offset;
    long minOffset = 0;
    long maxOffset = 0;

    // lazy init when find msg.
    GetMessageResult getResult = null;
    // 代表当前主服务器消息存储文件的最大偏移量
    final long maxOffsetPy = this.commitLog.getMaxOffset();

    ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
    if (consumeQueue != null) {
      // 消息偏移量异常情况校对下一次拉取偏移量
      minOffset = consumeQueue.getMinOffsetInQueue();
      maxOffset = consumeQueue.getMaxOffsetInQueue();
      // 1）表示当前消费队列中没有消息，拉取结果为 NO_MESSAGE_IN_QUEUE 。如果当前 Broker 为主节点，下次拉取偏移量为
      // 0 。如果当前 Broker 为从节点并且 offsetCheckInSlave 为 true ，设置下
      // 次拉取偏移量为 0 。其他情况下次拉取时使用原偏移量
      if (maxOffset == 0) {
        status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        nextBeginOffset = nextOffsetCorrection(offset, 0);
      }
      // 2）表示待拉取消息偏移量小于队列的起始偏 移量，拉取结果为 OFFSET_TOO_SMALL 。如果当前 Broker 为主节点，下
      // 次拉取偏移量为队列的最小偏移量。如果当前 Broker 为从节点并且
      // offsetCheckInSlave 为 true ，下次拉取偏移量为队列的最小偏移量。
      // 其他情况下次拉取时使用原偏移量
      else if (offset < minOffset) {
        status = GetMessageStatus.OFFSET_TOO_SMALL;
        nextBeginOffset = nextOffsetCorrection(offset, minOffset);
      }
      // 3）如果待拉取偏移量等于队列最大偏移量，拉取结果为 OFFSET_OVERFLOW_ONE ，则下次拉取偏移量依然为 offset
      else if (offset == maxOffset) {
        status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
        nextBeginOffset = nextOffsetCorrection(offset, offset);
      }
      // 4）表示偏移量越界，拉取结果为 OFFSET_OVERFLOW_BADLY 。此时需要考虑当前队列的偏移量是否为 0 ，如果当前队列的最小偏移量为 0
      // ，则使用最小偏移量纠正下次拉取偏移
      // 量。如果当前队列的最小偏移量不为 0 ，则使用该队列的最大偏移量来
      // 纠正下次拉取偏移量。纠正逻辑与 1 ）、 2 ）相同
      else if (offset > maxOffset) {
        status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
        if (0 == minOffset) {
          nextBeginOffset = nextOffsetCorrection(offset, minOffset);
        } else {
          nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
        }
      }
      // 5）如果待拉取偏移量大于 minOffset 并且小于 maxOffset，从当前 offset 处尝试拉取 32 条消息
      else {
        SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
        if (bufferConsumeQueue != null) {
          try {
            status = GetMessageStatus.NO_MATCHED_MESSAGE;

            long nextPhyFileStartOffset = Long.MIN_VALUE;
            // 此次拉取消息的最大偏移量
            long maxPhyOffsetPulling = 0;

            int i = 0;
            final int maxFilterMessageCount =
                Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
            final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();

            getResult = new GetMessageResult(maxMsgNums);

            ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
            for (;
                i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount;
                i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
              long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
              int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
              long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

              maxPhyOffsetPulling = offsetPy;

              if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                if (offsetPy < nextPhyFileStartOffset) continue;
              }

              boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

              if (this.isTheBatchFull(
                  sizePy,
                  maxMsgNums,
                  getResult.getBufferTotalSize(),
                  getResult.getMessageCount(),
                  isInDisk)) {
                break;
              }

              boolean extRet = false, isTagsCodeLegal = true;
              if (consumeQueue.isExtAddr(tagsCode)) {
                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                if (extRet) {
                  tagsCode = cqExtUnit.getTagsCode();
                } else {
                  // can't find ext content.Client will filter messages by tag also.
                  log.error(
                      "[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                      tagsCode,
                      offsetPy,
                      sizePy,
                      topic,
                      group);
                  isTagsCodeLegal = false;
                }
              }
              // 根据偏移量拉取消息后，首先根据 ConsumeQueue 条目进行消息过滤，如果不匹配则直接跳过该条消息，继续拉取下一条消
              if (messageFilter != null
                  && !messageFilter.isMatchedByConsumeQueue(
                      isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                if (getResult.getBufferTotalSize() == 0) {
                  status = GetMessageStatus.NO_MATCHED_MESSAGE;
                }

                continue;
              }

              SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
              if (null == selectResult) {
                if (getResult.getBufferTotalSize() == 0) {
                  status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                }

                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                continue;
              }
              // 如果消息根据 ConsumeQueue 条目进行过滤，则需要从 CommitLog 文件中加载整个消息体，然后根据属性进行过滤。当然如果 过滤方式是 TAG
              // 模式，该方法默认返回 true
              if (messageFilter != null
                  && !messageFilter.isMatchedByCommitLog(
                      selectResult.getByteBuffer().slice(), null)) {
                if (getResult.getBufferTotalSize() == 0) {
                  status = GetMessageStatus.NO_MATCHED_MESSAGE;
                }
                // release...
                selectResult.release();
                continue;
              }

              this.storeStatsService.getGetMessageTransferedMsgCount().add(1);
              getResult.addMessage(selectResult);
              status = GetMessageStatus.FOUND;
              nextPhyFileStartOffset = Long.MIN_VALUE;
            }

            if (diskFallRecorded) {
              long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
              brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
            }

            nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            // 对于 PullMesageService 线程来说，当前未被拉取到消息消费端的消息长度
            long diff = maxOffsetPy - maxPhyOffsetPulling;
            long memory =
                (long)
                    (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                        * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
            // 如果diff大于memory，表示当前需要拉取的消息已经超出了常驻
            // 内存的大小，表示主服务器繁忙，此时才建议从从服务器拉取消息
            getResult.setSuggestPullingFromSlave(diff > memory);
          } finally {
            bufferConsumeQueue.release();
          }
        } else {
          status = GetMessageStatus.OFFSET_FOUND_NULL;
          nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
          log.warn(
              "consumer request topic:"
                  + topic
                  + "offset:"
                  + offset
                  + "minOffset:"
                  + minOffset
                  + "maxOffset:"
                  + maxOffset
                  + ", but access logic queue failed.");
        }
      }
    } else {
      status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
      nextBeginOffset = nextOffsetCorrection(offset, 0);
    }

    if (GetMessageStatus.FOUND == status) {
      this.storeStatsService.getGetMessageTimesTotalFound().add(1);
    } else {
      this.storeStatsService.getGetMessageTimesTotalMiss().add(1);
    }
    long elapsedTime = this.getSystemClock().now() - beginTime;
    this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

    // lazy init no data found.
    if (getResult == null) {
      getResult = new GetMessageResult(0);
    }
    // 根据 PullResult 填充 responseHeader 的 NextBeginOffset 、 MinOffset 、 MaxOffset
    getResult.setStatus(status);
    getResult.setNextBeginOffset(nextBeginOffset);
    getResult.setMaxOffset(maxOffset);
    getResult.setMinOffset(minOffset);
    return getResult;
  }

  /**
   * 查找消费队列
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @return {@link ConsumeQueue}
   */
  public ConsumeQueue findConsumeQueue(String topic, int queueId) {
    ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
    if (null == map) {
      ConcurrentMap<Integer, ConsumeQueue> newMap =
          new ConcurrentHashMap<Integer, ConsumeQueue>(128);
      ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
      if (oldMap != null) {
        map = oldMap;
      } else {
        map = newMap;
      }
    }

    ConsumeQueue logic = map.get(queueId);
    if (null == logic) {
      ConsumeQueue newLogic =
          new ConsumeQueue(
              topic,
              queueId,
              StorePathConfigHelper.getStorePathConsumeQueue(
                  this.messageStoreConfig.getStorePathRootDir()),
              this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
              this);
      ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
      if (oldLogic != null) {
        logic = oldLogic;
      } else {
        if (MixAll.isLmq(topic)) {
          lmqConsumeQueueNum.getAndIncrement();
        }
        logic = newLogic;
      }
    }

    return logic;
  }

  /**
   * 下一次偏移校正
   *
   * @param oldOffset 旧偏移量
   * @param newOffset 新偏移量
   * @return long
   */
  private long nextOffsetCorrection(long oldOffset, long newOffset) {
    long nextOffset = oldOffset;
    if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
        || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
      nextOffset = newOffset;
    }
    return nextOffset;
  }

  /**
   * 按提交偏移量签入磁盘
   *
   * @param offsetPy 偏移 py
   * @param maxOffsetPy 最大偏移 py
   * @return boolean
   */
  private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
    long memory =
        (long)
            (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
    return (maxOffsetPy - offsetPy) > memory;
  }

  /**
   * 这批货满了吗
   *
   * @param sizePy 尺寸 py
   * @param maxMsgNums 最大消息数
   * @param bufferTotal 缓冲区总数
   * @param messageTotal 消息总数
   * @param isInDisk 在磁盘中
   * @return boolean
   */
  private boolean isTheBatchFull(
      int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

    if (0 == bufferTotal || 0 == messageTotal) {
      return false;
    }

    if (maxMsgNums <= messageTotal) {
      return true;
    }

    if (isInDisk) {
      if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
        return true;
      }

      if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
        return true;
      }
    } else {
      if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
        return true;
      }

      if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
        return true;
      }
    }

    return false;
  }

  /**
   * 获取队列中最大偏移量
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @return long
   */
  public long getMaxOffsetInQueue(String topic, int queueId) {
    ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
    if (logic != null) {
      long offset = logic.getMaxOffsetInQueue();
      return offset;
    }

    return 0;
  }

  /**
   * 获取队列中最小偏移量
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @return long
   */
  public long getMinOffsetInQueue(String topic, int queueId) {
    ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
    if (logic != null) {
      return logic.getMinOffsetInQueue();
    }

    return -1;
  }

  /**
   * 获取队列中提交日志偏移量
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @param consumeQueueOffset 使用队列偏移
   * @return long
   */
  @Override
  public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
    ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
    if (consumeQueue != null) {
      SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
      if (bufferConsumeQueue != null) {
        try {
          long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
          return offsetPy;
        } finally {
          bufferConsumeQueue.release();
        }
      }
    }

    return 0;
  }

  /**
   * 按时间获取队列中偏移量
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @param timestamp 时间戳
   * @return long
   */
  public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
    ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
    if (logic != null) {
      return logic.getOffsetInQueueByTime(timestamp);
    }

    return 0;
  }

  /**
   * 按偏移量选择一条消息
   *
   * @param commitLogOffset 提交日志偏移量
   * @return {@link SelectMappedBufferResult}
   */
  @Override
  public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
    SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
    if (null != sbr) {
      try {
        // 1 TOTALSIZE
        int size = sbr.getByteBuffer().getInt();
        return this.commitLog.getMessage(commitLogOffset, size);
      } finally {
        sbr.release();
      }
    }

    return null;
  }

  /**
   * 按偏移量选择一条消息
   *
   * @param commitLogOffset 提交日志偏移量
   * @param msgSize 消息大小
   * @return {@link SelectMappedBufferResult}
   */
  @Override
  public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
    return this.commitLog.getMessage(commitLogOffset, msgSize);
  }

  /**
   * 获取运行数据信息
   *
   * @return {@link String}
   */
  public String getRunningDataInfo() {
    return this.storeStatsService.toString();
  }

  /**
   * 获取运行时信息
   *
   * @return {@link HashMap}<{@link String}, {@link String}>
   */
  @Override
  public HashMap<String, String> getRuntimeInfo() {
    HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

    {
      double minPhysicsUsedRatio = Double.MAX_VALUE;
      String commitLogStorePath = getStorePathPhysic();
      String[] paths = commitLogStorePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
      for (String clPath : paths) {
        double physicRatio =
            UtilAll.isPathExists(clPath) ? UtilAll.getDiskPartitionSpaceUsedPercent(clPath) : -1;
        result.put(
            RunningStats.commitLogDiskRatio.name() + "_" + clPath, String.valueOf(physicRatio));
        minPhysicsUsedRatio = Math.min(minPhysicsUsedRatio, physicRatio);
      }
      result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(minPhysicsUsedRatio));
    }

    {
      double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathLogic());
      result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
    }

    {
      if (this.scheduleMessageService != null) {
        this.scheduleMessageService.buildRunningStats(result);
      }
    }

    result.put(
        RunningStats.commitLogMinOffset.name(),
        String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
    result.put(
        RunningStats.commitLogMaxOffset.name(),
        String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

    return result;
  }

  /**
   * 获取最小 phy 偏移
   *
   * @return long
   */
  @Override
  public long getMinPhyOffset() {
    return this.commitLog.getMinOffset();
  }

  /**
   * 获取最早消息时间
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @return long
   */
  @Override
  public long getEarliestMessageTime(String topic, int queueId) {
    ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
    if (logicQueue != null) {
      long minLogicOffset = logicQueue.getMinLogicOffset();

      SelectMappedBufferResult result =
          logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
      return getStoreTime(result);
    }

    return -1;
  }

  /**
   * 获取存储时间
   *
   * @param result 后果
   * @return long
   */
  private long getStoreTime(SelectMappedBufferResult result) {
    if (result != null) {
      try {
        final long phyOffset = result.getByteBuffer().getLong();
        final int size = result.getByteBuffer().getInt();
        long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
        return storeTime;
      } catch (Exception e) {
      } finally {
        result.release();
      }
    }
    return -1;
  }

  /**
   * 获取最早消息时间
   *
   * @return long
   */
  @Override
  public long getEarliestMessageTime() {
    final long minPhyOffset = this.getMinPhyOffset();
    final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
    return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
  }

  /**
   * 获取消息存储时间戳
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @param consumeQueueOffset 使用队列偏移
   * @return long
   */
  @Override
  public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
    ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
    if (logicQueue != null) {
      SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
      return getStoreTime(result);
    }

    return -1;
  }

  /**
   * 获取队列中消息总数
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @return long
   */
  @Override
  public long getMessageTotalInQueue(String topic, int queueId) {
    ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
    if (logicQueue != null) {
      return logicQueue.getMessageTotalInQueue();
    }

    return -1;
  }

  /**
   * 获取提交日志数据
   *
   * @param offset 抵消
   * @return {@link SelectMappedBufferResult}
   */
  @Override
  public SelectMappedBufferResult getCommitLogData(final long offset) {
    if (this.shutdown) {
      log.warn("message store has shutdown, so getPhyQueueData is forbidden");
      return null;
    }

    return this.commitLog.getData(offset);
  }

  /**
   * 附加到提交日志
   *
   * @param startOffset 起始偏移量
   * @param data 数据
   * @param dataStart 数据启动
   * @param dataLength 数据长度
   * @return boolean
   */
  @Override
  public boolean appendToCommitLog(long startOffset, byte[] data, int dataStart, int dataLength) {
    if (this.shutdown) {
      log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
      return false;
    }

    boolean result = this.commitLog.appendData(startOffset, data, dataStart, dataLength);
    if (result) {
      this.reputMessageService.wakeup();
    } else {
      log.error("appendToPhyQueue failed" + startOffset + " " + data.length);
    }

    return result;
  }

  /** 手动执行删除文件 */
  @Override
  public void executeDeleteFilesManually() {
    this.cleanCommitLogService.executeDeleteFilesManually();
  }

  /**
   * 查询消息
   *
   * @param topic 话题
   * @param key 钥匙
   * @param maxNum 最大数量
   * @param begin 开始
   * @param end 终止
   * @return {@link QueryMessageResult}
   */
  @Override
  public QueryMessageResult queryMessage(
      String topic, String key, int maxNum, long begin, long end) {
    QueryMessageResult queryMessageResult = new QueryMessageResult();

    long lastQueryMsgTime = end;

    for (int i = 0; i < 3; i++) {
      QueryOffsetResult queryOffsetResult =
          this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
      if (queryOffsetResult.getPhyOffsets().isEmpty()) {
        break;
      }

      Collections.sort(queryOffsetResult.getPhyOffsets());

      queryMessageResult.setIndexLastUpdatePhyoffset(
          queryOffsetResult.getIndexLastUpdatePhyoffset());
      queryMessageResult.setIndexLastUpdateTimestamp(
          queryOffsetResult.getIndexLastUpdateTimestamp());

      for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
        long offset = queryOffsetResult.getPhyOffsets().get(m);

        try {

          boolean match = true;
          MessageExt msg = this.lookMessageByOffset(offset);
          if (0 == m) {
            lastQueryMsgTime = msg.getStoreTimestamp();
          }

          //                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
          //                    if (topic.equals(msg.getTopic())) {
          //                        for (String k : keyArray) {
          //                            if (k.equals(key)) {
          //                                match = true;
          //                                break;
          //                            }
          //                        }
          //                    }

          if (match) {
            SelectMappedBufferResult result = this.commitLog.getData(offset, false);
            if (result != null) {
              int size = result.getByteBuffer().getInt(0);
              result.getByteBuffer().limit(size);
              result.setSize(size);
              queryMessageResult.addMessage(result);
            }
          } else {
            log.warn("queryMessage hash duplicate, {} {}", topic, key);
          }
        } catch (Exception e) {
          log.error("queryMessage exception", e);
        }
      }

      if (queryMessageResult.getBufferTotalSize() > 0) {
        break;
      }

      if (lastQueryMsgTime < begin) {
        break;
      }
    }

    return queryMessageResult;
  }

  /**
   * 按偏移量查看消息
   *
   * @param commitLogOffset 提交日志偏移量
   * @return {@link MessageExt}
   */
  public MessageExt lookMessageByOffset(long commitLogOffset) {
    SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
    if (null != sbr) {
      try {
        // 1 TOTALSIZE
        int size = sbr.getByteBuffer().getInt();
        return lookMessageByOffset(commitLogOffset, size);
      } finally {
        sbr.release();
      }
    }

    return null;
  }

  /**
   * 按偏移量查看消息
   *
   * @param commitLogOffset 提交日志偏移量
   * @param size 大小
   * @return {@link MessageExt}
   */
  public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
    SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
    if (null != sbr) {
      try {
        return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
      } finally {
        sbr.release();
      }
    }

    return null;
  }

  /**
   * 更新 ha 主地址
   *
   * @param newAddr 新地址
   */
  @Override
  public void updateHaMasterAddress(String newAddr) {
    this.haService.updateMasterAddress(newAddr);
  }

  /**
   * 奴隶落后很多
   *
   * @return long
   */
  @Override
  public long slaveFallBehindMuch() {
    return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
  }

  /**
   * 现在
   *
   * @return long
   */
  @Override
  public long now() {
    return this.systemClock.now();
  }

  /**
   * 清除未使用主题
   *
   * @param topics 话题
   * @return int
   */
  @Override
  public int cleanUnusedTopic(Set<String> topics) {
    Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it =
        this.consumeQueueTable.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
      String topic = next.getKey();

      if (!topics.contains(topic)
          && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
          && !topic.equals(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC)
          && !MixAll.isLmq(topic)) {
        ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
        for (ConsumeQueue cq : queueTable.values()) {
          cq.destroy();
          log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned", cq.getTopic(), cq.getQueueId());

          this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
        }
        it.remove();

        if (this.brokerConfig.isAutoDeleteUnusedStats()) {
          this.brokerStatsManager.onTopicDeleted(topic);
        }

        log.info("cleanUnusedTopic: {},topic destroyed", topic);
      }
    }

    return 0;
  }

  /** 清除过期使用者队列 */
  public void cleanExpiredConsumerQueue() {
    long minCommitLogOffset = this.commitLog.getMinOffset();

    Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it =
        this.consumeQueueTable.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
      String topic = next.getKey();
      if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
        ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
        Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
        while (itQT.hasNext()) {
          Entry<Integer, ConsumeQueue> nextQT = itQT.next();
          long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

          if (maxCLOffsetInConsumeQueue == -1) {
            log.warn(
                "maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                nextQT.getValue().getTopic(),
                nextQT.getValue().getQueueId(),
                nextQT.getValue().getMaxPhysicOffset(),
                nextQT.getValue().getMinLogicOffset());
          } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
            log.info(
                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                topic,
                nextQT.getKey(),
                minCommitLogOffset,
                maxCLOffsetInConsumeQueue);

            DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(
                nextQT.getValue().getTopic(), nextQT.getValue().getQueueId());

            nextQT.getValue().destroy();
            itQT.remove();
          }
        }

        if (queueTable.isEmpty()) {
          log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
          it.remove();
        }
      }
    }
  }

  /**
   * 获取消息 ID
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @param minOffset 最小偏移量
   * @param maxOffset 最大偏移量
   * @param storeHost 存储主机
   * @return {@link Map}<{@link String}, {@link Long}>
   */
  public Map<String, Long> getMessageIds(
      final String topic,
      final int queueId,
      long minOffset,
      long maxOffset,
      SocketAddress storeHost) {
    Map<String, Long> messageIds = new HashMap<String, Long>();
    if (this.shutdown) {
      return messageIds;
    }

    ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
    if (consumeQueue != null) {
      minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
      maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

      if (maxOffset == 0) {
        return messageIds;
      }

      long nextOffset = minOffset;
      while (nextOffset < maxOffset) {
        SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
        if (bufferConsumeQueue != null) {
          try {
            int i = 0;
            for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
              long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
              InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
              int msgIdLength =
                  (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
              final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
              String msgId =
                  MessageDecoder.createMessageId(
                      msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
              messageIds.put(msgId, nextOffset++);
              if (nextOffset > maxOffset) {
                return messageIds;
              }
            }
          } finally {

            bufferConsumeQueue.release();
          }
        } else {
          return messageIds;
        }
      }
    }
    return messageIds;
  }

  /**
   * 按消耗偏移量签入磁盘
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @param consumeOffset 消耗偏移量
   * @return boolean
   */
  @Override
  public boolean checkInDiskByConsumeOffset(
      final String topic, final int queueId, long consumeOffset) {

    final long maxOffsetPy = this.commitLog.getMaxOffset();

    ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
    if (consumeQueue != null) {
      SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
      if (bufferConsumeQueue != null) {
        try {
          for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
            i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
            return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
          }
        } finally {

          bufferConsumeQueue.release();
        }
      } else {
        return false;
      }
    }
    return false;
  }

  /**
   * 脸红
   *
   * @return long
   */
  @Override
  public long flush() {
    return this.commitLog.flush();
  }

  /**
   * 重置写入偏移
   *
   * @param phyOffset phy 偏移
   * @return boolean
   */
  @Override
  public boolean resetWriteOffset(long phyOffset) {
    return this.commitLog.resetOffset(phyOffset);
  }

  /**
   * 获取确认偏移
   *
   * @return long
   */
  @Override
  public long getConfirmOffset() {
    return this.commitLog.getConfirmOffset();
  }

  /**
   * 设置确认偏移
   *
   * @param phyOffset phy 偏移
   */
  @Override
  public void setConfirmOffset(long phyOffset) {
    this.commitLog.setConfirmOffset(phyOffset);
  }

  /**
   * 获取临时存储池
   *
   * @return {@link TransientStorePool}
   */
  public TransientStorePool getTransientStorePool() {
    return transientStorePool;
  }

  /**
   * 获取分配映射文件服务
   *
   * @return {@link AllocateMappedFileService}
   */
  public AllocateMappedFileService getAllocateMappedFileService() {
    return allocateMappedFileService;
  }

  /**
   * 获取商店统计信息服务
   *
   * @return {@link StoreStatsService}
   */
  public StoreStatsService getStoreStatsService() {
    return storeStatsService;
  }

  /**
   * 获取访问权限
   *
   * @return {@link RunningFlags}
   */
  public RunningFlags getAccessRights() {
    return runningFlags;
  }

  /**
   * 获取消费队列表
   *
   * @return {@link ConcurrentMap}<{@link String}, {@link ConcurrentMap}<{@link Integer}, {@link
   *     ConsumeQueue}>>
   */
  public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
    return consumeQueueTable;
  }

  /**
   * 获取存储检查点
   *
   * @return {@link StoreCheckpoint}
   */
  public StoreCheckpoint getStoreCheckpoint() {
    return storeCheckpoint;
  }

  /**
   * 获得 ha 服务
   *
   * @return {@link HAService}
   */
  public HAService getHaService() {
    return haService;
  }

  /**
   * 获取计划消息服务
   *
   * @return {@link ScheduleMessageService}
   */
  public ScheduleMessageService getScheduleMessageService() {
    return scheduleMessageService;
  }

  /**
   * 获取运行标志
   *
   * @return {@link RunningFlags}
   */
  public RunningFlags getRunningFlags() {
    return runningFlags;
  }

  /**
   * 执行调度
   *
   * @param req 绿色
   */
  public void doDispatch(DispatchRequest req) {
    // dispatcherList 中有 CommitLogDispatcherBuildConsumeQueue 、 CommitLogDispatcherBuildIndex
    for (CommitLogDispatcher dispatcher : this.dispatcherList) {
      dispatcher.dispatch(req);
    }
  }

  /**
   * 放置消息位置信息 <br>
   * 代码清单 4-52
   *
   * @param dispatchRequest 调度请求
   */
  public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
    // 根据消息主题与队列 ID ，先获取对应的 ConsumeQueue 文件
    ConsumeQueue cq =
        this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
    cq.putMessagePositionInfoWrapper(dispatchRequest, checkMultiDispatchQueue(dispatchRequest));
  }

  /**
   * 检查多调度队列
   *
   * @param dispatchRequest 调度请求
   * @return boolean
   */
  private boolean checkMultiDispatchQueue(DispatchRequest dispatchRequest) {
    if (!this.messageStoreConfig.isEnableMultiDispatch()) {
      return false;
    }
    Map<String, String> prop = dispatchRequest.getPropertiesMap();
    if (prop == null && prop.isEmpty()) {
      return false;
    }
    String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
    String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
    if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
      return false;
    }
    return true;
  }

  /**
   * 获取经纪人统计信息管理器
   *
   * @return {@link BrokerStatsManager}
   */
  @Override
  public BrokerStatsManager getBrokerStatsManager() {
    return brokerStatsManager;
  }

  /**
   * 清理错过 lmq 主题
   *
   * @param topic 话题
   */
  @Override
  public void cleanUnusedLmqTopic(String topic) {
    if (this.consumeQueueTable.containsKey(topic)) {
      ConcurrentMap<Integer, ConsumeQueue> map = this.consumeQueueTable.get(topic);
      if (map != null) {
        ConsumeQueue cq = map.get(0);
        cq.destroy();
        log.info("cleanUnusedLmqTopic: {} {} ConsumeQueue cleaned", cq.getTopic(), cq.getQueueId());

        this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
        this.lmqConsumeQueueNum.getAndDecrement();
      }
      this.consumeQueueTable.remove(topic);
      if (this.brokerConfig.isAutoDeleteUnusedStats()) {
        this.brokerStatsManager.onTopicDeleted(topic);
      }
      log.info("cleanUnusedLmqTopic: {},topic destroyed", topic);
    }
  }

  /**
   * 临时存储池是否不足
   *
   * @return boolean
   */
  @Override
  public boolean isTransientStorePoolDeficient() {
    return remainTransientStoreBufferNumbs() == 0;
  }

  /**
   * 保持临时存储缓冲区编号
   *
   * @return int
   */
  public int remainTransientStoreBufferNumbs() {
    return this.transientStorePool.availableBufferNums();
  }

  /**
   * 获取调度程序列表
   *
   * @return {@link LinkedList}<{@link CommitLogDispatcher}>
   */
  @Override
  public LinkedList<CommitLogDispatcher> getDispatcherList() {
    return this.dispatcherList;
  }

  /**
   * 获取消费队列
   *
   * @param topic 话题
   * @param queueId 队列 id
   * @return {@link ConsumeQueue}
   */
  @Override
  public ConsumeQueue getConsumeQueue(String topic, int queueId) {
    ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
    if (map == null) {
      return null;
    }
    return map.get(queueId);
  }

  /**
   * 解锁映射文件
   *
   * @param mappedFile 映射文件
   */
  public void unlockMappedFile(final MappedFile mappedFile) {
    this.scheduledExecutorService.schedule(
        new Runnable() {
          @Override
          public void run() {
            mappedFile.munlock();
          }
        },
        6,
        TimeUnit.SECONDS);
  }

  /**
   * 提交日志调度程序生成消耗队列
   *
   * @author shui4
   */
  class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

    /**
     * 派遣
     *
     * @param request 要求
     */
    @Override
    public void dispatch(DispatchRequest request) {
      final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
      switch (tranType) {
        case MessageSysFlag.TRANSACTION_NOT_TYPE:
        case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
          DefaultMessageStore.this.putMessagePositionInfo(request);
          break;
        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
          break;
      }
    }
  }

  /**
   * 提交日志调度程序构建索引
   *
   * @author shui4
   */
  class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

    /**
     * 派遣 <br>
     * 代码清单 4-54
     *
     * @param request 要求
     */
    @Override
    public void dispatch(DispatchRequest request) {
      // ? 消息索引启动
      if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
        DefaultMessageStore.this.indexService.buildIndex(request);
      }
    }
  }

  /**
   * 清理提交日志服务
   *
   * @author shui4
   */
  class CleanCommitLogService {

    /** 手动删除文件最大次数 */
    private static final int MAX_MANUAL_DELETE_FILE_TIMES = 20;
    /** 磁盘空间警告级别比率 */
    private final double diskSpaceWarningLevelRatio =
        Double.parseDouble(
            System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

    /** 磁盘空间强制清理比率 */
    private final double diskSpaceCleanForciblyRatio =
        Double.parseDouble(
            System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
    /** 上次重新删除时间戳 */
    private long lastRedeleteTimestamp = 0;

    /** 多次手动删除文件 */
    private volatile int manualDeleteFileSeveralTimes = 0;

    /** 立即清洁 */
    private volatile boolean cleanImmediately = false;

    /** 手动执行删除文件 */
    public void executeDeleteFilesManually() {
      this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
      DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
    }

    public void run() {
      try {
        // 删除过期文件
        this.deleteExpiredFiles();
        // 重新删除挂起的文件
        this.redeleteHangedFile();
      } catch (Throwable e) {
        DefaultMessageStore.log.warn(this.getServiceName() + "service has exception.", e);
      }
    }

    /** 删除过期文件 */
    private void deleteExpiredFiles() {
      int deleteCount = 0;
      // 文件过期时间阈值，默认 72 小时
      long fileReservedTime =
          DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
      // 删除物理文件的间隔时间，默认 100
      // 一次清除过程中，可能需要被删除的文件不止一个，该值指定两次删除文件的间隔时间
      int deletePhysicFilesInterval =
          DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
      // 强制销毁映射文件间隔，默认值 120_000
      // 在清除过期文件时，如果该文件被其它线程占用（引用次数大于 0，比如读取消息），此时会阻止此次删除任务，同时在第一次视图删除该文件时记录当前时间戳，
      // destroyMapedFileIntervalForcibly  表示
      // 第一次拒绝之后能保留文件的最大时间，在此时间内，同时可以被拒绝删除，超过该时间后，会将引用次数设置为负数，文件将被强制删除
      int destroyMapedFileIntervalForcibly =
          DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
      // ? 当前时间是否可以删除，看当前时间是否匹配 deleteWhen 配置，默认凌晨 4 点
      boolean timeup = this.isTimeToDelete();
      // ? 磁盘空间满了
      // * 应该触发过期文件删除操作
      boolean spacefull = this.isSpaceToDelete();
      // 多次手动删除
      boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;
      // ? timeup
      // ? spacefull
      // ? manualDelete ：
      if (timeup || spacefull || manualDelete) {

        if (manualDelete) this.manualDeleteFileSeveralTimes--;
        // ? 强制删除 以及 立即删除
        boolean cleanAtOnce =
            DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable()
                && this.cleanImmediately;

        log.info(
            "begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
            fileReservedTime,
            timeup,
            spacefull,
            manualDeleteFileSeveralTimes,
            cleanAtOnce);
        // 小时转毫秒
        fileReservedTime *= 60 * 60 * 1000;

        deleteCount =
            DefaultMessageStore.this.commitLog.deleteExpiredFile(
                fileReservedTime,
                deletePhysicFilesInterval,
                destroyMapedFileIntervalForcibly,
                cleanAtOnce);
        if (deleteCount > 0) {
        }
        // 删除结尾为 0
        else if (spacefull) {
          // 磁盘空间即将满，但删除文件失败。
          log.warn("disk space will be full soon, but delete file failed.");
        }
      }
    }

    /** 重新删除挂起文件 */
    private void redeleteHangedFile() {
      int interval =
          DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
      long currentTimestamp = System.currentTimeMillis();
      if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
        this.lastRedeleteTimestamp = currentTimestamp;
        int destroyMapedFileIntervalForcibly =
            DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
        if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(
            destroyMapedFileIntervalForcibly)) {}
      }
    }

    /**
     * 获取服务名称
     *
     * @return {@link String}
     */
    public String getServiceName() {
      return CleanCommitLogService.class.getSimpleName();
    }

    /**
     * 是时候删除了
     *
     * @return boolean
     */
    private boolean isTimeToDelete() {
      String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
      if (UtilAll.isItTimeToDo(when)) {
        DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
        return true;
      }

      return false;
    }

    /**
     * 磁盘满了，需要删除
     *
     * @return boolean
     */
    @SuppressWarnings("SpellCheckingInspection")
    private boolean isSpaceToDelete() {
      // 阈值：10~95，默认为 75
      // 表示 CommitLog 文件、 ConsumeQueue 文件所在磁盘分区的最大使用量，如果超过该值，则需要立即清除过期文件
      double ratio =
          DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
      // 表示是否需要立即执行清除过期文件的操作
      cleanImmediately = false;

      {
        // 获取存储物理路径，默认 （ user.home/store/commitlog ）
        String commitLogStorePath = DefaultMessageStore.this.getStorePathPhysic();
        /// commitLogStorePath 多存储路径，用逗号分割
        String[] storePaths =
            commitLogStorePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        Set<String> fullStorePath = new HashSet<>();
        double minPhysicRatio = 100;
        String minStorePath = null;
        for (String storePathPhysic : storePaths) {
          // 当前 CommitLog 目录所在的磁盘分区的磁盘使用率，通过 File#getTotalSpace 方法获取文件所在磁盘分区的总容量，
          // 通过 File#getFreeSpace 方法获取文件所在磁盘分区的剩余容量
          double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
          if (minPhysicRatio > physicRatio) {
            // 最小阈值
            minPhysicRatio = physicRatio;
            // 最小 store 目录路径
            minStorePath = storePathPhysic;
          }
          // diskSpaceCleanForciblyRatio 默认为 0.85
          // ? 超过阈值（ 0.85 ），加入 fullStorePath（强制删除路径）
          if (physicRatio > diskSpaceCleanForciblyRatio) {
            fullStorePath.add(storePathPhysic);
          }
        }
        DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
        // diskSpaceWarningLevelRatio ：通过系统参数 Drocketmq.broker.diskSpaceWarningLevelRatio 进行设置，默认
        // 0.90。如果磁盘分区使用率超过该阈值，将设置磁盘为不可写，此时 会拒绝写入新消息
        // ? 当前最小比例 > 0.95，
        // 看着打印 error 日志 ，并且设置立即清理字段 cleanImmediately 为 true
        if (minPhysicRatio > diskSpaceWarningLevelRatio) {
          // 标记 拒绝写入状态
          boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
          if (diskok) {
            DefaultMessageStore.log.error(
                "physic disk maybe full soon"
                    + minPhysicRatio
                    + ", so mark disk full, storePathPhysic="
                    + minStorePath);
          }

          cleanImmediately = true;
        }
        // 通过系统参数 Drocketmq.broker.diskSpaceCleanForciblyRatio 进行设置，默认 0.85
        // 。如果磁盘分区使用超过该阈值，建议立即执行过期文件删除，
        // 但不会拒绝写入新消息
        // ? 当前最小比例 > 0.85
        // 设置立即清理字段 cleanImmediately 为 true
        else if (minPhysicRatio > diskSpaceCleanForciblyRatio) {
          cleanImmediately = true;
        }
        // N （当前最小比例 < 0.85）恢复可写状态
        else {
          boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
          if (!diskok) {
            DefaultMessageStore.log.info(
                "physic disk space OK"
                    + minPhysicRatio
                    + ", so mark disk ok, storePathPhysic="
                    + minStorePath);
          }
        }
        // ? 当前最小比例小于 0 或者超过 0.75 ，参考 MessageStoreConfig#getDiskMaxUsedSpaceRatio
        // * 打印日志 表示磁盘不足
        if (minPhysicRatio < 0 || minPhysicRatio > ratio) {
          DefaultMessageStore.log.info(
              "physic disk maybe full soon, so reclaim space,"
                  + minPhysicRatio
                  + ", storePathPhysic="
                  + minStorePath);
          return true;
        }
      }
      // ConsumeQueue 目录，与上面类似
      {
        String storePathLogics = DefaultMessageStore.this.getStorePathLogic();
        double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
        if (logicsRatio > diskSpaceWarningLevelRatio) {
          boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
          if (diskok) {
            DefaultMessageStore.log.error(
                "logics disk maybe full soon" + logicsRatio + ", so mark disk full");
          }

          cleanImmediately = true;
        } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
          cleanImmediately = true;
        } else {
          boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
          if (!diskok) {
            DefaultMessageStore.log.info(
                "logics disk space OK" + logicsRatio + ", so mark disk ok");
          }
        }

        if (logicsRatio < 0 || logicsRatio > ratio) {
          DefaultMessageStore.log.info(
              "logics disk maybe full soon, so reclaim space," + logicsRatio);
          return true;
        }
      }

      return false;
    }

    /**
     * 多次手动删除文件
     *
     * @return int
     */
    public int getManualDeleteFileSeveralTimes() {
      return manualDeleteFileSeveralTimes;
    }

    /**
     * 多次设置手动删除文件
     *
     * @param manualDeleteFileSeveralTimes 多次手动删除文件
     */
    public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
      this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
    }

    /**
     * 空间是否已满
     *
     * @return boolean
     */
    public boolean isSpaceFull() {
      double physicRatio = calcStorePathPhysicRatio();
      double ratio =
          DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
      if (physicRatio > ratio) {
        DefaultMessageStore.log.info("physic disk of commitLog used:" + physicRatio);
      }
      if (physicRatio > this.diskSpaceWarningLevelRatio) {
        boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
        if (diskok) {
          DefaultMessageStore.log.error(
              "physic disk of commitLog maybe full soon, used"
                  + physicRatio
                  + ", so mark disk full");
        }

        return true;
      } else {
        boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();

        if (!diskok) {
          DefaultMessageStore.log.info(
              "physic disk space of commitLog OK" + physicRatio + ", so mark disk ok");
        }

        return false;
      }
    }

    /**
     * 计算存储路径物理比
     *
     * @return double
     */
    public double calcStorePathPhysicRatio() {
      Set<String> fullStorePath = new HashSet<>();
      String storePath = getStorePathPhysic();
      String[] paths = storePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
      double minPhysicRatio = 100;
      for (String path : paths) {
        double physicRatio =
            UtilAll.isPathExists(path) ? UtilAll.getDiskPartitionSpaceUsedPercent(path) : -1;
        minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
        if (physicRatio > diskSpaceCleanForciblyRatio) {
          fullStorePath.add(path);
        }
      }
      DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
      return minPhysicRatio;
    }
  }

  /**
   * 清理 ConsumeQueue 服务
   *
   * @author shui4
   */
  class CleanConsumeQueueService {
    /** 最后物理最小偏移 */
    private long lastPhysicalMinOffset = 0;

    /** 跑 */
    public void run() {
      try {
        this.deleteExpiredFiles();
      } catch (Throwable e) {
        DefaultMessageStore.log.warn(this.getServiceName() + "service has exception.", e);
      }
    }

    /** 删除过期文件 */
    private void deleteExpiredFiles() {
      int deleteLogicsFilesInterval =
          DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

      long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
      if (minOffset > this.lastPhysicalMinOffset) {
        this.lastPhysicalMinOffset = minOffset;

        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables =
            DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
          for (ConsumeQueue logic : maps.values()) {
            int deleteCount = logic.deleteExpiredFile(minOffset);

            if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
              try {
                Thread.sleep(deleteLogicsFilesInterval);
              } catch (InterruptedException ignored) {
              }
            }
          }
        }

        DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
      }
    }

    /**
     * 获取服务名称
     *
     * @return {@link String}
     */
    public String getServiceName() {
      return CleanConsumeQueueService.class.getSimpleName();
    }
  }

  /**
   * 刷新消费队列服务
   *
   * @author shui4
   */
  class FlushConsumeQueueService extends ServiceThread {
    /** 重试次数超过 */
    private static final int RETRY_TIMES_OVER = 3;
    /** 上次刷新时间戳 */
    private long lastFlushTimestamp = 0;

    /** 跑 */
    public void run() {
      DefaultMessageStore.log.info(this.getServiceName() + "service started");

      while (!this.isStopped()) {
        try {
          int interval =
              DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
          this.waitForRunning(interval);
          this.doFlush(1);
        } catch (Exception e) {
          DefaultMessageStore.log.warn(this.getServiceName() + "service has exception.", e);
        }
      }

      this.doFlush(RETRY_TIMES_OVER);

      DefaultMessageStore.log.info(this.getServiceName() + "service end");
    }

    /**
     * 获取服务名称
     *
     * @return {@link String}
     */
    @Override
    public String getServiceName() {
      return FlushConsumeQueueService.class.getSimpleName();
    }

    /**
     * 做冲洗
     *
     * @param retryTimes 重试次数
     */
    private void doFlush(int retryTimes) {
      int flushConsumeQueueLeastPages =
          DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

      if (retryTimes == RETRY_TIMES_OVER) {
        flushConsumeQueueLeastPages = 0;
      }

      long logicsMsgTimestamp = 0;

      int flushConsumeQueueThoroughInterval =
          DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
        this.lastFlushTimestamp = currentTimeMillis;
        flushConsumeQueueLeastPages = 0;
        logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
      }

      ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables =
          DefaultMessageStore.this.consumeQueueTable;

      for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
        for (ConsumeQueue cq : maps.values()) {
          boolean result = false;
          for (int i = 0; i < retryTimes && !result; i++) {
            result = cq.flush(flushConsumeQueueLeastPages);
          }
        }
      }

      if (0 == flushConsumeQueueLeastPages) {
        if (logicsMsgTimestamp > 0) {
          DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
        }
        DefaultMessageStore.this.getStoreCheckpoint().flush();
      }
    }

    /**
     * 获得加入时间
     *
     * @return long
     */
    @Override
    public long getJointime() {
      return 1000 * 60;
    }
  }

  /**
   * 转发消息服务 <br>
   * 转发 CommitLog 的更新事件 到 ConsumeQueue 和 Index
   */
  class ReputMessageService extends ServiceThread {
    /**
     * 从哪个物理偏移量开始转发消息给 ConsumeQueue 和 Index 文件。如果允许重复转发，将 reputFromOffset 设置为 CommitLog
     * 文件的提交指针。如果不允许重复转发，将 reputFromOffset 设置为 CommitLog 文件的内存中最大偏移量
     */
    private volatile long reputFromOffset = 0;

    /**
     * 从抵消中获得声誉
     *
     * @return long
     */
    public long getReputFromOffset() {
      return reputFromOffset;
    }

    /**
     * 设置从偏移量转发
     *
     * @param reputFromOffset 从偏移量转发
     */
    public void setReputFromOffset(long reputFromOffset) {
      this.reputFromOffset = reputFromOffset;
    }

    /** 停机 */
    @Override
    public void shutdown() {
      for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
      }

      if (this.isCommitLogAvailable()) {
        log.warn(
            "shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
            DefaultMessageStore.this.commitLog.getMaxOffset(),
            this.reputFromOffset);
      }

      super.shutdown();
    }

    /**
     * 提交日志可用吗
     *
     * @return boolean
     */
    private boolean isCommitLogAvailable() {
      return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
    }

    /**
     * 在…后面
     *
     * @return long
     */
    public long behind() {
      return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
    }

    // 代码清单 4-49
    @Override
    public void run() {
      DefaultMessageStore.log.info(this.getServiceName() + "service started");

      while (!this.isStopped()) {
        try {
          Thread.sleep(1);
          // 尝试推送消息到 ConsumeQueue 和 Index 文件中
          this.doReput();
        } catch (Exception e) {
          DefaultMessageStore.log.warn(this.getServiceName() + "service has exception.", e);
        }
      }

      DefaultMessageStore.log.info(this.getServiceName() + "service end");
    }

    /**
     * 获取服务名称
     *
     * @return {@link String}
     */
    @Override
    public String getServiceName() {
      return ReputMessageService.class.getSimpleName();
    }

    /** 做转发 */
    private void doReput() {
      if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
        log.warn(
            "The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
            this.reputFromOffset,
            DefaultMessageStore.this.commitLog.getMinOffset());
        this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
      }
      // ? reputFromOffset<CommitLog 的最大写入偏移量
      for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
        // 代码清单 4-48
        // ? 重复启用 && reputFromOffset>= 确认偏移量
        // * 跳出循环
        if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
            && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
          break;
        }
        // 代码清单 4-50
        // 返回 reputFromOffset 偏移量开始的全部有效数据（CommitLog 文件）
        SelectMappedBufferResult result =
            DefaultMessageStore.this.commitLog.getData(reputFromOffset);
        if (result != null) {
          try {
            this.reputFromOffset = result.getStartOffset();
            // 然后循环读取每一条消息
            for (int readSize = 0; readSize < result.getSize() && doNext; ) {
              DispatchRequest dispatchRequest =
                  DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(
                      result.getByteBuffer(), false, false);
              int size =
                  dispatchRequest.getBufferSize() == -1
                      ? dispatchRequest.getMsgSize()
                      : dispatchRequest.getBufferSize();

              if (dispatchRequest.isSuccess()) {
                // 如果消息长度大于 0
                if (size > 0) {
                  // 调度 , 分别调用 CommitLogDispatcherBuildConsumeQueue（构建消息消费队列） 、
                  // CommitLogDispatcherBuildIndex（构建索引文件）
                  DefaultMessageStore.this.doDispatch(dispatchRequest);
                  // 当新消息达到 CommitLog 文件时， ReputMessageService 线程负责
                  // 将消息转发给 Consume Queue 文件和 Index 文件，如果 Broker 端开启了
                  // 长轮询模式并且当前节点角色主节点，则将调用
                  // PullRequestHoldService 线程的 notifyMessageArriving() 方法唤醒挂
                  // 起线程，判断当前消费队列最大偏移量是否大于待拉取偏移量，如果
                  // 大于则拉取消息。长轮询模式实现了准实时消息拉取。
                  if (BrokerRole.SLAVE
                          != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                      && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                      && DefaultMessageStore.this.messageArrivingListener != null) {
                    DefaultMessageStore.this.messageArrivingListener.arriving(
                        dispatchRequest.getTopic(),
                        dispatchRequest.getQueueId(),
                        dispatchRequest.getConsumeQueueOffset() + 1,
                        dispatchRequest.getTagsCode(),
                        dispatchRequest.getStoreTimestamp(),
                        dispatchRequest.getBitMap(),
                        dispatchRequest.getPropertiesMap());
                    notifyMessageArrive4MultiQueue(dispatchRequest);
                  }

                  this.reputFromOffset += size;
                  readSize += size;
                  if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                      == BrokerRole.SLAVE) {
                    DefaultMessageStore.this
                        .storeStatsService
                        .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic())
                        .add(1);
                    DefaultMessageStore.this
                        .storeStatsService
                        .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                        .add(dispatchRequest.getMsgSize());
                  }
                } else if (size == 0) {
                  this.reputFromOffset =
                      DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                  // 此时不满足循环条件，跳出内层循环，然后遍历下一个文件
                  readSize = result.getSize();
                }
              } else if (!dispatchRequest.isSuccess()) {

                if (size > 0) {
                  log.error(
                      "[BUG]read total count not equals msg total size. reputFromOffset={}",
                      reputFromOffset);
                  this.reputFromOffset += size;
                } else {
                  doNext = false;
                  // If user open the dledger pattern or the broker is master node,
                  // it will not ignore the exception and fix the reputFromOffset variable
                  if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()
                      || DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                    log.error(
                        "[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                        this.reputFromOffset);
                    this.reputFromOffset += result.getSize() - readSize;
                  }
                }
              }
            }
          } finally {
            result.release();
          }
        } else {
          doNext = false;
        }
      }
    }

    /**
     * 通知消息到达 4 多队列
     *
     * @param dispatchRequest 调度请求
     */
    private void notifyMessageArrive4MultiQueue(DispatchRequest dispatchRequest) {
      Map<String, String> prop = dispatchRequest.getPropertiesMap();
      if (prop == null) {
        return;
      }
      String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
      String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
      if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
        return;
      }
      String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
      String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
      if (queues.length != queueOffsets.length) {
        return;
      }
      for (int i = 0; i < queues.length; i++) {
        String queueName = queues[i];
        long queueOffset = Long.parseLong(queueOffsets[i]);
        int queueId = dispatchRequest.getQueueId();
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableLmq()
            && MixAll.isLmq(queueName)) {
          queueId = 0;
        }
        DefaultMessageStore.this.messageArrivingListener.arriving(
            queueName,
            queueId,
            queueOffset + 1,
            dispatchRequest.getTagsCode(),
            dispatchRequest.getStoreTimestamp(),
            dispatchRequest.getBitMap(),
            dispatchRequest.getPropertiesMap());
      }
    }
  }
}
