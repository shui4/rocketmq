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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HAService
 *
 * @author shui4
 */
@SuppressWarnings("AlibabaRemoveCommentedCode")
public class HAService {
  /** log */
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

  /** connectionCount */
  private final AtomicInteger connectionCount = new AtomicInteger(0);

  /** connectionList */
  private final List<HAConnection> connectionList = new LinkedList<>();

  /** acceptSocketService */
  private final AcceptSocketService acceptSocketService;

  /** defaultMessageStore */
  private final DefaultMessageStore defaultMessageStore;

  /** waitNotifyObject */
  private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

  /** push2SlaveMaxOffset */
  private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

  /** groupTransferService */
  private final GroupTransferService groupTransferService;

  /** haClient */
  private final HAClient haClient;

  /**
   * HAService
   *
   * @param defaultMessageStore ignore
   * @throws IOException ignore
   */
  public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
    this.defaultMessageStore = defaultMessageStore;
    this.acceptSocketService =
        new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
    this.groupTransferService = new GroupTransferService();
    this.haClient = new HAClient();
  }

  /**
   * updateMasterAddress
   *
   * @param newAddr ignore
   */
  public void updateMasterAddress(final String newAddr) {
    if (this.haClient != null) {
      this.haClient.updateMasterAddress(newAddr);
    }
  }

  /**
   * putRequest
   *
   * @param request ignore
   */
  public void putRequest(final CommitLog.GroupCommitRequest request) {
    this.groupTransferService.putRequest(request);
  }

  /**
   * isSlaveOK
   *
   * @param masterPutWhere ignore
   * @return ignore
   */
  public boolean isSlaveOK(final long masterPutWhere) {
    boolean result = this.connectionCount.get() > 0;
    result =
        result
            && ((masterPutWhere - this.push2SlaveMaxOffset.get())
                < this.defaultMessageStore.getMessageStoreConfig().getHaSlaveFallbehindMax());
    return result;
  }

  /**
   * notifyTransferSome
   *
   * @param offset ignore
   */
  public void notifyTransferSome(final long offset) {
    for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
      boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
      if (ok) {
        this.groupTransferService.notifyTransferSome();
        break;
      } else {
        value = this.push2SlaveMaxOffset.get();
      }
    }
  }

  /**
   * getConnectionCount
   *
   * @return ignore
   */
  public AtomicInteger getConnectionCount() {
    return connectionCount;
  }

  // public void notifyTransferSome() {
  // this.groupTransferService.notifyTransferSome();
  // }

  /**
   * start
   *
   * <ol>
   *   <li>主服务器启动，并在特定端口上监听从服务器的连接。
   *   <li>从服务主动连接主服务器，主服务器接收客户端的连接，并建立相关 TCP 连接。
   *   <li>从服务器主动向主服务器发送待拉取消息的偏移量，主服务器解析请求并返回消息给从服务器。
   *   <li>从服务器保存消息并继续发送新的消息同步请求。
   * </ol>
   *
   * @throws Exception ignore
   */
  public void start() throws Exception {
    this.acceptSocketService.beginAccept();
    this.acceptSocketService.start();
    this.groupTransferService.start();
    this.haClient.start();
  }

  /**
   * addConnection
   *
   * @param conn ignore
   */
  public void addConnection(final HAConnection conn) {
    synchronized (this.connectionList) {
      this.connectionList.add(conn);
    }
  }

  /**
   * removeConnection
   *
   * @param conn ignore
   */
  public void removeConnection(final HAConnection conn) {
    synchronized (this.connectionList) {
      this.connectionList.remove(conn);
    }
  }

  /** shutdown */
  public void shutdown() {
    this.haClient.shutdown();
    this.acceptSocketService.shutdown(true);
    this.destroyConnections();
    this.groupTransferService.shutdown();
  }

  /** destroyConnections */
  public void destroyConnections() {
    synchronized (this.connectionList) {
      for (HAConnection c : this.connectionList) {
        c.shutdown();
      }

      this.connectionList.clear();
    }
  }

  /**
   * getDefaultMessageStore
   *
   * @return ignore
   */
  public DefaultMessageStore getDefaultMessageStore() {
    return defaultMessageStore;
  }

  /**
   * getWaitNotifyObject
   *
   * @return ignore
   */
  public WaitNotifyObject getWaitNotifyObject() {
    return waitNotifyObject;
  }

  /**
   * getPush2SlaveMaxOffset
   *
   * @return ignore
   */
  public AtomicLong getPush2SlaveMaxOffset() {
    return push2SlaveMaxOffset;
  }

  /**
   * 高可用主服务器监听客户端连接实现类
   *
   * <p>主服务器监听从服务器的连接
   *
   * @author shui4
   */
  class AcceptSocketService extends ServiceThread {
    /** Brokerfuw 监听套接字（本地 IP+ 端口） */
    private final SocketAddress socketAddressListen;

    /** 服务端 Socket 通道，基于 NIO */
    private ServerSocketChannel serverSocketChannel;

    /** 事件选择器，基于 NIO */
    private Selector selector;

    /**
     * AcceptSocketService
     *
     * @param port ignore
     */
    public AcceptSocketService(final int port) {
      this.socketAddressListen = new InetSocketAddress(port);
    }

    /**
     * 开始侦听从连接。
     *
     * <p>创建 ServerSocketChannel 和 Selector、设置 TCP reuseAddress、 绑定监听端口、设置为非阻塞模式，并注册 OP_ACCEPT（连接事件
     * )
     *
     * @throws Exception If fails.
     */
    public void beginAccept() throws Exception {
      this.serverSocketChannel = ServerSocketChannel.open();
      this.selector = RemotingUtil.openSelector();
      this.serverSocketChannel.socket().setReuseAddress(true);
      this.serverSocketChannel.socket().bind(this.socketAddressListen);
      this.serverSocketChannel.configureBlocking(false);
      this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown(final boolean interrupt) {
      super.shutdown(interrupt);
      try {
        this.serverSocketChannel.close();
        this.selector.close();
      } catch (IOException e) {
        log.error("AcceptSocketService shutdown exception", e);
      }
    }

    /**
     * 该方法是标准的基于 NIO 的服务端程序实例，选择器每 1s 处理一次 连接事件。连接事件就绪后，调用 {@link ServerSocketChannel#accept()} 创建
     * {@link SocketChannel}。然后为每一个连接创建一个 {@link HAConnection} 对 象，该 {@link HAConnection}
     * 将负责主从数据同步逻辑
     */
    @Override
    public void run() {
      log.info(this.getServiceName() + "service started");

      while (!this.isStopped()) {
        try {
          this.selector.select(1000);
          Set<SelectionKey> selected = this.selector.selectedKeys();

          if (selected != null) {
            for (SelectionKey k : selected) {
              if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                if (sc != null) {
                  HAService.log.info(
                      "HAService receive new connection," + sc.socket().getRemoteSocketAddress());

                  try {
                    HAConnection conn = new HAConnection(HAService.this, sc);
                    conn.start();
                    HAService.this.addConnection(conn);
                  } catch (Exception e) {
                    log.error("new HAConnection exception", e);
                    sc.close();
                  }
                }
              } else {
                log.warn("Unexpected ops in select" + k.readyOps());
              }
            }

            selected.clear();
          }
        } catch (Exception e) {
          log.error(this.getServiceName() + "service has exception.", e);
        }
      }

      log.info(this.getServiceName() + "service end");
    }

    /** {@inheritDoc} */
    @Override
    public String getServiceName() {
      return AcceptSocketService.class.getSimpleName();
    }
  }

  /**
   * 主从同步通知实现类
   *
   * <p>如果是主 从同步模式，消息发送者将消息写入磁盘后，需要继续等待新数据被 传输到从服务器，从服务器数据的复制是在另外一个线程 {@link HAConnection}
   * 中拉取的，所以消息发送者在这里需要等待数据传输的 结果。{@link GroupTransferService} 实现了该功能，该类的整体结构与同步 刷盘实现类（{@link
   * CommitLog.GroupCommitService}***）类似。<br>
   * {@link GroupTransferService} 负责在主从同步复制结束后，通知由于等 待同步结果而阻塞的消息发送者线程。判断主从同步是否完成的依据
   * 是从服务器中已成功复制的消息最大偏移量是否大于、等于消息生产 者发送消息后消息服务端返回下一条消息的起始偏移量，如果是则表 示主从同步复制已经完成，唤醒消息发送线程，否则等待 1s 再次判
   * 断，每一个任务在一批任务中循环判断 5 次。消息发送者返回有两种情况：等待超过 5s 或 {@link GroupTransferService} 通知主从复制完成。可以通 过 {@link
   * MessageStoreConfig#syncFlushTimeout}*** 来设置发送线程的等待超时时间。
   *
   * @author shui4
   */
  @SuppressWarnings("JavadocReference")
  class GroupTransferService extends ServiceThread {

    private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
    private final PutMessageSpinLock lock = new PutMessageSpinLock();
    private volatile LinkedList<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
    private volatile LinkedList<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

    public void putRequest(final CommitLog.GroupCommitRequest request) {
      lock.lock();
      try {
        this.requestsWrite.add(request);
      } finally {
        lock.unlock();
      }
      this.wakeup();
    }

    public void notifyTransferSome() {
      this.notifyTransferObject.wakeup();
    }

    private void swapRequests() {
      lock.lock();
      try {
        LinkedList<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
        this.requestsWrite = this.requestsRead;
        this.requestsRead = tmp;
      } finally {
        lock.unlock();
      }
    }

    /**
     * 该方法在主服务器收到从服务器的拉取请求后被调用，表示从服 务器当前已同步的偏移量，既然收到了从服务器的反馈信息，就需要 唤醒某些消息发送者线程。如果从服务器收到的确认偏移量大于
     * {@link push2SlaveMaxOffset}，则更新 {@link push2SlaveMaxOffset}，然后唤醒 {@link GroupTransferService}
     * 线程，最后各消息发送者线程再次判断本次发 送的消息是否已经成功复制到了从服务器。
     */
    private void doWaitTransfer() {
      if (!this.requestsRead.isEmpty()) {
        for (CommitLog.GroupCommitRequest req : this.requestsRead) {
          boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
          long deadLine = req.getDeadLine();
          while (!transferOK && deadLine - System.nanoTime() > 0) {
            this.notifyTransferObject.waitForRunning(1000);
            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
          }

          req.wakeupCustomer(
              transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
        }

        this.requestsRead = new LinkedList<>();
      }
    }

    public void run() {
      log.info(this.getServiceName() + "service started");

      while (!this.isStopped()) {
        try {
          this.waitForRunning(10);
          this.doWaitTransfer();
        } catch (Exception e) {
          log.warn(this.getServiceName() + "service has exception.", e);
        }
      }

      log.info(this.getServiceName() + "service end");
    }

    @Override
    protected void onWaitEnd() {
      this.swapRequests();
    }

    @Override
    public String getServiceName() {
      return GroupTransferService.class.getSimpleName();
    }
  }

  /**
   * HA 客户端实现类。
   *
   * <p>HAClient 是主从同步从服务端的核心实现类。
   *
   * @author shui4
   */
  class HAClient extends ServiceThread {
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    /** 主服务器地址 */
    private final AtomicReference<String> masterAddress = new AtomicReference<>();

    /** 从服务器向主服务器发起主从同步的拉取偏移量 */
    private final ByteBuffer reportOffset = ByteBuffer.allocate(8);

    /** 网络传输通道 */
    private SocketChannel socketChannel;

    /** NIO 事件选择器 */
    private Selector selector;

    /** 上一次写入消息的时间戳 */
    private long lastWriteTimestamp = System.currentTimeMillis();

    /** 反馈从服务器当前的复制进度，即 CommitLog 文件的最大偏移量 */
    private long currentReportedOffset = 0;

    /** 本次已处理读缓存区的指针 */
    private int dispatchPosition = 0;

    /** 读缓存区，大小为 4MB */
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

    /** 读缓存区备份，与 BufferRead 进行交换 */
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

    public HAClient() throws IOException {
      this.selector = RemotingUtil.openSelector();
    }

    public void updateMasterAddress(final String newAddr) {
      String currentAddr = this.masterAddress.get();
      if (currentAddr == null || !currentAddr.equals(newAddr)) {
        this.masterAddress.set(newAddr);
        log.info("update master address, OLD:" + currentAddr + "NEW:" + newAddr);
      }
    }

    private boolean isTimeToReportOffset() {
      long interval =
          HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
      boolean needHeart =
          interval
              > HAService.this
                  .defaultMessageStore
                  .getMessageStoreConfig()
                  .getHaSendHeartbeatInterval();

      return needHeart;
    }

    /**
     * 向主服务器反馈拉取消息偏移量。
     *
     * <p>这里有两重含义，对 于从服务器来说，是发送下次待拉取消息的偏移量，而对于主服务器 来说，既可以认为是从服务器本次请求拉取的消息偏移量，也可以理 解为从服务器的消息同步 ACK
     * 确认消息。<br>
     * <br>
     * 注意：RocketMQ 提供了一个基于 NIO 的网络写示例程序：首先将 ByteBuffer 的 position 设置为 0，limit 设置为待写入字节长度；然后调用
     * putLong 将 待拉取消息的偏移量写入 ByteBuffer，需要将 ByteBuffer 从写模式切 换到读模式，这里的做法是手动将 position 设置为 0，limit
     * 设置为可 读长度，其实也可以直接调用 ByteBuffer 的 flip()方法来切换 ByteBuffer 的读写状态。特别需要留意的是，调用网络通道的 write() 方法是在一个
     * while 循环中反复判断 byteBuffer 是否全部写入通道中， 这是由于 NIO 是一个非阻塞 I/O，调用一次 write() 方法不一定能将 ByteBuffer
     * 可读字节全部写入。
     *
     * @return ignore
     */
    private boolean reportSlaveMaxOffset(final long maxOffset) {
      this.reportOffset.position(0);
      this.reportOffset.limit(8);
      this.reportOffset.putLong(maxOffset);
      this.reportOffset.position(0);
      this.reportOffset.limit(8);

      for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
        try {
          this.socketChannel.write(this.reportOffset);
        } catch (IOException e) {
          log.error(
              this.getServiceName() + "reportSlaveMaxOffset this.socketChannel.write exception", e);
          return false;
        }
      }

      lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
      return !this.reportOffset.hasRemaining();
    }

    private void reallocateByteBuffer() {
      int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
      if (remain > 0) {
        this.byteBufferRead.position(this.dispatchPosition);

        this.byteBufferBackup.position(0);
        this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
        this.byteBufferBackup.put(this.byteBufferRead);
      }

      this.swapByteBuffer();

      this.byteBufferRead.position(remain);
      this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
      this.dispatchPosition = 0;
    }

    private void swapByteBuffer() {
      ByteBuffer tmp = this.byteBufferRead;
      this.byteBufferRead = this.byteBufferBackup;
      this.byteBufferBackup = tmp;
    }

    /**
     * 处理网络请求。
     *
     * <p>即处理从主服务器传回的消息数据。 RocketMQ给出了一个处理网络读请求的NIO示例。循环判断 readByteBuffer是否还有剩余空间，如果存在剩余空间，则调用
     * {@link SocketChannel#read(ByteBuffer)}方法，将通道中 的数据读入读缓存区。
     *
     * <ol>
     *   <li>如果读取到的字节数大于0，则重置读取到0字节的次数，并更 新最后一次写入消息的时间戳（lastWriteTimestamp），然后调用
     *       dispatchReadRequest方法将读取到的所有消息全部追加到消息内存映射文件中，再次反馈拉取进度给主服务器。
     *   <li>如果连续3次从网络通道读取到0个字节，则结束本次读任务，返回true。
     *   <li>如果读取到的字节数小于0或发生I/O异常，则返回false。
     * </ol>
     *
     * HAClient线程反复执行上述5个步骤完成主从同步复制功能。
     *
     * @return ignore
     */
    private boolean processReadEvent() {
      int readSizeZeroTimes = 0;
      while (this.byteBufferRead.hasRemaining()) {
        try {
          int readSize = this.socketChannel.read(this.byteBufferRead);
          if (readSize > 0) {
            readSizeZeroTimes = 0;
            boolean result = this.dispatchReadRequest();
            if (!result) {
              log.error("HAClient, dispatchReadRequest error");
              return false;
            }
          } else if (readSize == 0) {
            if (++readSizeZeroTimes >= 3) {
              break;
            }
          } else {
            log.info("HAClient, processReadEvent read socket < 0");
            return false;
          }
        } catch (IOException e) {
          log.info("HAClient, processReadEvent read socket exception", e);
          return false;
        }
      }

      return true;
    }

    private boolean dispatchReadRequest() {
      final int msgHeaderSize = 8 + 4; // phyoffset + size

      while (true) {
        int diff = this.byteBufferRead.position() - this.dispatchPosition;
        if (diff >= msgHeaderSize) {
          long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
          int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

          long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

          if (slavePhyOffset != 0) {
            if (slavePhyOffset != masterPhyOffset) {
              log.error(
                  "master pushed offset not equal the max phy offset in slave, SLAVE:"
                      + slavePhyOffset
                      + "MASTER:"
                      + masterPhyOffset);
              return false;
            }
          }

          if (diff >= (msgHeaderSize + bodySize)) {
            byte[] bodyData = byteBufferRead.array();
            int dataStart = this.dispatchPosition + msgHeaderSize;

            HAService.this.defaultMessageStore.appendToCommitLog(
                masterPhyOffset, bodyData, dataStart, bodySize);

            this.dispatchPosition += msgHeaderSize + bodySize;

            if (!reportSlaveMaxOffsetPlus()) {
              return false;
            }

            continue;
          }
        }

        if (!this.byteBufferRead.hasRemaining()) {
          this.reallocateByteBuffer();
        }

        break;
      }

      return true;
    }

    private boolean reportSlaveMaxOffsetPlus() {
      boolean result = true;
      long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
      if (currentPhyOffset > this.currentReportedOffset) {
        this.currentReportedOffset = currentPhyOffset;
        result = this.reportSlaveMaxOffset(this.currentReportedOffset);
        if (!result) {
          this.closeMaster();
          log.error("HAClient, reportSlaveMaxOffset error," + this.currentReportedOffset);
        }
      }

      return result;
    }

    /**
     * 连接主服务器。
     *
     * <p>从服务器连接主服务器。如果 socketChannel 为空，则尝 试连接主服务器。如果主服务器地址为空，返回 false。如果主服务器 地址不为空，则建立到主服务器的 TCP
     * 连接，然后注册 OP_READ（网络 读事件），初始化 currentReportedOffset 为 CommitLog 文件的最大偏 移量、lastWriteTimestamp
     * 上次写入时间戳为当前时间戳，并返回 true。在 Broker 启动时，如果 Broker 角色为从服务器，则读取 Broker 配置文件中的 haMasterAddress 属性并更新
     * HAClient 的 masterAddrees，如果角色为从服务器，但 haMasterAddress 为空，启 动 Broker 并不会报错，但不会执行主从同步复制，该方法最终返回是
     * 否成功连接上主服务器。
     *
     * @return ignore
     * @throws ClosedChannelException ignore
     */
    private boolean connectMaster() throws ClosedChannelException {
      if (null == socketChannel) {
        String addr = this.masterAddress.get();
        if (addr != null) {

          SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
          if (socketAddress != null) {
            this.socketChannel = RemotingUtil.connect(socketAddress);
            if (this.socketChannel != null) {
              this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            }
          }
        }

        this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

        this.lastWriteTimestamp = System.currentTimeMillis();
      }

      return this.socketChannel != null;
    }

    private void closeMaster() {
      if (null != this.socketChannel) {
        try {

          SelectionKey sk = this.socketChannel.keyFor(this.selector);
          if (sk != null) {
            sk.cancel();
          }

          this.socketChannel.close();

          this.socketChannel = null;
        } catch (IOException e) {
          log.warn("closeMaster exception.", e);
        }

        this.lastWriteTimestamp = 0;
        this.dispatchPosition = 0;

        this.byteBufferBackup.position(0);
        this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

        this.byteBufferRead.position(0);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
      }
    }

    @Override
    public void run() {
      log.info(this.getServiceName() + "service started");

      while (!this.isStopped()) {
        try {
          // 连接主服务器
          if (this.connectMaster()) {

            if (this.isTimeToReportOffset()) {
              boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
              if (!result) {
                this.closeMaster();
              }
            }
            // 进行事件选择，执行间隔时间为1s
            this.selector.select(1000);

            boolean ok = this.processReadEvent();
            if (!ok) {
              this.closeMaster();
            }
            // 向主服务器反馈拉取消息偏移量
            if (!reportSlaveMaxOffsetPlus()) {
              continue;
            }

            long interval =
                HAService.this.getDefaultMessageStore().getSystemClock().now()
                    - this.lastWriteTimestamp;
            if (interval
                > HAService.this
                    .getDefaultMessageStore()
                    .getMessageStoreConfig()
                    .getHaHousekeepingInterval()) {
              log.warn(
                  "HAClient, housekeeping, found this connection["
                      + this.masterAddress
                      + "] expired,"
                      + interval);
              this.closeMaster();
              log.warn("HAClient, master not response some time, so close connection");
            }
          } else {
            this.waitForRunning(1000 * 5);
          }
        } catch (Exception e) {
          log.warn(this.getServiceName() + "service has exception.", e);
          this.waitForRunning(1000 * 5);
        }
      }

      log.info(this.getServiceName() + "service end");
    }

    @Override
    public void shutdown() {
      super.shutdown();
      closeMaster();
    }

    // private void disableWriteFlag() {
    // if (this.socketChannel != null) {
    // SelectionKey sk = this.socketChannel.keyFor(this.selector);
    // if (sk != null) {
    // int ops = sk.interestOps();
    // ops &= ~SelectionKey.OP_WRITE;
    // sk.interestOps(ops);
    // }
    // }
    // }
    // private void enableWriteFlag() {
    // if (this.socketChannel != null) {
    // SelectionKey sk = this.socketChannel.keyFor(this.selector);
    // if (sk != null) {
    // int ops = sk.interestOps();
    // ops |= SelectionKey.OP_WRITE;
    // sk.interestOps(ops);
    // }
    // }
    // }

    @Override
    public String getServiceName() {
      return HAClient.class.getSimpleName();
    }
  }
}
