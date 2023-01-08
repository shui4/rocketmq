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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/** 内存映射文件 */
public class MappedFile extends ReferenceResource {
  /** 操作系统每页大小，默认4KB。 */
  public static final int OS_PAGE_SIZE = 1024 * 4;

  protected static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
  /** 当前JVM实实例中MappedFile的虚拟内存 */
  private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

  /** 当前JVM实例中的MappedFile对象个数 */
  private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
  // region 指针相关：wrotePosition -> committedPosition -> flushedPosition
  /** 当前文件的写指针，从0开始（内存映射文件中的写指针） */
  protected final AtomicInteger wrotePosition = new AtomicInteger(0);
  /**
   * 当前文件的提交指针，如果开启{@link MessageStoreConfig#setTransientStorePoolEnable}，则数据会存储在{@link
   * TransientStorePool} 中，然后提交到内存映射ByteBuffer中，再写入磁盘
   */
  protected final AtomicInteger committedPosition = new AtomicInteger(0);
  /** 将该指针之前的的数据持久化存储到磁盘中 */
  private final AtomicInteger flushedPosition = new AtomicInteger(0);
  // endregion
  /** 文件大小。取决于 如{@link MessageStoreConfig#setMappedFileSizeCommitLog}...等配置 */
  protected int fileSize;
  /** 文件通道 */
  protected FileChannel fileChannel;
  // region isTransientStorePoolEnable 相关
  /**
   * 堆外内存ByteBuffer，如果不为空，数据首先将存储在 该Buffer中，然后提交MappedFile创建的FileChannel中。{@link
   * MessageStoreConfig#isTransientStorePoolEnable()}为true时不为空
   */
  protected ByteBuffer writeBuffer = null;
  /**
   * 堆外内存池，该内存池中的内存会提供内存锁机制。{@link MessageStoreConfig#setTransientStorePoolEnable(boolean)}为true时启用
   */
  protected TransientStorePool transientStorePool = null;
  // endregion
  /** 文件名 */
  private String fileName;
  /**
   * 文件逻辑偏移量，即文件名
   */
  private long fileFromOffset;
  /** 物理文件 */
  private File file;
  /** 物理文件对应的内存映射Buffer ，备注：目前来看它的position永远为0，都是先slice */
  private MappedByteBuffer mappedByteBuffer;
  /** 文件最后一次写入内容的时间 */
  private volatile long storeTimestamp = 0;
  /** 是否是{@link MappedFileQueue}队列中的第一个文件 */
  private boolean firstCreateInQueue = false;

  public MappedFile() {}

  public MappedFile(final String fileName, final int fileSize) throws IOException {
    init(fileName, fileSize);
  }

  public MappedFile(
      final String fileName, final int fileSize, final TransientStorePool transientStorePool)
      throws IOException {
    init(fileName, fileSize, transientStorePool);
  }

  public static void ensureDirOK(final String dirName) {
    if (dirName != null) {
      File f = new File(dirName);
      if (!f.exists()) {
        boolean result = f.mkdirs();
        log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
      }
    }
  }

  public static void clean(final ByteBuffer buffer) {
    if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) return;
    invoke(invoke(viewed(buffer), "cleaner"), "clean");
  }

  private static Object invoke(
      final Object target, final String methodName, final Class<?>... args) {
    return AccessController.doPrivileged(
        new PrivilegedAction<Object>() {
          public Object run() {
            try {
              Method method = method(target, methodName, args);
              method.setAccessible(true);
              return method.invoke(target);
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
          }
        });
  }

  private static Method method(Object target, String methodName, Class<?>[] args)
      throws NoSuchMethodException {
    try {
      return target.getClass().getMethod(methodName, args);
    } catch (NoSuchMethodException e) {
      return target.getClass().getDeclaredMethod(methodName, args);
    }
  }

  private static ByteBuffer viewed(ByteBuffer buffer) {
    String methodName = "viewedBuffer";
    Method[] methods = buffer.getClass().getMethods();
    for (int i = 0; i < methods.length; i++) {
      if (methods[i].getName().equals("attachment")) {
        methodName = "attachment";
        break;
      }
    }

    ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
    if (viewedBuffer == null) return buffer;
    else return viewed(viewedBuffer);
  }

  public static int getTotalMappedFiles() {
    return TOTAL_MAPPED_FILES.get();
  }

  public static long getTotalMappedVirtualMemory() {
    return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
  }
  // 代码清单4-16 MappedFile初始化
  public void init(
      final String fileName, final int fileSize, final TransientStorePool transientStorePool)
      throws IOException {
    init(fileName, fileSize);
    this.writeBuffer = transientStorePool.borrowBuffer();
    this.transientStorePool = transientStorePool;
  }

  private void init(final String fileName, final int fileSize) throws IOException {
    this.fileName = fileName;
    this.fileSize = fileSize;
    this.file = new File(fileName);
    this.fileFromOffset = Long.parseLong(this.file.getName());
    boolean ok = false;

    ensureDirOK(this.file.getParent());

    try {
      this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
      this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
      TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
      TOTAL_MAPPED_FILES.incrementAndGet();
      ok = true;
    } catch (FileNotFoundException e) {
      log.error("Failed to create file " + this.fileName, e);
      throw e;
    } catch (IOException e) {
      log.error("Failed to map file " + this.fileName, e);
      throw e;
    } finally {
      if (!ok && this.fileChannel != null) {
        this.fileChannel.close();
      }
    }
  }

  public long getLastModifiedTimestamp() {
    return this.file.lastModified();
  }

  public int getFileSize() {
    return fileSize;
  }

  public FileChannel getFileChannel() {
    return fileChannel;
  }

  public AppendMessageResult appendMessage(
      final MessageExtBrokerInner msg,
      final AppendMessageCallback cb,
      PutMessageContext putMessageContext) {
    return appendMessagesInner(msg, cb, putMessageContext);
  }

  public AppendMessageResult appendMessages(
      final MessageExtBatch messageExtBatch,
      final AppendMessageCallback cb,
      PutMessageContext putMessageContext) {
    return appendMessagesInner(messageExtBatch, cb, putMessageContext);
  }

  public AppendMessageResult appendMessagesInner(
      final MessageExt messageExt,
      final AppendMessageCallback cb,
      PutMessageContext putMessageContext) {
    assert messageExt != null;
    assert cb != null;

    // 代码清单4-3
    int currentPos = this.wrotePosition.get();
    // ? 没有超过文件最大限制
    if (currentPos < this.fileSize) {
      // 堆外内存+MappedByte分离、或者只使用 MappedByte，具体看配置
      // MessageStoreConfig.transientStorePoolEnable。临时存储池启用模式减轻 pageCache的压力，以免 broker busy
      // 然后 slice 公用buffer中同一个byte数组，这个方法能够让其与mappedByteBuffer独立下标
      ByteBuffer byteBuffer =
          writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
      // 设置当前位置
      byteBuffer.position(currentPos);
      AppendMessageResult result;
      // 普通消息
      if (messageExt instanceof MessageExtBrokerInner) {
        result =
            cb.doAppend(
                this.getFileFromOffset(),
                byteBuffer,
                // 还剩多少
                this.fileSize - currentPos,
                (MessageExtBrokerInner) messageExt,
                putMessageContext);
      }
      // 批量消息
      else if (messageExt instanceof MessageExtBatch) {
        result =
            cb.doAppend(
                this.getFileFromOffset(),
                byteBuffer,
                this.fileSize - currentPos,
                (MessageExtBatch) messageExt,
                putMessageContext);
      }
      // ? 超过文件最大限制
      else {
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
      }
      this.wrotePosition.addAndGet(result.getWroteBytes());
      this.storeTimestamp = result.getStoreTimestamp();
      return result;
    }
    // 文件写满？
    log.error(
        "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
        currentPos,
        this.fileSize);
    return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
  }

  public long getFileFromOffset() {
    return this.fileFromOffset;
  }

  public boolean appendMessage(final byte[] data) {
    int currentPos = this.wrotePosition.get();

    if ((currentPos + data.length) <= this.fileSize) {
      try {
        ByteBuffer buf = this.mappedByteBuffer.slice();
        buf.position(currentPos);
        buf.put(data);
      } catch (Throwable e) {
        log.error("Error occurred when append message to mappedFile.", e);
      }
      this.wrotePosition.addAndGet(data.length);
      return true;
    }

    return false;
  }

  /**
   * Content of data from offset to offset + length will be wrote to file.
   *
   * @param offset The offset of the subarray to be used.
   * @param length The length of the subarray to be used.
   */
  public boolean appendMessage(final byte[] data, final int offset, final int length) {
    int currentPos = this.wrotePosition.get();

    if ((currentPos + length) <= this.fileSize) {
      try {
        ByteBuffer buf = this.mappedByteBuffer.slice();
        buf.position(currentPos);
        buf.put(data, offset, length);
      } catch (Throwable e) {
        log.error("Error occurred when append message to mappedFile.", e);
      }
      this.wrotePosition.addAndGet(length);
      return true;
    }

    return false;
  }

  /**
   * 刷盘 <br>
   * 代码清单4-21
   *
   * @param flushLeastPages 最少刷盘页数量，需要达到这个量才会刷盘
   * @return The current flushed position
   */
  public int flush(final int flushLeastPages) {
    if (this.isAbleToFlush(flushLeastPages)) {
      if (this.hold()) {
        int value = getReadPosition();

        try {
          // 我们只将数据附加到 fileChannel 或 mappedByteBuffer，而不是两者。
          // 预防数据不完整，因为如果 启用 transientStorePoolEnable ,在执行commit的时候，可能执行到一半，所以不应该从 mappedByteBuffer
          // 执行 force
          if (writeBuffer != null || this.fileChannel.position() != 0) {
            this.fileChannel.force(false);
          } else {
            this.mappedByteBuffer.force();
          }
        } catch (Throwable e) {
          log.error("Error occurred when force data to disk.", e);
        }

        this.flushedPosition.set(value);
        // 释放，取消占用即引用计数器 -1
        this.release();
      } else {
        log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
        this.flushedPosition.set(getReadPosition());
      }
    }
    return this.getFlushedPosition();
  }

  /**
   * 提交，将堆外内存数据提交到内存映射上，只有 transientStorePoolEnable 这个动作才会生效 <br>
   * 代码清单4-18
   *
   * @param commitLeastPages 本次提交的最小页数，如果待提交数据不满足 commitLeastPages ，则不执行本次提交操作，等待下次提交
   * @return 提交的写指针
   */
  public int commit(final int commitLeastPages) {
    if (writeBuffer == null) {
      // 无需将数据提交到文件通道，因此只需将 writePosition 视为committedPosition。
      return this.wrotePosition.get();
    }
    // ? 能够提交
    if (this.isAbleToCommit(commitLeastPages)) {
      if (this.hold()) {
        commit0();
        // 释放，到引用计数器 -1 ，因为前面才 commit0() 方法对引用计数器 +1 了，
        // 这里如果 引用计数器 为 0 ，并且为不可用状态， mappedByteBuffer( 内存映射 ) 将被销毁
        this.release();
      } else {
        log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
      }
    }

    // 文件写满，并且 transientStorePool 不为空以及 writeBuffer 堆外内存不为空，那它现在就不需要了，进行回收
    if (writeBuffer != null
        && this.transientStorePool != null
        && this.fileSize == this.committedPosition.get()) {
      // 将 writeBuffer 归还
      this.transientStorePool.returnBuffer(writeBuffer);
      this.writeBuffer = null;
    }
    return this.committedPosition.get();
  }

  protected void commit0() {
    int writePos = this.wrotePosition.get();
    int lastCommittedPosition = this.committedPosition.get();
    // 是否要从堆外内存中将数据提交至内存映射上
    if (writePos - lastCommittedPosition > 0) {
      try {
        ByteBuffer byteBuffer = writeBuffer.slice();
        byteBuffer.position(lastCommittedPosition);
        byteBuffer.limit(writePos);
        this.fileChannel.position(lastCommittedPosition);
        this.fileChannel.write(byteBuffer);
        this.committedPosition.set(writePos);
      } catch (Throwable e) {
        log.error("Error occurred when commit data to FileChannel.", e);
      }
    }
  }

  private boolean isAbleToFlush(final int flushLeastPages) {
    int flush = this.flushedPosition.get();
    int write = getReadPosition();
    // ? 满了
    if (this.isFull()) {
      return true;
    }

    if (flushLeastPages > 0) {
      return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
    }

    return write > flush;
  }

  protected boolean isAbleToCommit(final int commitLeastPages) {
    int flush = this.committedPosition.get();
    int write = this.wrotePosition.get();
    // ? 该文件已满（当堆外内存写指针达到文件大小）
    if (this.isFull()) {
      return true;
    }
    if (commitLeastPages > 0) {
      // ? 堆外内存写指针转换的page  与  已提交到内存映射指针转换的page 相差（称为脏页数量） 超过 commitLeastPages
      return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
    }
    // commitLeastPages<0
    return write > flush;
  }

  public int getFlushedPosition() {
    return flushedPosition.get();
  }

  public void setFlushedPosition(int pos) {
    this.flushedPosition.set(pos);
  }

  public boolean isFull() {
    return this.fileSize == this.wrotePosition.get();
  }

  public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
    int readPosition = getReadPosition();
    if ((pos + size) <= readPosition) {
      if (this.hold()) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(pos);
        ByteBuffer byteBufferNew = byteBuffer.slice();
        byteBufferNew.limit(size);
        return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
      } else {
        log.warn(
            "matched, but hold failed, request pos: "
                + pos
                + ", fileFromOffset: "
                + this.fileFromOffset);
      }
    } else {
      log.warn(
          "selectMappedBuffer request pos invalid, request pos: "
              + pos
              + ", size: "
              + size
              + ", fileFromOffset: "
              + this.fileFromOffset);
    }

    return null;
  }

  /**
   * @param pos 查询它之后的可用信息
   * @return 查询映射内存
   */
  public SelectMappedBufferResult selectMappedBuffer(int pos) {
    int readPosition = getReadPosition();
    // pos between 0,readPosition
    if (pos < readPosition && pos >= 0) {
      if (this.hold()) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(pos);
        // 可读长度（因为后面全是空数据）
        int size = readPosition - pos;
        ByteBuffer byteBufferNew = byteBuffer.slice();
        byteBufferNew.limit(size);
        return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
      }
    }

    return null;
  }

  @Override
  public boolean cleanup(final long currentRef) {
    if (this.isAvailable()) {
      log.error(
          "this file[REF:"
              + currentRef
              + "] "
              + this.fileName
              + " have not shutdown, stop unmapping.");
      return false;
    }

    if (this.isCleanupOver()) {
      log.error(
          "this file[REF:"
              + currentRef
              + "] "
              + this.fileName
              + " have cleanup, do not do it again.");
      return true;
    }
    // 通过反射 对堆外 内存销毁
    clean(this.mappedByteBuffer);
    TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
    TOTAL_MAPPED_FILES.decrementAndGet();
    log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
    return true;
  }

  /**
   * 销毁
   *
   * @param intervalForcibly 强制间隔时间，单位：毫秒，多次尝试销毁，由于其它线程占用该资源因此销毁失败（引用计数器>0），
   *     对于重复执行该方法，如果在此间隔范围内，则直接忽略
   * @return boolean
   */
  public boolean destroy(final long intervalForcibly) {
    // 销毁资源
    this.shutdown(intervalForcibly);
    // ? 清理结束了，因为上面的方法不一定能成功销毁，因为可能有其它线程正在访问（引用计数器>0）
    // 关闭文件通道、删除文件
    if (this.isCleanupOver()) {
      try {
        // 关闭文件通道
        this.fileChannel.close();
        log.info("close file channel " + this.fileName + " OK");

        long beginTime = System.currentTimeMillis();
        // 删除文件
        boolean result = this.file.delete();
        log.info(
            "delete file[REF:"
                + this.getRefCount()
                + "] "
                + this.fileName
                + (result ? " OK, " : " Failed, ")
                + "W:"
                + this.getWrotePosition()
                + " M:"
                + this.getFlushedPosition()
                + ", "
                + UtilAll.computeElapsedTimeMilliseconds(beginTime));
      } catch (Exception e) {
        log.warn("close file channel " + this.fileName + " Failed. ", e);
      }

      return true;
    } else {
      log.warn(
          "destroy mapped file[REF:"
              + this.getRefCount()
              + "] "
              + this.fileName
              + " Failed. cleanupOver: "
              + this.cleanupOver);
    }

    return false;
  }

  /**
   * 得到写指针
   *
   * @return int
   */
  public int getWrotePosition() {
    return wrotePosition.get();
  }

  public void setWrotePosition(int pos) {
    this.wrotePosition.set(pos);
  }

  /**
   * 获取 最大可读指针，如果启用 transientStorePoolEnable，则会读 committedPosition（内存映射读指针），因为这才是可靠的信息
   *
   * @return 有有效数据的最大位置
   */
  public int getReadPosition() {
    return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
  }

  public void setCommittedPosition(int pos) {
    this.committedPosition.set(pos);
  }

  public void warmMappedFile(FlushDiskType type, int pages) {
    long beginTime = System.currentTimeMillis();
    ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
    int flush = 0;
    long time = System.currentTimeMillis();
    for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
      byteBuffer.put(i, (byte) 0);
      // force flush when flush disk type is sync
      if (type == FlushDiskType.SYNC_FLUSH) {
        if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
          flush = i;
          mappedByteBuffer.force();
        }
      }

      // prevent gc
      if (j % 1000 == 0) {
        log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
        try {
          Thread.sleep(0);
        } catch (InterruptedException e) {
          log.error("Interrupted", e);
        }
      }
    }

    // force flush when prepare load finished
    if (type == FlushDiskType.SYNC_FLUSH) {
      log.info(
          "mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
          this.getFileName(),
          System.currentTimeMillis() - beginTime);
      mappedByteBuffer.force();
    }
    log.info(
        "mapped file warm-up done. mappedFile={}, costTime={}",
        this.getFileName(),
        System.currentTimeMillis() - beginTime);

    this.mlock();
  }

  public String getFileName() {
    return fileName;
  }

  public MappedByteBuffer getMappedByteBuffer() {
    return mappedByteBuffer;
  }

  public ByteBuffer sliceByteBuffer() {
    return this.mappedByteBuffer.slice();
  }

  public long getStoreTimestamp() {
    return storeTimestamp;
  }

  public boolean isFirstCreateInQueue() {
    return firstCreateInQueue;
  }

  public void setFirstCreateInQueue(boolean firstCreateInQueue) {
    this.firstCreateInQueue = firstCreateInQueue;
  }

  public void mlock() {
    final long beginTime = System.currentTimeMillis();
    final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
    Pointer pointer = new Pointer(address);
    {
      int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
      log.info(
          "mlock {} {} {} ret = {} time consuming = {}",
          address,
          this.fileName,
          this.fileSize,
          ret,
          System.currentTimeMillis() - beginTime);
    }

    {
      int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
      log.info(
          "madvise {} {} {} ret = {} time consuming = {}",
          address,
          this.fileName,
          this.fileSize,
          ret,
          System.currentTimeMillis() - beginTime);
    }
  }

  public void munlock() {
    final long beginTime = System.currentTimeMillis();
    final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
    Pointer pointer = new Pointer(address);
    int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
    log.info(
        "munlock {} {} {} ret = {} time consuming = {}",
        address,
        this.fileName,
        this.fileSize,
        ret,
        System.currentTimeMillis() - beginTime);
  }

  // testable
  File getFile() {
    return this.file;
  }

  @Override
  public String toString() {
    return this.fileName;
  }
}
