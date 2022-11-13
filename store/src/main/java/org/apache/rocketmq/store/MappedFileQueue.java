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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * 映射文件队列，在RocketMQ中的文件存储 都是依赖于pageCache即 {@link MappedByteBuffer}，该类作为 {@link MappedFile}的成员变量。
 * 当前这个类是 {@link MappedFileQueue}的容器，可以理解为它的目录，比如 commitLog的文件大小限制为{@link
 * #mappedFileSize}，写满以后将创建新的文件，通过全局偏移量作为它的名字。
 */
public class MappedFileQueue {
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
  private static final InternalLogger LOG_ERROR =
      InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

  private static final int DELETE_FILES_BATCH_MAX = 10;
  /** 存储目录 */
  private final String storePath;
  /** 单个文件的存储大小，它根据配置类{@link MessageStoreConfig#getMappedFileSizeCommitLog}设置 */
  protected final int mappedFileSize;
  /** {@link MappedFile}集合 */
  protected final CopyOnWriteArrayList<MappedFile> mappedFiles =
      new CopyOnWriteArrayList<MappedFile>();

  /** 创建MappedFile服务类 */
  private final AllocateMappedFileService allocateMappedFileService;

  /** 当前刷盘指针，表示该指针之前的所有数据全部持久化到磁盘 */
  protected long flushedWhere = 0;
  /** 当前数据提交指针，内存中{@link ByteBuffer} 当前的写指针，该值 >= {@link #flushedWhere} */
  private long committedWhere = 0;

  private volatile long storeTimestamp = 0;

  public MappedFileQueue(
      final String storePath,
      int mappedFileSize,
      AllocateMappedFileService allocateMappedFileService) {
    this.storePath = storePath;
    this.mappedFileSize = mappedFileSize;
    this.allocateMappedFileService = allocateMappedFileService;
  }
  /**
   * 该方法由定时任务调用：org/apache/rocketmq/store/DefaultMessageStore.java:1467(CTRL+SHT+N)。<br>
   * 检查两个文件名称之前的偏移量确认为1GB
   */
  // 检查
  public void checkSelf() {

    if (!this.mappedFiles.isEmpty()) {
      Iterator<MappedFile> iterator = mappedFiles.iterator();
      MappedFile pre = null;
      while (iterator.hasNext()) {
        MappedFile cur = iterator.next();

        if (pre != null) {
          // ? 文件名与文件名之间的偏移量间隔不是1GB──打印日志
          if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
            LOG_ERROR.error(
                "[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                pre.getFileName(),
                cur.getFileName());
          }
        }
        pre = cur;
      }
    }
  }

  /**
   * 代码清单4-11<br>
   * 根据时间戳 查询 MappedFile
   *
   * @param timestamp 时间戳
   * @return 从List中 的 一个 >= timestamp 的 MappedFile，如果没有，则拿最后一个
   */
  public MappedFile getMappedFileByTime(final long timestamp) {
    Object[] mfs = this.copyMappedFiles(0);

    if (null == mfs) return null;

    for (int i = 0; i < mfs.length; i++) {
      MappedFile mappedFile = (MappedFile) mfs[i];
      if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
        return mappedFile;
      }
    }

    return (MappedFile) mfs[mfs.length - 1];
  }

  private Object[] copyMappedFiles(final int reservedMappedFiles) {
    Object[] mfs;

    if (this.mappedFiles.size() <= reservedMappedFiles) {
      return null;
    }

    mfs = this.mappedFiles.toArray();
    return mfs;
  }

  /**
   * 截断文件
   *
   * @param offset 截断 offset>= 文件名称
   */
  public void truncateDirtyFiles(long offset) {
    List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
    // 删除 offset 之后的所有文件。遍历目录下的文件
    for (MappedFile file : this.mappedFiles) {
      long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
      // ? 尾部的偏移量大于 offset ，则进一步比较 offset 与文件的开始偏移量。
      if (fileTailOffset > offset) {
        //  ? offset 大于文件的起始偏移量，说明当前文件包含了有效偏移量，设置 MappedFile 的 flushedPosition 和 committedPosition
        if (offset >= file.getFileFromOffset()) {
          file.setWrotePosition((int) (offset % this.mappedFileSize));
          file.setCommittedPosition((int) (offset % this.mappedFileSize));
          file.setFlushedPosition((int) (offset % this.mappedFileSize));
        }
        //  offset 小 于文件的起始偏移量，说明该文件是有效文件后面创建的，则调用 MappedFile#destory 方法释放 MappedFile
        // 占用的内存资源（内存映射与内存通道等），
        //  然后加入待删除文件列表中，最终调用 deleteExpiredFile 将文件从物理磁盘上删除
        else {
          file.destroy(1000);
          willRemoveFiles.add(file);
        }
      }
      // 文件的尾部偏移量小于 offset 则跳过该文件
    }

    this.deleteExpiredFile(willRemoveFiles);
  }

  void deleteExpiredFile(List<MappedFile> files) {

    if (!files.isEmpty()) {

      Iterator<MappedFile> iterator = files.iterator();
      while (iterator.hasNext()) {
        MappedFile cur = iterator.next();
        if (!this.mappedFiles.contains(cur)) {
          iterator.remove();
          log.info(
              "This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
        }
      }

      try {
        if (!this.mappedFiles.removeAll(files)) {
          log.error("deleteExpiredFile remove failed.");
        }
      } catch (Exception e) {
        log.error("deleteExpiredFile has exception.", e);
      }
    }
  }

  /**
   * 将 {@link #storePath} 路径下的文件进行加载
   *
   * @return
   */
  public boolean load() {
    File dir = new File(this.storePath);
    File[] ls = dir.listFiles();
    if (ls != null) {
      return doLoad(Arrays.asList(ls));
    }
    return true;
  }

  /**
   * 加载文件，对这些文件进行虚拟内存映射(即创建 {@link MappedByteBuffer} 实例到{@link MappedFile})，并放到{@link #mappedFiles}
   *
   * @param files 文件<code>List</code>
   * @return
   *     <li><code>false</code>──文件大小不为1GB（默认）、文件不存在{@link FileNotFoundException }（执行这个过程的途中被删除） ，
   *     <li><code>true</code>──正常
   */
  public boolean doLoad(List<File> files) {
    // 代码清单4-60
    // 根据文件名升序
    files.sort(Comparator.comparing(File::getName));

    for (File file : files) {
      // ? 如果配置文件大小不一致，将忽略目录下的所有文件，然后创建 MappedFile 对象
      if (file.length() != this.mappedFileSize) {
        log.warn(
            file
                + "\t"
                + file.length()
                + " length not matched message store config value, please check it manually");
        return false;
      }

      try {
        // 这里会将 wrotePosition 、 flushedPosition 、 committedPosition 三个指针都设置为文件大小。
        MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

        mappedFile.setWrotePosition(this.mappedFileSize);
        mappedFile.setFlushedPosition(this.mappedFileSize);
        mappedFile.setCommittedPosition(this.mappedFileSize);
        this.mappedFiles.add(mappedFile);
        log.info("load " + file.getPath() + " OK");
      } catch (IOException e) {
        log.error("load file " + file + " error", e);
        return false;
      }
    }
    return true;
  }

  /**
   * 对比磁盘
   *
   * @return pageCache与刷盘相差多少。 0──刷盘偏移量为0,
   */
  public long howMuchFallBehind() {
    if (this.mappedFiles.isEmpty()) return 0;

    long committed = this.flushedWhere;
    if (committed != 0) {
      MappedFile mappedFile = this.getLastMappedFile(0, false);
      if (mappedFile != null) {
        // pageCache的偏移量
        long l = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        // 相差多少
        return l - committed;
      }
    }

    return 0;
  }

  /**
   * 获取最后一个文件，拿不到的时候会根据 needCreate 创建
   *
   * @param startOffset 不知道它的含义。<br>
   *     因为每个文件都为1GB，从 org/apache/rocketmq/store/ha/HAService.java:458 会传递 物理偏移量（全局偏移量）。
   * @param needCreate 需要创建？
   * @return
   */
  public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
    long createOffset = -1;
    MappedFile mappedFileLast = getLastMappedFile();

    if (mappedFileLast == null) {
      createOffset = startOffset - (startOffset % this.mappedFileSize);
    }

    if (mappedFileLast != null && mappedFileLast.isFull()) {
      createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
    }

    if (createOffset != -1 && needCreate) {
      return tryCreateMappedFile(createOffset);
    }

    return mappedFileLast;
  }

  /**
   * 尝试创建映射文件
   *
   * @param createOffset 偏移量
   * @return {@link MappedFile}
   */
  protected MappedFile tryCreateMappedFile(long createOffset) {
    String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
    String nextNextFilePath =
        this.storePath
            + File.separator
            + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
    return doCreateMappedFile(nextFilePath, nextNextFilePath);
  }

  protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
    MappedFile mappedFile = null;

    if (this.allocateMappedFileService != null) {
      mappedFile =
          this.allocateMappedFileService.putRequestAndReturnMappedFile(
              nextFilePath, nextNextFilePath, this.mappedFileSize);
    } else {
      try {
        mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
      } catch (IOException e) {
        log.error("create mappedFile exception", e);
      }
    }

    if (mappedFile != null) {
      if (this.mappedFiles.isEmpty()) {
        mappedFile.setFirstCreateInQueue(true);
      }
      this.mappedFiles.add(mappedFile);
    }

    return mappedFile;
  }

  public MappedFile getLastMappedFile(final long startOffset) {
    return getLastMappedFile(startOffset, true);
  }

  public MappedFile getLastMappedFile() {
    MappedFile mappedFileLast = null;

    while (!this.mappedFiles.isEmpty()) {
      try {
        mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
        break;
      } catch (IndexOutOfBoundsException e) {
        // continue;
      } catch (Exception e) {
        log.error("getLastMappedFile has exception.", e);
        break;
      }
    }

    return mappedFileLast;
  }

  public boolean resetOffset(long offset) {
    MappedFile mappedFileLast = getLastMappedFile();

    if (mappedFileLast != null) {
      long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition();
      long diff = lastOffset - offset;

      final int maxDiff = this.mappedFileSize * 2;
      if (diff > maxDiff) return false;
    }

    ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

    while (iterator.hasPrevious()) {
      mappedFileLast = iterator.previous();
      if (offset >= mappedFileLast.getFileFromOffset()) {
        int where = (int) (offset % mappedFileLast.getFileSize());
        mappedFileLast.setFlushedPosition(where);
        mappedFileLast.setWrotePosition(where);
        mappedFileLast.setCommittedPosition(where);
        break;
      } else {
        iterator.remove();
      }
    }
    return true;
  }

  /**
   * 代码清单4-13 <br>
   *
   * @return 获取最小偏移量
   */
  public long getMinOffset() {

    if (!this.mappedFiles.isEmpty()) {
      try {
        return this.mappedFiles.get(0).getFileFromOffset();
      } catch (IndexOutOfBoundsException e) {
        // continue;
      } catch (Exception e) {
        log.error("getMinOffset has exception.", e);
      }
    }
    return -1;
  }

  /**
   * 代码清单4-14
   *
   * @return 得到最大偏移
   */
  public long getMaxOffset() {
    // 最后一个文件
    MappedFile mappedFile = getLastMappedFile();
    if (mappedFile != null) {
      return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
    }
    return 0;
  }

  /**
   * 与 {@link #getMaxOffset()}不同点：wrotePosition可能拿到的是 {@link MappedFile#writeBuffer}的指针，{@link
   * MappedFile#committedPosition} 是放到 pageCache之后的
   *
   * @return 获取最大写指针偏移量
   */
  public long getMaxWrotePosition() {
    MappedFile mappedFile = getLastMappedFile();
    if (mappedFile != null) {
      return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
    }
    return 0;
  }

  /**
   * @return 仍然要提交（刷盘）多少数据
   */
  public long remainHowManyDataToCommit() {
    return getMaxWrotePosition() - committedWhere;
  }

  /**
   * 对于 pageCache 与 堆buffer 与 刷盘
   *
   * @return 剩下要刷新多少数据
   */
  public long remainHowManyDataToFlush() {
    return getMaxOffset() - flushedWhere;
  }

  /** 删除最后一个文件 */
  public void deleteLastMappedFile() {
    MappedFile lastMappedFile = getLastMappedFile();
    if (lastMappedFile != null) {
      lastMappedFile.destroy(1000);
      this.mappedFiles.remove(lastMappedFile);
      log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());
    }
  }

  /**
   * 删除过期文件时间
   *
   * @param expiredTime 过期时间
   * @param deleteFilesInterval 删除文件时间间隔
   * @param intervalForcibly 间隔强行
   * @param cleanImmediately 立即清洁
   * @return int
   */
  public int deleteExpiredFileByTime(
      final long expiredTime,
      final int deleteFilesInterval,
      final long intervalForcibly,
      final boolean cleanImmediately) {
    Object[] mfs = this.copyMappedFiles(0);

    if (null == mfs) return 0;

    int mfsLength = mfs.length - 1;
    int deleteCount = 0;
    List<MappedFile> files = new ArrayList<MappedFile>();
    if (null != mfs) {
      for (int i = 0; i < mfsLength; i++) {
        MappedFile mappedFile = (MappedFile) mfs[i];
        // 修改修改时间+72小时（默认值）
        long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
        // ? 当前时间 超过 liveMaxTimestamp
        if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
          // ? 释放成功
          if (mappedFile.destroy(intervalForcibly)) {
            files.add(mappedFile);
            deleteCount++;
            // ? 超过 10个文件
            // 结束循环
            if (files.size() >= DELETE_FILES_BATCH_MAX) {
              break;
            }
            // ? !@#!@%!@%
            // sleep
            if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
              try {
                Thread.sleep(deleteFilesInterval);
              } catch (InterruptedException e) {
              }
            }
          }
          // 释放失败，结束循环
          else {
            break;
          }
        } else {
          // avoid deleting files in the middle
          break;
        }
      }
    }

    // 删除 files 中的文件
    deleteExpiredFile(files);

    return deleteCount;
  }

  /**
   * 删除过期文件根据偏移量
   *
   * @param offset 偏移量
   * @param unitSize 单位大小
   * @return int
   */
  public int deleteExpiredFileByOffset(long offset, int unitSize) {
    Object[] mfs = this.copyMappedFiles(0);

    List<MappedFile> files = new ArrayList<MappedFile>();
    int deleteCount = 0;
    if (null != mfs) {

      int mfsLength = mfs.length - 1;

      for (int i = 0; i < mfsLength; i++) {
        boolean destroy;
        MappedFile mappedFile = (MappedFile) mfs[i];
        SelectMappedBufferResult result =
            mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
        if (result != null) {
          long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
          result.release();
          destroy = maxOffsetInLogicQueue < offset;
          if (destroy) {
            log.info(
                "physic min offset "
                    + offset
                    + ", logics in current mappedFile max offset "
                    + maxOffsetInLogicQueue
                    + ", delete it");
          }
        } else if (!mappedFile.isAvailable()) { // Handle hanged file.
          log.warn("Found a hanged consume queue file, attempting to delete it.");
          destroy = true;
        } else {
          log.warn("this being not executed forever.");
          break;
        }

        if (destroy && mappedFile.destroy(1000 * 60)) {
          files.add(mappedFile);
          deleteCount++;
        } else {
          break;
        }
      }
    }

    deleteExpiredFile(files);

    return deleteCount;
  }

  /**
   * 冲洗
   *
   * @param flushLeastPages 冲洗至少页面
   * @return boolean
   */
  public boolean flush(final int flushLeastPages) {
    boolean result = true;
    MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
    if (mappedFile != null) {
      long tmpTimeStamp = mappedFile.getStoreTimestamp();
      int offset = mappedFile.flush(flushLeastPages);
      long where = mappedFile.getFileFromOffset() + offset;
      result = where == this.flushedWhere;
      this.flushedWhere = where;
      if (0 == flushLeastPages) {
        this.storeTimestamp = tmpTimeStamp;
      }
    }

    return result;
  }

  /**
   * 提交
   *
   * @param commitLeastPages 提交至少页面
   * @return boolean
   */
  public boolean commit(final int commitLeastPages) {
    boolean result = true;
    MappedFile mappedFile =
        this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
    if (mappedFile != null) {
      int offset = mappedFile.commit(commitLeastPages);
      long where = mappedFile.getFileFromOffset() + offset;
      result = where == this.committedWhere;
      this.committedWhere = where;
    }

    return result;
  }

  /**
   * 代码清单4-12 <br>
   * 按偏移量查找映射文件
   *
   * @param offset Offset.
   * @param returnFirstOnNotFound 如果没有找到映射的文件，则返回第一个文件。
   * @return 映射文件或null(当没有找到并且returnFirstOnNotFound是<code>false</code>)。
   */
  public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
    try {
      MappedFile firstMappedFile = this.getFirstMappedFile();
      MappedFile lastMappedFile = this.getLastMappedFile();
      if (firstMappedFile != null && lastMappedFile != null) {
        // ? 偏移量不在范围内
        if (offset < firstMappedFile.getFileFromOffset()
            || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
          LOG_ERROR.warn(
              "Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
              offset,
              firstMappedFile.getFileFromOffset(),
              lastMappedFile.getFileFromOffset() + this.mappedFileSize,
              this.mappedFileSize,
              this.mappedFiles.size());
        }
        // 偏移量在queue的范围内
        else {
          // 根据偏移量计算出处属于哪个MappedFile
          int index =
              (int)
                  ((offset / this.mappedFileSize)
                      - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
          MappedFile targetFile = null;
          try {
            targetFile = this.mappedFiles.get(index);
          } catch (Exception ignored) {
          }

          if (targetFile != null
              && offset >= targetFile.getFileFromOffset()
              && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
            return targetFile;
          }
          // 到这里 可能是 越界了，或者未找到合适的
          // 按顺序遍历获取一个符合的
          for (MappedFile tmpMappedFile : this.mappedFiles) {
            if (offset >= tmpMappedFile.getFileFromOffset()
                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
              return tmpMappedFile;
            }
          }
        }

        if (returnFirstOnNotFound) {
          return firstMappedFile;
        }
      }
    } catch (Exception e) {
      log.error("findMappedFileByOffset Exception", e);
    }

    return null;
  }

  /**
   * 得到第一个映射文件
   *
   * @return {@link MappedFile}
   */
  public MappedFile getFirstMappedFile() {
    MappedFile mappedFileFirst = null;

    if (!this.mappedFiles.isEmpty()) {
      try {
        mappedFileFirst = this.mappedFiles.get(0);
      } catch (IndexOutOfBoundsException e) {
        // ignore
      } catch (Exception e) {
        log.error("getFirstMappedFile has exception.", e);
      }
    }

    return mappedFileFirst;
  }

  /**
   * 找到映射文件偏移量
   *
   * @param offset 抵消
   * @return {@link MappedFile}
   */
  public MappedFile findMappedFileByOffset(final long offset) {
    return findMappedFileByOffset(offset, false);
  }

  /**
   * 得到映射内存大小
   *
   * @return long
   */
  public long getMappedMemorySize() {
    long size = 0;

    Object[] mfs = this.copyMappedFiles(0);
    if (mfs != null) {
      for (Object mf : mfs) {
        if (((ReferenceResource) mf).isAvailable()) {
          size += this.mappedFileSize;
        }
      }
    }

    return size;
  }

  /**
   * 重试删除第一个文件
   *
   * @param intervalForcibly 间隔强行
   * @return boolean
   */
  public boolean retryDeleteFirstFile(final long intervalForcibly) {
    MappedFile mappedFile = this.getFirstMappedFile();
    if (mappedFile != null) {
      if (!mappedFile.isAvailable()) {
        log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
        boolean result = mappedFile.destroy(intervalForcibly);
        if (result) {
          log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
          List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
          tmpFiles.add(mappedFile);
          this.deleteExpiredFile(tmpFiles);
        } else {
          log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
        }

        return result;
      }
    }

    return false;
  }

  public void shutdown(final long intervalForcibly) {
    for (MappedFile mf : this.mappedFiles) {
      mf.shutdown(intervalForcibly);
    }
  }

  public void destroy() {
    for (MappedFile mf : this.mappedFiles) {
      mf.destroy(1000 * 3);
    }
    this.mappedFiles.clear();
    this.flushedWhere = 0;

    // delete parent directory
    File file = new File(storePath);
    if (file.isDirectory()) {
      file.delete();
    }
  }

  public long getFlushedWhere() {
    return flushedWhere;
  }

  public void setFlushedWhere(long flushedWhere) {
    this.flushedWhere = flushedWhere;
  }

  public long getStoreTimestamp() {
    return storeTimestamp;
  }

  public List<MappedFile> getMappedFiles() {
    return mappedFiles;
  }

  public int getMappedFileSize() {
    return mappedFileSize;
  }

  public long getCommittedWhere() {
    return committedWhere;
  }

  public void setCommittedWhere(final long committedWhere) {
    this.committedWhere = committedWhere;
  }
}
