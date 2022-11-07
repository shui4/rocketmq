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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * 索引文件
 *
 * @author shui4
 */
public class IndexFile {
  /** 日志 */
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
  /** 哈希槽大小 */
  private static int hashSlotSize = 4;
  /** 索引大小 */
  private static int indexSize = 20;
  /** 无效索引 */
  private static int invalidIndex = 0;
  /** 哈希槽编号 */
  private final int hashSlotNum;
  /** 最大索引条目数,{@link MessageStoreConfig#getMaxIndexNum()} */
  private final int indexNum;
  /** 映射文件 */
  private final MappedFile mappedFile;
  /** 文件通道 */
  private final FileChannel fileChannel;
  /** 映射字节缓冲器 */
  private final MappedByteBuffer mappedByteBuffer;
  /** 索引标头 */
  private final IndexHeader indexHeader;

  /**
   * 索引文件
   *
   * @param fileName 文件名
   * @param hashSlotNum 哈希槽编号
   * @param indexNum 索引编号
   * @param endPhyOffset 结束phy偏移
   * @param endTimestamp 结束时间戳
   * @throws IOException IO异常
   */
  public IndexFile(
      final String fileName,
      final int hashSlotNum,
      final int indexNum,
      final long endPhyOffset,
      final long endTimestamp)
      throws IOException {
    int fileTotalSize =
        IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
    this.mappedFile = new MappedFile(fileName, fileTotalSize);
    this.fileChannel = this.mappedFile.getFileChannel();
    this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
    this.hashSlotNum = hashSlotNum;
    this.indexNum = indexNum;

    ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
    this.indexHeader = new IndexHeader(byteBuffer);

    if (endPhyOffset > 0) {
      this.indexHeader.setBeginPhyOffset(endPhyOffset);
      this.indexHeader.setEndPhyOffset(endPhyOffset);
    }

    if (endTimestamp > 0) {
      this.indexHeader.setBeginTimestamp(endTimestamp);
      this.indexHeader.setEndTimestamp(endTimestamp);
    }
  }

  /**
   * 获取文件名
   *
   * @return {@link String}
   */
  public String getFileName() {
    return this.mappedFile.getFileName();
  }

  /** 负载 */
  public void load() {
    this.indexHeader.load();
  }

  /** 脸红 */
  public void flush() {
    long beginTime = System.currentTimeMillis();
    if (this.mappedFile.hold()) {
      this.indexHeader.updateByteBuffer();
      this.mappedByteBuffer.force();
      this.mappedFile.release();
      log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
    }
  }

  /**
   * 写入已满
   *
   * @return boolean
   */
  public boolean isWriteFull() {
    return this.indexHeader.getIndexCount() >= this.indexNum;
  }

  /**
   * 摧毁
   *
   * @param intervalForcibly 强制间隔
   * @return boolean
   */
  public boolean destroy(final long intervalForcibly) {
    return this.mappedFile.destroy(intervalForcibly);
  }

  /**
   * put键
   *
   * @param key 索引Key
   * @param phyOffset 消息物理偏移量
   * @param storeTimestamp 存储时间戳
   * @return boolean
   */
  public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
    // ? 未超过最大限制
    // 代码清单4-38
    if (this.indexHeader.getIndexCount() < this.indexNum) {
      // 根据key算出哈希码
      int keyHash = indexKeyHashMethod(key);
      // 根据 keyHash 对哈希槽数量取余定位到哈希码对应的哈希槽下标
      int slotPos = keyHash % this.hashSlotNum;
      // 哈希码对应的哈希槽的物理地址为 IndexHeader （ 40 字节） + 下标 * 每个哈希槽的大小（ 4 字节）
      int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

      FileLock fileLock = null;

      try {

        // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,false);
        // 读取哈希槽中存储的数据
        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
        // ? 哈希槽存储的数据小于0 或大于当前Index文件中的索引条目
        if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
          // 设置为 0
          slotValue = invalidIndex;
        }
        // 代码清单4-40
        // 读取哈希槽中存储的数据，如果哈希槽存储的数据小于 0 或大于当前 Index 文件中的索引条目，则将 slotValue 设置为 0
        long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
        // 计算待存储消息的时间戳与第一条消息时间戳的差值，并转换为秒
        timeDiff = timeDiff / 1000;

        if (this.indexHeader.getBeginTimestamp() <= 0) {
          timeDiff = 0;
        } else if (timeDiff > Integer.MAX_VALUE) {
          timeDiff = Integer.MAX_VALUE;
        } else if (timeDiff < 0) {
          timeDiff = 0;
        }
        // 计算新添加条目的起始物理偏移量：头部字节长度 + 哈希槽数量×单个哈希槽大小（ 4 个字节） + 当前 Index 条目个数×单个 Index 条 目大小（ 20 个字节）
        int absIndexPos =
            IndexHeader.INDEX_HEADER_SIZE
                + this.hashSlotNum * hashSlotSize
                + this.indexHeader.getIndexCount() * indexSize;
        // 依次将哈希码
        this.mappedByteBuffer.putInt(absIndexPos, keyHash);
        // 消息物理偏移量
        this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
        // 消息存储时间戳与 Index 文件时间戳
        this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
        // 当前哈希槽的值存入 MappedByteBuffer
        this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
        // 将当前 Index 文件中包含的条目数量存入哈希槽中，覆盖原先 哈希槽的值
        this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
        // 更新文件索引头信息
        // ? 当前文件只包含一个条目
        if (this.indexHeader.getIndexCount() <= 1) {
          // 更新beginPhyOffset、beginTimestamp
          this.indexHeader.setBeginPhyOffset(phyOffset);
          this.indexHeader.setBeginTimestamp(storeTimestamp);
        }

        if (invalidIndex == slotValue) {
          this.indexHeader.incHashSlotCount();
        }
        this.indexHeader.incIndexCount();
        // 更新 endPyhOffset
        this.indexHeader.setEndPhyOffset(phyOffset);
        // 最新存储时间戳
        this.indexHeader.setEndTimestamp(storeTimestamp);

        return true;
      } catch (Exception e) {
        log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
      } finally {
        if (fileLock != null) {
          try {
            fileLock.release();
          } catch (IOException e) {
            log.error("Failed to release the lock", e);
          }
        }
      }
    } else {
      log.warn(
          "Over index file capacity: index count = "
              + this.indexHeader.getIndexCount()
              + "; index max num = "
              + this.indexNum);
    }

    return false;
  }

  /**
   * 索引键哈希方法
   *
   * @param key 钥匙
   * @return int
   */
  public int indexKeyHashMethod(final String key) {
    int keyHash = key.hashCode();
    int keyHashPositive = Math.abs(keyHash);
    if (keyHashPositive < 0) keyHashPositive = 0;
    return keyHashPositive;
  }

  /**
   * 获取开始时间戳
   *
   * @return long
   */
  public long getBeginTimestamp() {
    return this.indexHeader.getBeginTimestamp();
  }

  /**
   * 获取结束时间戳
   *
   * @return long
   */
  public long getEndTimestamp() {
    return this.indexHeader.getEndTimestamp();
  }

  /**
   * 获取结束phy偏移
   *
   * @return long
   */
  public long getEndPhyOffset() {
    return this.indexHeader.getEndPhyOffset();
  }

  /**
   * 时间匹配
   *
   * @param begin 开始
   * @param end 终止
   * @return boolean
   */
  public boolean isTimeMatched(final long begin, final long end) {
    boolean result =
        begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
    result =
        result
            || (begin >= this.indexHeader.getBeginTimestamp()
                && begin <= this.indexHeader.getEndTimestamp());
    result =
        result
            || (end >= this.indexHeader.getBeginTimestamp()
                && end <= this.indexHeader.getEndTimestamp());
    return result;
  }

  /**
   * 查询物理偏移量
   *
   * @param phyOffsets 物理偏移量（out）
   * @param key KEY
   * @param maxNum 本次查找最大消息的条数
   * @param begin 开始时间戳
   * @param end 结束时间搓
   * @param lock 锁吗？
   */
  public void selectPhyOffset(
      final List<Long> phyOffsets,
      final String key,
      final int maxNum,
      final long begin,
      final long end,
      boolean lock) {
    if (this.mappedFile.hold()) {
      // 根据 key 算出 key 的哈希码， keyHash 对哈希槽数量取余，定位到哈希码对应的哈希槽下标，
      int keyHash = indexKeyHashMethod(key);
      int slotPos = keyHash % this.hashSlotNum;
      /// 哈希槽的物理地址为 IndexHeader （ 40 字节） + 下标 * 哈希槽的大小（ 4 字节）
      int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

      FileLock fileLock = null;
      try {
        if (lock) {
          // fileLock = this.fileChannel.lock(absSlotPos,
          // hashSlotSize, true);
        }

        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
        // if (fileLock != null) {
        // fileLock.release();
        // fileLock = null;
        // }
        // 如果对应的哈希槽中存储的数据小于 1 或大于当前索引条目个数，表示该哈希码没有对应的条目，直接返回
        if (slotValue <= invalidIndex
            || slotValue > this.indexHeader.getIndexCount()
            || this.indexHeader.getIndexCount() <= 1) {
        } else {
          // 因为会存在哈希冲突，所以根据 slotValue 定位该哈希槽 最新的一个 Item 条目，将存储的物理偏移量加入 phyOffsets ，然后继
          // 续验证 Item 条目中存储的上一个 Index 下标，如果大于、等于 1 并且小
          // 于当前文件的最大条目数，则继续查找，否则结束查找
          for (int nextIndexToRead = slotValue; ; ) {
            if (phyOffsets.size() >= maxNum) {
              break;
            }
            // 查找消息偏移量
            // 根据 Index 下标定位到条目的起始物理偏移量，然后依次读取哈希码、物理偏移量、时间戳、上一个条目的 Index 下标
            int absIndexPos =
                IndexHeader.INDEX_HEADER_SIZE
                    + this.hashSlotNum * hashSlotSize
                    + nextIndexToRead * indexSize;

            int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
            long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

            long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
            int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);
            if (timeDiff < 0) {
              break;
            }

            timeDiff *= 1000L;

            long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
            boolean timeMatched = (timeRead >= begin) && (timeRead <= end);
            // 如果哈希匹 配并且消息存储时间介于待查找时间 start 、 end 之间，则将消息物理偏移量加入 phyOffsets ，并验证条目的前一个 Index 索引
            if (keyHash == keyHashRead && timeMatched) {
              phyOffsets.add(phyOffsetRead);
            }
            // 如果存储的时间戳小于0，则直接结束查找
            if (prevIndexRead <= invalidIndex
                || prevIndexRead > this.indexHeader.getIndexCount()
                || prevIndexRead == nextIndexToRead
                || timeRead < begin) {
              break;
            }
            // 如果索引大于、等于1并且小于Index条目数，则继续查找
            nextIndexToRead = prevIndexRead;
          }
        }
      } catch (Exception e) {
        log.error("selectPhyOffset exception ", e);
      } finally {
        if (fileLock != null) {
          try {
            fileLock.release();
          } catch (IOException e) {
            log.error("Failed to release the lock", e);
          }
        }

        this.mappedFile.release();
      }
    }
  }
}
