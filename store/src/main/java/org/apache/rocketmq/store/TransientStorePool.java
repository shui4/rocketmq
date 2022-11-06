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
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 短暂的存储池
 *
 * @author shui4
 */
public class TransientStorePool {
  /** 日志 */
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

  /** availableBuffers个数，，默认为5。 {@link MessageStoreConfig#getTransientStorePoolSize()} */
  private final int poolSize;
  /** 文件大小。{@link MessageStoreConfig#getMappedFileSizeCommitLog()} */
  private final int fileSize;
  /** ByteBuffer容器，双端队列 */
  private final Deque<ByteBuffer> availableBuffers;
  /** 存储配置 */
  private final MessageStoreConfig storeConfig;

  /**
   * @param storeConfig 存储配置
   */
  public TransientStorePool(final MessageStoreConfig storeConfig) {
    this.storeConfig = storeConfig;
    this.poolSize = storeConfig.getTransientStorePoolSize();
    this.fileSize = storeConfig.getMappedFileSizeCommitLog();
    this.availableBuffers = new ConcurrentLinkedDeque<>();
  }

  /** 初始化 It's a heavy init method. */
  public void init() {
    for (int i = 0; i < poolSize; i++) {
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

      final long address = ((DirectBuffer) byteBuffer).address();
      Pointer pointer = new Pointer(address);
      // 利用 com.sun.jna.Library类库 锁定该批内存，避免被 置换到交换区，以便提高存储性能
      LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

      availableBuffers.offer(byteBuffer);
    }
  }

  /** 摧毁 */
  public void destroy() {
    for (ByteBuffer byteBuffer : availableBuffers) {
      final long address = ((DirectBuffer) byteBuffer).address();
      Pointer pointer = new Pointer(address);
      LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
    }
  }

  /**
   * 返回缓冲区
   *
   * @param byteBuffer 字节缓冲区
   */
  public void returnBuffer(ByteBuffer byteBuffer) {
    byteBuffer.position(0);
    byteBuffer.limit(fileSize);
    this.availableBuffers.offerFirst(byteBuffer);
  }

  /**
   * 借ByteBuffer
   *
   * @return {@link ByteBuffer}
   */
  public ByteBuffer borrowBuffer() {
    ByteBuffer buffer = availableBuffers.pollFirst();
    if (availableBuffers.size() < poolSize * 0.4) {
      log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
    }
    return buffer;
  }

  /**
   * 可用缓冲区num
   *
   * @return int
   */
  public int availableBufferNums() {
    if (storeConfig.isTransientStorePoolEnable()) {
      return availableBuffers.size();
    }
    return Integer.MAX_VALUE;
  }
}
