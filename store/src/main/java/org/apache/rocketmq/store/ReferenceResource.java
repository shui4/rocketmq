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

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
  protected final AtomicLong refCount = new AtomicLong(1);
  protected volatile boolean available = true;
  protected volatile boolean cleanupOver = false;
  /** 首次Shutdown时间戳 */
  private volatile long firstShutdownTimestamp = 0;

  /**
   * 资源占用<br>
   * 对 {@link MappedFile} 进行相关操作途中，可能恰好进行文件回收，这时为了避免这个文件被回收，调用此方法可以延迟回收<br>
   *
   * @return 占用成功？
   */
  public synchronized boolean hold() {
    if (this.isAvailable()) {
      if (this.refCount.getAndIncrement() > 0) {
        return true;
      } else {
        this.refCount.getAndDecrement();
      }
    }

    return false;
  }

  public boolean isAvailable() {
    return this.available;
  }

  public void shutdown(final long intervalForcibly) {
    if (this.available) {
      this.available = false;
      this.firstShutdownTimestamp = System.currentTimeMillis();
      // 释放
      this.release();
    }
    // ? 非首次 shutdown 并且 引用计数 >0
    else if (this.getRefCount() > 0) {
      // ? 如果 强制的延迟
      if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
        // 引用直接变为 -1000 - xxx，(反正小于0就完事了)
        this.refCount.set(-1000 - this.getRefCount());
        this.release();
      }
    }
  }

  /**
   * 释放，减少引用计数器，表示不再占用该文件，这里如果 引用计数器 <=0 ，则会进行清除，当然如果它目前还是可用的话，将打印 error 日志，停止清除<br>
   * 成功则释放内存映射实例
   */
  public void release() {
    // 和Netty的好像...，引用计算
    long value = this.refCount.decrementAndGet();
    if (value > 0) return;
    // 映射计数<=0
    synchronized (this) {
      this.cleanupOver = this.cleanup(value);
    }
  }

  public long getRefCount() {
    return this.refCount.get();
  }

  public abstract boolean cleanup(final long currentRef);

  public boolean isCleanupOver() {
    return this.refCount.get() <= 0 && this.cleanupOver;
  }
}
