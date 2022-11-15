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

/** $Id: PullMessageResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $ */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/** 拉取消息响应头 */
public class PullMessageResponseHeader implements CommandCustomHeader {
  /** 下一次拉取任务的BrokerId */
  @CFNotNull private Long suggestWhichBrokerId;

  /** 待查找队列的偏移量 */
  @CFNotNull private Long nextBeginOffset;
  /** 当前消息队列的最小偏移量 */
  @CFNotNull private Long minOffset;
  /** 当前CommitLog文件的最大偏移量 */
  @CFNotNull private Long maxOffset;

  /**
   * 检查字段
   *
   * @throws RemotingCommandException 远程命令异常
   */
  @Override
  public void checkFields() throws RemotingCommandException {}

  public Long getNextBeginOffset() {
    return nextBeginOffset;
  }

  public void setNextBeginOffset(Long nextBeginOffset) {
    this.nextBeginOffset = nextBeginOffset;
  }

  public Long getMinOffset() {
    return minOffset;
  }

  public void setMinOffset(Long minOffset) {
    this.minOffset = minOffset;
  }

  public Long getMaxOffset() {
    return maxOffset;
  }

  public void setMaxOffset(Long maxOffset) {
    this.maxOffset = maxOffset;
  }

  public Long getSuggestWhichBrokerId() {
    return suggestWhichBrokerId;
  }

  public void setSuggestWhichBrokerId(Long suggestWhichBrokerId) {
    this.suggestWhichBrokerId = suggestWhichBrokerId;
  }
}
