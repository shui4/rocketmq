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
package org.apache.rocketmq.client.trace.hook;

import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

import java.util.ArrayList;

public class SendMessageTraceHookImpl implements SendMessageHook {

  private TraceDispatcher localDispatcher;

  public SendMessageTraceHookImpl(TraceDispatcher localDispatcher) {
    this.localDispatcher = localDispatcher;
  }

  @Override
  public String hookName() {
    return "SendMessageTraceHook";
  }

  @Override
  public void sendMessageBefore(SendMessageContext context) {
    // 如果是邮件跟踪数据，则不记录
    if (context == null
        || context
            .getMessage()
            .getTopic()
            .startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
      return;
    }
    // 在消息发送之前先收集消息的 topic、tag、key、存储 Broker 的 IP 地址、消息体 的长度等基础信息
    // ，并将消息轨迹数据先存储在调用上下文中 构建 TuxeTraceContext 的上下文内容
    TraceContext tuxeContext = new TraceContext();
    tuxeContext.setTraceBeans(new ArrayList<TraceBean>(1));
    context.setMqTraceContext(tuxeContext);
    tuxeContext.setTraceType(TraceType.Pub);
    tuxeContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
    // 构建消息跟踪的数据 Bean 对象
    TraceBean traceBean = new TraceBean();
    traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
    traceBean.setTags(context.getMessage().getTags());
    traceBean.setKeys(context.getMessage().getKeys());
    traceBean.setStoreHost(context.getBrokerAddr());
    traceBean.setBodyLength(context.getMessage().getBody().length);
    traceBean.setMsgType(context.getMsgType());
    tuxeContext.getTraceBeans().add(traceBean);
  }

  @Override
  public void sendMessageAfter(SendMessageContext context) {
    // 如果是邮件跟踪数据，则不记录

    // 如果调用的时候上下文环境为空，那么发送消息的topic
    // 和消息轨迹存储的topic，如果服务端未开启消息轨迹跟踪配置，则直
    // 接返回，即不记录消息轨迹数据
    if (context == null
        || context
            .getMessage()
            .getTopic()
            .startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())
        || context.getMqTraceContext() == null) {
      return;
    }
    if (context.getSendResult() == null) {
      return;
    }

    if (context.getSendResult().getRegionId() == null || !context.getSendResult().isTraceOn()) {
      // if switch is false,skip it
      return;
    }
    // 从MqTraceContext中获取跟踪的TraceBean，虽然设计成
    // List结构体，但在消息发送场景，这里的数据永远只有一条，即使是
    // 批量发送也不例外。然后设置costTime（消息发送耗时）、
    // success（是否发送成功）、regionId（发送到Broker所在的分区）、
    // msgId（消息ID，全局唯一）、offsetMsgId（消息物理偏移量，如果
    // 是批量消息，则是最后一条消息的物理偏移量）、storeTime。注意这
    // 个存储时间并没有取消息的实际存储时间，而是取一个估算值，即客户端发送时间一半的耗时来表示消息的存储时间
    TraceContext tuxeContext = (TraceContext) context.getMqTraceContext();
    TraceBean traceBean = tuxeContext.getTraceBeans().get(0);
    int costTime =
        (int)
            ((System.currentTimeMillis() - tuxeContext.getTimeStamp())
                / tuxeContext.getTraceBeans().size());
    tuxeContext.setCostTime(costTime);
    if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
      tuxeContext.setSuccess(true);
    } else {
      tuxeContext.setSuccess(false);
    }
    tuxeContext.setRegionId(context.getSendResult().getRegionId());
    traceBean.setMsgId(context.getSendResult().getMsgId());
    traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
    traceBean.setStoreTime(tuxeContext.getTimeStamp() + costTime / 2);
    // 使用AsyncTraceDispatcher异步将消息轨迹数据发送到消息服务器（Broker）上
    localDispatcher.append(tuxeContext);
  }
}
