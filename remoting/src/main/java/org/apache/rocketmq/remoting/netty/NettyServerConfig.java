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
package org.apache.rocketmq.remoting.netty;

public class NettyServerConfig implements Cloneable {
  /** 监听端口 */
  private int listenPort = 8888;
  /** Netty业务线程数 */
  private int serverWorkerThreads = 8;
  /**
   * 。 Netty
   * 任务线程池线程个数。Netty会根据业务类型创建不同的线程池，不如处理消息发送、消息消费、心跳检测等。如果该业务类型（RequestCode）未注册线程池，则由public线程池执行
   */
  private int serverCallbackExecutorThreads = 0;

  /**
   * I/O线程池个数，主要是NameServer、Broker端解析请求、返回相应的线程个数。这类线程主要用于处理网络请求，先解析请求包、返回相应的线程个数。
   * 这类先洗个主要用于处理网络请求，先解析请求包，然后转发到各个业务线程完成具体的业务操作，最后结果返回给调用方
   */
  private int serverSelectorThreads = 3;
  /** 消息请求的并发度(Broker端参数） */
  private int serverOnewaySemaphoreValue = 256;
  /** 异步消息发送的最大并发度 (Broker端参数） */
  private int serverAsyncSemaphoreValue = 64;

  /** 网络连接最大空闲时 间，默认为120S。如果连接空闲时间超过该参数设置的值，连接将被关闭 */
  private int serverChannelMaxIdleTimeSeconds = 120;

  /** 网络socket发送缓存区大小，默认64kb */
  private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

  /** 网络socket接收缓存区大小，默认64kb */
  private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

  private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;
  private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;
  private int serverSocketBacklog = NettySystemConfig.socketBacklog;
  /** ByteBuffer是否开启缓存，建议开启 */
  private boolean serverPooledByteBufAllocatorEnable = true;

  /**
   * 是否启用Epoll I/O 模型，Linux环境下建议开启
   *
   * <p>make make install
   *
   * <p>../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
   * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
   */
  private boolean useEpollNativeSelector = false;

  public int getListenPort() {
    return listenPort;
  }

  public void setListenPort(int listenPort) {
    this.listenPort = listenPort;
  }

  public int getServerWorkerThreads() {
    return serverWorkerThreads;
  }

  public void setServerWorkerThreads(int serverWorkerThreads) {
    this.serverWorkerThreads = serverWorkerThreads;
  }

  public int getServerSelectorThreads() {
    return serverSelectorThreads;
  }

  public void setServerSelectorThreads(int serverSelectorThreads) {
    this.serverSelectorThreads = serverSelectorThreads;
  }

  public int getServerOnewaySemaphoreValue() {
    return serverOnewaySemaphoreValue;
  }

  public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
    this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
  }

  public int getServerCallbackExecutorThreads() {
    return serverCallbackExecutorThreads;
  }

  public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
    this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
  }

  public int getServerAsyncSemaphoreValue() {
    return serverAsyncSemaphoreValue;
  }

  public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
    this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
  }

  public int getServerChannelMaxIdleTimeSeconds() {
    return serverChannelMaxIdleTimeSeconds;
  }

  public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
    this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
  }

  public int getServerSocketSndBufSize() {
    return serverSocketSndBufSize;
  }

  public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
    this.serverSocketSndBufSize = serverSocketSndBufSize;
  }

  public int getServerSocketRcvBufSize() {
    return serverSocketRcvBufSize;
  }

  public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
    this.serverSocketRcvBufSize = serverSocketRcvBufSize;
  }

  public int getServerSocketBacklog() {
    return serverSocketBacklog;
  }

  public void setServerSocketBacklog(int serverSocketBacklog) {
    this.serverSocketBacklog = serverSocketBacklog;
  }

  public boolean isServerPooledByteBufAllocatorEnable() {
    return serverPooledByteBufAllocatorEnable;
  }

  public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
    this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
  }

  public boolean isUseEpollNativeSelector() {
    return useEpollNativeSelector;
  }

  public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
    this.useEpollNativeSelector = useEpollNativeSelector;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return (NettyServerConfig) super.clone();
  }

  public int getWriteBufferLowWaterMark() {
    return writeBufferLowWaterMark;
  }

  public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
    this.writeBufferLowWaterMark = writeBufferLowWaterMark;
  }

  public int getWriteBufferHighWaterMark() {
    return writeBufferHighWaterMark;
  }

  public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
    this.writeBufferHighWaterMark = writeBufferHighWaterMark;
  }
}
