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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class BrokerLiveInfo {
  private Channel channel;
  private DataVersion dataVersion;
  private String haServerAddr;
  /** 存储上次收到Broker心跳包的时间 */
  private long lastUpdateTimestamp;

  public BrokerLiveInfo(
      long lastUpdateTimestamp, DataVersion dataVersion, Channel channel, String haServerAddr) {
    this.lastUpdateTimestamp = lastUpdateTimestamp;
    this.dataVersion = dataVersion;
    this.channel = channel;
    this.haServerAddr = haServerAddr;
  }

  public Channel getChannel() {
    return channel;
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  public DataVersion getDataVersion() {
    return dataVersion;
  }

  public void setDataVersion(DataVersion dataVersion) {
    this.dataVersion = dataVersion;
  }

  public String getHaServerAddr() {
    return haServerAddr;
  }

  public void setHaServerAddr(String haServerAddr) {
    this.haServerAddr = haServerAddr;
  }

  public long getLastUpdateTimestamp() {
    return lastUpdateTimestamp;
  }

  public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
    this.lastUpdateTimestamp = lastUpdateTimestamp;
  }

  @Override
  public String toString() {
    return "BrokerLiveInfo [lastUpdateTimestamp="
        + lastUpdateTimestamp
        + ", dataVersion="
        + dataVersion
        + ", channel="
        + channel
        + ", haServerAddr="
        + haServerAddr
        + "]";
  }
}

/** <a href="https://www.liuchengtu.com/lct/#X493c32ba6198558fa0d55f9ac2513af0">集群状态存储</a> */
// 代码清单2-6
public class RouteInfoManager {
  private static final long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
  /** Broker基础信息，包含brokerNames所属集群名称、主备Broker地址。 */
  private final HashMap<String /* brokerName */, BrokerData> brokerAddrTable;

  /** Broker状态信息，NameServer每次收到心跳包时会替换该信息。 */
  private final HashMap<String /* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
  /** 集群addr表 */
  private final HashMap<String /* clusterName */, Set<String /* brokerName */>> clusterAddrTable;

  /** Broker上的FilterServer列表，用于类模式消息过滤。类模式过滤机制在4.4及以后版本被废弃。 */
  private final HashMap<String /* brokerAddr */, List<String> /* Filter Server */>
      filterServerTable;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  /** topic消息队列的路由信息，消息发送时根据路由表进行负载均衡。 */
  private final HashMap<String /* topic */, List<QueueData>> topicQueueTable;

  public RouteInfoManager() {
    this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
    this.brokerAddrTable = new HashMap<String, BrokerData>(128);
    this.clusterAddrTable = new HashMap<String, Set<String>>(32);
    this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
    this.filterServerTable = new HashMap<String, List<String>>(256);
  }

  public int addWritePermOfBrokerByLock(final String brokerName) {
    return operateWritePermOfBrokerByLock(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
  }

  private int operateWritePermOfBrokerByLock(final String brokerName, final int requestCode) {
    try {
      try {
        this.lock.writeLock().lockInterruptibly();
        return operateWritePermOfBroker(brokerName, requestCode);
      } finally {
        this.lock.writeLock().unlock();
      }
    } catch (Exception e) {
      log.error("operateWritePermOfBrokerByLock Exception", e);
    }

    return 0;
  }

  private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
    int topicCnt = 0;
    for (Entry<String, List<QueueData>> entry : this.topicQueueTable.entrySet()) {
      List<QueueData> qdList = entry.getValue();

      for (QueueData qd : qdList) {
        if (qd.getBrokerName().equals(brokerName)) {
          int perm = qd.getPerm();
          switch (requestCode) {
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
              perm &= ~PermName.PERM_WRITE;
              break;
            case RequestCode.ADD_WRITE_PERM_OF_BROKER:
              perm = PermName.PERM_READ | PermName.PERM_WRITE;
              break;
          }
          qd.setPerm(perm);
          topicCnt++;
        }
      }
    }

    return topicCnt;
  }

  public void deleteTopic(final String topic) {
    try {
      try {
        this.lock.writeLock().lockInterruptibly();
        this.topicQueueTable.remove(topic);
      } finally {
        this.lock.writeLock().unlock();
      }
    } catch (Exception e) {
      log.error("deleteTopic Exception", e);
    }
  }

  public byte[] getAllClusterInfo() {
    ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
    clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
    clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
    return clusterInfoSerializeWrapper.encode();
  }

  public byte[] getAllTopicList() {
    TopicList topicList = new TopicList();
    try {
      try {
        this.lock.readLock().lockInterruptibly();
        topicList.getTopicList().addAll(this.topicQueueTable.keySet());
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("getAllTopicList Exception", e);
    }

    return topicList.encode();
  }

  public byte[] getHasUnitSubTopicList() {
    TopicList topicList = new TopicList();
    try {
      try {
        this.lock.readLock().lockInterruptibly();
        Iterator<Entry<String, List<QueueData>>> topicTableIt =
            this.topicQueueTable.entrySet().iterator();
        while (topicTableIt.hasNext()) {
          Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
          String topic = topicEntry.getKey();
          List<QueueData> queueDatas = topicEntry.getValue();
          if (queueDatas != null
              && queueDatas.size() > 0
              && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSysFlag())) {
            topicList.getTopicList().add(topic);
          }
        }
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("getAllTopicList Exception", e);
    }

    return topicList.encode();
  }

  public byte[] getHasUnitSubUnUnitTopicList() {
    TopicList topicList = new TopicList();
    try {
      try {
        this.lock.readLock().lockInterruptibly();
        Iterator<Entry<String, List<QueueData>>> topicTableIt =
            this.topicQueueTable.entrySet().iterator();
        while (topicTableIt.hasNext()) {
          Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
          String topic = topicEntry.getKey();
          List<QueueData> queueDatas = topicEntry.getValue();
          if (queueDatas != null
              && queueDatas.size() > 0
              && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSysFlag())
              && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSysFlag())) {
            topicList.getTopicList().add(topic);
          }
        }
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("getAllTopicList Exception", e);
    }

    return topicList.encode();
  }

  public byte[] getSystemTopicList() {
    TopicList topicList = new TopicList();
    try {
      try {
        this.lock.readLock().lockInterruptibly();
        for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
          topicList.getTopicList().add(entry.getKey());
          topicList.getTopicList().addAll(entry.getValue());
        }

        if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
          Iterator<String> it = brokerAddrTable.keySet().iterator();
          while (it.hasNext()) {
            BrokerData bd = brokerAddrTable.get(it.next());
            HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
            if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
              Iterator<Long> it2 = brokerAddrs.keySet().iterator();
              topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
              break;
            }
          }
        }
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("getAllTopicList Exception", e);
    }

    return topicList.encode();
  }

  public byte[] getTopicsByCluster(String cluster) {
    TopicList topicList = new TopicList();
    try {
      try {
        this.lock.readLock().lockInterruptibly();
        Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
        for (String brokerName : brokerNameSet) {
          Iterator<Entry<String, List<QueueData>>> topicTableIt =
              this.topicQueueTable.entrySet().iterator();
          while (topicTableIt.hasNext()) {
            Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
            String topic = topicEntry.getKey();
            List<QueueData> queueDatas = topicEntry.getValue();
            for (QueueData queueData : queueDatas) {
              if (brokerName.equals(queueData.getBrokerName())) {
                topicList.getTopicList().add(topic);
                break;
              }
            }
          }
        }
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("getAllTopicList Exception", e);
    }

    return topicList.encode();
  }

  public byte[] getUnitTopics() {
    TopicList topicList = new TopicList();
    try {
      try {
        this.lock.readLock().lockInterruptibly();
        Iterator<Entry<String, List<QueueData>>> topicTableIt =
            this.topicQueueTable.entrySet().iterator();
        while (topicTableIt.hasNext()) {
          Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
          String topic = topicEntry.getKey();
          List<QueueData> queueDatas = topicEntry.getValue();
          if (queueDatas != null
              && queueDatas.size() > 0
              && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSysFlag())) {
            topicList.getTopicList().add(topic);
          }
        }
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("getAllTopicList Exception", e);
    }

    return topicList.encode();
  }

  public TopicRouteData pickupTopicRouteData(final String topic) {
    TopicRouteData topicRouteData = new TopicRouteData();
    boolean foundQueueData = false;
    boolean foundBrokerData = false;
    Set<String> brokerNameSet = new HashSet<String>();
    List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
    topicRouteData.setBrokerDatas(brokerDataList);

    HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
    topicRouteData.setFilterServerTable(filterServerMap);

    try {
      try {
        this.lock.readLock().lockInterruptibly();
        List<QueueData> queueDataList = this.topicQueueTable.get(topic);
        if (queueDataList != null) {
          topicRouteData.setQueueDatas(queueDataList);
          foundQueueData = true;

          Iterator<QueueData> it = queueDataList.iterator();
          while (it.hasNext()) {
            QueueData qd = it.next();
            brokerNameSet.add(qd.getBrokerName());
          }

          for (String brokerName : brokerNameSet) {
            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (null != brokerData) {
              BrokerData brokerDataClone =
                  new BrokerData(
                      brokerData.getCluster(),
                      brokerData.getBrokerName(),
                      (HashMap<Long, String>) brokerData.getBrokerAddrs().clone());
              brokerDataList.add(brokerDataClone);
              foundBrokerData = true;
              for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                filterServerMap.put(brokerAddr, filterServerList);
              }
            }
          }
        }
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("pickupTopicRouteData Exception", e);
    }

    log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

    if (foundBrokerData && foundQueueData) {
      return topicRouteData;
    }

    return null;
  }

  public void printAllPeriodically() {
    try {
      try {
        this.lock.readLock().lockInterruptibly();
        log.info("--------------------------------------------------------");
        {
          log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
          Iterator<Entry<String, List<QueueData>>> it = this.topicQueueTable.entrySet().iterator();
          while (it.hasNext()) {
            Entry<String, List<QueueData>> next = it.next();
            log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
          }
        }

        {
          log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
          Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
          while (it.hasNext()) {
            Entry<String, BrokerData> next = it.next();
            log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
          }
        }

        {
          log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
          Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
          while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
          }
        }

        {
          log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
          Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
          while (it.hasNext()) {
            Entry<String, Set<String>> next = it.next();
            log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
          }
        }
      } finally {
        this.lock.readLock().unlock();
      }
    } catch (Exception e) {
      log.error("printAllPeriodically Exception", e);
    }
  }
  // 如果Broker请求类型为 REGISTER_BROKER，则请求最终转发到这里
  // 代码清单2-10 clusterAddrTable的维护
  public RegisterBrokerResult registerBroker(
      final String clusterName,
      final String brokerAddr,
      final String brokerName,
      final long brokerId,
      final String haServerAddr,
      final TopicConfigSerializeWrapper topicConfigWrapper,
      final List<String> filterServerList,
      final Channel channel) {
    RegisterBrokerResult result = new RegisterBrokerResult();
    try {
      try {
        // 上锁，防止并发修改 路由表
        // 因此对于注册多个请求来说，它是串行的
        this.lock.writeLock().lockInterruptibly();

        Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
        // ? 从中没有获取到
        if (null == brokerNames) {
          // 创建Set
          brokerNames = new HashSet<String>();
          this.clusterAddrTable.put(clusterName, brokerNames);
        }
        brokerNames.add(brokerName);

        boolean registerFirst = false;
        // 代码清单2-11 brokerAddrTable的维护
        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (null == brokerData) {
          // 标记第一次注册
          registerFirst = true;
          brokerData = new BrokerData(clusterName, brokerName, new HashMap<Long, String>());
          this.brokerAddrTable.put(brokerName, brokerData);
        }
        Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
        // Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
        // The same IP:PORT must only have one record in brokerAddrTable
        Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
        while (it.hasNext()) {
          Entry<Long, String> item = it.next();
          if (null != brokerAddr
              && brokerAddr.equals(item.getValue())
              && brokerId != item.getKey()) {
            it.remove();
          }
        }
        String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
        registerFirst = registerFirst || (null == oldAddr);
        // 代码清单2-12 topicQueueTable的维护

        // ? 主节点
        if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {
          // ? （发生变化||首次注册），则需要更新topic路由元数据，并填充topicQueueTable，
          if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())
              || registerFirst) {
            ConcurrentMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();
            if (tcTable != null) {
              for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                this.createAndUpdateQueueData(brokerName, entry.getValue());
              }
            }
          }
        }

        //  代码清单2-15 更新BrokerLiveInfo，存储状态正常的Broker信息列表
        // BrokerLiveInfo是执行路由删除操作的重要依据
        BrokerLiveInfo prevBrokerLiveInfo =
            this.brokerLiveTable.put(
                brokerAddr,
                new BrokerLiveInfo(
                    System.currentTimeMillis(),
                    topicConfigWrapper.getDataVersion(),
                    channel,
                    haServerAddr));
        if (null == prevBrokerLiveInfo) {
          log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
        }
        // 一个Broker上会关联多个FilterServer消息过滤服务器。
        if (filterServerList != null) {
          if (filterServerList.isEmpty()) {
            this.filterServerTable.remove(brokerAddr);
          } else {
            this.filterServerTable.put(brokerAddr, filterServerList);
          }
        }
        // 如果此Broker为从节点，则需要查找该Broker的主节点信息，并更新对应的masterAddr属性。
        if (MixAll.MASTER_ID != brokerId) {
          String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
          if (masterAddr != null) {
            BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
            if (brokerLiveInfo != null) {
              result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
              result.setMasterAddr(masterAddr);
            }
          }
        }
      } finally {
        this.lock.writeLock().unlock();
      }
    } catch (Exception e) {
      log.error("registerBroker Exception", e);
    }

    return result;
  }

  public boolean isBrokerTopicConfigChanged(
      final String brokerAddr, final DataVersion dataVersion) {
    DataVersion prev = queryBrokerTopicConfig(brokerAddr);
    return null == prev || !prev.equals(dataVersion);
  }

  // 其实就是为默认主题自动注册路由信息，其中包含 MixAll.DEFAULT_TOPIC的路由信息。当消息生产者发送主题时，如果该主题未创建，并且
  // autoCreateTopicEnable为true，则返回MixAll.DEFAULT_TOPIC的路由信息
  // 代码清单2-13
  // 代码清单2-14 根据topicConfig创建QueueData数据结构，然后更新topicQueueTable
  private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
    QueueData queueData = new QueueData();
    queueData.setBrokerName(brokerName);
    queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
    queueData.setReadQueueNums(topicConfig.getReadQueueNums());
    queueData.setPerm(topicConfig.getPerm());
    queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

    List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
    if (null == queueDataList) {
      queueDataList = new LinkedList<QueueData>();
      queueDataList.add(queueData);
      this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
      log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
    } else {
      boolean addNewOne = true;

      Iterator<QueueData> it = queueDataList.iterator();
      while (it.hasNext()) {
        QueueData qd = it.next();
        if (qd.getBrokerName().equals(brokerName)) {
          if (qd.equals(queueData)) {
            addNewOne = false;
          } else {
            log.info(
                "topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd, queueData);
            it.remove();
          }
        }
      }

      if (addNewOne) {
        queueDataList.add(queueData);
      }
    }
  }

  public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
    BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
    if (prev != null) {
      return prev.getDataVersion();
    }
    return null;
  }

  public void scanNotActiveBroker() {
    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, BrokerLiveInfo> next = it.next();
      long last = next.getValue().getLastUpdateTimestamp();
      // ? 超过 120秒
      if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
        // 移除
        RemotingUtil.closeChannel(next.getValue().getChannel());
        it.remove();
        log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
        // 触发Netty Channel 销毁
        this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
      }
    }
  }

  public void onChannelDestroy(String remoteAddr, Channel channel) {
    String brokerAddrFound = null;
    if (channel != null) {
      try {
        try {
          this.lock.readLock().lockInterruptibly();
          Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable =
              this.brokerLiveTable.entrySet().iterator();
          while (itBrokerLiveTable.hasNext()) {
            Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
            if (entry.getValue().getChannel() == channel) {
              brokerAddrFound = entry.getKey();
              break;
            }
          }
        } finally {
          this.lock.readLock().unlock();
        }
      } catch (Exception e) {
        log.error("onChannelDestroy Exception", e);
      }
    }

    if (null == brokerAddrFound) {
      brokerAddrFound = remoteAddr;
    } else {
      log.info(
          "the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
    }

    if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

      try {
        try {
          this.lock.writeLock().lockInterruptibly();
          this.brokerLiveTable.remove(brokerAddrFound);
          this.filterServerTable.remove(brokerAddrFound);
          String brokerNameFound = null;
          boolean removeBrokerName = false;
          Iterator<Entry<String, BrokerData>> itBrokerAddrTable =
              this.brokerAddrTable.entrySet().iterator();
          while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
            BrokerData brokerData = itBrokerAddrTable.next().getValue();

            Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
            while (it.hasNext()) {
              Entry<Long, String> entry = it.next();
              Long brokerId = entry.getKey();
              String brokerAddr = entry.getValue();
              if (brokerAddr.equals(brokerAddrFound)) {
                brokerNameFound = brokerData.getBrokerName();
                it.remove();
                log.info(
                    "remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                    brokerId,
                    brokerAddr);
                break;
              }
            }

            if (brokerData.getBrokerAddrs().isEmpty()) {
              removeBrokerName = true;
              itBrokerAddrTable.remove();
              log.info(
                  "remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                  brokerData.getBrokerName());
            }
          }

          if (brokerNameFound != null && removeBrokerName) {
            Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
            while (it.hasNext()) {
              Entry<String, Set<String>> entry = it.next();
              String clusterName = entry.getKey();
              Set<String> brokerNames = entry.getValue();
              boolean removed = brokerNames.remove(brokerNameFound);
              if (removed) {
                log.info(
                    "remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                    brokerNameFound,
                    clusterName);

                if (brokerNames.isEmpty()) {
                  log.info(
                      "remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                      clusterName);
                  it.remove();
                }

                break;
              }
            }
          }

          if (removeBrokerName) {
            Iterator<Entry<String, List<QueueData>>> itTopicQueueTable =
                this.topicQueueTable.entrySet().iterator();
            while (itTopicQueueTable.hasNext()) {
              Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
              String topic = entry.getKey();
              List<QueueData> queueDataList = entry.getValue();

              Iterator<QueueData> itQueueData = queueDataList.iterator();
              while (itQueueData.hasNext()) {
                QueueData queueData = itQueueData.next();
                if (queueData.getBrokerName().equals(brokerNameFound)) {
                  itQueueData.remove();
                  log.info(
                      "remove topic[{} {}], from topicQueueTable, because channel destroyed",
                      topic,
                      queueData);
                }
              }

              if (queueDataList.isEmpty()) {
                itTopicQueueTable.remove();
                log.info(
                    "remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                    topic);
              }
            }
          }
        } finally {
          this.lock.writeLock().unlock();
        }
      } catch (Exception e) {
        log.error("onChannelDestroy Exception", e);
      }
    }
  }

  public void unregisterBroker(
      final String clusterName,
      final String brokerAddr,
      final String brokerName,
      final long brokerId) {
    try {
      try {
        this.lock.writeLock().lockInterruptibly();
        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
        log.info(
            "unregisterBroker, remove from brokerLiveTable {}, {}",
            brokerLiveInfo != null ? "OK" : "Failed",
            brokerAddr);

        this.filterServerTable.remove(brokerAddr);

        boolean removeBrokerName = false;
        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (null != brokerData) {
          String addr = brokerData.getBrokerAddrs().remove(brokerId);
          log.info(
              "unregisterBroker, remove addr from brokerAddrTable {}, {}",
              addr != null ? "OK" : "Failed",
              brokerAddr);

          if (brokerData.getBrokerAddrs().isEmpty()) {
            this.brokerAddrTable.remove(brokerName);
            log.info("unregisterBroker, remove name from brokerAddrTable OK, {}", brokerName);

            removeBrokerName = true;
          }
        }

        if (removeBrokerName) {
          Set<String> nameSet = this.clusterAddrTable.get(clusterName);
          if (nameSet != null) {
            boolean removed = nameSet.remove(brokerName);
            log.info(
                "unregisterBroker, remove name from clusterAddrTable {}, {}",
                removed ? "OK" : "Failed",
                brokerName);

            if (nameSet.isEmpty()) {
              this.clusterAddrTable.remove(clusterName);
              log.info("unregisterBroker, remove cluster from clusterAddrTable {}", clusterName);
            }
          }
          this.removeTopicByBrokerName(brokerName);
        }
      } finally {
        this.lock.writeLock().unlock();
      }
    } catch (Exception e) {
      log.error("unregisterBroker Exception", e);
    }
  }

  private void removeTopicByBrokerName(final String brokerName) {
    Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
    while (itMap.hasNext()) {
      Entry<String, List<QueueData>> entry = itMap.next();

      String topic = entry.getKey();
      List<QueueData> queueDataList = entry.getValue();
      Iterator<QueueData> it = queueDataList.iterator();
      while (it.hasNext()) {
        QueueData qd = it.next();
        if (qd.getBrokerName().equals(brokerName)) {
          log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
          it.remove();
        }
      }

      if (queueDataList.isEmpty()) {
        log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
        itMap.remove();
      }
    }
  }

  public void updateBrokerInfoUpdateTimestamp(final String brokerAddr) {
    BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
    if (prev != null) {
      prev.setLastUpdateTimestamp(System.currentTimeMillis());
    }
  }

  public int wipeWritePermOfBrokerByLock(final String brokerName) {
    return operateWritePermOfBrokerByLock(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
  }
}
