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

package org.apache.rocketmq.common.protocol;

/**
 * RequestCode
 *
 * @author shui4
 */
public class RequestCode {
  /** 发送消息 */
  public static final int SEND_MESSAGE = 10;
  /** 拉取消息 */
  public static final int PULL_MESSAGE = 11;

  /** 消息查询 */
  public static final int QUERY_MESSAGE = 12;

  /** QUERY_BROKER_OFFSET */
  public static final int QUERY_BROKER_OFFSET = 13;

  /** QUERY_CONSUMER_OFFSET */
  public static final int QUERY_CONSUMER_OFFSET = 14;

  /** 更新消息消费进度 */
  public static final int UPDATE_CONSUMER_OFFSET = 15;

  /** UPDATE_AND_CREATE_TOPIC */
  public static final int UPDATE_AND_CREATE_TOPIC = 17;

  /** GET_ALL_TOPIC_CONFIG */
  public static final int GET_ALL_TOPIC_CONFIG = 21;

  /** GET_TOPIC_CONFIG_LIST */
  public static final int GET_TOPIC_CONFIG_LIST = 22;

  /** GET_TOPIC_NAME_LIST */
  public static final int GET_TOPIC_NAME_LIST = 23;

  /** UPDATE_BROKER_CONFIG */
  public static final int UPDATE_BROKER_CONFIG = 25;

  /** GET_BROKER_CONFIG */
  public static final int GET_BROKER_CONFIG = 26;

  /** TRIGGER_DELETE_FILES */
  public static final int TRIGGER_DELETE_FILES = 27;

  /** GET_BROKER_RUNTIME_INFO */
  public static final int GET_BROKER_RUNTIME_INFO = 28;

  /** SEARCH_OFFSET_BY_TIMESTAMP */
  public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;

  /** GET_MAX_OFFSET */
  public static final int GET_MAX_OFFSET = 30;

  /** GET_MIN_OFFSET */
  public static final int GET_MIN_OFFSET = 31;

  /** GET_EARLIEST_MSG_STORETIME */
  public static final int GET_EARLIEST_MSG_STORETIME = 32;

  /** VIEW_MESSAGE_BY_ID */
  public static final int VIEW_MESSAGE_BY_ID = 33;

  /** 心跳请求 */
  public static final int HEART_BEAT = 34;

  /** 取消注册 */
  public static final int UNREGISTER_CLIENT = 35;

  /** 消费组重试发送消息 */
  public static final int CONSUMER_SEND_MSG_BACK = 36;

  /** END_TRANSACTION */
  public static final int END_TRANSACTION = 37;

  /** 根据消费组获取消费者列表 */
  public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

  /** CHECK_TRANSACTION_STATE */
  public static final int CHECK_TRANSACTION_STATE = 39;

  /** NOTIFY_CONSUMER_IDS_CHANGED */
  public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

  /** LOCK_BATCH_MQ */
  public static final int LOCK_BATCH_MQ = 41;

  /** UNLOCK_BATCH_MQ */
  public static final int UNLOCK_BATCH_MQ = 42;

  /** GET_ALL_CONSUMER_OFFSET */
  public static final int GET_ALL_CONSUMER_OFFSET = 43;

  /** GET_ALL_DELAY_OFFSET */
  public static final int GET_ALL_DELAY_OFFSET = 45;

  /** CHECK_CLIENT_CONFIG */
  public static final int CHECK_CLIENT_CONFIG = 46;

  /** UPDATE_AND_CREATE_ACL_CONFIG */
  public static final int UPDATE_AND_CREATE_ACL_CONFIG = 50;

  /** DELETE_ACL_CONFIG */
  public static final int DELETE_ACL_CONFIG = 51;

  /** GET_BROKER_CLUSTER_ACL_INFO */
  public static final int GET_BROKER_CLUSTER_ACL_INFO = 52;

  /** UPDATE_GLOBAL_WHITE_ADDRS_CONFIG */
  public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53;

  /** GET_BROKER_CLUSTER_ACL_CONFIG */
  public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 54;

  /** PUT_KV_CONFIG */
  public static final int PUT_KV_CONFIG = 100;

  /** GET_KV_CONFIG */
  public static final int GET_KV_CONFIG = 101;

  /** DELETE_KV_CONFIG */
  public static final int DELETE_KV_CONFIG = 102;
  /** Broker 注册 */
  public static final int REGISTER_BROKER = 103;

  /** UNREGISTER_BROKER */
  public static final int UNREGISTER_BROKER = 104;
  /** 拉取路由信息 */
  public static final int GET_ROUTEINFO_BY_TOPIC = 105;

  /** GET_BROKER_CLUSTER_INFO */
  public static final int GET_BROKER_CLUSTER_INFO = 106;

  /** UPDATE_AND_CREATE_SUBSCRIPTIONGROUP */
  public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;

  /** GET_ALL_SUBSCRIPTIONGROUP_CONFIG */
  public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;

  /** GET_TOPIC_STATS_INFO */
  public static final int GET_TOPIC_STATS_INFO = 202;

  /** GET_CONSUMER_CONNECTION_LIST */
  public static final int GET_CONSUMER_CONNECTION_LIST = 203;

  /** GET_PRODUCER_CONNECTION_LIST */
  public static final int GET_PRODUCER_CONNECTION_LIST = 204;

  /** WIPE_WRITE_PERM_OF_BROKER */
  public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

  /** GET_ALL_TOPIC_LIST_FROM_NAMESERVER */
  public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;

  /** DELETE_SUBSCRIPTIONGROUP */
  public static final int DELETE_SUBSCRIPTIONGROUP = 207;

  /** GET_CONSUME_STATS */
  public static final int GET_CONSUME_STATS = 208;

  /** SUSPEND_CONSUMER */
  public static final int SUSPEND_CONSUMER = 209;

  /** RESUME_CONSUMER */
  public static final int RESUME_CONSUMER = 210;

  /** RESET_CONSUMER_OFFSET_IN_CONSUMER */
  public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;

  /** RESET_CONSUMER_OFFSET_IN_BROKER */
  public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

  /** ADJUST_CONSUMER_THREAD_POOL */
  public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

  /** WHO_CONSUME_THE_MESSAGE */
  public static final int WHO_CONSUME_THE_MESSAGE = 214;

  /** DELETE_TOPIC_IN_BROKER */
  public static final int DELETE_TOPIC_IN_BROKER = 215;

  /** DELETE_TOPIC_IN_NAMESRV */
  public static final int DELETE_TOPIC_IN_NAMESRV = 216;

  /** GET_KVLIST_BY_NAMESPACE */
  public static final int GET_KVLIST_BY_NAMESPACE = 219;

  /** RESET_CONSUMER_CLIENT_OFFSET */
  public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;

  /** GET_CONSUMER_STATUS_FROM_CLIENT */
  public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;

  /** INVOKE_BROKER_TO_RESET_OFFSET */
  public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;

  /** INVOKE_BROKER_TO_GET_CONSUMER_STATUS */
  public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

  /** QUERY_TOPIC_CONSUME_BY_WHO */
  public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

  /** GET_TOPICS_BY_CLUSTER */
  public static final int GET_TOPICS_BY_CLUSTER = 224;

  /** REGISTER_FILTER_SERVER */
  public static final int REGISTER_FILTER_SERVER = 301;

  /** REGISTER_MESSAGE_FILTER_CLASS */
  public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

  /** QUERY_CONSUME_TIME_SPAN */
  public static final int QUERY_CONSUME_TIME_SPAN = 303;

  /** GET_SYSTEM_TOPIC_LIST_FROM_NS */
  public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;

  /** GET_SYSTEM_TOPIC_LIST_FROM_BROKER */
  public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

  /** CLEAN_EXPIRED_CONSUMEQUEUE */
  public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

  /** GET_CONSUMER_RUNNING_INFO */
  public static final int GET_CONSUMER_RUNNING_INFO = 307;

  /** QUERY_CORRECTION_OFFSET */
  public static final int QUERY_CORRECTION_OFFSET = 308;

  /** CONSUME_MESSAGE_DIRECTLY */
  public static final int CONSUME_MESSAGE_DIRECTLY = 309;
  /** 发送消息 CODE */
  public static final int SEND_MESSAGE_V2 = 310;

  /** GET_UNIT_TOPIC_LIST */
  public static final int GET_UNIT_TOPIC_LIST = 311;

  /** GET_HAS_UNIT_SUB_TOPIC_LIST */
  public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;

  /** GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST */
  public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;

  /** CLONE_GROUP_OFFSET */
  public static final int CLONE_GROUP_OFFSET = 314;

  /** VIEW_BROKER_STATS_DATA */
  public static final int VIEW_BROKER_STATS_DATA = 315;

  /** CLEAN_UNUSED_TOPIC */
  public static final int CLEAN_UNUSED_TOPIC = 316;

  /** GET_BROKER_CONSUME_STATS */
  public static final int GET_BROKER_CONSUME_STATS = 317;

  /** update the config of name server */
  public static final int UPDATE_NAMESRV_CONFIG = 318;

  /** get config from name server */
  public static final int GET_NAMESRV_CONFIG = 319;

  /** SEND_BATCH_MESSAGE */
  public static final int SEND_BATCH_MESSAGE = 320;

  /** QUERY_CONSUME_QUEUE */
  public static final int QUERY_CONSUME_QUEUE = 321;

  /** QUERY_DATA_VERSION */
  public static final int QUERY_DATA_VERSION = 322;

  /**
   * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before
   */
  public static final int RESUME_CHECK_HALF_MESSAGE = 323;

  /** SEND_REPLY_MESSAGE */
  public static final int SEND_REPLY_MESSAGE = 324;

  /** SEND_REPLY_MESSAGE_V2 */
  public static final int SEND_REPLY_MESSAGE_V2 = 325;

  /** PUSH_REPLY_MESSAGE_TO_CLIENT */
  public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;

  /** ADD_WRITE_PERM_OF_BROKER */
  public static final int ADD_WRITE_PERM_OF_BROKER = 327;
}
