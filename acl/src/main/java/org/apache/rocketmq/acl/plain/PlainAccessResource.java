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
package org.apache.rocketmq.acl.plain;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.common.MixAll;

/**
 * PlainAccessResource
 *
 * @author shui4
 */
public class PlainAccessResource implements AccessResource {

  /** 访问 Key，用户名 */
  // Identify the user
  private String accessKey;

  /** 用户密钥 */
  private String secretKey;

  /** 远程 IP 地址白名单 */
  private String whiteRemoteAddress;

  /** 是否是管理员角色 */
  private boolean admin;

  /** 默认 topic 的访问权限，如果没有 配置 topic 的权限，则 topic 默认的访问权限为 1，表示 DENY */
  private byte defaultTopicPerm = 1;

  /** 消费组默认访问权限，默认为 DENY */
  private byte defaultGroupPerm = 1;

  /** 资源需要的访问权限映射表 */
  private Map<String, Byte> resourcePermMap;

  /** 远程 IP 地址验证策略 */
  private RemoteAddressStrategy remoteAddressStrategy;

  /** 当前请求的 <code>requestCode</code> */
  private int requestCode;

  /**
   * 请求头与请求体的内容。
   * <p> 要计算的内容。</p>
   */
  private byte[] content;

  /**
   * 签名字符串，这是通常的套路，在客户 端，首先将请求参数排序，然后使用 secretKey 生成签名字符串，<br>
   * 在服务端重复这个步骤，然后对比签名字符串，如果相同，则认为登录成功，否则失败
   */
  private String signature;

  /** 密钥令牌 */
  private String secretToken;

  /** 保留字段，目前未被使用 */
  private String recognition;

  /** PlainAccessResource */
  public PlainAccessResource(){}

  /**
   * isRetryTopic
   *
   * @param topic ignore
   * @return ignore
   */
  public static boolean isRetryTopic(String topic) {
    return null != topic && topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
  }

  /**
   * printStr
   *
   * @param resource ignore
   * @param isGroup ignore
   * @return ignore
   */
  public static String printStr(String resource, boolean isGroup) {
    if (resource == null) {
      return null;
    }
    if (isGroup) {
      return String.format("%s:%s", "group", getGroupFromRetryTopic(resource));
    } else {
      return String.format("%s:%s", "topic", resource);
    }
  }

  /**
   * getGroupFromRetryTopic
   *
   * @param retryTopic ignore
   * @return ignore
   */
  public static String getGroupFromRetryTopic(String retryTopic) {
    if (retryTopic == null) {
      return null;
    }
    return retryTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
  }

  /**
   * getRetryTopic
   *
   * @param group ignore
   * @return ignore
   */
  public static String getRetryTopic(String group) {
    if (group == null) {
      return null;
    }
    return MixAll.getRetryTopic(group);
  }

  /**
   * addResourceAndPerm
   *
   * @param resource ignore
   * @param perm ignore
   */
  public void addResourceAndPerm(String resource, byte perm) {
    if (resource == null) {
      return;
    }
    if (resourcePermMap == null) {
      resourcePermMap = new HashMap<>();
    }
    resourcePermMap.put(resource, perm);
  }

  /**
   * getAccessKey
   *
   * @return ignore
   */
  public String getAccessKey() {
    return accessKey;
  }

  /**
   * setAccessKey
   *
   * @param accessKey ignore
   */
  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  /**
   * getSecretKey
   *
   * @return ignore
   */
  public String getSecretKey() {
    return secretKey;
  }

  /**
   * setSecretKey
   *
   * @param secretKey ignore
   */
  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  /**
   * getWhiteRemoteAddress
   *
   * @return ignore
   */
  public String getWhiteRemoteAddress() {
    return whiteRemoteAddress;
  }

  /**
   * setWhiteRemoteAddress
   *
   * @param whiteRemoteAddress ignore
   */
  public void setWhiteRemoteAddress(String whiteRemoteAddress) {
    this.whiteRemoteAddress = whiteRemoteAddress;
  }

  /**
   * isAdmin
   *
   * @return ignore
   */
  public boolean isAdmin() {
    return admin;
  }

  /**
   * setAdmin
   *
   * @param admin ignore
   */
  public void setAdmin(boolean admin) {
    this.admin = admin;
  }

  /**
   * getDefaultTopicPerm
   *
   * @return ignore
   */
  public byte getDefaultTopicPerm() {
    return defaultTopicPerm;
  }

  /**
   * setDefaultTopicPerm
   *
   * @param defaultTopicPerm ignore
   */
  public void setDefaultTopicPerm(byte defaultTopicPerm) {
    this.defaultTopicPerm = defaultTopicPerm;
  }

  /**
   * getDefaultGroupPerm
   *
   * @return ignore
   */
  public byte getDefaultGroupPerm() {
    return defaultGroupPerm;
  }

  /**
   * setDefaultGroupPerm
   *
   * @param defaultGroupPerm ignore
   */
  public void setDefaultGroupPerm(byte defaultGroupPerm) {
    this.defaultGroupPerm = defaultGroupPerm;
  }

  /**
   * getResourcePermMap
   *
   * @return ignore
   */
  public Map<String, Byte> getResourcePermMap() {
    return resourcePermMap;
  }

  /**
   * getRecognition
   *
   * @return ignore
   */
  public String getRecognition() {
    return recognition;
  }

  /**
   * setRecognition
   *
   * @param recognition ignore
   */
  public void setRecognition(String recognition) {
    this.recognition = recognition;
  }

  /**
   * getRequestCode
   *
   * @return ignore
   */
  public int getRequestCode() {
    return requestCode;
  }

  /**
   * setRequestCode
   *
   * @param requestCode ignore
   */
  public void setRequestCode(int requestCode) {
    this.requestCode = requestCode;
  }

  /**
   * getSecretToken
   *
   * @return ignore
   */
  public String getSecretToken() {
    return secretToken;
  }

  /**
   * setSecretToken
   *
   * @param secretToken ignore
   */
  public void setSecretToken(String secretToken) {
    this.secretToken = secretToken;
  }

  /**
   * getRemoteAddressStrategy
   *
   * @return ignore
   */
  public RemoteAddressStrategy getRemoteAddressStrategy() {
    return remoteAddressStrategy;
  }

  /**
   * setRemoteAddressStrategy
   *
   * @param remoteAddressStrategy ignore
   */
  public void setRemoteAddressStrategy(RemoteAddressStrategy remoteAddressStrategy) {
    this.remoteAddressStrategy = remoteAddressStrategy;
  }

  /**
   * getSignature
   *
   * @return ignore
   */
  public String getSignature() {
    return signature;
  }

  /**
   * setSignature
   *
   * @param signature ignore
   */
  public void setSignature(String signature) {
    this.signature = signature;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  /**
   * getContent
   *
   * @return ignore
   */
  public byte[] getContent() {
    return content;
  }

  /**
   * setContent
   *
   * @param content ignore
   */
  public void setContent(byte[] content) {
    this.content = content;
  }
}