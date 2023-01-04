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

package org.apache.rocketmq.acl;

import java.util.List;
import java.util.Map;

import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 访问验证器
 *
 * @author shui4
 */
public interface AccessValidator {

  /**
   * 从请求头中解析本次请求对应的访问资源，即本次请求需要的访问权限
   *
   * @param request ignore
   * @param remoteAddr ignore
   * @return 普通访问资源结果 ，包括访问密钥、签名和其他一些访问属性
   */
  AccessResource parse(RemotingCommand request, String remoteAddr);

  /**
   * 根据本次 需要访问的权限，与请求用户拥有的权限进行对比验证，判断请求用 户是否拥有权限。如果请求用户没有访问该操作的权限，则抛出异常，否则放行
   *
   * @param accessResource ignore
   */
  void validate(AccessResource accessResource);

  /**
   * 更新 ACL 访问控制列表的配置
   *
   * @param plainAccessConfig ignore
   * @return ignore
   */
  boolean updateAccessConfig(PlainAccessConfig plainAccessConfig);

  /**
   * 根据账户名称删除访问授权规则
   *
   * @return
   */
  boolean deleteAccessConfig(String accesskey);

  /**
   * 获取 ACL 配置当前的版本号
   *
   * @return ignore
   */
  @Deprecated
  String getAclConfigVersion();

  /**
   * 更新全局白名单 IP 列表
   *
   * @param globalWhiteAddrsList ignore
   * @return ignore
   */
  boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList);

  /**
   * 获取 ACL 相关的配置信息
   *
   * @return ignore
   */
  AclConfig getAllAclConfig();

  /**
   * get all access resource config version information
   *
   * @return
   */
  Map<String, DataVersion> getAllAclConfigVersion();
}