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
package org.apache.rocketmq.acl.common;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.acl.common.SessionCredentials.ACCESS_KEY;
import static org.apache.rocketmq.acl.common.SessionCredentials.SECURITY_TOKEN;
import static org.apache.rocketmq.acl.common.SessionCredentials.SIGNATURE;

public class AclClientRPCHook implements RPCHook {
  private final SessionCredentials sessionCredentials;
  protected ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]> fieldCache =
      new ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]>();

  public AclClientRPCHook(SessionCredentials sessionCredentials) {
    this.sessionCredentials = sessionCredentials;
  }

  @Override
  public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
    // 1）将当前访问的用户名加入请求参数，然后对请求参数进行排序
    // 2）排序的实现比较简单，就是遍历所有的请求参数，然后存储到 SortedMap 中，即利用 SortedMap 的排序特性来实现请求参数的排序
    // 3）遍历 SortedMap，将其参数追加到 StringBuffer 中，然后与 secretKey 一起生成签名字符串，并使用 MD5 算法生成验证签名。
    // 值得 注意的是，secretKey 不会通过网络传输。
    // 4）将生成的验证参数传递到服务端

    // 这里是一种经典的编程技巧，即实现验证签名。首先客户端会将请求
    // 参数进行排序，然后组装成字符串，并与密钥一起使用 md5 算法生成签
    // 名字符串，服务端在收到请求参数后同样对请求参数进行排序，以同
    // 样的方式生成签名字符串。如果客户端与服务端生成的签名字符串相
    // 同，则认为验证签名通过，数据未被篡改

    byte[] total =
        AclUtils.combineRequestContent(
            request,
            parseRequestContent(
                request, sessionCredentials.getAccessKey(), sessionCredentials.getSecurityToken()));
    String signature = AclUtils.calSignature(total, sessionCredentials.getSecretKey());
    request.addExtField(SIGNATURE, signature);
    request.addExtField(ACCESS_KEY, sessionCredentials.getAccessKey());

    // The SecurityToken value is unneccessary,user can choose this one.
    if (sessionCredentials.getSecurityToken() != null) {
      request.addExtField(SECURITY_TOKEN, sessionCredentials.getSecurityToken());
    }
  }

  @Override
  public void doAfterResponse(
      String remoteAddr, RemotingCommand request, RemotingCommand response) {}

  protected SortedMap<String, String> parseRequestContent(
      RemotingCommand request, String ak, String securityToken) {
    CommandCustomHeader header = request.readCustomHeader();
    // Sort property
    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put(ACCESS_KEY, ak);
    if (securityToken != null) {
      map.put(SECURITY_TOKEN, securityToken);
    }
    try {
      // Add header properties
      if (null != header) {
        Field[] fields = fieldCache.get(header.getClass());
        if (null == fields) {
          fields = header.getClass().getDeclaredFields();
          for (Field field : fields) {
            field.setAccessible(true);
          }
          Field[] tmp = fieldCache.putIfAbsent(header.getClass(), fields);
          if (null != tmp) {
            fields = tmp;
          }
        }

        for (Field field : fields) {
          Object value = field.get(header);
          if (null != value && !field.isSynthetic()) {
            map.put(field.getName(), value.toString());
          }
        }
      }
      return map;
    } catch (Exception e) {
      throw new RuntimeException("incompatible exception.", e);
    }
  }

  public SessionCredentials getSessionCredentials() {
    return sessionCredentials;
  }
}
