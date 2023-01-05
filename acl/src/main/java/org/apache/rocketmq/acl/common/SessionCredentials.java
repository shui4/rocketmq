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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import org.apache.rocketmq.common.MixAll;

/**
 * 会话凭证
 *
 * @author shui4
 */
public class SessionCredentials {
  /** CHARSET */
  public static final Charset CHARSET = Charset.forName("UTF-8");

  /** 用户访问 key，对应配置在 Broker 端的用户名 */
  public static final String ACCESS_KEY = "AccessKey";

  /** 用户访问密钥，客户端首先会对请求参数进 行排序，然后使用该密钥对请求参数生成签名验证参数，并随着 请求传递到服务端。注意该密钥是不会在网络上进行传输的 */
  public static final String SECRET_KEY = "SecretKey";

  /** 签名字符串 */
  public static final String SIGNATURE = "Signature";

  /** 安全会话令牌，通常只需使用 accessKey 和 secretKey */
  public static final String SECURITY_TOKEN = "SecurityToken";

  /** KEY_FILE */
  public static final String KEY_FILE =
      System.getProperty(
          "rocketmq.client.keyFile", System.getProperty("user.home") + File.separator + "key");

  /** accessKey */
  private String accessKey;

  /** secretKey */
  private String secretKey;

  /** securityToken */
  private String securityToken;

  /** signature */
  private String signature;

  /** SessionCredentials */
  public SessionCredentials() {
    String keyContent = null;
    try {
      keyContent = MixAll.file2String(KEY_FILE);
    } catch (IOException ignore) {
    }
    if (keyContent != null) {
      Properties prop = MixAll.string2Properties(keyContent);
      if (prop != null) {
        this.updateContent(prop);
      }
    }
  }

  /**
   * SessionCredentials
   *
   * @param accessKey ignore
   * @param secretKey ignore
   */
  public SessionCredentials(String accessKey, String secretKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  /**
   * SessionCredentials
   *
   * @param accessKey ignore
   * @param secretKey ignore
   * @param securityToken ignore
   */
  public SessionCredentials(String accessKey, String secretKey, String securityToken) {
    this(accessKey, secretKey);
    this.securityToken = securityToken;
  }

  /**
   * updateContent
   *
   * @param prop ignore
   */
  public void updateContent(Properties prop) {
    {
      String value = prop.getProperty(ACCESS_KEY);
      if (value != null) {
        this.accessKey = value.trim();
      }
    }
    {
      String value = prop.getProperty(SECRET_KEY);
      if (value != null) {
        this.secretKey = value.trim();
      }
    }
    {
      String value = prop.getProperty(SECURITY_TOKEN);
      if (value != null) {
        this.securityToken = value.trim();
      }
    }
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

  /**
   * getSecurityToken
   *
   * @return ignore
   */
  public String getSecurityToken() {
    return securityToken;
  }

  /**
   * setSecurityToken
   *
   * @param securityToken ignore
   */
  public void setSecurityToken(final String securityToken) {
    this.securityToken = securityToken;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((accessKey == null) ? 0 : accessKey.hashCode());
    result = prime * result + ((secretKey == null) ? 0 : secretKey.hashCode());
    result = prime * result + ((signature == null) ? 0 : signature.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;

    SessionCredentials other = (SessionCredentials) obj;
    if (accessKey == null) {
      if (other.accessKey != null) return false;
    } else if (!accessKey.equals(other.accessKey)) return false;

    if (secretKey == null) {
      if (other.secretKey != null) return false;
    } else if (!secretKey.equals(other.secretKey)) return false;

    if (signature == null) {
      if (other.signature != null) return false;
    } else if (!signature.equals(other.signature)) return false;

    return true;
  }

  @Override
  public String toString() {
    return "SessionCredentials [accessKey="
        + accessKey
        + ", secretKey="
        + secretKey
        + ", signature="
        + signature
        + ", SecurityToken="
        + securityToken
        + "]";
  }
}