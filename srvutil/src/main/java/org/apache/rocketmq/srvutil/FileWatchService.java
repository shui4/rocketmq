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

package org.apache.rocketmq.srvutil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * FileWatchService
 *
 * @author shui4
 */
public class FileWatchService extends ServiceThread {
  /** log */
  private static final InternalLogger log =
      InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
  /** 检查配置是否频率变更 */
  private static final int WATCH_INTERVAL = 500;
  /** 需要监听的文件列表 */
  private final List<String> watchFiles;
  /** 当前监听一次文件对应的哈希值 */
  private final List<String> fileCurrentHash;
  /** 配置变更监听器，即配置发生变化后需要执行的处理逻辑 */
  private final Listener listener;
  /** 对文件的内容进行 md5 加密，计算文件的哈希值 */
  private MessageDigest md = MessageDigest.getInstance("MD5");

  /**
   * FileWatchService
   *
   * @param watchFiles ignore
   * @param listener ignore
   * @throws Exception ignore
   */
  public FileWatchService(final String[] watchFiles, final Listener listener) throws Exception {
    this.listener = listener;
    this.watchFiles = new ArrayList<>();
    this.fileCurrentHash = new ArrayList<>();
    // 如果文件存在，则先将文件路径加入 watchFiles，
    // 然后在 fileCurrentHash 对应的位置调用 hash() 方法，计算文件的 md5 值
    for (int i = 0; i < watchFiles.length; i++) {
      if (StringUtils.isNotEmpty(watchFiles[i]) && new File(watchFiles[i]).exists()) {
        this.watchFiles.add(watchFiles[i]);
        // 调用了 MessageDigest 相关的 API，将配置文件的所
        // 有内容进行 md5 加密，得到其 md5 值。文件的内容一旦发生了变化，其
        // 生成的 md5 值就会不同，我们可以以此来判断配置文件的内容是否发生变化
        this.fileCurrentHash.add(hash(watchFiles[i]));
      }
    }
  }

  @Override
  public String getServiceName() {
    return "FileWatchService";
  }

  @Override
  public void run() {
    log.info(this.getServiceName() + "service started");

    while (!this.isStopped()) {
      try {
        // 每隔 500ms 校验一次文件内容
        this.waitForRunning(WATCH_INTERVAL);
        // 遍历该线程监听的文件
        for (int i = 0; i < watchFiles.size(); i++) {
          String newHash;
          try {
            // 计算当前配置文件的 md5 值
            newHash = hash(watchFiles.get(i));
          } catch (Exception ignored) {
            log.warn(
                this.getServiceName() + "service has exception when calculate the file hash.",
                ignored);
            continue;
          }
          // 如果两次计算的 md5 值不同，意味着文件内容发生变化，则调用对应的事件处理方法
          if (!newHash.equals(fileCurrentHash.get(i))) {
            fileCurrentHash.set(i, newHash);
            listener.onChanged(watchFiles.get(i));
          }
        }
      } catch (Exception e) {
        log.warn(this.getServiceName() + "service has exception.", e);
      }
    }
    log.info(this.getServiceName() + "service end");
  }

  /**
   * hash
   *
   * @param filePath ignore
   * @return ignore
   * @throws IOException ignore
   * @throws NoSuchAlgorithmException ignore
   */
  private String hash(String filePath) throws IOException, NoSuchAlgorithmException {
    Path path = Paths.get(filePath);
    md.update(Files.readAllBytes(path));
    byte[] hash = md.digest();
    return UtilAll.bytes2string(hash);
  }

  /**
   * Listener
   *
   * @author shui4
   */
  public interface Listener {
    /**
     * Will be called when the target files are changed
     *
     * @param path the changed file path
     */
    void onChanged(String path);
  }
}