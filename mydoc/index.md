## 第一章

模块介绍

```
rocketmq
├── acl：权限控制模块
├── broker：broker模块（broker启动进程）
├── client：消息客户端，包含消息生产者和消费消费者相关类
├── common：公共包
├── dev：开发者信息（非源码）
├── distribution：打包分发目录（非源码）
├── exmpale：RocketMQ示例代码
├── filter：消息过滤相关基础类
├── logging：日志实现相关类
├── namesrv：NameServer实现相关类（NameServer启动进程）
├── openmessaging：消息开放标准，已发布
├── remoting：远程通行模块，基于Netty
├── srvutil：服务器工具类
├── store：消息存储实现类
├── style：checkstyle相关实现
├── test：测试相关类
└── tools：工具类，监控命令相关实现类
```

设计目标

1. 架构模式
2. 顺序消费
3. 消息过滤
4. 消息存储
5. 消息高可用性
6. 消息到达（消费）低延迟
7. 确保消息必须被消费一次
8. 回溯消息
9. 消息堆积
10. 定时消息
11. 消息重试机制

## 第二章-RocketMQ路由中心

#### NameServer架构设计

![ch2](img/ch2.png)

## 第三章-RocketMQ发送

普通消息3种实现：

- 可靠同步发送：发送消息等待响应。
- 可靠异步发送：发送消息，不阻塞，服务端响应之后，在另外的线程执行。
- 单向发送：只发，不管。

本章内容：

- RocketMQ消息结构。
- 消息生产者启动流程。
- 消息发送过程。
- 批量消息发送。

RocketMQ消息发送需要考虑3个问题：

- 消息队列如何进行负载？
- 消息发送如何实现高可用？
- 批量消息发送如何实现一致性？

消息发送高可用设计

- 1）消息发送重试机制RocketMQ在消息发送时如果出现失败，默认会重试两次。

- 2）故障规避机制当消息第一次发送失败时，如果下一次消息还是发送到刚刚失败

  的Broker上，其消息发送大概率还是会失败，因此为了保证重试的可靠性，在重试时会尽量避开刚刚接收失败的Broker，而是选择其他Broker上的队列进行发送，从而提高消息发送的成功率。

## 第四章-消息存储

### RocketMQ存储概要设计

- 文件：ComitLog、ConsumeQueue、Index。
- ComitLog：所有主题消息存在在同一个文件中，确保消息发送时顺序写文件。
- ConsumeQueue：每个消息主题包含多个消息队列，每个消息队列有一个消息文件。
- Index：加速消息的检索性能，根据消息的属性从ComitLog中快速检索消息。

---

协同：

1. ComitLog：消息存储在这里。
2. ConsumeQueue：消息消费队列，消息到达ComitLog文件后，将异步转发到ConsumeQueue文件中，供消费者消费。
3. Index：消息索引，主要存在消息key与offset的对应关系。

---



- 消息发送存储流程。
- 存储文件组织与内存映射机制。
- RocketMQ存储文件。
- 消息消费队列、索引文件构建机制。
- RocketMQ文件恢复机制。
- RocketMQ刷盘机制。
- RocketMQ文件删除机制。
- 同步双写机制。

### 存储文件组织与内存映射

- MappedFileQueue
- MappedFile

### RocketMQ存储文件	

```
store
├── checkpoint									
├── commitlog
│   └── 00000000000000000000
├── config
│   ├── consumerFilter.json			
│   ├── consumerFilter.json.bak			
│   ├── consumerOffset.json			
│   ├── consumerOffset.json.bak
│   ├── delayOffset.json			
│   ├── delayOffset.json.bak
│   ├── topics.json					
│   └── topics.json.bak
├── consumequeue
│   └── TopicTest
│       ├── 0
│       │   └── 00000000000000000000
│       ├── 1
│       │   └── 00000000000000000000
│       ├── 2
│       │   └── 00000000000000000000
│       └── 3
│           └── 00000000000000000000
├── index						
│   └── 20221031202849693
└── lock
```

1. commitlog：消息存储目录。
2. config：运行期间的一些配置信息，主要包括下列信息
   - consumerFilter.json：主题消息过滤信息。
   - consumerOffset.json：集群消费模式下的消息消费进度。
   - delayOffset.json：延时消息队列拉取进度
   - subscriptionGroup.json：消息消费组的配置信息
   - topics.json：topic配置属性。
3. consumequeue：消息消费队列存储目录
4. index：消息索引文件存储目录
5. abort：如果存在abort文件，说明Broker非正常关闭，该文件默认在启动Broker时创建，在正常退出之前删除。
6. checkpoint：检测点文件，存储CommitLog文件最后一次刷盘时间戳、ConsumeQueue最后一次刷盘时间、index文件最后一次刷盘时间戳

