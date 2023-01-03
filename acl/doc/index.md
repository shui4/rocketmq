# ACL

> - ACL(Access Control List) 访问控制列表.
> - 访问控制列表广泛应用于路由器和交换机上，起到十分重要的作用，如限制网络流量、提高网络性能、控制通信流量（如 ACL
    可以限定或简化路由更新信息的长度，从而限制通过路由器某一网段的通信流量）、提供网络安全访问的基本手段、在路由器端口处决定哪种类型的通信流量被转发或被阻塞.

## 使用 ACL

1) Broker 端配置文件

```properties
aclEnable=true
```

> 需要重启 Broker

2) 配置 plain_acl.yml

3) 生产者

```java
 SessionCredentials credentials = new SessionCredentials("rocketmq", "12345678");
    DefaultMQProducer producer =
        new DefaultMQProducer(
            "test", new AclClientRPCHook(credentials));
```

4) 消费者

````java
 DefaultMQPushConsumer consumer =
        new DefaultMQPushConsumer(
            "test", new AclClientRPCHook(credentials), new AllocateMessageQueueAveragely());
````