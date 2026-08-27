# Mini RPC Framework

> 一个面向学习与面试的轻量级 Java RPC 框架，基于 Netty 实现高性能网络通信，麻雀虽小五脏俱全。

[![Java](https://img.shields.io/badge/Java-17-orange)]()
[![Netty](https://img.shields.io/badge/Netty-4.1.108-blue)]()
[![License](https://img.shields.io/badge/License-MIT-green.svg)]()

## ✨ 项目简介

MiniRPC 是一个从零实现的 RPC 框架，覆盖了一个生产级 RPC 框架的核心链路：**网络通信 → 协议编解码 → 序列化 → 服务注册发现 → 负载均衡 → 容错 → 动态代理**。

项目以「面试可讲、源码可读」为目标，每个核心类都附带详细的中文注释与面试考点说明，适合：

- 想深入理解 RPC 原理的 Java 开发者
- 准备分布式系统/中间件面试的同学
- 想阅读简洁中间件源码的学习者

## 🚀 核心特性

| 能力 | 说明 |
|------|------|
| 🌐 高性能通信 | 基于 Netty Reactor 模型，IO 线程与业务线程分离 |
| 📦 自定义协议 | 魔数 + 版本 + 长度字段，解决 TCP 粘包/拆包 |
| 🔁 网络序列化 | Hessian2 / Protostuff（Protobuf 兼容）；网络链路拒绝 JDK 原生反序列化 |
| 🗂️ 服务注册发现 | ZooKeeper（Curator）+ 本地缓存 + 变更订阅 |
| ⚖️ 负载均衡 | 随机（带预热权重）/ 轮询 / 一致性哈希 |
| 🛡️ 容错策略 | Failover / Failfast / Failback |
| ⚡ 熔断限流 | 三态熔断器 + 令牌桶限流 |
| 🔗 连接池 | 每地址多 Channel 轮询复用，断线自动重建 |
| 🔄 异步调用 | CompletableFuture 非阻塞调用 |
| 🎯 泛化调用 | 无需接口 JAR，按方法名+参数动态调用 |
| 🔌 SPI 扩展 | 可插拔的注册中心 / 序列化器 |
| 🌱 Spring Boot Starter | 自动装配 + `@RpcReference` 注解注入 |
| 🧭 服务路由 | 按接口、版本和分组发现服务；`@RpcIdempotent` 方法才允许故障重试 |
| 🔥 服务预热 | 慢启动权重线性增长，保护新节点 |
| 🛬 优雅停机 | 摘流量 → 等缓存 → 等 in-flight → 关连接 |
| 🧩 Filter 链 | Trace / Metrics 可扩展拦截器 |

## 📁 模块结构

```
mini-rpc
├── rpc-common              # 基础层：协议、序列化、SPI、异常、工具
├── rpc-core                # 核心层：Netty 通信、代理、负载均衡、容错、Filter
├── rpc-registry            # 注册中心：Registry SPI + ZooKeeper 实现 + 服务发现
├── rpc-spring-boot-starter # Spring Boot 自动装配
├── rpc-sample-api          # 示例：服务接口定义
├── rpc-sample-server       # 示例：服务提供者
└── rpc-sample-client       # 示例：服务消费者
```

## ⚡ 快速开始

### 环境要求

- JDK 17+
- Maven 3.6+
- （可选）ZooKeeper 3.6+

> 网络 RPC 仅支持 `hessian2`（默认）和 `protostuff`。`jdk` 序列化配置会在启动时被拒绝。

### 1. 编译 & 测试

```bash
mvn clean test
```

### 2. 直连模式（无需 ZooKeeper）

**启动服务端：**
```bash
mvn compile exec:java -pl rpc-sample-server \
  -Dexec.mainClass="com.minirpc.sample.server.ServerApplication"
```

**启动客户端（另开终端）：**
```bash
mvn compile exec:java -pl rpc-sample-client \
  -Dexec.mainClass="com.minirpc.sample.client.ClientApplication"
```

预期输出：
```
sayHello result: Hello, Interview!
add result: 3 + 5 = 8
=== RPC Call Success! ===
```

### 3. 注册中心模式（需要 ZooKeeper）

```bash
# 服务端
mvn compile exec:java -pl rpc-sample-server \
  -Dexec.mainClass="com.minirpc.sample.server.ServerApplication" \
  -Dzk.address=127.0.0.1:2181

# 客户端
mvn compile exec:java -pl rpc-sample-client \
  -Dexec.mainClass="com.minirpc.sample.client.ClientApplication" \
  -Dzk.address=127.0.0.1:2181
```

## 💻 编程式使用

### 服务提供者

```java
RpcServer server = new RpcServer(9090);
server.setRegistry(new ZookeeperRegistry("127.0.0.1:2181")); // 可选
server.registerService(new HelloServiceImpl());
server.start(); // 阻塞
```

### 服务消费者

```java
// 直连模式
RpcClient client = new RpcClient("127.0.0.1", 9090);

// 注册中心模式
ServiceDiscovery discovery = new ServiceDiscovery(new ZookeeperRegistry("127.0.0.1:2181"));
RpcClient client = new RpcClient(discovery, new RoundRobinLoadBalancer());

HelloService service = client.createProxy(HelloService.class);
String result = service.sayHello("World"); // 像本地方法一样调用
```

### 异步调用

```java
HelloServiceAsync async = client.createAsyncProxy(HelloServiceAsync.class);
CompletableFuture<String> future = async.sayHello("World");
future.thenAccept(System.out::println);
```

### 泛化调用

```java
GenericService generic = client.createGenericProxy("com.example.EchoService");
Object result = generic.$invoke("echo",
        new String[]{"java.lang.String"},
        new Object[]{"hello"});
```

## 📚 文档导航

| 文档 | 内容 |
|------|------|
| [架构设计](docs/ARCHITECTURE.md) | 整体架构、调用链路、线程模型 |
| [协议规范](docs/PROTOCOL.md) | 自定义协议字节布局 |
| [核心特性](docs/FEATURES.md) | 各项能力的设计与实现要点 |
| [更新日志](CHANGELOG.md) | 版本迭代记录 |

## 🛠️ 技术栈

- **网络通信**：Netty 4.1.108
- **序列化**：Hessian2 4.0.66 / Protostuff 1.8.0
- **注册中心**：Apache Curator 5.6.0（ZooKeeper）
- **日志**：SLF4J 2.0.12 + Logback 1.5.3
- **集成**：Spring Boot 3.2.4
- **测试**：JUnit 5.10.2

## 📄 License

[MIT License](LICENSE)
