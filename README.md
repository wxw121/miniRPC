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

### 1. 安装、编译 & 测试

```bash
mvn clean install
```

这会先把各模块安装到本地 Maven 仓库；之后的单模块示例启动命令才能解析 `rpc-core` 和 `rpc-sample-api`。

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

## 安全、重试与可观测性配置

`rpc-sample-*/src/main/resources/application.yml` 是 Spring Boot Starter 的配置模板；示例应用以编程方式启动，并可通过 `-Drpc.security.mode=plaintext|tls|mtls` 运行三种传输模式。完整的本地证书生成与启动命令见 [`docs/examples/transport-samples.md`](docs/examples/transport-samples.md)；Spring Boot TLS/mTLS 模板仍位于 [`docs/examples/tls/application.yml`](docs/examples/tls/application.yml) 与 [`docs/examples/mtls/application.yml`](docs/examples/mtls/application.yml)。

- 重试配置：`minirpc.client.retry.max-retries`（默认 `2`）、`base-backoff-ms`（默认 `100`）与 `max-backoff-ms`（默认 `1000`）。只有 `@RpcIdempotent` 方法在 `UNAVAILABLE` 或 `OVERLOADED` 时才会重试；延迟采用有界抖动，并且不会越过请求的绝对 deadline。
- TLS 配置：Starter 使用 `minirpc.client.tls.*` 与 `minirpc.server.tls.*`；编程式样例使用 `rpc.tls.client.*` 与 `rpc.tls.server.*` 系统属性。两者均支持 `enabled`、`key-store-path`、`key-store-password`、`trust-store-path`、`trust-store-password`、`mutual-authentication`。启用 TLS 时必须提供 PKCS#12 key store 及其密码；mTLS 还必须提供 trust store。证书、私钥和密码由部署环境提供，不能提交到仓库。
- 可观测性：Micrometer 在类路径中有 `MeterRegistry` 时默认启用，可用 `minirpc.observability.micrometer-enabled=false` 关闭；OpenTelemetry 由 `minirpc.observability.opentelemetry-enabled=true` 显式开启。

Micrometer 指标为 `minirpc.rpc.calls`、`minirpc.rpc.duration`、`minirpc.rpc.retries`、`minirpc.rpc.timeouts`、`minirpc.rpc.circuit_breaker.rejections`、`minirpc.rpc.business_executor.rejections`，以及 `minirpc.rpc.connection_pool.active` Gauge；固定标签只有 `side`、`service`、`outcome`、`error_code`，trace ID 与端点绝不会写入指标标签。每次调用尝试可创建一个名为 `minirpc.rpc` 的 OpenTelemetry span，属性为 `rpc.side`、`rpc.service`、`rpc.retry_attempt`、`rpc.error_code`，以及用于关联的 `rpc.trace_id`、`rpc.target_endpoint`。业务参数、响应体和凭据不会导出。日志仍可用 `requestId`、`traceId` 和服务键关联调用，但不得记录业务参数或凭据。

启用 Actuator 后，`minirpcReadiness` 仅在 RPC Server 已运行、未进入优雅停机且业务执行器未饱和时为 UP；`minirpcLiveness` 在未进入优雅停机时为 UP。两者均包含 `running`、`shuttingDown`、`inFlightRequests`、注册中心配置/已注册服务数、活跃连接数，以及业务线程池活跃数、队列状态和饱和标志。对非回环地址的明文服务端绑定会发出安全告警。
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
