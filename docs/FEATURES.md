# 核心特性

本文档详述 MiniRPC 各项核心能力的设计思路与实现要点。

## 目录

1. [网络通信](#1-网络通信)
2. [序列化](#2-序列化)
3. [服务注册与发现](#3-服务注册与发现)
4. [负载均衡](#4-负载均衡)
5. [容错策略](#5-容错策略)
6. [熔断与限流](#6-熔断与限流)
7. [连接池](#7-连接池)
8. [异步调用](#8-异步调用)
9. [泛化调用](#9-泛化调用)
10. [服务预热](#10-服务预热)
11. [优雅停机](#11-优雅停机)
12. [Filter 链](#12-filter-链)
13. [SPI 扩展](#13-spi-扩展)
14. [Spring Boot Starter](#14-spring-boot-starter)
15. [可靠性、可观测性与传输安全](#15-可靠性可观测性与传输安全)

---

## 1. 网络通信

**实现**：`NettyServer` / `NettyClient`

- 基于 Netty Reactor 模型，Boss 线程接受连接，Worker 线程处理 IO
- **IO 线程与业务线程分离**：Worker 只做编解码，反射调用提交到独立业务线程池
- 心跳保活：客户端写空闲 30s 发心跳，服务端读空闲超时关连接

**运行约束**：业务逻辑由独立线程池执行，避免阻塞 Netty Worker 的 I/O 处理。

## 2. 序列化

**实现**：`Serializer` SPI + `SerializerFactory`

| 序列化器 | 特点 |
|----------|------|
| JdkSerializer | 无需依赖，但体积大、速度慢、要求 Serializable |
| Hessian2Serializer | 默认，跨语言、体积小、性能好 |
| ProtostuffSerializer | Protobuf 兼容，无需 .proto，RuntimeSchema 动态生成 |

> `JdkSerializer` 仅保留给本地显式使用。网络编解码器和 `RpcConfig` 会拒绝 JDK 序列化，避免不受控的原生反序列化入口。

**Protostuff 优化**：
- `ConcurrentHashMap` 缓存 Schema，避免重复生成
- `ThreadLocal<LinkedBuffer>` 复用缓冲区，减少 GC

## 3. 服务注册与发现

**实现**：`Registry` SPI + `ZookeeperRegistry` + `ServiceDiscovery`

- 提供者启动时注册 `ServiceInstance`（接口名/host/port/版本/分组/权重）到 ZK
- 消费者通过 `ServiceDiscovery` 拉取实例列表并**本地缓存**
- 订阅 ZK 节点变更，实例上下线时实时刷新缓存

**运行行为**：注册中心不可用时，消费者继续使用本地缓存中的服务实例。

## 4. 负载均衡

**实现**：`LoadBalancer` 接口

| 策略 | 说明 |
|------|------|
| RandomLoadBalancer | 加权随机，集成服务预热权重 |
| RoundRobinLoadBalancer | 轮询，适合实例性能均匀 |
| ConsistentHashLoadBalancer | 一致性哈希，相同参数路由到同一节点（缓存友好） |

## 5. 容错策略

**实现**：`ClusterStrategy` 接口

| 策略 | 行为 | 适用场景 |
|------|------|----------|
| Failover | 失败自动切换其他节点重试 | 读操作、幂等写 |
| Failfast | 失败立即报错 | 非幂等写操作 |
| Failback | 首次失败向调用方报错，后台在原 deadline 内定时补偿 | 可接受首次失败、允许重试的幂等通知 |

同步、异步代理统一经过限流、熔断、FilterChain、容错策略和端点健康选址；仅标记 `@RpcIdempotent` 的方法使用 Failover，非幂等方法和泛化调用使用 Failfast。创建代理前可通过 `RpcClient.setClusterStrategy(ClusterStrategy)` 替换幂等方法的策略（同步、异步共用）；Starter 自动获取唯一（或 `@Primary`）的 `ClusterStrategy` Bean。例如 `new FailbackClusterStrategy(1, 3)` 表示间隔 1 秒、最多 3 次后台重试（也是无参构造的默认值），仅处理 `UNAVAILABLE` / `OVERLOADED`。补偿不重置 deadline，剩余预算不足时不排队，执行前过期也会停止；它是进程内的有限重试，不保证持久化或最终投递。非幂等方法和泛化调用不启用补偿。策略由客户端独占，`RpcClient.shutdown()` 会调用其幂等的 `close()`，取消未执行任务并关闭调度器。

## 6. 熔断与限流

**熔断器** `CircuitBreaker`：三态状态机

```
CLOSED ──连续失败达阈值──→ OPEN ──超时──→ HALF_OPEN
   ↑                                        │
   └──────────试探成功──────────────────────┘
```

- CLOSED：正常放行，统计失败
- OPEN：快速失败，避免雪崩
- HALF_OPEN：放行少量请求试探，成功则恢复

**限流器** `TokenBucketRateLimiter`：令牌桶算法，平滑限流，允许一定突发。

## 7. 连接池

**实现**：`ChannelPool`

- 每个地址维护多个 Channel（默认 4 个）
- 轮询选取（位运算防整数溢出），分散写入压力
- 连接断开时惰性重建，Double-check 保证并发安全

**运行行为**：连接池通过多个 Channel 分散高并发写入压力。

## 8. 异步调用

**实现**：`RpcClient.createAsyncProxy()`

- 代理方法返回 `CompletableFuture<T>`
- 同步和异步代理共用 Future 调用链；同步入口等待结果，异步入口直接返回业务 Future
- 内置 Failover 通过 Future 回调和延迟调度重试，不在响应线程中阻塞等待或休眠；重试共享原 deadline
- 过滤器按整次逻辑调用统计，事件与端点健康按每次尝试记录；异常完成同样回报熔断器
- 取消代理 Future 会传递到底层请求，并停止后续 Failover 尝试；超时返回 `DEADLINE_EXCEEDED`，不发送取消帧
- 同步策略/过滤器扩展接口保持兼容：默认异步入口在 JDK 公共线程池桥接；扩展可覆盖 `invokeAsync` / `filterAsync`，避免桥接线程等待
- 服务发现和获取连接仍可能同步等待，异步入口并非全程无阻塞
- 可并行调用多个服务，提升吞吐量

## 9. 泛化调用

**实现**：`GenericService` + `RpcClient.createGenericProxy()`

```java
GenericService generic = client.createGenericProxy("com.example.EchoService");
Object result = generic.$invoke("echo",
        new String[]{"java.lang.String"}, new Object[]{"hello"});
```

- 无需依赖服务接口 JAR
- 通过方法名 + 参数类型名 + 参数值动态调用
- 适用场景：API 网关、测试平台

## 10. 服务预热

**实现**：`ServiceInstance.getWarmupWeight()` + `RandomLoadBalancer`

新启动节点权重随运行时间线性增长：

```
effectiveWeight = max(1, weight × uptime / warmupPeriod)
```

- 默认预热周期 10 分钟
- 避免新节点（JIT 未编译、缓存未预热）被大流量打垮

## 11. 优雅停机

**实现**：`GracefulShutdownManager` + `RpcServer.stop()`

五步编排：

```
① markShuttingDown()    新请求立即拒绝
② unregister            从注册中心摘流量
③ sleep(2s)             等消费者刷新本地缓存
④ awaitInFlight(30s)    等已接收请求处理完毕
⑤ close Netty+executor  关闭连接和线程池
```

- `volatile shuttingDown` + `AtomicInteger inFlightRequests`
- `acquireRequest()` Double-check 防竞态
- `stop()` 幂等（AtomicBoolean），防重复调用

## 12. Filter 链

**实现**：`FilterChain` + `RpcFilter`

| Filter | 职责 |
|--------|------|
| TraceFilter | 链路追踪埋点（traceId 传递） |
| MetricsFilter | 调用次数、耗时、成功率统计 |

责任链模式，可扩展自定义拦截器（鉴权、日志等）。

## 13. SPI 扩展

**实现**：`SpiLoader`

- 通过 Java `ServiceLoader` 读取 `META-INF/services/<接口全限定名>`，文件内容为具有无参构造方法的实现类全限定名
- `SerializerFactory` 实际调用 `SpiLoader.loadAll(Serializer.class)`；实现的 `getType()` 声明对应的 `SerializerType`。内置实现作为回退，自定义实现优先，多个不同自定义类声明同一类型时明确报错，不依赖配置文件发现顺序
- 序列化扩展必须兼容对应枚举值的线格式，客户端与服务端应部署相同实现；SPI 不新增协议类型码，也不允许网络报文选择 JDK 原生序列化
- Starter 在 `minirpc.registry.type=spi` 时通过 `SpiLoader.loadAll(Registry.class)` 为每个容器新建唯一的注册中心实现；没有提供者或存在多个提供者时启动失败。用户显式声明的 `Registry` Bean 优先于自动配置，`local` / `zookeeper` 保持原有选择方式

## 14. Spring Boot Starter

**实现**：`MiniRpcAutoConfiguration` + `@RpcReference`

- `@AutoConfiguration` 自动装配 RpcServer / RpcClient / Registry
- `@ConditionalOnProperty` 支持开关（`minirpc.enabled`）
- 服务引用统一使用 `com.minirpc.core.annotation.RpcReference`；Starter 的 `RpcReferenceBeanPostProcessor` 扫描注解字段（含继承字段），自动注入代理
- `@RpcReference` 支持 `version`、`group`、`timeout`；服务发现使用接口、版本、分组三元组匹配实例。`timeout=0`（默认）沿用全局超时，正数覆盖当前引用的毫秒预算，负数启动失败；不会改变其他引用的配置
- 仅标记 `@RpcIdempotent` 的同步和异步方法使用自定义 `ClusterStrategy` Bean（未提供则 failover），其他方法默认 fail-fast，避免写操作重复执行
- 自动创建的服务端使用应用提供的唯一或 `@Primary` 的 `RpcAuthenticator` Bean，在业务执行前鉴权；无 Bean 时默认放行，多个 Bean 无法唯一选择时启动失败。自定义 `RpcServer` Bean 仍由应用配置
- 配置前缀 `minirpc.*`，支持注册中心、端口、序列化类型等

```yaml
minirpc:
  enabled: true
  server:
    port: 9090
  registry:
    type: zookeeper
    address: 127.0.0.1:2181

```
## 15. 可靠性、可观测性与传输安全

**调用事件与指标**：核心通过 `RpcInvocationListener` 发布每次客户端/服务端调用尝试。Starter 可选地记录 `minirpc.rpc.calls`、`minirpc.rpc.duration`、`minirpc.rpc.retries`、`minirpc.rpc.timeouts`、熔断/业务执行器拒绝计数器和 `minirpc.rpc.connection_pool.active` Gauge；固定低基数标签为 `side`、`service`、`outcome`、`error_code`，不会把请求 ID、trace ID、端点、参数或响应体写入标签。显式开启 OpenTelemetry 时，每次尝试创建 `minirpc.rpc` span，写入 `rpc.side`、`rpc.service`、`rpc.retry_attempt`、`rpc.error_code` 及关联属性 `rpc.trace_id`、`rpc.target_endpoint`；业务参数、响应体和凭据不导出。

**重试治理**：仅 `@RpcIdempotent` 方法可 failover，且只针对 `UNAVAILABLE`、`OVERLOADED`。重试预算、指数退避上限和绝对 deadline 共同限制后续尝试；deadline 已耗尽不会启动新请求。客户端端点失败会临时摘除，恢复期只允许一个探测请求。

**TLS/mTLS**：`RpcConfig` 的 TLS 是 opt-in。TLS handler 在协议帧之前安装；mTLS 要求客户端证书。启用时使用部署环境提供的 PKCS#12 key/trust store，缺少必需配置会在启动期失败。服务端以非回环地址明文绑定会记录告警。

**健康检查与认证**：Starter 提供 `minirpcReadiness`（服务运行、未停机且业务执行器未饱和）和 `minirpcLiveness`（未进入优雅停机），均带 `running`、`shuttingDown`、`inFlightRequests`、注册中心配置/服务数、活跃连接数，以及业务线程池活跃数、队列容量/积压和饱和状态详情。应用层认证在业务执行前完成，失败返回 `UNAUTHORIZED`，不会占用业务线程池。
