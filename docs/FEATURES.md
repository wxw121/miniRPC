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

---

## 1. 网络通信

**实现**：`NettyServer` / `NettyClient`

- 基于 Netty Reactor 模型，Boss 线程接受连接，Worker 线程处理 IO
- **IO 线程与业务线程分离**：Worker 只做编解码，反射调用提交到独立业务线程池
- 心跳保活：客户端写空闲 30s 发心跳，服务端读空闲超时关连接

**面试考点**：为什么不能在 IO 线程执行业务？→ 阻塞一个 Worker 会卡死其上所有连接。

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

**面试考点**：注册中心宕机为何不影响调用？→ 消费者有本地缓存。

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
| Failback | 失败后记录，后台定时重试 | 消息通知、最终一致性 |

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

**面试考点**：单 Channel 在高并发下写入竞争导致 RT 抖动，多连接可分散压力。

## 8. 异步调用

**实现**：`RpcClient.createAsyncProxy()`

- 代理方法返回 `CompletableFuture<T>`
- 底层统一基于 Future，同步调用 = `future.get(timeout)`
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

- 类似 Dubbo SPI，通过配置文件加载接口实现
- 注册中心、序列化器均可插拔替换
- 符合开闭原则，扩展无需修改框架代码

## 14. Spring Boot Starter

**实现**：`MiniRpcAutoConfiguration` + `@RpcReference`

- `@AutoConfiguration` 自动装配 RpcServer / RpcClient / Registry
- `@ConditionalOnProperty` 支持开关（`minirpc.enabled`）
- `RpcReferenceBeanPostProcessor` 扫描 `@RpcReference` 字段，自动注入代理
- `@RpcReference` 可指定 `version` 和 `group`；服务发现使用接口、版本、分组三元组匹配实例
- 仅标记 `@RpcIdempotent` 的方法使用 failover，其他方法默认 fail-fast，避免写操作重复执行
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
