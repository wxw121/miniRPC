# 架构设计

本文档描述 MiniRPC 的整体架构、分层设计、调用链路与线程模型。

## 一、整体架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                          服务消费者 (Consumer)                         │
│                                                                       │
│   业务代码                                                              │
│      │  helloService.sayHello("World")  ← 像调用本地方法               │
│      ▼                                                                │
│   ┌──────────────────┐                                                │
│   │  RpcClientProxy  │  JDK 动态代理 (InvocationHandler)               │
│   └──────────────────┘                                                │
│      │                                                                │
│      ▼                                                                │
│   ┌──────────────────┐   ┌──────────────┐   ┌──────────────────┐     │
│   │   RateLimiter    │ → │CircuitBreaker│ → │   FilterChain    │     │
│   │  (令牌桶限流)    │   │ (三态熔断)    │   │ (Trace/Metrics)  │     │
│   └──────────────────┘   └──────────────┘   └──────────────────┘     │
│      │                                                                │
│      ▼                                                                │
│   ┌──────────────────┐   ┌──────────────────────────────────────┐   │
│   │ ServiceDiscovery │ → │ LoadBalancer (随机/轮询/一致性哈希)    │   │
│   │  (本地缓存+订阅)  │   │  + 服务预热权重                        │   │
│   └──────────────────┘   └──────────────────────────────────────┘   │
│      │                              │ 选出目标节点                     │
│      ▼                              ▼                                 │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │  NettyClient + ChannelPool (连接池, 轮询复用)                  │  │
│   │  RpcMessageEncoder → 自定义协议字节流                          │  │
│   └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │ TCP
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          服务提供者 (Provider)                         │
│                                                                       │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │  NettyServer (Boss/Worker EventLoopGroup)                     │  │
│   │  LengthFieldBasedFrameDecoder → RpcMessageDecoder             │  │
│   └──────────────────────────────────────────────────────────────┘  │
│      │ IO 线程只做编解码                                              │
│      ▼                                                                │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │  RpcServerHandler                                             │  │
│   │  - GracefulShutdownManager (停机拒绝 + in-flight 计数)         │  │
│   │  - 提交到业务线程池                                             │  │
│   └──────────────────────────────────────────────────────────────┘  │
│      │                                                                │
│      ▼                                                                │
│   ┌──────────────────┐   ┌──────────────────────────────────────┐   │
│   │ 业务线程池         │ → │ LocalServiceRegistry → 反射调用实现类  │   │
│   └──────────────────┘   └──────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼ 注册/注销
                  ┌───────────────────────┐
                  │   Registry (ZooKeeper) │
                  └───────────────────────┘
```

## 二、模块分层

| 层 | 模块 | 职责 |
|----|------|------|
| 基础层 | `rpc-common` | 协议定义、序列化、SPI 加载器、异常、工具类 |
| 核心层 | `rpc-core` | Netty 通信、动态代理、负载均衡、容错、Filter、服务端 |
| 注册层 | `rpc-registry` | Registry SPI、ZooKeeper 实现、服务发现与缓存 |
| 集成层 | `rpc-spring-boot-starter` | Spring Boot 自动装配、注解注入 |
| 示例层 | `rpc-sample-*` | 可运行的 Provider / Consumer 示例 |

依赖方向（单向）：

```
sample → spring-boot-starter → core → registry → common
```

## 三、客户端调用链路

同步与异步代理共用以下调用链，只有结果获取方式不同：

```text
1. RpcClientProxy.invoke()          构建请求及 deadline，检查 @RpcIdempotent
        ↓
2. RateLimiter → CircuitBreaker    每次逻辑调用执行一次准入检查
        ↓
3. FilterChain.executeAsync()       TraceFilter → MetricsFilter
        ↓
4. ClusterStrategy.invokeAsync()    幂等方法可重试，其他方法使用 Failfast
        ↓ 每次尝试重新执行下面的选址和发送
5. copyForAttempt()                保留 requestId/traceId/deadline，记录尝试编号
        ↓
6. ServiceDiscovery → 失败节点排除 → EndpointHealthTracker → LoadBalancer
        ↓ 直连模式使用配置地址
7. NettyClient.sendAsync()          先登记 Future，再获取连接并发送
        ↓
8. RpcClientHandler                按 requestId 完成响应 Future
        ↓
9. 尝试完成回调                      记录端点健康和调用事件，策略判断是否再次尝试
        ↓
10. 过滤器与熔断器记录最终结果        提取 response.data
        ↓
11. 同步入口等待业务 Future；异步入口直接返回业务 Future
```

内置 Failover 使用延迟调度和 Future 回调，不在响应线程中休眠或等待下次响应。
同步 SPI 保持兼容，旧扩展通过公共线程池桥接；内置策略与过滤器提供原生异步实现。
服务发现和连接获取仍可能同步等待。取消代理 Future 会传递到底层当前请求；
超时为异常完成，不发送 CANCEL。

## 四、服务端处理链路

```
1. Netty Boss 线程接收连接 → 注册到 Worker
        ↓
2. Worker IO 线程读取字节流
        ↓
3. LengthFieldBasedFrameDecoder     按长度字段切包，解决粘包/拆包
        ↓
4. RpcMessageDecoder                校验魔数/版本 → 反序列化 Body → RpcMessage
        ↓
5. RpcServerHandler.channelRead0()
        - 心跳消息：IO 线程直接处理
        - 业务请求：shutdownManager.acquireRequest() 计数
        ↓
6. 提交到业务线程池（避免阻塞 IO 线程）
        ↓
7. LocalServiceRegistry.getService() 查找实现类
        ↓
8. Method.invoke() 反射调用
        ↓
9. shutdownManager.releaseRequest() 释放计数
        ↓
10. 编码 RpcResponse → writeAndFlush 返回
```

## 五、线程模型

```
┌─────────────────────────────────────────────────────┐
│  服务端                                               │
│  ┌──────────────┐   ┌──────────────────────────┐    │
│  │ BossGroup    │   │ WorkerGroup               │    │
│  │ (1 线程)      │   │ (2*CPU 线程, IO 读写+编解码)│    │
│  │ 接受连接      │ → │                           │    │
│  └──────────────┘   └──────────────────────────┘    │
│                              │ submit                │
│                              ▼                       │
│                     ┌──────────────────────┐        │
│                     │ 业务线程池             │        │
│                     │ (反射调用, 核心数*2)   │        │
│                     └──────────────────────┘        │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  客户端                                               │
│  ┌──────────────────────────┐                        │
│  │ NioEventLoopGroup         │  网络 IO + 编解码       │
│  └──────────────────────────┘                        │
│  ┌──────────────────────────┐                        │
│  │ rpc-future-cleanup (守护) │  定时清理超时 Future    │
│  └──────────────────────────┘                        │
└─────────────────────────────────────────────────────┘
```

**设计要点**：IO 线程绝不执行业务逻辑，否则一个慢请求会阻塞整个 Worker，导致所有连接卡死。这是 Netty Reactor 模型的最佳实践，也是 Dubbo 的线程模型。

## 六、请求-响应匹配

客户端通过唯一 `requestId` 关联请求与响应：

```
发送时：RequestFutureHolder.register(requestId) → Map<requestId, CompletableFuture>
接收时：RpcClientHandler 读取响应 requestId → complete(response)
```

**内存泄漏防护**：`RequestFutureHolder` 内置守护线程，按每个请求注册时的超时预算清理未完成 Future；不再使用固定 30 秒阈值。

## 七、关键设计决策

| 决策 | 理由 |
|------|------|
| 自定义协议而非 HTTP | 更紧凑的头部（9 字节），二进制序列化更高效 |
| 连接池而非单连接 | 高并发下单 Channel 写入竞争导致 RT 抖动 |
| 本地缓存服务列表 | 避免每次调用都访问注册中心，注册中心宕机不影响调用 |
| CompletableFuture | 统一同步/异步模型，同步 = future.get() |
| SPI 机制 | 注册中心、序列化器可插拔，符合开闭原则 |

## 八、可靠性、可观测性与安全边界

`rpc-core` 通过无框架依赖的 `RpcInvocationEvent` / `RpcInvocationListener` 发布每次客户端或服务端调用尝试的结果。事件记录调用侧、服务键、稳定错误码、耗时、尝试次数和目标端点；Spring Boot Starter 在边缘可选地将其适配为 Micrometer 和 OpenTelemetry，核心模块不依赖这些库。

客户端代理只为 `@RpcIdempotent` 调用选择 failover。`RetryPolicy` 只接收 `UNAVAILABLE` 和 `OVERLOADED`，采用有界抖动退避，并在开始下一次尝试前再次检查绝对 deadline。端点摘除与单个恢复探测只保存在客户端本地，不回写注册中心。

TLS 在 Netty pipeline 的帧拆分、编解码和心跳之前安装；mTLS 在握手阶段要求并验证客户端证书。应用层认证在请求解码和 deadline 检查之后、业务线程池提交之前执行，拒绝映射为 `UNAUTHORIZED`，线程池拒绝映射为 `OVERLOADED`。这保证未认证请求不会占用业务执行器。

Starter 将 `RpcServerStatus` 暴露为独立的 `minirpcReadiness` 与 `minirpcLiveness` 健康指标。除了运行状态、优雅停机状态和 in-flight 请求数，详情还包含注册中心配置与已注册服务数、活跃连接数，以及业务线程池活跃数、队列状态和饱和标志；业务执行器饱和时 readiness 为 DOWN。
