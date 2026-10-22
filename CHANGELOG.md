# 更新日志

本项目所有重要变更都将记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## 2026-10-22

- 同步、异步代理统一 Future 调用链，异步接入幂等准入、Failover/Failback、端点隔离与恢复探测、追踪及指标。
- 内置重试通过延迟调度执行，保留同步 SPI 兼容桥接；补齐异常 Future 的熔断统计、截止时间与代理取消传播。

## [Unreleased]

### Added

- 服务版本与分组路由，Spring `@RpcService` 自动注册，以及按请求超时预算清理 Future
- `RpcErrorCode` 稳定错误类别与启动期 `RpcConfig` 参数校验
- `@RpcIdempotent` 注解：只有显式幂等的方法才可执行 failover 重试
- 无依赖调用事件 SPI，以及可选 Micrometer 调用计数/耗时/重试/超时/熔断拒绝/线程池拒绝指标及连接池活跃 Gauge、带 trace 与端点关联字段的 OpenTelemetry span 和 `minirpcReadiness` / `minirpcLiveness` 健康指标（含注册、连接、线程池快照）
- 按 deadline 限制的幂等 failover 重试、客户端本地端点摘除/恢复探测，以及稳定 `UNAVAILABLE` / `OVERLOADED` / `UNAUTHORIZED` 错误边界
- 可选 TLS/mTLS Netty 传输与 Starter 配置模板；TLS 在协议编解码前握手，证书材料由部署环境提供

### Changed

- 网络协议拒绝 JDK 原生序列化；请改用 Hessian2 或 Protostuff
- 服务端线程池饱和时返回过载响应，不再在 Netty IO 线程执行调用者任务

### Fixed

- 修复半开熔断器并发探测超额、客户端配置未生效、Starter 未自动发布服务和基准测试干扰默认测试套件的问题
- Starter 识别 core 引用注解并应用独立超时，自动安装 `RpcAuthenticator` 和 `ClusterStrategy` Bean
- 接通 Failback 后台补偿与客户端关闭流程，补齐错误类别、原 deadline、次数限制和跨线程失败节点隔离
- 保留响应 Future 的明确错误类别，将实际连接/写入失败归为 `UNAVAILABLE`、编码失败归为 `SERIALIZATION_ERROR`，使传输故障可以进入重试策略
- 序列化器工厂与 Starter 注册中心实际加载 SPI 扩展；清理未使用的序列号和字符串工具方法

### Compatibility

- 删除 Starter 中重复的 `com.minirpc.spring.annotation.RpcReference`，引用统一导入 `com.minirpc.core.annotation.RpcReference`
- `@RpcReference.timeout` 默认值调整为 `0`（使用全局配置）；core 注解原默认值为 `5000`
- 自定义 `Serializer` 实现需新增 `getType()`，声明兼容的现有 `SerializerType`

## [1.0.0] - 2026-07-22

### Added（新增）

**核心通信**
- 基于 Netty Reactor 模型的网络通信（`NettyServer` / `NettyClient`）
- IO 线程与业务线程分离，业务逻辑提交独立线程池
- 自定义协议编解码器（魔数 + 版本 + 长度字段），解决 TCP 粘包/拆包
- 心跳保活机制（客户端写空闲发送，服务端读空闲检测）

**序列化**
- JDK 原生序列化
- Hessian2 序列化（默认）
- Protostuff 序列化（Protobuf 兼容，Schema 缓存 + ThreadLocal Buffer 优化）

**服务治理**
- 服务注册与发现（ZooKeeper + Curator）
- 服务本地缓存 + 变更订阅
- 负载均衡：随机（带预热权重）/ 轮询 / 一致性哈希
- 容错策略：Failover / Failfast / Failback
- 三态熔断器（CLOSED / OPEN / HALF_OPEN）
- 令牌桶限流器
- 服务预热/慢启动（权重线性增长，默认 10 分钟）

**调用能力**
- JDK 动态代理透明调用
- 异步调用（CompletableFuture）
- 泛化调用（无需接口 JAR）
- 连接池（每地址多 Channel 轮询复用，断线自动重建）
- 请求-响应匹配 + 超时 Future 清理（防内存泄漏）

**扩展与集成**
- SPI 扩展机制（注册中心 / 序列化器可插拔）
- Filter 责任链（TraceFilter / MetricsFilter）
- Spring Boot Starter 自动装配 + `@RpcReference` 注解注入

**运维**
- 优雅停机五步编排（摘流量 → 等缓存 → 等 in-flight → 关连接）
- ShutdownHook 自动触发优雅关闭

**文档**
- README、架构设计、协议规范、核心特性文档

### Fixed（修复）

- 修复 `ChannelPool` 轮询索引 `Math.abs(Integer.MIN_VALUE)` 整数溢出导致的数组越界
- 修复 `RpcMessageDecoder` 缺少 bodyLength 上限校验导致的潜在 OOM 风险
- 修复 `NettyClient.sendAsync` 连接获取失败时 Future 泄漏问题
- 修复 `RpcServerHandler` 客户端断开后仍写响应导致的异常日志
- 修复 `NettyServer.stop()` 可能被重复调用的幂等性问题
- 修复泛化调用缺少参数校验导致的 NPE/AIOOBE

### Changed（变更）

- `ChannelPool` 构造函数预解析 host/port，避免重复字符串分割
- 服务端请求日志由 `info` 降级为 `debug`，降低高 QPS 下日志量

## [0.1.0] - 初始版本

### Added
- 项目骨架搭建（多模块 Maven 工程）
- 基础 RPC 调用链路（直连模式）
