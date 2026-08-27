# 更新日志

本项目所有重要变更都将记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- 服务版本与分组路由，Spring `@RpcService` 自动注册，以及按请求超时预算清理 Future
- `RpcErrorCode` 稳定错误类别与启动期 `RpcConfig` 参数校验
- `@RpcIdempotent` 注解：只有显式幂等的方法才可执行 failover 重试

### Changed

- 网络协议拒绝 JDK 原生序列化；请改用 Hessian2 或 Protostuff
- 服务端线程池饱和时返回过载响应，不再在 Netty IO 线程执行调用者任务

### Fixed

- 修复半开熔断器并发探测超额、客户端配置未生效、Starter 未自动发布服务和基准测试干扰默认测试套件的问题

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
