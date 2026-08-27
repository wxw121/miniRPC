/**
 * rpc-registry —— MiniRPC 注册中心模块。
 * <p>
 * 本模块定义服务注册与发现的统一抽象，并提供 ZooKeeper 实现。
 * <p>
 * 核心组件：
 * <ul>
 *   <li>{@link com.minirpc.registry.Registry} —— 注册中心 SPI 接口（注册/注销/查找/订阅）</li>
 *   <li>{@link com.minirpc.registry.ServiceInstance} —— 服务实例（含预热权重计算）</li>
 *   <li>{@link com.minirpc.registry.ServiceDiscovery} —— 服务发现（本地缓存 + 变更订阅）</li>
 *   <li>{@link com.minirpc.registry.zk.ZookeeperRegistry} —— 基于 Curator 的 ZooKeeper 实现</li>
 * </ul>
 * <p>
 * 设计要点：消费者通过本地缓存服务实例列表，注册中心宕机不影响已有调用。
 *
 * @see com.minirpc.registry.Registry
 * @see com.minirpc.registry.ServiceDiscovery
 */
package com.minirpc.registry;
