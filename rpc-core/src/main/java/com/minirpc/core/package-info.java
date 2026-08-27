/**
 * rpc-core —— MiniRPC 核心层模块。
 * <p>
 * 本模块封装 RPC 框架的核心运行时逻辑，包括网络通信、动态代理、服务治理与容错。
 * <p>
 * 核心入口：
 * <ul>
 *   <li>{@link com.minirpc.core.RpcServer} —— 服务端门面：服务注册、启动、优雅停机</li>
 *   <li>{@link com.minirpc.core.RpcClient} —— 客户端门面：代理创建、同步/异步/泛化调用</li>
 * </ul>
 * <p>
 * 主要子包：
 * <ul>
 *   <li>{@link com.minirpc.core.server} —— Netty 服务端、请求处理、优雅停机管理</li>
 *   <li>{@link com.minirpc.core.client} —— Netty 客户端、连接池、请求-响应匹配</li>
 *   <li>{@link com.minirpc.core.codec} —— 协议编解码器</li>
 *   <li>{@link com.minirpc.core.proxy} —— JDK 动态代理</li>
 *   <li>{@link com.minirpc.core.loadbalance} —— 负载均衡策略</li>
 *   <li>{@link com.minirpc.core.cluster} —— 容错策略、熔断器、限流器</li>
 *   <li>{@link com.minirpc.core.filter} —— Filter 责任链</li>
 *   <li>{@link com.minirpc.core.registry} —— 本地服务注册表</li>
 * </ul>
 *
 * @see com.minirpc.core.RpcServer
 * @see com.minirpc.core.RpcClient
 */
package com.minirpc.core;
