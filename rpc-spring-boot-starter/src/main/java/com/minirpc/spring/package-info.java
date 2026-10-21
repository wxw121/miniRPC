/**
 * rpc-spring-boot-starter —— MiniRPC Spring Boot 自动装配模块。
 * <p>
 * 本模块提供 Spring Boot 集成能力，实现 RPC 服务的零配置启动与依赖注入。
 * <p>
 * 核心组件：
 * <ul>
 *   <li>{@link com.minirpc.spring.MiniRpcAutoConfiguration} —— 自动装配入口</li>
 *   <li>{@link com.minirpc.spring.MiniRpcProperties} —— 配置属性（前缀 minirpc.*）</li>
 *   <li>{@link com.minirpc.spring.RpcReferenceBeanPostProcessor} —— 扫描 @RpcReference 注入代理</li>
 *   <li>{@link com.minirpc.core.annotation.RpcReference} —— core 提供的统一服务引用注解</li>
 * </ul>
 * <p>
 * 使用方式：引入本模块依赖后，通过 {@code minirpc.*} 配置项即可自动装配
 * RpcServer / RpcClient / Registry，并用 {@code @RpcReference} 注入远程服务代理。
 *
 * @see com.minirpc.spring.MiniRpcAutoConfiguration
 */
package com.minirpc.spring;
