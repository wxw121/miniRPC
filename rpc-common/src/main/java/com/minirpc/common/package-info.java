/**
 * rpc-common —— MiniRPC 基础层模块。
 * <p>
 * 本模块提供整个 RPC 框架的基础设施，被所有其他模块依赖，自身不依赖任何业务模块。
 * <p>
 * 主要子包：
 * <ul>
 *   <li>{@link com.minirpc.common.protocol} —— 协议定义：消息封装、请求/响应、消息类型、序列化类型</li>
 *   <li>{@link com.minirpc.common.serialize} —— 序列化 SPI：JDK / Hessian2 / Protostuff 实现</li>
 *   <li>{@link com.minirpc.common.spi} —— SPI 扩展加载器</li>
 *   <li>{@link com.minirpc.common.generic} —— 泛化调用接口定义</li>
 *   <li>{@link com.minirpc.common.exception} —— 统一异常类型</li>
 *   <li>{@link com.minirpc.common.util} —— 通用工具类</li>
 * </ul>
 *
 * @see com.minirpc.common.protocol.RpcMessage
 * @see com.minirpc.common.serialize.Serializer
 */
package com.minirpc.common;
