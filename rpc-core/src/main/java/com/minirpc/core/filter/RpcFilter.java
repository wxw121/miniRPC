package com.minirpc.core.filter;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;

/**
 * RPC Filter SPI 接口（责任链模式）
 * <p>
 * 类似 Dubbo 的 Filter 机制，支持在 RPC 调用前后进行拦截和增强。
 * 典型用途：日志记录、链路追踪、权限校验、调用统计、限流等。
 */
public interface RpcFilter {

    /**
     * 过滤器处理逻辑
     *
     * @param request RPC 请求
     * @param invoker 调用链中的下一个节点
     * @return RPC 响应
     */
    RpcResponse filter(RpcRequest request, FilterInvoker invoker);

    /**
     * 过滤器优先级（数值越小优先级越高）
     */
    default int getOrder() {
        return 0;
    }
}
