package com.minirpc.core.cluster;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;

import java.util.function.Supplier;

/**
 * 容错策略 SPI 接口
 * <p>
 * 定义 RPC 调用失败时的容错处理策略。
 * 内置 Failover（失败重试）、Failfast（快速失败）、Failback（延迟恢复）三种策略。
 */
@FunctionalInterface
public interface ClusterStrategy extends AutoCloseable {

    /**
     * 执行 RPC 调用（带容错策略）
     *
     * @param request  RPC 请求
     * @param invoker  实际调用逻辑（Supplier 封装）
     * @return RPC 响应
     */
    RpcResponse invoke(RpcRequest request, Supplier<RpcResponse> invoker);

    /**
     * 释放策略持有的资源。实现必须允许重复关闭，供客户端及容器生命周期共同调用。
     */
    @Override
    default void close() {
    }
}
