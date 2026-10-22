package com.minirpc.core.cluster;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;

/**
 * Failfast 容错策略（快速失败）
 * <p>
 * 调用失败时立即抛出异常，不进行重试。
 * 适用于非幂等写操作（如新增订单），避免重复执行。
 */
public class FailfastClusterStrategy implements ClusterStrategy {

    private static final Logger log = LoggerFactory.getLogger(FailfastClusterStrategy.class);

    @Override
    public CompletableFuture<RpcResponse> invokeAsync(
            RpcRequest request, Supplier<CompletableFuture<RpcResponse>> invoker) {
        try {
            return invoker.get();
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public RpcResponse invoke(RpcRequest request, Supplier<RpcResponse> invoker) {
        try {
            return invoker.get();
        } catch (Exception e) {
            log.error("Failfast: RPC call failed immediately for request: {}", request.getRequestId());
            throw new RpcException("Failfast: call failed for request: " + request.getRequestId(), e);
        }
    }
}
