package com.minirpc.core.cluster;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Failfast 容错策略（快速失败）
 * <p>
 * 调用失败时立即抛出异常，不进行重试。
 * 适用于非幂等写操作（如新增订单），避免重复执行。
 * <p>
 * 面试考点：
 * - 幂等性概念：同一操作执行多次与执行一次效果相同
 * - 为什么写操作不能用 Failover
 */
public class FailfastClusterStrategy implements ClusterStrategy {

    private static final Logger log = LoggerFactory.getLogger(FailfastClusterStrategy.class);

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
