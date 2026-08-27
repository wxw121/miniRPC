package com.minirpc.core.filter;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;

/**
 * Filter 调用器接口
 * <p>
 * 代表调用链中的下一个节点，可以是下一个 Filter，也可以是最终的 RPC 调用。
 */
@FunctionalInterface
public interface FilterInvoker {

    /**
     * 执行调用
     */
    RpcResponse invoke(RpcRequest request);
}
