package com.minirpc.core.filter;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Filter 责任链
 * <p>
 * 将所有注册的 Filter 按优先级排序，组成调用链。
 * 请求依次经过每个 Filter，最终到达实际的 RPC 调用。
 * <p>
 * 调用链路：Filter1 -> Filter2 -> ... -> FilterN -> 实际RPC调用
 */
public class FilterChain {

    private final List<RpcFilter> filters = new ArrayList<>();

    /**
     * 添加 Filter
     */
    public void addFilter(RpcFilter filter) {
        filters.add(filter);
        filters.sort(Comparator.comparingInt(RpcFilter::getOrder));
    }

    /**
     * 执行 Filter 链
     *
     * @param request       RPC 请求
     * @param finalInvoker  最终的调用逻辑（实际 RPC 调用）
     * @return RPC 响应
     */
    public RpcResponse execute(RpcRequest request, FilterInvoker finalInvoker) {
        if (filters.isEmpty()) {
            return finalInvoker.invoke(request);
        }

        // 构建责任链：从最后一个 Filter 开始包装
        FilterInvoker chain = finalInvoker;
        for (int i = filters.size() - 1; i >= 0; i--) {
            final RpcFilter filter = filters.get(i);
            final FilterInvoker next = chain;
            chain = req -> filter.filter(req, next);
        }

        return chain.invoke(request);
    }

    public int getFilterCount() {
        return filters.size();
    }

    public CompletableFuture<RpcResponse> executeAsync(RpcRequest request,
            Function<RpcRequest, CompletableFuture<RpcResponse>> finalInvoker) {
        Function<RpcRequest, CompletableFuture<RpcResponse>> chain = finalInvoker;
        for (int i = filters.size() - 1; i >= 0; i--) {
            RpcFilter filter = filters.get(i);
            Function<RpcRequest, CompletableFuture<RpcResponse>> next = chain;
            chain = req -> filter.filterAsync(req, next);
        }
        try {
            return chain.apply(request);
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
