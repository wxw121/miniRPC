package com.minirpc.core.filter;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcContext;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * 链路追踪 Filter
 * <p>
 * 在 RPC 调用链路中透传 TraceId：
 * 1. 如果当前线程 MDC 中已有 TraceId，则复用（说明是上游调用）
 * 2. 如果没有，则生成新的 TraceId（说明是调用链起点）
 * 3. 将 TraceId 设置到请求中，传递给下游
 */
public class TraceFilter implements RpcFilter {

    private static final Logger log = LoggerFactory.getLogger(TraceFilter.class);

    public static final String TRACE_ID_KEY = "traceId";

    @Override
    public CompletableFuture<RpcResponse> filterAsync(RpcRequest request,
            Function<RpcRequest, CompletableFuture<RpcResponse>> invoker) {
        RpcContext context = request.getContext();
        if (context == null) {
            context = new RpcContext();
            request.setContext(context);
        }
        String previous = MDC.get(TRACE_ID_KEY);
        String trace = context.getTraceId();
        if (trace == null || trace.isEmpty()) trace = previous;
        if (trace == null || trace.isEmpty()) trace = generateTraceId();
        context.setTraceId(trace);
        MDC.put(TRACE_ID_KEY, trace);
        try {
            return invoker.apply(request);
        } finally {
            // Restore this caller's MDC now, not later on a response thread.
            if (previous == null) MDC.remove(TRACE_ID_KEY);
            else MDC.put(TRACE_ID_KEY, previous);
        }
    }

    @Override
    public RpcResponse filter(RpcRequest request, FilterInvoker invoker) {
        // 获取或生成 TraceId
        RpcContext context = request.getContext();
        String traceId = context == null ? null : context.getTraceId();
        if (traceId == null || traceId.isEmpty()) {
            traceId = MDC.get(TRACE_ID_KEY);
        }
        boolean isNewTrace = false;

        if (traceId == null || traceId.isEmpty()) {
            traceId = generateTraceId();
            MDC.put(TRACE_ID_KEY, traceId);
            isNewTrace = true;
        }

        if (context == null) {
            context = new RpcContext();
            request.setContext(context);
        }
        context.setTraceId(traceId);

        try {
            log.debug("[Trace:{}] RPC call: {}.{}",
                    traceId, request.getInterfaceName(), request.getMethodName());
            return invoker.invoke(request);
        } finally {
            // 如果是新生成的 TraceId，调用结束后清理
            if (isNewTrace) {
                MDC.remove(TRACE_ID_KEY);
            }
        }
    }

    @Override
    public int getOrder() {
        return -100; // 最高优先级，确保 TraceId 最先设置
    }

    private String generateTraceId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
