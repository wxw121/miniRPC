package com.minirpc.core.observability;

import com.minirpc.common.protocol.RpcErrorCode;

import java.util.Map;

/** Immutable outcome of one RPC invocation attempt. */
public record RpcInvocationEvent(
        String side,
        String service,
        RpcErrorCode errorCode,
        long durationNanos,
        int attempt,
        String targetEndpoint,
        String traceId,
        Map<String, String> attributes) {

    public RpcInvocationEvent {
        attributes = Map.copyOf(attributes);
    }

    public RpcInvocationEvent(String side, String service, RpcErrorCode errorCode, long durationNanos,
                              int attempt, String targetEndpoint, Map<String, String> attributes) {
        this(side, service, errorCode, durationNanos, attempt, targetEndpoint, "", attributes);
    }
}
