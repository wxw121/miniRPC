package com.minirpc.core.cluster;

import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;

import java.util.EnumSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/** Admits only retry attempts that can complete before their request deadline. */
public final class RetryPolicy {

    private final int maxRetries;
    private final long baseBackoffMs;
    private final long maxBackoffMs;
    private final Set<RpcErrorCode> retryableCodes = EnumSet.of(
            RpcErrorCode.UNAVAILABLE, RpcErrorCode.OVERLOADED);

    public RetryPolicy(int maxRetries, long baseBackoffMs, long maxBackoffMs) {
        if (maxRetries < 0 || baseBackoffMs < 0 || maxBackoffMs < baseBackoffMs) {
            throw new IllegalArgumentException("invalid retry policy configuration");
        }
        this.maxRetries = maxRetries;
        this.baseBackoffMs = baseBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
    }

    public OptionalLong nextDelayMillis(RpcRequest request, int retryIndex,
                                        RpcErrorCode errorCode, long nowMillis) {
        if (retryIndex < 1 || !retryableCodes.contains(errorCode) || retryIndex > maxRetries) {
            return OptionalLong.empty();
        }
        long cap = Math.min(maxBackoffMs, baseBackoffMs << Math.min(retryIndex - 1, 20));
        if (!deadlineAllows(request, nowMillis, cap)) {
            return OptionalLong.empty();
        }
        long delay = ThreadLocalRandom.current().nextLong(cap + 1);
        return deadlineAllows(request, nowMillis, delay) ? OptionalLong.of(delay) : OptionalLong.empty();
    }

    public boolean isDeadlineExceeded(RpcRequest request, long nowMillis) {
        return request.getContext() != null && request.getContext().isExpired(nowMillis);
    }

    private boolean deadlineAllows(RpcRequest request, long nowMillis, long delayMillis) {
        return request.getContext() == null
                || request.getContext().getDeadlineEpochMillis() <= 0
                || nowMillis + delayMillis < request.getContext().getDeadlineEpochMillis();
    }
}
