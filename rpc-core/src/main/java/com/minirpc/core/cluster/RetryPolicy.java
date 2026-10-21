package com.minirpc.core.cluster;

import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;

import java.util.EnumSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 重试策略：判断是否允许重试，并计算等待时间；实际等待和 RPC 调用由外部执行。
 */
public final class RetryPolicy {

    // 最大重试次数，不包含首次调用。
    private final int maxRetries;
    // 基础退避时间和最大退避时间，单位均为毫秒。
    private final long baseBackoffMs;
    private final long maxBackoffMs;
    // 只对服务不可用、服务过载这两类错误重试。
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

    /**
     * retryIndex 从 1 开始；返回空表示不允许重试，有值表示需要等待的毫秒数，0 表示立即重试。
     */
    public OptionalLong nextDelayMillis(RpcRequest request, int retryIndex,
                                        RpcErrorCode errorCode, long nowMillis) {
        // 重试编号无效、错误码不支持或次数超限时，停止重试。
        if (retryIndex < 1 || !retryableCodes.contains(errorCode) || retryIndex > maxRetries) {
            return OptionalLong.empty();
        }
        // 左移相当于乘以 2 的幂：上限按基础值、2 倍、4 倍增长，指数最多为 20，并受最大退避时间限制。
        long cap = Math.min(maxBackoffMs, baseBackoffMs << Math.min(retryIndex - 1, 20));
        // 保守检查：即使等满本轮上限，也必须早于请求的截止时间。
        if (!deadlineAllows(request, nowMillis, cap)) {
            return OptionalLong.empty();
        }
        // 从 [0, cap] 随机选择等待时间，分散并发请求的重试时机，避免同时重试。
        long delay = ThreadLocalRandom.current().nextLong(cap + 1);
        // 按实际等待时间再次检查，返回本轮允许的等待时间。
        return deadlineAllows(request, nowMillis, delay) ? OptionalLong.of(delay) : OptionalLong.empty();
    }

    /** 判断请求是否已到达截止时间。 */
    public boolean isDeadlineExceeded(RpcRequest request, long nowMillis) {
        return request.getContext() != null && request.getContext().isExpired(nowMillis);
    }

    private boolean deadlineAllows(RpcRequest request, long nowMillis, long delayMillis) {
        // 没有设置截止时间则不限制；否则等待结束时必须尚未过期，不保证后续 RPC 一定能在截止前完成。
        return request.getContext() == null
                || request.getContext().getDeadlineEpochMillis() <= 0
                || nowMillis + delayMillis < request.getContext().getDeadlineEpochMillis();
    }
}
