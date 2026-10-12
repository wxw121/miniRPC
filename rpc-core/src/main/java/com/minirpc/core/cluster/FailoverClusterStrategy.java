package com.minirpc.core.cluster;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Failover 容错策略（失败重试）
 * <p>
 * 调用失败时自动重试其他节点，使用指数退避避免重试风暴。
 * 适用于读操作或幂等写操作。
 */
public class FailoverClusterStrategy implements ClusterStrategy {

    private static final Logger log = LoggerFactory.getLogger(FailoverClusterStrategy.class);

    /**
     * 最大重试次数
     */
    private final int maxRetries;

    /**
     * 基础延迟（毫秒）
     */
    private final RetryPolicy retryPolicy;

    private final long baseDelayMs;

    public FailoverClusterStrategy() {
        this(2, 100, 1_000);
    }

    public FailoverClusterStrategy(int maxRetries, long baseDelayMs) {
        this(maxRetries, baseDelayMs, Math.max(baseDelayMs, 1_000));
    }

    public FailoverClusterStrategy(int maxRetries, long baseDelayMs, long maxBackoffMs) {
        this.maxRetries = maxRetries;
        this.baseDelayMs = baseDelayMs;
        this.retryPolicy = new RetryPolicy(maxRetries, baseDelayMs, maxBackoffMs);
    }

    @Override
    public RpcResponse invoke(RpcRequest request, Supplier<RpcResponse> invoker) {
        int attempt = 0;
        while (true) {
            if (attempt > 0 && retryPolicy.isDeadlineExceeded(request, System.currentTimeMillis())) {
                throw deadlineExceeded(request);
            }

            RpcResponse response = null;
            Exception failure = null;
            try {
                response = invoker.get();
                if (response.getStatus() == RpcResponse.STATUS_SUCCESS) {
                    return response;
                }
            } catch (Exception e) {
                failure = e;
                log.warn("RPC call failed (attempt {}/{}): {}",
                        attempt + 1, maxRetries + 1, e.getMessage());
            }

            RpcErrorCode errorCode = failure == null
                    ? response.getErrorCode()
                    : errorCodeOf(failure);
            int retryIndex = attempt + 1;
            OptionalLong delay = retryPolicy.nextDelayMillis(
                    request, retryIndex, errorCode, System.currentTimeMillis());
            if (delay.isEmpty()) {
                if (failure == null) {
                    return response;
                }
                throw terminalFailure(request, failure, errorCode);
            }

            sleepBeforeRetry(request, retryIndex, delay.getAsLong());
            attempt++;
        }
    }

    private void sleepBeforeRetry(RpcRequest request, int retryIndex, long delayMillis) {
        log.warn("Retry #{} for request: {}, delay: {}ms",
                retryIndex, request.getRequestId(), delayMillis);
        try {
            if (delayMillis > 0) {
                Thread.sleep(delayMillis);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException(RpcErrorCode.INTERNAL, "Retry interrupted", e);
        }
        if (retryPolicy.isDeadlineExceeded(request, System.currentTimeMillis())) {
            throw deadlineExceeded(request);
        }
    }

    private RpcErrorCode errorCodeOf(Exception failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof RpcException rpcException) {
                return rpcException.getErrorCode();
            }
            current = current.getCause();
        }
        return RpcErrorCode.UNAVAILABLE;
    }

    private RpcException terminalFailure(RpcRequest request, Exception failure, RpcErrorCode errorCode) {
        if (failure instanceof RpcException rpcException) {
            return rpcException;
        }
        return new RpcException(errorCode,
                "Failover attempts exhausted for request: " + request.getRequestId(), failure);
    }

    private RpcException deadlineExceeded(RpcRequest request) {
        return new RpcException(RpcErrorCode.DEADLINE_EXCEEDED,
                "Request deadline exceeded before retry: " + request.getRequestId());
    }
}
