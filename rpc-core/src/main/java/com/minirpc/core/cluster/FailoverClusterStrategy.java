package com.minirpc.core.cluster;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import com.minirpc.core.client.FutureSupport;

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
    public CompletableFuture<RpcResponse> invokeAsync(
            RpcRequest request, Supplier<CompletableFuture<RpcResponse>> invoker) {
        CompletableFuture<RpcResponse> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<RpcResponse>> active = new AtomicReference<>();
        result.whenComplete((response, failure) -> {
            if (result.isCancelled()) {
                CompletableFuture<RpcResponse> attempt = active.get();
                if (attempt != null) attempt.cancel(false);
            }
        });
        class RetryLoop {
            void attempt(int index) {
                if (result.isDone()) return;
                if (retryPolicy.isDeadlineExceeded(request, System.currentTimeMillis())) {
                    result.completeExceptionally(deadlineExceeded(request));
                    return;
                }
                CompletableFuture<RpcResponse> current;
                try {
                    current = invoker.get();
                } catch (Exception failure) {
                    current = CompletableFuture.failedFuture(failure);
                }
                active.set(current);
                if (result.isCancelled()) current.cancel(false);
                current.whenComplete((response, error) -> {
                    if (result.isDone()) return;
                    Throwable failure = error == null ? null : FutureSupport.unwrap(error);
                    if (failure instanceof CancellationException) {
                        result.cancel(false);
                        return;
                    }
                    if (failure == null && response.getStatus() == RpcResponse.STATUS_SUCCESS) {
                        result.complete(response);
                        return;
                    }
                    RpcErrorCode code = failure == null ? response.getErrorCode() : errorCodeOf(failure);
                    OptionalLong delay = retryPolicy.nextDelayMillis(
                            request, index + 1, code, System.currentTimeMillis());
                    if (delay.isEmpty()) {
                        if (failure == null) result.complete(response);
                        else result.completeExceptionally(failure);
                        return;
                    }
                    // Even zero-delay retries leave the response thread before discovery/connection work.
                    CompletableFuture.delayedExecutor(delay.getAsLong(), TimeUnit.MILLISECONDS)
                            .execute(() -> attempt(index + 1));
                });
            }
        }
        new RetryLoop().attempt(0);
        return result;
    }

    /**
     * 在当前调用线程中执行 RPC，失败后按重试策略再次尝试，直到成功或不再允许重试。
     * 调用方需要等待这些尝试结束，才能获得最终响应或异常。
     * 1. 调用 invoker.get() 发起一次实际调用，成功响应立即返回。
     * 2. 将失败分为“返回失败响应”和“直接抛出异常”两种情况，提取错误码供重试策略判断。
     * 3. 根据错误码、重试次数和剩余 deadline 预算，决定是否重试及等待多久。
     * 4. 允许重试则在当前线程中等待退避时间，再进入下一轮；不允许则返回失败响应或抛出异常。
     *
     * @param request 本次调用的请求，用于读取请求标识和 deadline 等信息
     * @param invoker 一次实际 RPC 调用的执行逻辑，每次重试都会重新调用
     * @return 成功响应，或停止重试时最后一次调用返回的失败响应
     * @throws RpcException 异常终止重试、重试等待被中断或重试前 deadline 已过期时抛出
     */
    @Override
    public RpcResponse invoke(RpcRequest request, Supplier<RpcResponse> invoker) {
        // attempt 为 0 表示首次调用，为 1 表示第 1 次重试，以此类推。
        int attempt = 0;
        while (true) {
            // 后续重试开始前再次检查 deadline，避免等待或线程调度耗尽预算后仍发起重试。
            // 首次调用不在此处检查，项目中的发送逻辑还会检查剩余时间。
            if (attempt > 0 && retryPolicy.isDeadlineExceeded(request, System.currentTimeMillis())) {
                throw deadlineExceeded(request);
            }

            // response 保存正常返回的响应，failure 保存抛出的异常，供下面统一判断是否重试。
            RpcResponse response = null;
            Exception failure = null;
            try {
                response = invoker.get();
                // 收到响应仍需检查状态；只有明确成功才直接结束，失败响应也可能需要重试。
                if (response.getStatus() == RpcResponse.STATUS_SUCCESS) {
                    return response;
                }
            } catch (Exception e) {
                // 暂存异常而不立即抛出，让重试策略判断这类失败是否还有恢复机会。
                failure = e;
                log.warn("RPC call failed (attempt {}/{}): {}",
                        attempt + 1, maxRetries + 1, e.getMessage());
            }

            // 失败响应直接读取错误码；异常则沿原因链查找 RpcException，找不到时按 UNAVAILABLE 处理。
            RpcErrorCode errorCode = failure == null
                    ? response.getErrorCode()
                    : errorCodeOf(failure);
            // 即将申请的是下一次重试，编号从 1 开始，不包含首次调用。
            int retryIndex = attempt + 1;
            OptionalLong delay = retryPolicy.nextDelayMillis(
                    request, retryIndex, errorCode, System.currentTimeMillis());
            // empty 表示不允许重试；有值且为 0 则表示允许立即重试，二者含义不同。
            if (delay.isEmpty()) {
                if (failure == null) {
                    // 保留服务端的失败响应，交给上层处理；RpcClientProxy 会将其转为调用异常。
                    return response;
                }
                // 抛异常的调用最终仍以异常结束，避免将“未获得响应”伪装成正常返回。
                throw terminalFailure(request, failure, errorCode);
            }

            // 同步等待退避时间，期间占用当前调用线程；等待后还会检查 deadline，中断则终止重试。
            sleepBeforeRetry(request, retryIndex, delay.getAsLong());
            // 下一轮会再次执行 invoker.get()，而不是重复使用上一轮的失败结果。
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

    private RpcErrorCode errorCodeOf(Throwable failure) {
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
