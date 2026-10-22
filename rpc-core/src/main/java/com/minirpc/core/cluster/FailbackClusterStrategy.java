package com.minirpc.core.cluster;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import com.minirpc.core.client.FutureSupport;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Failback 容错策略（延迟恢复）
 * <p>
 * 首次调用的失败响应或异常原样交给调用方，仅对可恢复失败进行后台定时重试。
 * 适用于幂等调用；后台重试共享请求原有的绝对 deadline，预算不足时不安排重试。
 */
public class FailbackClusterStrategy implements ClusterStrategy {

    private static final Logger log = LoggerFactory.getLogger(FailbackClusterStrategy.class);

    /**
     * 后台重试调度器
     */
    private final ScheduledExecutorService retryScheduler;

    /**
     * 重试延迟（秒）
     */
    private final long retryDelaySeconds;

    /**
     * 最大重试次数
     */
    private final int maxRetries;

    private final RetryPolicy retryPolicy;

    private final LongSupplier currentTimeMillis;

    private final AtomicBoolean closed = new AtomicBoolean();

    public FailbackClusterStrategy() {
        this(1, 3);
    }

    public FailbackClusterStrategy(long retryDelaySeconds, int maxRetries) {
        this(retryDelaySeconds, maxRetries, createRetryScheduler(retryDelaySeconds, maxRetries),
                System::currentTimeMillis);
    }

    FailbackClusterStrategy(long retryDelaySeconds, int maxRetries,
                            ScheduledExecutorService retryScheduler, LongSupplier currentTimeMillis) {
        validateConfiguration(retryDelaySeconds, maxRetries);
        this.retryDelaySeconds = retryDelaySeconds;
        this.maxRetries = maxRetries;
        this.retryScheduler = Objects.requireNonNull(retryScheduler);
        this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis);
        // 复用错误分类、次数和 deadline 判断；Failback 自己使用固定延迟，不使用随机退避。
        this.retryPolicy = new RetryPolicy(maxRetries, 0, 0);
    }

    @Override
    public RpcResponse invoke(RpcRequest request, Supplier<RpcResponse> invoker) {
        if (closed.get()) {
            throw new IllegalStateException("Failback strategy is closed");
        }
        RpcResponse response;
        try {
            response = invoker.get();
        } catch (RuntimeException failure) {
            scheduleRetry(request, invoker, 1, errorCodeOf(failure));
            throw failure;
        }
        if (response != null && response.getStatus() != RpcResponse.STATUS_SUCCESS) {
            scheduleRetry(request, invoker, 1, response.getErrorCode());
        }
        return response;
    }

    @Override
    public CompletableFuture<RpcResponse> invokeAsync(
            RpcRequest request, Supplier<CompletableFuture<RpcResponse>> invoker) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Failback strategy is closed"));
        }
        CompletableFuture<RpcResponse> first;
        try {
            first = invoker.get();
        } catch (Exception failure) {
            first = CompletableFuture.failedFuture(failure);
        }
        return FutureSupport.handle(first, (response, failure) -> {
            if (!(failure instanceof CancellationException)
                    && (failure != null || response.getStatus() != RpcResponse.STATUS_SUCCESS)) {
                scheduleAsyncRetry(request, invoker, 1,
                        failure == null ? response.getErrorCode() : errorCodeOf(failure));
            }
            if (failure != null) throw FutureSupport.propagate(failure);
            return response;
        });
    }

    private void scheduleAsyncRetry(RpcRequest request, Supplier<CompletableFuture<RpcResponse>> invoker,
                                    int attempt, RpcErrorCode code) {
        long now = currentTimeMillis.getAsLong();
        long deadline = request.getContext() == null ? 0 : request.getContext().getDeadlineEpochMillis();
        if (closed.get() || retryPolicy.nextDelayMillis(request, attempt, code, now).isEmpty()
                || (deadline > 0 && TimeUnit.SECONDS.toMillis(retryDelaySeconds) >= deadline - now)) {
            return;
        }
        try {
            retryScheduler.schedule(() -> {
                if (closed.get() || retryPolicy.isDeadlineExceeded(request, currentTimeMillis.getAsLong())) return;
                CompletableFuture<RpcResponse> next;
                try {
                    next = invoker.get();
                } catch (Exception failure) {
                    next = CompletableFuture.failedFuture(failure);
                }
                next.whenComplete((response, error) -> {
                    Throwable failure = error == null ? null : FutureSupport.unwrap(error);
                    if (!(failure instanceof CancellationException)
                            && (failure != null || response.getStatus() != RpcResponse.STATUS_SUCCESS)) {
                        scheduleAsyncRetry(request, invoker, attempt + 1,
                                failure == null ? response.getErrorCode() : errorCodeOf(failure));
                    }
                });
            }, retryDelaySeconds, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ignored) {
            // Closing the scheduler must not replace the original RPC failure.
        }
    }

    private void scheduleRetry(RpcRequest request, Supplier<RpcResponse> invoker,
                               int attempt, RpcErrorCode errorCode) {
        long now = currentTimeMillis.getAsLong();
        if (closed.get() || retryPolicy.nextDelayMillis(request, attempt, errorCode, now).isEmpty()) {
            return;
        }
        long deadline = request.getContext() == null ? 0 : request.getContext().getDeadlineEpochMillis();
        // 用剩余预算比较，避免把很大的秒数转换后再加到当前时间造成溢出。
        if (deadline > 0 && TimeUnit.SECONDS.toMillis(retryDelaySeconds) >= deadline - now) {
            return;
        }
        try {
            retryScheduler.schedule(() -> retry(request, invoker, attempt), retryDelaySeconds, TimeUnit.SECONDS);
        } catch (RejectedExecutionException failure) {
            // 关闭可能与入队同时发生；不能用调度失败覆盖首次 RPC 的真实结果。
            log.debug("Failback: retry scheduler stopped for request: {}", request.getRequestId());
        }
    }

    private void retry(RpcRequest request, Supplier<RpcResponse> invoker, int attempt) {
        if (closed.get() || retryPolicy.isDeadlineExceeded(request, currentTimeMillis.getAsLong())) {
            return;
        }
        log.info("Failback: retrying request: {} (attempt {}/{})", request.getRequestId(), attempt, maxRetries);
        try {
            RpcResponse response = invoker.get();
            if (response != null && response.getStatus() != RpcResponse.STATUS_SUCCESS) {
                scheduleRetry(request, invoker, attempt + 1, response.getErrorCode());
            }
        } catch (RuntimeException failure) {
            scheduleRetry(request, invoker, attempt + 1, errorCodeOf(failure));
        }
    }

    private RpcErrorCode errorCodeOf(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof RpcException rpcException) {
                return rpcException.getErrorCode();
            }
        }
        return RpcErrorCode.UNAVAILABLE;
    }

    private static ScheduledExecutorService createRetryScheduler(long retryDelaySeconds, int maxRetries) {
        validateConfiguration(retryDelaySeconds, maxRetries);
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "failback-retry");
            thread.setDaemon(true);
            return thread;
        });
    }

    private static void validateConfiguration(long retryDelaySeconds, int maxRetries) {
        if (retryDelaySeconds < 0 || maxRetries < 0) {
            throw new IllegalArgumentException("Failback retry delay and retry count must not be negative");
        }
    }

    /**
     * 关闭重试调度器，释放线程资源
     */
    public void shutdown() {
        if (closed.compareAndSet(false, true)) {
            for (Runnable pending : retryScheduler.shutdownNow()) {
                if (pending instanceof Future<?> future) {
                    future.cancel(false);
                }
            }
            log.info("FailbackClusterStrategy shutdown");
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
