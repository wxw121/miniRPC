package com.minirpc.core.cluster;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Failback 容错策略（延迟恢复）
 * <p>
 * 调用失败时记录请求，后台定时重试。
 * 适用于对实时性要求不高、最终一致性即可的场景（如消息通知、日志记录）。
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

    public FailbackClusterStrategy() {
        this(5, 3);
    }

    public FailbackClusterStrategy(long retryDelaySeconds, int maxRetries) {
        this.retryDelaySeconds = retryDelaySeconds;
        this.maxRetries = maxRetries;
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "failback-retry");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public RpcResponse invoke(RpcRequest request, Supplier<RpcResponse> invoker) {
        try {
            return invoker.get();
        } catch (Exception e) {
            log.warn("Failback: call failed, scheduling retry for request: {}", request.getRequestId());
            // 记录失败请求，后台延迟重试
            scheduleRetry(request, invoker, 1);
            // 返回一个失败响应（不抛异常，避免影响主流程）
            return RpcResponse.fail(request.getRequestId(),
                    "Failback: call failed, retry scheduled. Error: " + e.getMessage());
        }
    }

    private void scheduleRetry(RpcRequest request, Supplier<RpcResponse> invoker, int attempt) {
        if (attempt > maxRetries) {
            log.error("Failback: max retries ({}) exceeded for request: {}", maxRetries, request.getRequestId());
            return;
        }

        retryScheduler.schedule(() -> {
            try {
                log.info("Failback: retrying request: {} (attempt {}/{})",
                        request.getRequestId(), attempt, maxRetries);
                RpcResponse response = invoker.get();
                if (response.getStatus() == RpcResponse.STATUS_SUCCESS) {
                    log.info("Failback: retry succeeded for request: {}", request.getRequestId());
                } else {
                    scheduleRetry(request, invoker, attempt + 1);
                }
            } catch (Exception e) {
                log.warn("Failback: retry failed for request: {}, attempt: {}",
                        request.getRequestId(), attempt);
                scheduleRetry(request, invoker, attempt + 1);
            }
        }, retryDelaySeconds, TimeUnit.SECONDS);
    }

    /**
     * 关闭重试调度器，释放线程资源
     */
    public void shutdown() {
        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("FailbackClusterStrategy shutdown");
    }
}
