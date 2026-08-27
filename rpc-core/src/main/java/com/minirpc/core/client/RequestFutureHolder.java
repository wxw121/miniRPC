package com.minirpc.core.client;

import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 请求-响应匹配持有器
 * <p>
 * 维护 requestId -> CompletableFuture 的映射关系。
 * 客户端发送请求时注册 Future，收到响应时通过 requestId 匹配并完成 Future。
 * <p>
 * 内存泄漏防护：
 * - 定时清理超时的 Future（防止服务端永不响应导致 Future 永远留在 Map 中）
 * - 注册时记录时间戳，超过 TTL 的 Future 会被强制完成并移除
 * <p>
 * 面试考点：
 * - 为什么用 ConcurrentHashMap：多线程环境下保证线程安全
 * - 为什么用 CompletableFuture：支持异步非阻塞等待，可设置超时
 * - 请求-响应匹配原理：通过唯一 requestId 关联请求和响应
 * - 内存泄漏防护：定时清理机制
 */
public class RequestFutureHolder {

    private static final Logger log = LoggerFactory.getLogger(RequestFutureHolder.class);

    /**
     * Future 超时时间（毫秒）：超过此时间未完成的 Future 会被清理
     */
    private static final long DEFAULT_FUTURE_TTL_MS = 30_000;

    /**
     * 清理任务执行间隔（秒）
     */
    private static final long CLEANUP_INTERVAL_SECONDS = 10;

    /**
     * requestId -> CompletableFuture 映射
     */
    private static final Map<String, CompletableFuture<RpcResponse>> FUTURE_MAP = new ConcurrentHashMap<>();

    /**
     * requestId -> 注册时间戳（用于超时清理）
     */
    private static final Map<String, Long> TIMESTAMP_MAP = new ConcurrentHashMap<>();

    /**
     * 定时清理线程：防止内存泄漏
     */
    private static final ScheduledExecutorService CLEANUP_EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "rpc-future-cleanup");
                t.setDaemon(true);
                return t;
            });

    static {
        // 启动定时清理任务
        CLEANUP_EXECUTOR.scheduleAtFixedRate(
                RequestFutureHolder::cleanupExpiredFutures,
                CLEANUP_INTERVAL_SECONDS,
                CLEANUP_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    /**
     * 注册一个请求的 Future
     */
    public static CompletableFuture<RpcResponse> register(String requestId) {
        return register(requestId, DEFAULT_FUTURE_TTL_MS);
    }

    public static CompletableFuture<RpcResponse> register(String requestId, long timeoutMs) {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        FUTURE_MAP.put(requestId, future);
        TIMESTAMP_MAP.put(requestId, System.currentTimeMillis() + Math.max(1, timeoutMs));
        return future;
    }

    /**
     * 完成一个请求的 Future（收到响应时调用）
     */
    public static void complete(String requestId, RpcResponse response) {
        CompletableFuture<RpcResponse> future = FUTURE_MAP.remove(requestId);
        TIMESTAMP_MAP.remove(requestId);
        if (future != null) {
            future.complete(response);
        }
    }

    /**
     * 异常完成一个请求的 Future
     */
    public static void completeExceptionally(String requestId, Throwable throwable) {
        CompletableFuture<RpcResponse> future = FUTURE_MAP.remove(requestId);
        TIMESTAMP_MAP.remove(requestId);
        if (future != null) {
            future.completeExceptionally(throwable);
        }
    }

    /**
     * 移除一个请求的 Future（超时清理等场景）
     */
    public static void remove(String requestId) {
        FUTURE_MAP.remove(requestId);
        TIMESTAMP_MAP.remove(requestId);
    }

    /**
     * 清理超时的 Future，防止内存泄漏
     */
    private static void cleanupExpiredFutures() {
        long now = System.currentTimeMillis();
        int cleaned = 0;

        Iterator<Map.Entry<String, Long>> it = TIMESTAMP_MAP.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (now >= entry.getValue()) {
                String requestId = entry.getKey();
                CompletableFuture<RpcResponse> future = FUTURE_MAP.remove(requestId);
                it.remove();
                if (future != null && !future.isDone()) {
                    future.completeExceptionally(
                            new RuntimeException("Request timeout, cleaned by TTL: " + requestId));
                }
                cleaned++;
            }
        }

        if (cleaned > 0) {
            log.warn("Cleaned {} expired futures", cleaned);
        }
    }
}
