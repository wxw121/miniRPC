package com.minirpc.core.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 优雅停机管理器
 * <p>
 * 核心职责：
 * 1. 维护 shuttingDown 标志：标记后拒绝新请求
 * 2. 维护 inFlightRequests 计数器：追踪正在处理中的请求数
 * 3. 提供 awaitInFlight() 方法：等待所有 in-flight 请求完成（或超时）
 * <p>
 * 优雅停机完整流程（由 RpcServer.stop() 编排）：
 * <pre>
 * ① markShuttingDown()       → 新请求立即拒绝，返回 "server is shutting down"
 * ② unregister from registry → 消费者不再发现本节点
 * ③ sleep(2s)                → 等待消费者刷新本地缓存（消费者缓存 TTL 通常 1-5s）
 * ④ awaitInFlight(timeout)   → 等待已接收的请求处理完毕
 * ⑤ close Netty + executor   → 关闭连接和线程池
 * </pre>
 * <p>
 * 面试考点：
 * - 为什么摘流量后还要等？（消费者有本地缓存，不会立即感知节点下线）
 * - 为什么要拒绝新请求？（避免关闭过程中接收的请求丢失）
 * - in-flight 计数器用 AtomicInteger 保证并发安全
 * - CountDownLatch 用于高效等待（比轮询 sleep 更优雅）
 */
public class GracefulShutdownManager {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownManager.class);

    /**
     * 停机标志：一旦设为 true，新请求将被拒绝
     */
    private volatile boolean shuttingDown = false;

    /**
     * 正在处理中的请求数
     */
    private final AtomicInteger inFlightRequests = new AtomicInteger(0);

    /**
     * 标记开始停机，拒绝新请求
     */
    public void markShuttingDown() {
        this.shuttingDown = true;
        log.info("Server marked as shutting down, new requests will be rejected");
    }

    /**
     * 是否正在停机
     */
    public boolean isShuttingDown() {
        return shuttingDown;
    }

    /**
     * 请求开始处理时调用（+1）
     *
     * @return true 表示成功注册（未停机），false 表示应拒绝该请求
     */
    public boolean acquireRequest() {
        if (shuttingDown) {
            return false;
        }
        inFlightRequests.incrementAndGet();
        // Double-check：防止在 increment 和 shuttingDown 检查之间发生状态变更
        if (shuttingDown) {
            inFlightRequests.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * 请求处理完成时调用（-1）
     */
    public void releaseRequest() {
        int remaining = inFlightRequests.decrementAndGet();
        if (remaining == 0 && shuttingDown) {
            log.info("All in-flight requests completed");
        }
    }

    /**
     * 获取当前 in-flight 请求数
     */
    public int getInFlightCount() {
        return inFlightRequests.get();
    }

    /**
     * 等待所有 in-flight 请求完成
     *
     * @param timeout 最大等待时间
     * @param unit    时间单位
     * @return true 表示所有请求已完成，false 表示超时
     */
    public boolean awaitInFlight(long timeout, TimeUnit unit) {
        int current = inFlightRequests.get();
        if (current == 0) {
            log.info("No in-flight requests, proceeding with shutdown");
            return true;
        }

        log.info("Waiting for {} in-flight requests to complete (timeout: {}ms)...",
                current, unit.toMillis(timeout));

        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        while (inFlightRequests.get() > 0) {
            if (System.currentTimeMillis() >= deadline) {
                log.warn("Timeout waiting for in-flight requests, {} requests still pending",
                        inFlightRequests.get());
                return false;
            }
            try {
                Thread.sleep(50); // 短轮询，50ms 粒度足够
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        log.info("All in-flight requests completed successfully");
        return true;
    }
}
