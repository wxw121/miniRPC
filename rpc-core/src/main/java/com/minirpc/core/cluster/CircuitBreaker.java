package com.minirpc.core.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 熔断器（Circuit Breaker）
 * <p>
 * 三种状态：
 * - CLOSED（关闭）：正常状态，请求正常通过。失败次数达到阈值时切换到 OPEN。
 * - OPEN（打开）：熔断状态，所有请求直接失败（快速失败）。经过超时时间后切换到 HALF_OPEN。
 * - HALF_OPEN（半开）：试探状态，允许少量请求通过。成功则恢复 CLOSED，失败则回到 OPEN。
 */
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State {
        CLOSED,     // 正常
        OPEN,       // 熔断
        HALF_OPEN   // 半开（试探）
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);

    /**
     * 失败计数器
     */
    private final AtomicInteger failureCount = new AtomicInteger(0);

    /**
     * 成功计数器（HALF_OPEN 状态使用）
     */
    private final AtomicInteger successCount = new AtomicInteger(0);

    /**
     * HALF_OPEN 状态下已放行的请求数（CAS 扣减保证不超额）
     */
    private final AtomicInteger halfOpenPermits = new AtomicInteger(0);

    /**
     * 上次状态切换时间
     */
    private final AtomicLong lastStateChangeTime = new AtomicLong(System.currentTimeMillis());

    /**
     * 失败阈值：连续失败多少次后触发熔断
     */
    private final int failureThreshold;

    /**
     * 熔断超时时间（毫秒）：OPEN 状态持续多久后进入 HALF_OPEN
     */
    private final long openTimeoutMs;

    /**
     * HALF_OPEN 状态下允许通过的请求数
     */
    private final int halfOpenMaxRequests;

    public CircuitBreaker() {
        this(5, 10000, 3);
    }

    public CircuitBreaker(int failureThreshold, long openTimeoutMs, int halfOpenMaxRequests) {
        this.failureThreshold = failureThreshold;
        this.openTimeoutMs = openTimeoutMs;
        this.halfOpenMaxRequests = halfOpenMaxRequests;
    }

    /**
     * 判断请求是否允许通过
     */
    public boolean allowRequest() {
        State currentState = state.get();
        switch (currentState) {
            case CLOSED:
                return true;
            case OPEN:
                // 检查是否超时，超时则切换到 HALF_OPEN
                if (System.currentTimeMillis() - lastStateChangeTime.get() > openTimeoutMs) {
                    // CAS 保证只有一个线程能切换状态
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        onStateChange(State.OPEN, State.HALF_OPEN);
                        halfOpenPermits.incrementAndGet();
                        return true;
                    }
                    return allowRequest();
                }
                return false;
            case HALF_OPEN:
                // 半开状态允许有限请求通过（CAS 扣减许可，保证不超额）
                while (true) {
                    int current = halfOpenPermits.get();
                    if (current >= halfOpenMaxRequests) {
                        return false;
                    }
                    if (halfOpenPermits.compareAndSet(current, current + 1)) {
                        return true;
                    }
                }
            default:
                return false;
        }
    }

    /**
     * 记录调用成功
     */
    public void recordSuccess() {
        State currentState = state.get();
        switch (currentState) {
            case HALF_OPEN:
                int success = successCount.incrementAndGet();
                if (success >= halfOpenMaxRequests) {
                    // CAS 保证只有一个线程能切换状态
                    if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                        onStateChange(State.HALF_OPEN, State.CLOSED);
                    }
                }
                break;
            case CLOSED:
                // 注意：每次成功都重置失败计数，意味着只有“连续失败”才触发熔断。
                // 生产级可升级为滑动窗口统计失败率（如 Resilience4j 的 slidingWindow）
                failureCount.set(0);
                break;
            default:
                break;
        }
    }

    /**
     * 记录调用失败
     */
    public void recordFailure() {
        State currentState = state.get();
        switch (currentState) {
            case CLOSED:
                int failures = failureCount.incrementAndGet();
                if (failures >= failureThreshold) {
                    // CAS 保证只有一个线程能切换状态
                    if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                        onStateChange(State.CLOSED, State.OPEN);
                    }
                }
                break;
            case HALF_OPEN:
                // 半开状态下失败，立即回到熔断
                if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                    onStateChange(State.HALF_OPEN, State.OPEN);
                }
                break;
            default:
                break;
        }
    }

    /**
     * 状态切换后的清理工作（重置计数器）
     */
    private void onStateChange(State oldState, State newState) {
        this.lastStateChangeTime.set(System.currentTimeMillis());
        this.failureCount.set(0);
        this.successCount.set(0);
        this.halfOpenPermits.set(0);
        log.info("CircuitBreaker state transition: {} -> {}", oldState, newState);
    }

    public State getState() {
        return state.get();
    }
}
