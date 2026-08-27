package com.minirpc.core.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 熔断器单元测试
 * <p>
 * 验证三态转换：CLOSED -> OPEN -> HALF_OPEN -> CLOSED
 */
class CircuitBreakerTest {

    @Test
    @DisplayName("初始状态为 CLOSED，允许所有请求")
    void testInitialStateClosed() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000, 2);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowRequest());
    }

    @Test
    @DisplayName("连续失败达到阈值后切换到 OPEN")
    void testTransitionToOpen() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000, 2);

        cb.recordFailure();
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());

        cb.recordFailure(); // 第3次失败，达到阈值
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest());
    }

    @Test
    @DisplayName("OPEN 状态超时后切换到 HALF_OPEN")
    void testTransitionToHalfOpen() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(2, 100, 2); // 100ms 超时

        cb.recordFailure();
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        Thread.sleep(150); // 等待超时
        assertTrue(cb.allowRequest()); // 触发切换到 HALF_OPEN
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
    }

    @Test
    @DisplayName("HALF_OPEN 状态下成功恢复 CLOSED")
    void testHalfOpenToClosed() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(2, 100, 2);

        cb.recordFailure();
        cb.recordFailure();
        Thread.sleep(150);
        cb.allowRequest(); // 触发 HALF_OPEN

        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());

        cb.recordSuccess();
        cb.recordSuccess(); // 达到 halfOpenMaxRequests=2
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    @DisplayName("HALF_OPEN 状态下失败回到 OPEN")
    void testHalfOpenToOpen() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(2, 100, 2);

        cb.recordFailure();
        cb.recordFailure();
        Thread.sleep(150);
        cb.allowRequest(); // 触发 HALF_OPEN

        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());

        cb.recordFailure(); // 半开状态失败
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }

    @Test
    @DisplayName("HALF_OPEN 的探测请求不超过许可数")
    void testHalfOpenProbePermitLimit() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(1, 20, 3);
        cb.recordFailure();
        Thread.sleep(30);

        assertTrue(cb.allowRequest());
        assertTrue(cb.allowRequest());
        assertTrue(cb.allowRequest());
        assertFalse(cb.allowRequest());
    }

    @Test
    @DisplayName("CLOSED 状态下成功重置失败计数")
    void testSuccessResetsFailureCount() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000, 2);

        cb.recordFailure();
        cb.recordFailure();
        cb.recordSuccess(); // 重置计数

        cb.recordFailure();
        cb.recordFailure();
        // 只有2次连续失败，不应触发熔断
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }
}
