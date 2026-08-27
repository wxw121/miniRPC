package com.minirpc.core.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 令牌桶限流器单元测试
 * <p>
 * 验证令牌桶的基本行为：容量限制、令牌补充、突发流量
 */
class TokenBucketRateLimiterTest {

    @Test
    @DisplayName("初始状态桶满，可以获取 capacity 个令牌")
    void testInitialCapacity() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 1);

        // 初始桶满，应该能获取5个令牌
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.tryAcquire(), "Should acquire token #" + (i + 1));
        }

        // 第6个应该失败
        assertFalse(limiter.tryAcquire(), "Should be rate limited after capacity exhausted");
    }

    @Test
    @DisplayName("令牌耗尽后等待补充")
    void testTokenRefill() throws InterruptedException {
        // 每秒补充10个令牌
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 10);

        // 耗尽所有令牌
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }
        assertFalse(limiter.tryAcquire());

        // 等待200ms，应补充约2个令牌
        Thread.sleep(250);
        assertTrue(limiter.tryAcquire(), "Should have refilled tokens after waiting");
    }

    @Test
    @DisplayName("桶容量上限：补充不超过 capacity")
    void testCapacityUpperBound() throws InterruptedException {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(3, 100);

        // 等待一段时间，令牌不应超过容量
        Thread.sleep(100);
        double available = limiter.getAvailableTokens();
        assertTrue(available <= 3.0, "Tokens should not exceed capacity, got: " + available);
    }

    @Test
    @DisplayName("tryAcquire(int) 批量获取令牌")
    void testBatchAcquire() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 1);

        assertTrue(limiter.tryAcquire(5), "Should acquire 5 tokens");
        assertTrue(limiter.tryAcquire(5), "Should acquire another 5 tokens");
        assertFalse(limiter.tryAcquire(1), "Should be empty now");
    }

    @Test
    @DisplayName("高并发下不超发令牌")
    void testConcurrencySafety() throws InterruptedException {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 0); // 不补充
        int threadCount = 10;
        int attemptsPerThread = 20;
        java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < attemptsPerThread; j++) {
                    if (limiter.tryAcquire()) {
                        successCount.incrementAndGet();
                    }
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        // 总共200次尝试，但桶只有100个令牌且不补充
        assertEquals(100, successCount.get(), "Should not exceed capacity");
    }
}
