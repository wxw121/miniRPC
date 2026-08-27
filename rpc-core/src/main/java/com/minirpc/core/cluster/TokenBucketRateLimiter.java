package com.minirpc.core.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 令牌桶限流器
 * <p>
 * 原理：
 * 1. 以固定速率向桶中添加令牌
 * 2. 每个请求消耗一个令牌
 * 3. 桶满时多余令牌被丢弃
 * 4. 桶空时请求被拒绝
 * <p>
 * 优点：允许一定程度的突发流量（桶中可以积累令牌）
 * <p>
 * 面试考点：
 * - 令牌桶 vs 漏桶算法的区别
 * - 令牌桶允许突发，漏桶强制匀速
 * - Guava RateLimiter 的实现原理
 * - 分布式限流（Redis + Lua）
 */
public class TokenBucketRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(TokenBucketRateLimiter.class);

    /**
     * 桶容量（最大令牌数）
     */
    private final long capacity;

    /**
     * 每秒生成的令牌数
     */
    private final double tokensPerSecond;

    /**
     * 当前令牌数
     */
    private double currentTokens;

    /**
     * 上次补充令牌的时间戳
     */
    private long lastRefillTimestamp;

    public TokenBucketRateLimiter(long capacity, double tokensPerSecond) {
        this.capacity = capacity;
        this.tokensPerSecond = tokensPerSecond;
        this.currentTokens = capacity;
        this.lastRefillTimestamp = System.nanoTime();
    }

    /**
     * 尝试获取一个令牌
     *
     * @return true=获取成功（允许通过），false=获取失败（被限流）
     */
    public synchronized boolean tryAcquire() {
        refill();
        if (currentTokens >= 1) {
            currentTokens -= 1;
            return true;
        }
        log.debug("Rate limited: no tokens available");
        return false;
    }

    /**
     * 尝试获取指定数量的令牌
     */
    public synchronized boolean tryAcquire(int tokens) {
        refill();
        if (currentTokens >= tokens) {
            currentTokens -= tokens;
            return true;
        }
        return false;
    }

    /**
     * 补充令牌（基于时间差计算）
     */
    private void refill() {
        long now = System.nanoTime();
        double elapsed = (now - lastRefillTimestamp) / 1_000_000_000.0;
        double tokensToAdd = elapsed * tokensPerSecond;

        if (tokensToAdd > 0) {
            currentTokens = Math.min(capacity, currentTokens + tokensToAdd);
            lastRefillTimestamp = now;
        }
    }

    /**
     * 获取当前可用令牌数
     */
    public synchronized double getAvailableTokens() {
        refill();
        return currentTokens;
    }
}
