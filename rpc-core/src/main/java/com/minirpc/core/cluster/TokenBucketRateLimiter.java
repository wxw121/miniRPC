package com.minirpc.core.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 令牌桶限流器
 * 令牌以固定速率补充；请求消耗令牌，令牌耗尽时被拒绝。
 * 桶内积累的令牌允许短时突发流量。
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
     * 按需补充令牌：调用时根据经过的时间计算应补充的数量，无需定时线程。
     */
    private void refill() {
        // nanoTime 用于计算经过的时长，避免系统日期时间调整影响补充计算。
        long now = System.nanoTime();
        // 纳秒换算为秒，使用浮点除法保留不足 1 秒的部分。
        double elapsed = (now - lastRefillTimestamp) / 1_000_000_000.0;
        // 经过的秒数 × 每秒速率，例如 0.2 秒 × 10 个/秒 = 2 个令牌；小数令牌也会保留。
        double tokensToAdd = elapsed * tokensPerSecond;

        if (tokensToAdd > 0) {
            // 补充后不能超过桶容量，超出的部分直接丢弃，限制空闲后可积累的最大令牌数。
            currentTokens = Math.min(capacity, currentTokens + tokensToAdd);
            // 更新计算起点，避免下次重复计算同一段时间。
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
