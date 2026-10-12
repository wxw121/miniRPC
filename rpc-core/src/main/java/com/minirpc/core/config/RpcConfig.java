package com.minirpc.core.config;

import com.minirpc.common.protocol.SerializerType;
import com.minirpc.core.security.TlsConfig;

import java.util.Objects;

/**
 * RPC 统一配置类
 * <p>
 * 集中管理所有可配置参数，避免硬编码散落在各个组件中。
 * 使用 Builder 模式构建，支持链式调用。
 */
public class RpcConfig {

    // ========== 通用配置 ==========

    /** 序列化类型 */
    private final SerializerType serializerType;

    /** 请求超时时间（毫秒） */
    private final long timeoutMs;

    // ========== 服务端配置 ==========

    /** 服务端端口 */
    private final int port;

    /** 业务线程池核心线程数 */
    private final int bizCorePoolSize;

    /** 业务线程池最大线程数 */
    private final int bizMaxPoolSize;

    /** 业务线程池队列容量 */
    private final int bizQueueCapacity;

    // ========== 客户端配置 ==========

    /** 连接超时时间（毫秒） */
    private final int connectTimeoutMs;

    /** 心跳间隔（秒） */
    private final int heartbeatIntervalSeconds;

    /** Number of connections retained for each provider address. */
    private final int connectionPoolSize;

    // ========== 容错配置 ==========

    /** 熔断器失败阈值 */
    private final int circuitBreakerFailureThreshold;

    /** 熔断器打开超时时间（毫秒） */
    private final long circuitBreakerOpenTimeoutMs;

    /** 限流器桶容量 */
    private final long rateLimiterCapacity;

    /** 限流器每秒令牌数 */
    private final double rateLimiterTokensPerSecond;

    /** Failover 最大重试次数 */
    private final int failoverMaxRetries;
    /** Initial retry backoff cap in milliseconds. */
    private final long retryBaseBackoffMs;

    /** Maximum retry backoff cap in milliseconds. */
    private final long retryMaxBackoffMs;
    private final TlsConfig tlsConfig;


    private RpcConfig(Builder builder) {
        this.serializerType = builder.serializerType;
        this.timeoutMs = builder.timeoutMs;
        this.port = builder.port;
        this.bizCorePoolSize = builder.bizCorePoolSize;
        this.bizMaxPoolSize = builder.bizMaxPoolSize;
        this.bizQueueCapacity = builder.bizQueueCapacity;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.heartbeatIntervalSeconds = builder.heartbeatIntervalSeconds;
        this.connectionPoolSize = builder.connectionPoolSize;
        this.circuitBreakerFailureThreshold = builder.circuitBreakerFailureThreshold;
        this.circuitBreakerOpenTimeoutMs = builder.circuitBreakerOpenTimeoutMs;
        this.rateLimiterCapacity = builder.rateLimiterCapacity;
        this.rateLimiterTokensPerSecond = builder.rateLimiterTokensPerSecond;
        this.failoverMaxRetries = builder.failoverMaxRetries;
        this.retryBaseBackoffMs = builder.retryBaseBackoffMs;
        this.retryMaxBackoffMs = builder.retryMaxBackoffMs;
        this.tlsConfig = builder.tlsConfig;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * 获取默认配置
     */
    public static RpcConfig defaultConfig() {
        return new Builder().build();
    }

    // ========== Getters ==========

    public SerializerType getSerializerType() { return serializerType; }
    public long getTimeoutMs() { return timeoutMs; }
    public int getPort() { return port; }
    public int getBizCorePoolSize() { return bizCorePoolSize; }
    public int getBizMaxPoolSize() { return bizMaxPoolSize; }
    public int getBizQueueCapacity() { return bizQueueCapacity; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getHeartbeatIntervalSeconds() { return heartbeatIntervalSeconds; }
    public int getConnectionPoolSize() { return connectionPoolSize; }
    public int getCircuitBreakerFailureThreshold() { return circuitBreakerFailureThreshold; }
    public long getCircuitBreakerOpenTimeoutMs() { return circuitBreakerOpenTimeoutMs; }
    public long getRateLimiterCapacity() { return rateLimiterCapacity; }
    public double getRateLimiterTokensPerSecond() { return rateLimiterTokensPerSecond; }
    public int getFailoverMaxRetries() { return failoverMaxRetries; }
    public long getRetryBaseBackoffMs() { return retryBaseBackoffMs; }
    public long getRetryMaxBackoffMs() { return retryMaxBackoffMs; }
    public TlsConfig getTlsConfig() { return tlsConfig; }

    @Override
    public String toString() {
        return "RpcConfig{" +
                "serializerType=" + serializerType +
                ", timeoutMs=" + timeoutMs +
                ", port=" + port +
                ", bizCorePoolSize=" + bizCorePoolSize +
                ", bizMaxPoolSize=" + bizMaxPoolSize +
                ", connectTimeoutMs=" + connectTimeoutMs +
                ", heartbeatIntervalSeconds=" + heartbeatIntervalSeconds +
                ", circuitBreakerFailureThreshold=" + circuitBreakerFailureThreshold +
                ", rateLimiterCapacity=" + rateLimiterCapacity +
                ", failoverMaxRetries=" + failoverMaxRetries +
                ", retryBaseBackoffMs=" + retryBaseBackoffMs +
                ", retryMaxBackoffMs=" + retryMaxBackoffMs +
                '}';
    }

    /**
     * Builder 模式构建器
     */
    public static class Builder {
        private SerializerType serializerType = SerializerType.HESSIAN2;
        private long timeoutMs = 5000;
        private int port = 9090;
        private int bizCorePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        private int bizMaxPoolSize = 256;
        private int bizQueueCapacity = 1024;
        private int connectTimeoutMs = 5000;
        private int heartbeatIntervalSeconds = 30;
        private int connectionPoolSize = 4;
        private int circuitBreakerFailureThreshold = 5;
        private long circuitBreakerOpenTimeoutMs = 10000;
        private long rateLimiterCapacity = 1000;
        private double rateLimiterTokensPerSecond = 500;
        private int failoverMaxRetries = 2;
        private long retryBaseBackoffMs = 100;
        private long retryMaxBackoffMs = 1_000;
        private TlsConfig tlsConfig = TlsConfig.disabled();

        public Builder serializerType(SerializerType serializerType) {
            this.serializerType = serializerType;
            return this;
        }

        public Builder timeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder bizCorePoolSize(int bizCorePoolSize) {
            this.bizCorePoolSize = bizCorePoolSize;
            return this;
        }

        public Builder bizMaxPoolSize(int bizMaxPoolSize) {
            this.bizMaxPoolSize = bizMaxPoolSize;
            return this;
        }

        public Builder bizQueueCapacity(int bizQueueCapacity) {
            this.bizQueueCapacity = bizQueueCapacity;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder heartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
            this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
            return this;
        }

        public Builder connectionPoolSize(int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        public Builder circuitBreakerFailureThreshold(int threshold) {
            this.circuitBreakerFailureThreshold = threshold;
            return this;
        }

        public Builder circuitBreakerOpenTimeoutMs(long timeoutMs) {
            this.circuitBreakerOpenTimeoutMs = timeoutMs;
            return this;
        }

        public Builder rateLimiterCapacity(long capacity) {
            this.rateLimiterCapacity = capacity;
            return this;
        }

        public Builder rateLimiterTokensPerSecond(double tokensPerSecond) {
            this.rateLimiterTokensPerSecond = tokensPerSecond;
            return this;
        }

        public Builder failoverMaxRetries(int maxRetries) {
            this.failoverMaxRetries = maxRetries;
            return this;
        }

        public Builder retryBaseBackoffMs(long baseBackoffMs) {
            this.retryBaseBackoffMs = baseBackoffMs;
            return this;
        }

        public Builder retryMaxBackoffMs(long maxBackoffMs) {
            this.retryMaxBackoffMs = maxBackoffMs;
            return this;
        }

        public Builder tlsConfig(TlsConfig tlsConfig) {
            this.tlsConfig = Objects.requireNonNull(tlsConfig, "tlsConfig must not be null");
            return this;
        }

        public RpcConfig build() {
            validate();
            return new RpcConfig(this);
        }

        private void validate() {
            Objects.requireNonNull(serializerType, "serializerType must not be null");
            if (serializerType == SerializerType.JDK) {
                throw new IllegalArgumentException(
                        "JDK serialization is not permitted for network RPC; use HESSIAN2 or PROTOSTUFF");
            }
            if (timeoutMs <= 0 || connectTimeoutMs <= 0) {
                throw new IllegalArgumentException("request and connect timeouts must be positive");
            }
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("port must be between 0 and 65535");
            }
            if (bizCorePoolSize <= 0 || bizMaxPoolSize < bizCorePoolSize || bizQueueCapacity <= 0) {
                throw new IllegalArgumentException("invalid business executor configuration");
            }
            if (heartbeatIntervalSeconds <= 0 || connectionPoolSize <= 0) {
                throw new IllegalArgumentException("heartbeat interval and connection pool size must be positive");
            }
            if (circuitBreakerFailureThreshold <= 0 || circuitBreakerOpenTimeoutMs <= 0) {
                throw new IllegalArgumentException("invalid circuit breaker configuration");
            }
            if (rateLimiterCapacity <= 0 || rateLimiterTokensPerSecond <= 0 || failoverMaxRetries < 0
                    || retryBaseBackoffMs < 0
                    || retryMaxBackoffMs < retryBaseBackoffMs) {
                throw new IllegalArgumentException("invalid rate limiter or retry configuration");
            }
        }
    }
}
