package com.minirpc.spring;

import com.minirpc.core.RpcClient;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Objects;

/**
 * 将 RPC 客户端连接池的活跃连接数接入 Micrometer，便于监控连接数量的变化。
 *
 * Gauge 用于表示可增可减的当前值：采集时读取连接数，无须在连接建立、断开时手动加减指标。
 * 这里统计该客户端所有连接池中 Channel.isActive() 为 true 的连接，当前空闲的连接也会计入。
 */
public final class MiniRpcConnectionPoolMetrics {

    /**
     * 创建对象时完成指标注册。
     *
     * 1. 校验指标注册表和被监控的客户端。
     * 2. 将客户端及其读取连接数的方法绑定到 Gauge。
     * 3. 把 Gauge 注册到 MeterRegistry，供监控系统采集。
     */
    public MiniRpcConnectionPoolMetrics(MeterRegistry meterRegistry, RpcClient rpcClient) {
        // 缺少必要依赖时立即报错，避免到采集指标时才发现问题。
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        Objects.requireNonNull(rpcClient, "rpcClient must not be null");

        // 三个参数依次为：指标名称、被监控的对象、读取该对象数值的方法。
        // 方法引用等价于 client -> client.getActiveConnectionCount()，采集时才调用。
        // 读取路径：RpcClient → NettyClient → 汇总各 ChannelPool 中的活跃连接数。
        Gauge.builder("minirpc.rpc.connection_pool.active", rpcClient, RpcClient::getActiveConnectionCount)
                // 指标的说明文字，方便监控系统展示其含义。
                .description("Active channels retained by MiniRPC client connection pools")
                // 注册 Gauge，后续采集通过上面的回调读取当前值。
                .register(meterRegistry);
    }
}
