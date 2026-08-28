package com.minirpc.spring;

import com.minirpc.core.RpcClient;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Objects;

/** Registers a live gauge for active channels retained by the RPC client pool. */
public final class MiniRpcConnectionPoolMetrics {

    public MiniRpcConnectionPoolMetrics(MeterRegistry meterRegistry, RpcClient rpcClient) {
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        Objects.requireNonNull(rpcClient, "rpcClient must not be null");
        Gauge.builder("minirpc.rpc.connection_pool.active", rpcClient, RpcClient::getActiveConnectionCount)
                .description("Active channels retained by MiniRPC client connection pools")
                .register(meterRegistry);
    }
}
