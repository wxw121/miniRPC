package com.minirpc.spring;

import com.minirpc.core.RpcServer;
import com.minirpc.core.RpcServerStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.Objects;

/** Actuator adapter for the in-process RPC server status. */
public final class MiniRpcHealthIndicator implements HealthIndicator {

    private final RpcServer rpcServer;
    private final Kind kind;

    public MiniRpcHealthIndicator(RpcServer rpcServer, Kind kind) {
        this.rpcServer = Objects.requireNonNull(rpcServer, "rpcServer must not be null");
        this.kind = Objects.requireNonNull(kind, "kind must not be null");
    }

    @Override
    public Health health() {
        RpcServerStatus status = rpcServer.getStatus();
        boolean healthy = kind == Kind.READINESS ? status.isReady() : !status.shuttingDown();
        Health.Builder builder = healthy ? Health.up() : Health.down();
        return builder.withDetail("running", status.running())
                .withDetail("shuttingDown", status.shuttingDown())
                .withDetail("inFlightRequests", status.inFlightRequests())
                .withDetail("registryConfigured", status.registryConfigured())
                .withDetail("registeredServiceCount", status.registeredServiceCount())
                .withDetail("activeConnections", status.activeConnections())
                .withDetail("activeBusinessThreads", status.activeBusinessThreads())
                .withDetail("queuedBusinessTasks", status.queuedBusinessTasks())
                .withDetail("businessQueueCapacity", status.businessQueueCapacity())
                .withDetail("businessExecutorSaturated", status.businessExecutorSaturated())
                .build();
    }

    public enum Kind {
        READINESS,
        LIVENESS
    }
}
