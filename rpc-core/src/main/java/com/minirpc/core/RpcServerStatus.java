package com.minirpc.core;

/** Readiness state exposed without coupling rpc-core to a web or metrics framework. */
public record RpcServerStatus(
        boolean running,
        boolean shuttingDown,
        int inFlightRequests,
        boolean registryConfigured,
        int registeredServiceCount,
        int activeConnections,
        int activeBusinessThreads,
        int queuedBusinessTasks,
        int businessQueueCapacity,
        boolean businessExecutorSaturated) {

    public RpcServerStatus(boolean running, boolean shuttingDown, int inFlightRequests) {
        this(running, shuttingDown, inFlightRequests, false, 0, 0, 0, 0, 0, false);
    }

    public boolean isReady() {
        return running && !shuttingDown && !businessExecutorSaturated;
    }
}
