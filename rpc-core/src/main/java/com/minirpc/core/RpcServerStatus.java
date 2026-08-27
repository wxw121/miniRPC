package com.minirpc.core;

/** Readiness state exposed without coupling rpc-core to a web or metrics framework. */
public record RpcServerStatus(boolean running, boolean shuttingDown, int inFlightRequests) {
    public boolean isReady() {
        return running && !shuttingDown;
    }
}
