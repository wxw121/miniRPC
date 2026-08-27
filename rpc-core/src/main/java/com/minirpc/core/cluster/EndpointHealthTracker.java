package com.minirpc.core.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Local, consumer-only endpoint ejection. Registry metadata is never mutated. */
public class EndpointHealthTracker {

    private final Map<String, EndpointHealth> endpoints = new ConcurrentHashMap<>();
    private final int failureThreshold;
    private final long ejectDurationMs;

    public EndpointHealthTracker() {
        this(3, 30_000L);
    }

    public EndpointHealthTracker(int failureThreshold, long ejectDurationMs) {
        this.failureThreshold = failureThreshold;
        this.ejectDurationMs = ejectDurationMs;
    }

    public boolean isAvailable(String address) {
        EndpointHealth health = endpoints.get(address);
        return health == null || !health.ejected;
    }

    public void recordSuccess(String address) {
        endpoints.remove(address);
    }

    public void recordFailure(String address) {
        EndpointHealth health = endpoints.computeIfAbsent(address, ignored -> new EndpointHealth());
        synchronized (health) {
            if (health.ejected) {
                if (health.recoveryProbeInFlight) {
                    eject(health);
                }
                return;
            }
            if (health.failures.incrementAndGet() >= failureThreshold) {
                eject(health);
            }
        }
    }

    /**
     * Grants the sole recovery probe after an endpoint's ejection period expires.
     * Normal traffic remains unavailable until that probe succeeds.
     */
    public boolean tryAcquireRecoveryProbe(String address) {
        EndpointHealth health = endpoints.get(address);
        if (health == null) {
            return false;
        }
        synchronized (health) {
            if (!health.ejected
                    || health.recoveryProbeInFlight
                    || System.currentTimeMillis() < health.ejectedUntilMillis) {
                return false;
            }
            health.recoveryProbeInFlight = true;
            return true;
        }
    }

    private void eject(EndpointHealth health) {
        health.ejected = true;
        health.recoveryProbeInFlight = false;
        health.ejectedUntilMillis = System.currentTimeMillis() + ejectDurationMs;
        health.failures.set(0);
    }

    private static final class EndpointHealth {
        private final AtomicInteger failures = new AtomicInteger();
        private volatile long ejectedUntilMillis;
        private volatile boolean ejected;
        private boolean recoveryProbeInFlight;
    }
}
