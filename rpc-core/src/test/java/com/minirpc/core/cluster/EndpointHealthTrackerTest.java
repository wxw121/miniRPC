package com.minirpc.core.cluster;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EndpointHealthTrackerTest {

    @Test
    void ejectsAfterThresholdAndRestoresOnSuccess() {
        EndpointHealthTracker tracker = new EndpointHealthTracker(2, 10_000);
        tracker.recordFailure("127.0.0.1:9090");
        assertTrue(tracker.isAvailable("127.0.0.1:9090"));

        tracker.recordFailure("127.0.0.1:9090");
        assertFalse(tracker.isAvailable("127.0.0.1:9090"));

        tracker.recordSuccess("127.0.0.1:9090");
        assertTrue(tracker.isAvailable("127.0.0.1:9090"));
    }

    @Test
    void allowsExactlyOneRecoveryProbeAfterEjectionExpires() throws InterruptedException {
        EndpointHealthTracker tracker = new EndpointHealthTracker(1, 20);
        tracker.recordFailure("127.0.0.1:9090");

        assertFalse(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));
        Thread.sleep(40);

        assertTrue(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));
        assertFalse(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));
    }

    @Test
    void probeSuccessClearsFailureStateAndProbeFailureReejectsEndpoint() throws InterruptedException {
        EndpointHealthTracker tracker = new EndpointHealthTracker(1, 20);
        tracker.recordFailure("127.0.0.1:9090");
        Thread.sleep(40);
        assertTrue(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));

        tracker.recordFailure("127.0.0.1:9090");
        assertFalse(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));

        Thread.sleep(40);
        assertTrue(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));
        tracker.recordSuccess("127.0.0.1:9090");

        assertTrue(tracker.isAvailable("127.0.0.1:9090"));
        assertFalse(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));
    }

    @Test
    void publishesEjectionStateToConcurrentAvailabilityReaders() throws ReflectiveOperationException {
        Class<?> endpointHealth = Class.forName(
                "com.minirpc.core.cluster.EndpointHealthTracker$EndpointHealth");

        assertTrue(Modifier.isVolatile(endpointHealth.getDeclaredField("ejected").getModifiers()));
    }
}
