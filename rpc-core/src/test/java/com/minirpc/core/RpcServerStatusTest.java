package com.minirpc.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcServerStatusTest {

    @Test
    void exposesRegistryConnectionAndBusinessExecutorState() {
        Set<String> fields = Arrays.stream(RpcServerStatus.class.getRecordComponents())
                .map(component -> component.getName())
                .collect(java.util.stream.Collectors.toSet());

        assertTrue(fields.containsAll(Set.of(
                "registryConfigured", "registeredServiceCount", "activeConnections",
                "activeBusinessThreads", "queuedBusinessTasks", "businessQueueCapacity",
                "businessExecutorSaturated")));
    }
}
