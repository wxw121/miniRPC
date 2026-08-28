package com.minirpc.spring;

import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.core.observability.RpcInvocationEvent;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MiniRpcMeterRegistryListenerTest {

    @Test
    void recordsOnlyFixedLowCardinalityTags() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MiniRpcMeterRegistryListener listener = new MiniRpcMeterRegistryListener(registry);

        listener.onComplete(new RpcInvocationEvent("client", "example.EchoService", RpcErrorCode.UNAVAILABLE,
                1_000_000, 1, "10.0.0.8:9090", Map.of("traceId", "secret-trace-id")));

        Meter.Id meterId = registry.find("minirpc.rpc.calls").meter().getId();
        Set<String> tagKeys = meterId.getTags().stream().map(tag -> tag.getKey()).collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of("side", "service", "outcome", "error_code"), tagKeys);
        assertEquals(1.0, registry.get("minirpc.rpc.retries").counter().count());
        listener.onComplete(new RpcInvocationEvent("client", "example.EchoService", RpcErrorCode.DEADLINE_EXCEEDED,
                1_000_000, 0, "10.0.0.8:9090", Map.of()));
        assertEquals(1.0, registry.get("minirpc.rpc.timeouts").counter().count());
        listener.onComplete(new RpcInvocationEvent("client", "example.EchoService", RpcErrorCode.UNAVAILABLE,
                1_000_000, 0, "", Map.of("rejection_reason", "circuit_breaker")));
        listener.onComplete(new RpcInvocationEvent("server", "example.EchoService", RpcErrorCode.OVERLOADED,
                1_000_000, 0, "", Map.of("rejection_reason", "business_executor")));
        assertEquals(1.0, registry.get("minirpc.rpc.circuit_breaker.rejections").counter().count());
        assertEquals(1.0, registry.get("minirpc.rpc.business_executor.rejections").counter().count());
    }
}
