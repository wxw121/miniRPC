package com.minirpc.core.observability;

import com.minirpc.common.protocol.RpcErrorCode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcInvocationEventTest {

    @Test
    void preservesOutcomeAndExposesImmutableAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("request_id", "req-1");

        RpcInvocationEvent event = new RpcInvocationEvent(
                "client", "com.minirpc.EchoService", RpcErrorCode.UNAVAILABLE,
                42L, 2, "127.0.0.1:9090", attributes);
        attributes.put("request_id", "changed-after-construction");

        assertEquals(RpcErrorCode.UNAVAILABLE, event.errorCode());
        assertEquals("req-1", event.attributes().get("request_id"));
        assertThrows(UnsupportedOperationException.class, () -> event.attributes().put("x", "y"));
    }
}
