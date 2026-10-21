package com.minirpc.core;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.cluster.ClusterStrategy;
import com.minirpc.core.config.RpcConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RpcClientExtensionsTest {

    @Test
    void directClientUsesCustomStrategyAndKeepsTimeoutsIsolatedPerProxy() {
        RpcClient client = new RpcClient("127.0.0.1", 1, RpcConfig.builder().timeoutMs(9000).build());
        List<RpcRequest> requests = new ArrayList<>();
        client.setClusterStrategy((request, invoker) -> {
            requests.add(request);
            return RpcResponse.success(request.getRequestId(), "custom");
        });
        try {
            EchoService shortReference = client.createProxy(EchoService.class, "2.0", "blue", 500);
            EchoService defaultReference = client.createProxy(EchoService.class);
            long before = System.currentTimeMillis();
            assertEquals("custom", shortReference.echo());
            long after = System.currentTimeMillis();
            assertTrue(requests.get(0).getContext().getDeadlineEpochMillis() >= before + 500);
            assertTrue(requests.get(0).getContext().getDeadlineEpochMillis() <= after + 500);
            assertEquals("2.0", requests.get(0).getVersion());
            assertEquals("blue", requests.get(0).getGroup());

            before = System.currentTimeMillis();
            assertEquals("custom", defaultReference.echo());
            assertTrue(requests.get(1).getContext().getDeadlineEpochMillis() >= before + 9000);
            assertThrows(IllegalArgumentException.class,
                    () -> client.createProxy(EchoService.class, "1.0.0", "default", -1));
        } finally {
            client.shutdown();
        }
    }

    @Test
    void clientShutdownClosesCustomStrategyOnlyOnce() {
        RpcClient client = new RpcClient("127.0.0.1", 1);
        AtomicInteger closed = new AtomicInteger();
        client.setClusterStrategy(new ClusterStrategy() {
            @Override
            public RpcResponse invoke(RpcRequest request, java.util.function.Supplier<RpcResponse> invoker) {
                return invoker.get();
            }

            @Override public void close() { closed.incrementAndGet(); }
        });

        client.shutdown();
        client.shutdown();
        assertEquals(1, closed.get());
    }

    interface EchoService {
        @RpcIdempotent String echo();
    }
}
