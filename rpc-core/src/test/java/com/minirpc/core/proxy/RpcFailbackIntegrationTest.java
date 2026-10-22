package com.minirpc.core.proxy;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.client.NettyClient;
import java.util.concurrent.CompletableFuture;
import com.minirpc.core.cluster.FailbackClusterStrategy;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.ServiceInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.util.concurrent.CompletionException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RpcFailbackIntegrationTest {

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void backgroundRetryAvoidsFailedEndpointAndPreservesRequestContext(boolean throwsException, boolean async) throws Exception {
        RecoveringClient client = new RecoveringClient(throwsException);
        FailbackClusterStrategy strategy = new FailbackClusterStrategy(0, 1);
        List<ServiceInstance> instances = List.of(
                new ServiceInstance(EchoService.class.getName(), "127.0.0.1", 9101),
                new ServiceInstance(EchoService.class.getName(), "127.0.0.1", 9102));
        ServiceDiscovery discovery = new ServiceDiscovery(null) {
            @Override
            public List<ServiceInstance> getInstances(String service, String version, String group) {
                return instances;
            }
        };
        try {
            RpcClientProxy proxy = new RpcClientProxy(client, discovery,
                    (candidates, key) -> candidates.get(0), RpcConfig.defaultConfig());
            proxy.setClusterStrategy(strategy);
            proxy.setAsyncMode(async);

            RpcException error;
            if (async) {
                CompletionException failure = assertThrows(CompletionException.class,
                        () -> proxy.createProxy(AsyncEchoService.class).echo().join());
                error = assertInstanceOf(RpcException.class, failure.getCause());
            } else {
                error = assertThrows(RpcException.class, () -> proxy.createProxy(EchoService.class).echo());
            }
            assertEquals(RpcErrorCode.UNAVAILABLE, error.getErrorCode());
            assertTrue(client.recovered.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(9101, 9102), client.ports);
            RpcRequest first = client.requests.get(0);
            RpcRequest retry = client.requests.get(1);
            assertEquals(first.getRequestId(), retry.getRequestId());
            assertEquals(first.getContext().getDeadlineEpochMillis(), retry.getContext().getDeadlineEpochMillis());
            assertEquals(first.getContext().getTraceId(), retry.getContext().getTraceId());
            assertEquals("0", first.getContext().getAttachments().get("retryAttempt"));
            assertEquals("1", retry.getContext().getAttachments().get("retryAttempt"));
            assertNotEquals(client.threads.get(0), client.threads.get(1));
        } finally {
            strategy.close();
            client.shutdown();
        }
    }

    interface EchoService {
        @RpcIdempotent String echo();
    }

    interface AsyncEchoService {
        @RpcIdempotent CompletableFuture<String> echo();
    }

    static class RecoveringClient extends NettyClient {
        final boolean throwsException;
        final List<Integer> ports = new CopyOnWriteArrayList<>();
        final List<RpcRequest> requests = new CopyOnWriteArrayList<>();
        final List<Long> threads = new CopyOnWriteArrayList<>();
        final CountDownLatch recovered = new CountDownLatch(1);

        RecoveringClient(boolean throwsException) {
            this.throwsException = throwsException;
        }

        @Override
        public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
            ports.add(port);
            requests.add(request);
            threads.add(Thread.currentThread().getId());
            if (port == 9101) {
                if (throwsException) {
                    throw new RpcException(RpcErrorCode.UNAVAILABLE, "endpoint unavailable");
                }
                return CompletableFuture.completedFuture(RpcResponse.fail(request.getRequestId(), RpcErrorCode.UNAVAILABLE, "endpoint unavailable"));
            }
            recovered.countDown();
            return CompletableFuture.completedFuture(RpcResponse.success(request.getRequestId(), "ok"));
        }
    }
}
