package com.minirpc.core.proxy;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.client.NettyClient;
import com.minirpc.core.cluster.EndpointHealthTracker;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.loadbalance.LoadBalancer;
import com.minirpc.registry.Registry;
import com.minirpc.registry.ServiceChangeListener;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.ServiceInstance;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcRetryPolicyIntegrationTest {

    @Test
    void idempotentOperationsDoNotRetryAfterTheirDeadline() {
        FailingClient client = new FailingClient(RpcErrorCode.UNAVAILABLE);
        try {
            ReadService service = proxy(client, RpcConfig.builder()
                    .timeoutMs(1)
                    .failoverMaxRetries(2)
                    .retryBaseBackoffMs(0)
                    .retryMaxBackoffMs(0)
                    .build()).createProxy(ReadService.class);

            assertThrows(RpcException.class, service::find);
            assertEquals(1, client.calls);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void propagatesTheRetryAttemptWithEachNetworkCall() {
        FailingClient client = new FailingClient(RpcErrorCode.UNAVAILABLE);
        try {
            ReadService service = proxy(client, retryConfig())
                    .createProxy(ReadService.class);

            assertThrows(RpcException.class, service::find);
            assertEquals(List.of("0", "1", "2"), client.retryAttempts);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void doesNotRetryNonRetryableErrorCodes() {
        FailingClient client = new FailingClient(RpcErrorCode.UNAUTHORIZED);
        try {
            ReadService service = proxy(client, retryConfig())
                    .createProxy(ReadService.class);

            assertThrows(RpcException.class, service::find);
            assertEquals(1, client.calls);
        } finally {
            client.shutdown();
        }
    }

    private RpcClientProxy proxy(NettyClient client, RpcConfig config) {
        return new RpcClientProxy(client, "127.0.0.1", 1, config);
    }

    interface ReadService {
        @RpcIdempotent
        String find();
    }

    private static class FailingClient extends NettyClient {
        private final RpcErrorCode errorCode;
        private final List<String> retryAttempts = new ArrayList<>();
        private int calls;

        FailingClient(RpcErrorCode errorCode) {
            super(RpcConfig.defaultConfig());
            this.errorCode = errorCode;
        }

        @Override
        public RpcResponse sendSync(String host, int port, RpcRequest request) {
            calls++;
            retryAttempts.add(request.getContext().getAttachments().get("retryAttempt"));
            throw new RpcException(errorCode, "simulated network failure");
        }
    }

    private RpcConfig retryConfig() {
        return RpcConfig.builder()
                .failoverMaxRetries(2)
                .retryBaseBackoffMs(0)
                .retryMaxBackoffMs(0)
                .build();
    }

    @Test
    void doesNotRetryNonRetryableFailedResponses() {
        FailedResponseClient client = new FailedResponseClient(RpcErrorCode.UNAUTHORIZED);
        try {
            ReadService service = proxy(client, retryConfig()).createProxy(ReadService.class);

            RpcException failure = assertThrows(RpcException.class, service::find);
            assertEquals(RpcErrorCode.UNAUTHORIZED, failure.getErrorCode());
            assertEquals(1, client.calls);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void recoveryProbeIsTheOnlyLoadBalancerCandidate() {
        String serviceName = ReadService.class.getName();
        ServiceInstance recovering = new ServiceInstance(serviceName, "127.0.0.1", 9101);
        ServiceInstance healthy = new ServiceInstance(serviceName, "127.0.0.1", 9102);
        EndpointHealthTracker tracker = new EndpointHealthTracker(1, 0);
        tracker.recordFailure(recovering.getAddress());
        RecordingLoadBalancer loadBalancer = new RecordingLoadBalancer();
        SuccessfulClient client = new SuccessfulClient();
        ServiceDiscovery discovery = new ServiceDiscovery(new StaticRegistry(List.of(recovering, healthy)));
        try {
            RpcClientProxy proxy = new RpcClientProxy(
                    client, discovery, loadBalancer, retryConfig(), tracker);

            assertEquals("ok", proxy.createProxy(ReadService.class).find());
            assertEquals(List.of(recovering), loadBalancer.candidates);
            assertEquals(List.of(recovering.getAddress()), client.addresses);
        } finally {
            client.shutdown();
            discovery.close();
        }
    }

    private static class FailedResponseClient extends NettyClient {
        private final RpcErrorCode errorCode;
        private int calls;

        FailedResponseClient(RpcErrorCode errorCode) {
            super(RpcConfig.defaultConfig());
            this.errorCode = errorCode;
        }

        @Override
        public RpcResponse sendSync(String host, int port, RpcRequest request) {
            calls++;
            return RpcResponse.fail(request.getRequestId(), errorCode, "simulated server failure");
        }
    }

    private static class SuccessfulClient extends NettyClient {
        private final List<String> addresses = new ArrayList<>();

        SuccessfulClient() {
            super(RpcConfig.defaultConfig());
        }

        @Override
        public RpcResponse sendSync(String host, int port, RpcRequest request) {
            addresses.add(host + ":" + port);
            return RpcResponse.success(request.getRequestId(), "ok");
        }
    }

    private static class RecordingLoadBalancer implements LoadBalancer {
        private List<ServiceInstance> candidates = List.of();

        @Override
        public ServiceInstance select(List<ServiceInstance> instances, String requestKey) {
            candidates = List.copyOf(instances);
            return instances.get(0);
        }
    }

    private record StaticRegistry(List<ServiceInstance> instances) implements Registry {
        @Override
        public void register(ServiceInstance instance) {
        }

        @Override
        public void unregister(ServiceInstance instance) {
        }

        @Override
        public List<ServiceInstance> lookup(String serviceName) {
            return instances;
        }

        @Override
        public void subscribe(String serviceName, ServiceChangeListener listener) {
        }

        @Override
        public void unsubscribe(String serviceName, ServiceChangeListener listener) {
        }

        @Override
        public void close() {
        }
    }
}
