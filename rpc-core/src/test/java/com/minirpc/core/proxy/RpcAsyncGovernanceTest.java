package com.minirpc.core.proxy;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.*;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.client.NettyClient;
import com.minirpc.core.cluster.EndpointHealthTracker;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.filter.MetricsFilter;
import com.minirpc.core.observability.RpcInvocationEvent;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.ServiceInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class RpcAsyncGovernanceTest {
    interface Service {
        @RpcIdempotent CompletableFuture<String> read();
        CompletableFuture<String> write();
    }

    private RpcConfig config() {
        return RpcConfig.builder().failoverMaxRetries(2)
                .retryBaseBackoffMs(0).retryMaxBackoffMs(0).build();
    }

    private RpcClientProxy proxy(NettyClient client, RpcConfig config) {
        RpcClientProxy proxy = new RpcClientProxy(client, "localhost", 9100, config);
        proxy.setAsyncMode(true);
        return proxy;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void retriesIdempotentCallsAndPreservesTraceDeadlineAndAttemptCopies(boolean exceptional) throws Exception {
        List<RpcRequest> requests = new CopyOnWriteArrayList<>();
        try (TestClient client = new TestClient(request -> {
            requests.add(request);
            if (requests.size() < 3) {
                return exceptional ? CompletableFuture.failedFuture(
                        new RpcException(RpcErrorCode.UNAVAILABLE, "offline"))
                        : CompletableFuture.completedFuture(RpcResponse.fail(
                                request.getRequestId(), RpcErrorCode.UNAVAILABLE, "offline"));
            }
            return CompletableFuture.completedFuture(RpcResponse.success(request.getRequestId(), "ok"));
        })) {
            List<RpcInvocationEvent> events = new CopyOnWriteArrayList<>();
            RpcClientProxy proxy = proxy(client, config());
            proxy.setInvocationListeners(List.of(events::add));
            MetricsFilter.ServiceMetrics before = MetricsFilter.getMetrics(Service.class.getName());
            long callsBefore = before == null ? 0 : before.totalCalls.get();
            assertEquals("ok", proxy.createProxy(Service.class).read().get(2, TimeUnit.SECONDS));
            assertEquals(3, requests.size());
            assertEquals(3, events.size());
            String trace = requests.get(0).getContext().getTraceId();
            assertNotNull(trace);
            assertFalse(trace.isBlank());
            for (int i = 0; i < 3; i++) {
                assertEquals(requests.get(0).getRequestId(), requests.get(i).getRequestId());
                assertEquals(trace, requests.get(i).getContext().getTraceId());
                assertEquals(requests.get(0).getContext().getDeadlineEpochMillis(),
                        requests.get(i).getContext().getDeadlineEpochMillis());
                assertEquals(Integer.toString(i), requests.get(i).getContext().getAttachments().get("retryAttempt"));
                assertEquals(i, events.get(i).attempt());
            }
            assertNotSame(requests.get(0).getContext(), requests.get(1).getContext());
            assertEquals(callsBefore + 1, MetricsFilter.getMetrics(Service.class.getName()).totalCalls.get());
        }
    }

    @Test
    void nonIdempotentAndNonRetryableCallsOnlySendOnce() {
        AtomicInteger calls = new AtomicInteger();
        try (TestClient client = new TestClient(request -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new RpcException(RpcErrorCode.UNAUTHORIZED, "denied"));
        })) {
            Service service = proxy(client, config()).createProxy(Service.class);
            assertEquals(RpcErrorCode.UNAUTHORIZED, failure(service.read()).getErrorCode());
            assertEquals(1, calls.get());
            assertEquals(RpcErrorCode.UNAUTHORIZED, failure(service.write()).getErrorCode());
            assertEquals(2, calls.get());
        }
        calls.set(0);
        try (TestClient client = new TestClient(request -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new RpcException(RpcErrorCode.UNAVAILABLE, "offline"));
        })) {
            failure(proxy(client, config()).createProxy(Service.class).write());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void filtersEjectedEndpointsAndRetriesAnotherEndpoint() throws Exception {
        List<Integer> ports = new CopyOnWriteArrayList<>();
        EndpointHealthTracker health = new EndpointHealthTracker(1, 60_000);
        health.recordFailure("localhost:9101");
        List<ServiceInstance> instances = List.of(9101, 9102, 9103).stream()
                .map(port -> new ServiceInstance(Service.class.getName(), "localhost", port)).toList();
        ServiceDiscovery discovery = new ServiceDiscovery(null) {
            @Override public List<ServiceInstance> getInstances(String service, String version, String group) {
                return instances;
            }
        };
        try (TestClient client = new TestClient(request -> { throw new AssertionError(); }) {
            @Override public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
                ports.add(port);
                return CompletableFuture.completedFuture(port == 9102
                        ? RpcResponse.fail(request.getRequestId(), RpcErrorCode.UNAVAILABLE, "offline")
                        : RpcResponse.success(request.getRequestId(), "ok"));
            }
        }) {
            RpcClientProxy proxy = new RpcClientProxy(client, discovery, (candidates, key) -> candidates.get(0),
                    config(), health);
            proxy.setAsyncMode(true);
            assertEquals("ok", proxy.createProxy(Service.class).read().get(2, TimeUnit.SECONDS));
            assertEquals(List.of(9102, 9103), ports);
            assertFalse(health.isAvailable("localhost:9102"));
        }
    }

    @Test
    void exceptionalFutureTripsCircuitBreaker() {
        AtomicInteger calls = new AtomicInteger();
        try (TestClient client = new TestClient(request -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new RpcException(RpcErrorCode.UNAVAILABLE, "offline"));
        })) {
            RpcConfig config = RpcConfig.builder().failoverMaxRetries(0)
                    .circuitBreakerFailureThreshold(1).build();
            Service service = proxy(client, config).createProxy(Service.class);
            failure(service.read());
            RpcException rejected = failure(service.read());
            assertTrue(rejected.getMessage().contains("Circuit breaker"));
            assertEquals(1, calls.get());
        }
    }

    @Test
    void cancellingProxyFutureCancelsPendingTransportAndPreventsRetry() {
        CompletableFuture<RpcResponse> transport = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        try (TestClient client = new TestClient(request -> { calls.incrementAndGet(); return transport; })) {
            CompletableFuture<String> future = proxy(client, config()).createProxy(Service.class).read();
            assertFalse(future.isDone());
            assertTrue(future.cancel(false));
            assertTrue(transport.isCancelled());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void retriesNeverSendFromResponseCompletionThread() throws Exception {
        CompletableFuture<RpcResponse> first = new CompletableFuture<>();
        List<String> threads = new CopyOnWriteArrayList<>();
        try (TestClient client = new TestClient(request -> {
            threads.add(Thread.currentThread().getName());
            return threads.size() == 1 ? first
                    : CompletableFuture.completedFuture(RpcResponse.success(request.getRequestId(), "ok"));
        })) {
            CompletableFuture<String> result = proxy(client, config()).createProxy(Service.class).read();
            Thread responseThread = new Thread(() -> first.completeExceptionally(
                    new RpcException(RpcErrorCode.UNAVAILABLE, "offline")), "simulated-netty-eventloop");
            responseThread.start();
            responseThread.join(1000);
            assertEquals("ok", result.get(2, TimeUnit.SECONDS));
            assertNotEquals("simulated-netty-eventloop", threads.get(1));
        }
    }

    @Test
    void deadlineCompletesAsyncCallWithoutCancellingTransport() throws Exception {
        CompletableFuture<RpcResponse> transport = new CompletableFuture<>();
        try (TestClient client = new TestClient(request -> transport)) {
            RpcClientProxy proxy = proxy(client, config());
            proxy.setRequestTimeoutMs(50);
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> proxy.createProxy(Service.class).read().get(2, TimeUnit.SECONDS));
            assertInstanceOf(RpcException.class, error.getCause());
            assertEquals(RpcErrorCode.DEADLINE_EXCEEDED, ((RpcException) error.getCause()).getErrorCode());
            assertFalse(transport.isCancelled());
        }
    }

    @Test
    void legacyCustomStrategyRunsOffCallerAndStillTransformsResponses() throws Exception {
        try (TestClient client = new TestClient(request -> CompletableFuture.completedFuture(
                RpcResponse.success(request.getRequestId(), "ok")))) {
            RpcClientProxy proxy = proxy(client, config());
            Thread caller = Thread.currentThread();
            proxy.setClusterStrategy((request, invoker) -> {
                assertNotSame(caller, Thread.currentThread());
                RpcResponse response = invoker.get();
                return RpcResponse.success(request.getRequestId(), response.getData() + "-custom");
            });
            assertEquals("ok-custom", proxy.createProxy(Service.class).read().get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void asyncRecoveryProbeIsTheOnlyCandidateAndSuccessRestoresEndpoint() throws Exception {
        EndpointHealthTracker health = new EndpointHealthTracker(1, 0);
        ServiceInstance recovering = new ServiceInstance(Service.class.getName(), "localhost", 9101);
        ServiceInstance healthy = new ServiceInstance(Service.class.getName(), "localhost", 9102);
        health.recordFailure(recovering.getAddress());
        ServiceDiscovery discovery = new ServiceDiscovery(null) {
            @Override public List<ServiceInstance> getInstances(String service, String version, String group) {
                return List.of(recovering, healthy);
            }
        };
        try (TestClient client = new TestClient(request -> CompletableFuture.completedFuture(
                RpcResponse.success(request.getRequestId(), "ok")))) {
            RpcClientProxy proxy = new RpcClientProxy(client, discovery, (candidates, key) -> {
                assertEquals(List.of(recovering), candidates);
                return candidates.get(0);
            }, config(), health);
            proxy.setAsyncMode(true);
            assertEquals("ok", proxy.createProxy(Service.class).read().get(2, TimeUnit.SECONDS));
            assertTrue(health.isAvailable(recovering.getAddress()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationDoesNotEjectEndpointOrOpenCircuitBreaker(boolean recovery) throws Exception {
        EndpointHealthTracker health = new EndpointHealthTracker(1, recovery ? 0 : 60_000);
        ServiceInstance instance = new ServiceInstance(Service.class.getName(), "localhost", 9101);
        if (recovery) health.recordFailure(instance.getAddress());
        ServiceDiscovery discovery = new ServiceDiscovery(null) {
            @Override public List<ServiceInstance> getInstances(String service, String version, String group) {
                return List.of(instance);
            }
        };
        AtomicInteger calls = new AtomicInteger();
        try (TestClient client = new TestClient(request -> calls.incrementAndGet() == 1
                ? new CompletableFuture<>()
                : CompletableFuture.completedFuture(RpcResponse.success(request.getRequestId(), "ok")))) {
            RpcClientProxy proxy = new RpcClientProxy(client, discovery, (candidates, key) -> candidates.get(0),
                    RpcConfig.builder().circuitBreakerFailureThreshold(1).build(), health);
            proxy.setAsyncMode(true);
            Service service = proxy.createProxy(Service.class);
            assertTrue(service.read().cancel(false));
            assertEquals(!recovery, health.isAvailable(instance.getAddress()));
            assertEquals("ok", service.read().get(2, TimeUnit.SECONDS));
            assertTrue(health.isAvailable(instance.getAddress()));
        }
    }

    @Test
    void cancellationDuringRetryPreflightEventuallyCancelsItsTransport() throws Exception {
        CountDownLatch enteredRetry = new CountDownLatch(1);
        CountDownLatch releaseRetry = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        CompletableFuture<RpcResponse> transport = new CompletableFuture<>();
        transport.whenComplete((response, failure) -> { if (transport.isCancelled()) cancelled.countDown(); });
        try (TestClient client = new TestClient(request -> {
            if (calls.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(new RpcException(RpcErrorCode.UNAVAILABLE, "offline"));
            }
            enteredRetry.countDown();
            try {
                assertTrue(releaseRetry.await(2, TimeUnit.SECONDS));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError(failure);
            }
            return transport;
        })) {
            CompletableFuture<String> result = proxy(client, config()).createProxy(Service.class).read();
            try {
                assertTrue(enteredRetry.await(2, TimeUnit.SECONDS));
                assertTrue(result.cancel(false));
            } finally {
                releaseRetry.countDown();
            }
            assertTrue(cancelled.await(2, TimeUnit.SECONDS));
            assertEquals(2, calls.get());
        }
    }

    private RpcException failure(CompletableFuture<?> future) {
        CompletionException error = assertThrows(CompletionException.class, future::join);
        return assertInstanceOf(RpcException.class, error.getCause());
    }

    private static class TestClient extends NettyClient implements AutoCloseable {
        private final Function<RpcRequest, CompletableFuture<RpcResponse>> sender;
        TestClient(Function<RpcRequest, CompletableFuture<RpcResponse>> sender) { this.sender = sender; }
        @Override public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
            return sender.apply(request);
        }
        @Override public void close() { shutdown(); }
    }
}
