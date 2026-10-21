package com.minirpc.core.cluster;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcContext;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailbackClusterStrategyTest {

    private final List<FailbackClusterStrategy> strategies = new ArrayList<>();
    private final List<ManualScheduler> schedulers = new ArrayList<>();

    @AfterEach
    void cleanup() {
        schedulers.forEach(ManualScheduler::shutdownNow);
        strategies.forEach(FailbackClusterStrategy::shutdown);
    }

    @Test
    void retriesUnavailableResponseInBackgroundAndPreservesInitialFailure() throws Exception {
        FailbackClusterStrategy strategy = new FailbackClusterStrategy(0, 1);
        RpcRequest request = request();
        RpcResponse failure = RpcResponse.fail(request.getRequestId(), RpcErrorCode.UNAVAILABLE, "offline");
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch retry = new CountDownLatch(1);
        try {
            assertSame(failure, strategy.invoke(request, () -> {
                if (calls.incrementAndGet() == 1) {
                    return failure;
                }
                retry.countDown();
                return RpcResponse.success(request.getRequestId(), "done");
            }));
            assertTrue(retry.await(2, TimeUnit.SECONDS), "transient failure should get a background retry");
        } finally {
            strategy.shutdown();
        }
    }

    @Test
    void defaultDelayLeavesRetryBudgetWithinTheDefaultRequestDeadline() throws Exception {
        FailbackClusterStrategy strategy = new FailbackClusterStrategy();
        RpcRequest request = requestWithDeadline(System.currentTimeMillis() + 5_000);
        RpcResponse failure = RpcResponse.fail("request", RpcErrorCode.UNAVAILABLE, "offline");
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch retry = new CountDownLatch(1);
        try {
            assertSame(failure, strategy.invoke(request, () -> {
                if (calls.incrementAndGet() == 1) {
                    return failure;
                }
                retry.countDown();
                return RpcResponse.success("request", "done");
            }));
            assertTrue(retry.await(3, TimeUnit.SECONDS), "default delay must leave room within a 5s deadline");
        } finally {
            strategy.shutdown();
        }
    }

    @Test
    void preservesInitialExceptionWhileRetryingInBackground() throws Exception {
        FailbackClusterStrategy strategy = new FailbackClusterStrategy(0, 1);
        RuntimeException failure = new IllegalStateException("connection reset");
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch retry = new CountDownLatch(1);
        try {
            assertSame(failure, assertThrows(RuntimeException.class, () -> strategy.invoke(request(), () -> {
                if (calls.incrementAndGet() == 1) {
                    throw failure;
                }
                retry.countDown();
                return RpcResponse.success("request", "done");
            })));
            assertTrue(retry.await(2, TimeUnit.SECONDS));
        } finally {
            strategy.shutdown();
        }
    }

    @Test
    void preservesPermanentExceptionWithoutSchedulingRetry() {
        FailbackClusterStrategy strategy = new FailbackClusterStrategy(0, 1);
        RpcException failure = new RpcException(RpcErrorCode.UNAUTHORIZED, "invalid token");
        try {
            assertSame(failure, assertThrows(RpcException.class,
                    () -> strategy.invoke(request(), () -> { throw failure; })));
        } finally {
            strategy.shutdown();
        }
    }

    @Test
    void rejectsNegativeRetryDelay() {
        assertThrows(IllegalArgumentException.class, () -> new FailbackClusterStrategy(-1, 1));
    }

    @Test
    void rejectsNegativeRetryCount() {
        assertThrows(IllegalArgumentException.class, () -> new FailbackClusterStrategy(1, -1));
    }

    @ParameterizedTest
    @EnumSource(value = RpcErrorCode.class, names = {"UNAVAILABLE", "OVERLOADED"})
    void queuesTransientResponsesUsingTheConfiguredFixedDelay(RpcErrorCode code) throws Exception {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(7, 2, scheduler, new AtomicLong(1_000));
        RpcResponse failure = RpcResponse.fail("request", code, "temporary");
        AtomicInteger calls = new AtomicInteger();

        assertSame(failure, strategy.invoke(request(), () -> calls.incrementAndGet() < 3
                ? failure : RpcResponse.success("request", "done")));
        assertEquals(1, calls.get(), "first result must return before a background retry runs");
        assertEquals(1, scheduler.pendingCount());
        scheduler.runNext();
        scheduler.runNext();

        assertEquals(3, calls.get());
        assertEquals(List.of(7L, 7L), scheduler.delaysSeconds);
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void successfulResponseDoesNotQueueWork() {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(1, 3, scheduler, new AtomicLong(1_000));
        RpcResponse success = RpcResponse.success("request", "done");

        assertSame(success, strategy.invoke(request(), () -> success));
        assertEquals(0, scheduler.pendingCount());
    }

    @ParameterizedTest
    @EnumSource(value = RpcErrorCode.class, names = {
            "UNAUTHORIZED", "INVALID_REQUEST", "SERIALIZATION_ERROR", "INTERNAL", "CANCELLED", "DEADLINE_EXCEEDED"})
    void permanentResponsesDoNotQueueWork(RpcErrorCode code) {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 3, scheduler, new AtomicLong(1_000));
        RpcResponse failure = RpcResponse.fail("request", code, "permanent");

        assertSame(failure, strategy.invoke(request(), () -> failure));
        assertEquals(0, scheduler.pendingCount());
    }

    @ParameterizedTest
    @EnumSource(value = RpcErrorCode.class, names = {
            "UNAUTHORIZED", "INVALID_REQUEST", "SERIALIZATION_ERROR", "INTERNAL", "CANCELLED", "DEADLINE_EXCEEDED"})
    void wrappedPermanentExceptionsDoNotQueueWork(RpcErrorCode code) {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 3, scheduler, new AtomicLong(1_000));
        RuntimeException failure = new RuntimeException(new RpcException(code, "permanent"));

        assertSame(failure, assertThrows(RuntimeException.class,
                () -> strategy.invoke(request(), () -> { throw failure; })));
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void wrappedTransientExceptionQueuesRetryWithoutReplacingTheException() throws Exception {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 1, scheduler, new AtomicLong(1_000));
        RuntimeException failure = new RuntimeException(new RpcException(RpcErrorCode.OVERLOADED, "busy"));
        AtomicInteger calls = new AtomicInteger();

        assertSame(failure, assertThrows(RuntimeException.class, () -> strategy.invoke(request(), () -> {
            if (calls.incrementAndGet() == 1) {
                throw failure;
            }
            return RpcResponse.success("request", "done");
        })));
        assertEquals(1, scheduler.pendingCount());
        scheduler.runNext();
        assertEquals(2, calls.get());
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void stopsAtTheConfiguredNumberOfBackgroundRetries() throws Exception {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 2, scheduler, new AtomicLong(1_000));
        RpcResponse failure = RpcResponse.fail("request", RpcErrorCode.UNAVAILABLE, "offline");
        AtomicInteger calls = new AtomicInteger();

        strategy.invoke(request(), () -> {
            if (calls.incrementAndGet() == 2) {
                throw new IllegalStateException("connection reset during retry");
            }
            return failure;
        });
        assertEquals(1, scheduler.pendingCount());
        scheduler.runNext();
        scheduler.runNext();

        assertEquals(3, calls.get());
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void permanentBackgroundFailureStopsFurtherRetries() throws Exception {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 3, scheduler, new AtomicLong(1_000));
        AtomicInteger calls = new AtomicInteger();

        strategy.invoke(request(), () -> RpcResponse.fail("request",
                calls.incrementAndGet() == 1 ? RpcErrorCode.UNAVAILABLE : RpcErrorCode.INTERNAL, "failed"));
        assertEquals(1, scheduler.pendingCount());
        scheduler.runNext();

        assertEquals(2, calls.get());
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void permanentBackgroundExceptionStopsFurtherRetries() throws Exception {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 3, scheduler, new AtomicLong(1_000));
        AtomicInteger calls = new AtomicInteger();

        strategy.invoke(request(), () -> {
            if (calls.incrementAndGet() == 1) {
                return RpcResponse.fail("request", RpcErrorCode.UNAVAILABLE, "offline");
            }
            throw new RpcException(RpcErrorCode.UNAUTHORIZED, "invalid token");
        });
        assertEquals(1, scheduler.pendingCount());
        scheduler.runNext();

        assertEquals(2, calls.get());
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void zeroRetriesPreservesTheExceptionWithoutQueueingWork() {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 0, scheduler, new AtomicLong(1_000));
        RuntimeException failure = new IllegalStateException("offline");

        assertSame(failure, assertThrows(RuntimeException.class,
                () -> strategy.invoke(request(), () -> { throw failure; })));
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void expiredRequestDoesNotQueueARetry() {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 3, scheduler, new AtomicLong(1_000));
        RpcResponse failure = RpcResponse.fail("request", RpcErrorCode.UNAVAILABLE, "offline");

        assertSame(failure, strategy.invoke(requestWithDeadline(1_000), () -> failure));
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void doesNotQueueWhenFixedDelayConsumesTheRemainingDeadline() {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(1, 3, scheduler, new AtomicLong(1_000));
        RpcException failure = new RpcException(RpcErrorCode.UNAVAILABLE, "offline");

        assertSame(failure, assertThrows(RpcException.class,
                () -> strategy.invoke(requestWithDeadline(2_000), () -> { throw failure; })));
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void queuedRetryChecksTheOriginalDeadlineAgainBeforeCallingInvoker() throws Exception {
        ManualScheduler scheduler = scheduler();
        AtomicLong now = new AtomicLong(1_000);
        FailbackClusterStrategy strategy = strategy(1, 3, scheduler, now);
        AtomicInteger calls = new AtomicInteger();

        strategy.invoke(requestWithDeadline(3_000), () -> {
            calls.incrementAndGet();
            return RpcResponse.fail("request", RpcErrorCode.UNAVAILABLE, "offline");
        });
        assertEquals(1, scheduler.pendingCount());
        now.set(3_000);
        scheduler.runNext();

        assertEquals(1, calls.get());
        assertEquals(0, scheduler.pendingCount());
    }

    @Test
    void closeCancelsQueuedRetriesPromptlyAndIsIdempotent() {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(60, 3, scheduler, new AtomicLong(1_000));
        AtomicInteger calls = new AtomicInteger();
        strategy.invoke(request(), () -> {
            calls.incrementAndGet();
            return RpcResponse.fail("request", RpcErrorCode.UNAVAILABLE, "offline");
        });
        assertEquals(1, scheduler.pendingCount());
        ScheduledFuture<?> pending = scheduler.tasks.getFirst();

        assertTimeout(Duration.ofSeconds(1), () -> {
            strategy.close();
            strategy.close();
            strategy.shutdown();
        });
        assertTrue(pending.isCancelled());
        assertTrue(scheduler.isShutdown());
        ((Runnable) pending).run();
        assertEquals(1, calls.get());
    }

    @Test
    void closedStrategyRejectsNewCallsBeforeInvokingSupplier() {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 3, scheduler, new AtomicLong(1_000));
        strategy.close();
        AtomicInteger calls = new AtomicInteger();

        assertThrows(IllegalStateException.class, () -> strategy.invoke(request(), () -> {
            calls.incrementAndGet();
            return RpcResponse.success("request", "done");
        }));
        assertEquals(0, calls.get());
    }

    @Test
    void schedulingRejectionDoesNotReplaceTheInitialFailure() {
        ManualScheduler scheduler = scheduler();
        FailbackClusterStrategy strategy = strategy(0, 3, scheduler, new AtomicLong(1_000));
        scheduler.shutdownNow();
        RpcException failure = new RpcException(RpcErrorCode.UNAVAILABLE, "offline");

        assertSame(failure, assertThrows(RpcException.class,
                () -> strategy.invoke(request(), () -> { throw failure; })));
    }

    @Test
    void statelessStrategyRemainsAFunctionalInterfaceWithNoOpClose() throws Exception {
        ClusterStrategy strategy = (request, invoker) -> invoker.get();
        RpcResponse success = RpcResponse.success("request", "done");

        strategy.close();
        strategy.close();
        assertSame(success, strategy.invoke(request(), () -> success));
    }

    private FailbackClusterStrategy strategy(long delaySeconds, int maxRetries,
                                             ManualScheduler scheduler, AtomicLong now) {
        FailbackClusterStrategy strategy = new FailbackClusterStrategy(delaySeconds, maxRetries, scheduler, now::get);
        strategies.add(strategy);
        return strategy;
    }

    private ManualScheduler scheduler() {
        ManualScheduler scheduler = new ManualScheduler();
        schedulers.add(scheduler);
        return scheduler;
    }

    private RpcRequest requestWithDeadline(long deadline) {
        RpcRequest request = request();
        RpcContext context = new RpcContext();
        context.setDeadlineEpochMillis(deadline);
        request.setContext(context.seal());
        return request;
    }

    private RpcRequest request() {
        RpcRequest request = new RpcRequest();
        request.setRequestId("request");
        return request;
    }

    /** Real scheduled futures with explicit dispatch, so timing assertions never rely on sleeping. */
    private static class ManualScheduler extends ScheduledThreadPoolExecutor {
        private final ArrayDeque<ScheduledFuture<?>> tasks = new ArrayDeque<>();
        private final List<Long> delaysSeconds = new ArrayList<>();

        ManualScheduler() {
            super(1, runnable -> {
                Thread thread = new Thread(runnable, "failback-test");
                thread.setDaemon(true);
                return thread;
            });
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            ScheduledFuture<?> future = super.schedule(command, 1, TimeUnit.DAYS);
            delaysSeconds.add(unit.toSeconds(delay));
            tasks.addLast(future);
            return future;
        }

        int pendingCount() {
            return (int) tasks.stream().filter(task -> !task.isDone()).count();
        }

        void runNext() throws Exception {
            RunnableScheduledFuture<?> future = (RunnableScheduledFuture<?>) tasks.removeFirst();
            getQueue().remove(future);
            future.run();
            future.get();
        }
    }
}
