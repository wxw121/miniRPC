package com.minirpc.core.server;

import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcCancellation;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.SerializerType;
import com.minirpc.core.registry.LocalServiceRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcServerHandlerCancellationTest {

    @Test
    void cancelsQueuedInvocationWithoutExecutingBusinessCode() throws Exception {
        CountDownLatch workerOccupied = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        GracefulShutdownManager shutdownManager = new GracefulShutdownManager();
        AtomicBoolean invoked = new AtomicBoolean();
        LocalServiceRegistry.registerService(CancellationService.class.getName(),
                (CancellationService) () -> invoked.set(true));
        executor.execute(() -> {
            workerOccupied.countDown();
            await(releaseWorker);
        });
        assertTrue(workerOccupied.await(3, TimeUnit.SECONDS));

        EmbeddedChannel channel = new EmbeddedChannel(new RpcServerHandler(executor, shutdownManager));
        try {
            channel.writeInbound(requestMessage("queued-cancellation"));
            assertEquals(1, shutdownManager.getInFlightCount());

            channel.writeInbound(new RpcMessage(MessageType.CANCEL, SerializerType.HESSIAN2,
                    new RpcCancellation("queued-cancellation")));
            releaseWorker.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

            assertFalse(invoked.get());
            assertEquals(0, shutdownManager.getInFlightCount());
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void doesNotInterruptInvocationThatHasAlreadyStarted() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        GracefulShutdownManager shutdownManager = new GracefulShutdownManager();
        AtomicBoolean invoked = new AtomicBoolean();
        LocalServiceRegistry.registerService(CancellationService.class.getName(), (CancellationService) () -> {
            invoked.set(true);
            started.countDown();
            await(release);
        });
        EmbeddedChannel channel = new EmbeddedChannel(new RpcServerHandler(executor, shutdownManager));
        try {
            channel.writeInbound(requestMessage("started-cancellation"));
            assertTrue(started.await(3, TimeUnit.SECONDS));

            channel.writeInbound(new RpcMessage(MessageType.CANCEL, SerializerType.HESSIAN2,
                    new RpcCancellation("started-cancellation")));
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

            assertTrue(invoked.get());
            assertEquals(0, shutdownManager.getInFlightCount());
        } finally {
            release.countDown();
            executor.shutdownNow();
            channel.finishAndReleaseAll();
        }
    }

    private static RpcMessage requestMessage(String requestId) {
        RpcRequest request = new RpcRequest();
        request.setRequestId(requestId);
        request.setInterfaceName(CancellationService.class.getName());
        request.setMethodName("run");
        return new RpcMessage(MessageType.REQUEST, SerializerType.HESSIAN2, request);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    interface CancellationService {
        void run();
    }
}
