package com.minirpc.core.server;

import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.common.protocol.SerializerType;
import com.minirpc.core.observability.RpcInvocationEvent;
import com.minirpc.core.security.RpcAuthenticator;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RpcServerHandlerTest {

    @Test
    void rejectsUnauthenticatedRequestsBeforeBusinessExecutorSubmission() {
        CountingExecutor executor = new CountingExecutor(false);
        EmbeddedChannel channel = new EmbeddedChannel(new RpcServerHandler(
                executor,
                new GracefulShutdownManager(),
                request -> RpcAuthenticator.AuthenticationResult.deny("missing credentials"),
                List.of()));

        channel.writeInbound(requestMessage());

        RpcResponse response = response(channel.readOutbound());
        assertEquals(RpcErrorCode.UNAUTHORIZED, response.getErrorCode());
        assertEquals(0, executor.submissionCount);
    }

    @Test
    void emitsOneServerEventForAuthenticationRejection() {
        CountingExecutor executor = new CountingExecutor(false);
        List<RpcInvocationEvent> events = new ArrayList<>();
        EmbeddedChannel channel = new EmbeddedChannel(new RpcServerHandler(
                executor,
                new GracefulShutdownManager(),
                request -> RpcAuthenticator.AuthenticationResult.deny("missing credentials"),
                List.of(events::add)));

        channel.writeInbound(requestMessage());

        assertEquals(1, events.size());
        assertEquals("server", events.get(0).side());
        assertEquals(RpcErrorCode.UNAUTHORIZED, events.get(0).errorCode());
        assertEquals(0, executor.submissionCount);
    }

    @Test
    void mapsRejectedBusinessSubmissionToOverloadedResponse() {
        CountingExecutor executor = new CountingExecutor(true);
        EmbeddedChannel channel = new EmbeddedChannel(new RpcServerHandler(
                executor,
                new GracefulShutdownManager(),
                RpcAuthenticator.allowAll(),
                List.of()));

        channel.writeInbound(requestMessage());

        RpcResponse response = response(channel.readOutbound());
        assertEquals(RpcErrorCode.OVERLOADED, response.getErrorCode());
        assertEquals(1, executor.submissionCount);
    }

    private RpcMessage requestMessage() {
        RpcRequest request = new RpcRequest();
        request.setRequestId("server-handler-test");
        request.setInterfaceName("example.Service");
        request.setMethodName("call");
        return new RpcMessage(MessageType.REQUEST, SerializerType.HESSIAN2, request);
    }

    private RpcResponse response(RpcMessage message) {
        return (RpcResponse) message.getData();
    }

    private static final class CountingExecutor extends AbstractExecutorService {
        private final boolean reject;
        private int submissionCount;

        private CountingExecutor(boolean reject) {
            this.reject = reject;
        }

        @Override
        public void execute(Runnable command) {
            submissionCount++;
            if (reject) {
                throw new RejectedExecutionException("test rejection");
            }
            command.run();
        }

        @Override public void shutdown() { }
        @Override public List<Runnable> shutdownNow() { return List.of(); }
        @Override public boolean isShutdown() { return false; }
        @Override public boolean isTerminated() { return false; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
    }
}
