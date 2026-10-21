package com.minirpc.core.client;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcContext;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import io.netty.handler.codec.EncoderException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcDeadlineTest {

    @Test
    void encodingFailureIsNotClassifiedAsRetryableTransportFailure() {
        NettyClient client = new NettyClient() {
            @Override
            public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
                return CompletableFuture.failedFuture(new EncoderException("serialization failed"));
            }
        };
        try {
            RpcRequest request = new RpcRequest();
            request.setRequestId("encoding-failure");
            RpcException failure = assertThrows(RpcException.class,
                    () -> client.sendSync("127.0.0.1", 1, request));
            assertEquals(RpcErrorCode.SERIALIZATION_ERROR, failure.getErrorCode());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void synchronousWaitPreservesTypedFailuresFromTheResponseFuture() {
        RpcException original = new RpcException(RpcErrorCode.UNAUTHORIZED, "invalid token");
        NettyClient client = new NettyClient() {
            @Override
            public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
                return CompletableFuture.failedFuture(original);
            }
        };
        try {
            RpcRequest request = new RpcRequest();
            request.setRequestId("typed-failure");
            RpcException failure = assertThrows(RpcException.class,
                    () -> client.sendSync("127.0.0.1", 1, request));
            org.junit.jupiter.api.Assertions.assertSame(original, failure);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void rejectsExpiredRequestsBeforeOpeningConnection() {
        NettyClient client = new NettyClient();
        try {
            RpcRequest request = new RpcRequest();
            request.setRequestId("expired-request");
            RpcContext context = new RpcContext();
            context.setDeadlineEpochMillis(System.currentTimeMillis() - 1);
            request.setContext(context);

            CompletionException error = assertThrows(CompletionException.class,
                    () -> client.sendAsync("127.0.0.1", 1, request).join());
            assertEquals(RpcErrorCode.DEADLINE_EXCEEDED,
                    ((RpcException) error.getCause()).getErrorCode());
        } finally {
            client.shutdown();
        }
    }
}
