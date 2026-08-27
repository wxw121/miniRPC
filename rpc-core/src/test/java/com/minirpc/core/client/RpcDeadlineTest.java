package com.minirpc.core.client;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcContext;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcDeadlineTest {

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
