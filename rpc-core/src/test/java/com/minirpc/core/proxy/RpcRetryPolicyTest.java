package com.minirpc.core.proxy;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.client.NettyClient;
import java.util.concurrent.CompletableFuture;
import com.minirpc.core.config.RpcConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcRetryPolicyTest {

    @Test
    void nonIdempotentOperationsFailFast() {
        FailingClient client = new FailingClient();
        try {
            WriteService service = proxy(client).createProxy(WriteService.class);
            assertThrows(RpcException.class, service::create);
            assertEquals(1, client.calls);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void idempotentOperationsMayUseConfiguredRetries() {
        FailingClient client = new FailingClient();
        try {
            ReadService service = proxy(client).createProxy(ReadService.class);
            assertThrows(RpcException.class, service::find);
            assertEquals(3, client.calls);
        } finally {
            client.shutdown();
        }
    }

    private RpcClientProxy proxy(NettyClient client) {
        RpcConfig config = RpcConfig.builder()
                .failoverMaxRetries(2)
                .retryBaseBackoffMs(0)
                .retryMaxBackoffMs(0)
                .build();
        return new RpcClientProxy(client, "127.0.0.1", 1, config);
    }

    interface WriteService {
        String create();
    }

    interface ReadService {
        @RpcIdempotent
        String find();
    }

    private static class FailingClient extends NettyClient {
        private int calls;

        FailingClient() {
            super(RpcConfig.defaultConfig());
        }

        @Override
        public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
            calls++;
            throw new RpcException(RpcErrorCode.UNAVAILABLE, "simulated network failure");
        }
    }
}
