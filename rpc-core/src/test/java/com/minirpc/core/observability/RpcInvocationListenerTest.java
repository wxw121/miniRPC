package com.minirpc.core.observability;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.client.NettyClient;
import java.util.concurrent.CompletableFuture;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.proxy.RpcClientProxy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcInvocationListenerTest {

    @Test
    void emitsOneClientEventForEachCompletedNetworkAttempt() {
        FailingClient client = new FailingClient();
        List<RpcInvocationEvent> events = new ArrayList<>();
        try {
            RpcClientProxy proxy = new RpcClientProxy(client, "127.0.0.1", 9100,
                    RpcConfig.builder().failoverMaxRetries(0).build());
            proxy.setInvocationListeners(List.of(events::add));
            ReadService service = proxy.createProxy(ReadService.class);

            assertThrows(RpcException.class, service::find);

            assertEquals(1, events.size());
            assertEquals("client", events.get(0).side());
            assertEquals(RpcErrorCode.UNAVAILABLE, events.get(0).errorCode());
            assertEquals("127.0.0.1:9100", events.get(0).targetEndpoint());
        } finally {
            client.shutdown();
        }
    }

    interface ReadService {
        @RpcIdempotent
        String find();
    }

    private static final class FailingClient extends NettyClient {
        private FailingClient() {
            super(RpcConfig.defaultConfig());
        }

        @Override
        public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
            throw new RpcException(RpcErrorCode.UNAVAILABLE, "simulated network failure");
        }
    }
}
