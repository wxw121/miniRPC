package com.minirpc.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

class RpcMessageLoggingTest {

    @Test
    void requestToStringDoesNotExposeBusinessArguments() {
        RpcRequest request = new RpcRequest();
        request.setParameters(new Object[]{"secret-request-value"});

        assertFalse(request.toString().contains("secret-request-value"));
    }

    @Test
    void responseToStringDoesNotExposeBusinessResult() {
        RpcResponse response = RpcResponse.success("request-1", "secret-response-value");

        assertFalse(response.toString().contains("secret-response-value"));
    }
}
