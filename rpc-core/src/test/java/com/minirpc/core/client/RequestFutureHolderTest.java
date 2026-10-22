package com.minirpc.core.client;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class RequestFutureHolderTest {
    @Test
    void expiredHolderEntryPreservesDeadlineErrorCategory() throws Exception {
        var future = RequestFutureHolder.register("holder-deadline", 1);
        Thread.sleep(5);
        RequestFutureHolder.cleanupExpiredFutures();
        CompletionException failure = assertThrows(CompletionException.class, future::join);
        RpcException cause = assertInstanceOf(RpcException.class, failure.getCause());
        assertEquals(RpcErrorCode.DEADLINE_EXCEEDED, cause.getErrorCode());
        assertFalse(RequestFutureHolder.contains("holder-deadline"));
    }
}
