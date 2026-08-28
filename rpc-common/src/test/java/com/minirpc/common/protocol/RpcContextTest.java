package com.minirpc.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcContextTest {

    @Test
    void detectsDeadlineAndProtectsAttachmentLimits() {
        RpcContext context = new RpcContext();
        context.setDeadlineEpochMillis(100L);

        assertFalse(context.isExpired(99L));
        assertTrue(context.isExpired(100L));

        context.putAttachment("auth.token", "local");
        assertThrows(IllegalArgumentException.class,
                () -> context.putAttachment("x".repeat(65), "value"));
    }

    @Test
    void sealsMetadataAndRestrictsAttachmentsToTheProtocolWhitelist() {
        RpcContext context = new RpcContext();
        context.putAttachment("retryAttempt", "0");
        context.seal();

        assertThrows(IllegalStateException.class, () -> context.putAttachment("retryAttempt", "1"));
        assertThrows(IllegalStateException.class, () -> context.setTraceId("next-trace"));
        assertThrows(UnsupportedOperationException.class,
                () -> context.getAttachments().put("retryAttempt", "1"));
        assertThrows(IllegalArgumentException.class, () -> new RpcContext().putAttachment("unknown", "value"));
    }

    @Test
    void createsASealedRetryCopyWithoutMutatingTheOriginalContext() {
        RpcContext context = new RpcContext();
        context.setTraceId("trace-1");

        RpcContext retry = context.withRetryAttempt(2);

        assertTrue(retry.isSealed());
        assertTrue(context.getAttachments().isEmpty());
        assertThrows(IllegalStateException.class, () -> retry.putAttachment("retryAttempt", "3"));
    }
}
