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

        context.putAttachment("tenant", "local");
        assertThrows(IllegalArgumentException.class,
                () -> context.putAttachment("x".repeat(65), "value"));
    }
}
