package com.minirpc.core.cluster;

import com.minirpc.common.protocol.RpcContext;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    @Test
    void admitsRetryableFailureWhenDeadlineCoversTheBackoffWindow() {
        RetryPolicy policy = new RetryPolicy(2, 100, 1_000);

        assertTrue(policy.nextDelayMillis(requestWithDeadline(1_000), 1,
                RpcErrorCode.UNAVAILABLE, 100).isPresent());
    }

    @Test
    void rejectsRetryWhenDeadlineCannotCoverTheBackoffWindow() {
        RetryPolicy policy = new RetryPolicy(2, 100, 1_000);

        assertTrue(policy.nextDelayMillis(requestWithDeadline(150), 1,
                RpcErrorCode.UNAVAILABLE, 100).isEmpty());
    }

    @Test
    void rejectsNonRetryableErrorCodes() {
        RetryPolicy policy = new RetryPolicy(2, 100, 1_000);

        assertTrue(policy.nextDelayMillis(requestWithDeadline(1_000), 1,
                RpcErrorCode.UNAUTHORIZED, 100).isEmpty());
    }

    private RpcRequest requestWithDeadline(long deadlineEpochMillis) {
        RpcRequest request = new RpcRequest();
        RpcContext context = new RpcContext();
        context.setDeadlineEpochMillis(deadlineEpochMillis);
        request.setContext(context);
        return request;
    }
}
