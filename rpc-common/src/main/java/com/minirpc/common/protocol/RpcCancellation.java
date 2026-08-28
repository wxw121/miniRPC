package com.minirpc.common.protocol;

import java.io.Serializable;

/** Control-frame body requesting cancellation of one queued invocation. */
public final class RpcCancellation implements Serializable {

    private static final long serialVersionUID = 1L;

    private String requestId;

    public RpcCancellation() {
    }

    public RpcCancellation(String requestId) {
        setRequestId(requestId);
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("cancellation requestId must not be blank");
        }
        this.requestId = requestId;
    }
}
