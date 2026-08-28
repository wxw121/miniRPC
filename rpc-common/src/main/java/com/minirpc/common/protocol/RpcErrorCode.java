package com.minirpc.common.protocol;

/**
 * Stable error categories returned over the RPC boundary.
 * Exception messages are diagnostic only; callers must make retry decisions from this code.
 */
public enum RpcErrorCode {
    OK,
    UNAVAILABLE,
    DEADLINE_EXCEEDED,
    OVERLOADED,
    UNAUTHORIZED,
    SERIALIZATION_ERROR,
    INVALID_REQUEST,
    CANCELLED,
    INTERNAL
}
