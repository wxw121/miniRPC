package com.minirpc.common.exception;

import com.minirpc.common.protocol.RpcErrorCode;

/**
 * RPC 框架统一异常
 * <p>
 * 所有 RPC 框架内部异常均继承此类，便于统一捕获和处理。
 */
public class RpcException extends RuntimeException {

    private final RpcErrorCode errorCode;

    public RpcException(String message) {
        this(RpcErrorCode.INTERNAL, message, null);
    }

    public RpcException(String message, Throwable cause) {
        this(RpcErrorCode.INTERNAL, message, cause);
    }

    public RpcException(Throwable cause) {
        this(RpcErrorCode.INTERNAL, cause == null ? null : cause.getMessage(), cause);
    }

    public RpcException(RpcErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    public RpcException(RpcErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode == null ? RpcErrorCode.INTERNAL : errorCode;
    }

    public RpcErrorCode getErrorCode() {
        return errorCode;
    }
}
