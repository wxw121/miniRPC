package com.minirpc.common.protocol;

import java.io.Serializable;

/**
 * RPC 响应对象
 * <p>
 * 封装远程方法调用的返回结果：
 * - requestId: 对应请求的ID，用于请求-响应匹配
 * - data: 方法返回值
 * - errorMessage: 异常信息（调用失败时）
 * - status: 响应状态码
 */
public class RpcResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 成功状态码
     */
    public static final byte STATUS_SUCCESS = 0;

    /**
     * 失败状态码
     */
    public static final byte STATUS_FAIL = 1;

    /**
     * 对应请求的ID
     */
    private String requestId;

    /**
     * 响应状态
     */
    private byte status;

    /**
     * 方法返回值
     */
    private Object data;

    /**
     * 异常信息
     */
    private String errorMessage;

    /** Stable failure category. Null is tolerated for responses produced by older peers. */
    private RpcErrorCode errorCode;

    public RpcResponse() {
    }

    public static RpcResponse success(String requestId, Object data) {
        RpcResponse response = new RpcResponse();
        response.setRequestId(requestId);
        response.setStatus(STATUS_SUCCESS);
        response.setData(data);
        response.setErrorCode(RpcErrorCode.OK);
        return response;
    }

    public static RpcResponse fail(String requestId, String errorMessage) {
        return fail(requestId, RpcErrorCode.INTERNAL, errorMessage);
    }

    public static RpcResponse fail(String requestId, RpcErrorCode errorCode, String errorMessage) {
        RpcResponse response = new RpcResponse();
        response.setRequestId(requestId);
        response.setStatus(STATUS_FAIL);
        response.setErrorCode(errorCode == null ? RpcErrorCode.INTERNAL : errorCode);
        response.setErrorMessage(errorMessage);
        return response;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public RpcErrorCode getErrorCode() {
        if (errorCode != null) {
            return errorCode;
        }
        return status == STATUS_SUCCESS ? RpcErrorCode.OK : RpcErrorCode.INTERNAL;
    }

    public void setErrorCode(RpcErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String toString() {
        return "RpcResponse{" +
                "requestId='" + requestId + '\'' +
                ", status=" + status +
                ", hasData=" + (data != null) +
                ", errorCode=" + getErrorCode() +
                '}';
    }
}
