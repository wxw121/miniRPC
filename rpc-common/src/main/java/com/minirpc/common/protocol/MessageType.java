package com.minirpc.common.protocol;

/**
 * RPC 消息类型枚举
 */
public enum MessageType {

    /**
     * 请求消息
     */
    REQUEST((byte) 1),

    /**
     * 响应消息
     */
    RESPONSE((byte) 2),

    /**
     * 心跳消息
     */
    HEARTBEAT((byte) 3),

    /**
     * 请求取消控制帧
     */
    CANCEL((byte) 4);

    private final byte code;

    MessageType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static MessageType fromCode(byte code) {
        for (MessageType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown MessageType code: " + code);
    }
}
