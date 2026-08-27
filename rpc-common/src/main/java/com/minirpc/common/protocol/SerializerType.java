package com.minirpc.common.protocol;

/**
 * 序列化类型枚举
 */
public enum SerializerType {

    /**
     * JDK 原生序列化
     */
    JDK((byte) 0),

    /**
     * Hessian2 序列化
     */
    HESSIAN2((byte) 1),

    /**
     * Protostuff 序列化（Protobuf 兼容，无需 .proto 文件）
     */
    PROTOSTUFF((byte) 2);

    private final byte code;

    SerializerType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static SerializerType fromCode(byte code) {
        for (SerializerType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown SerializerType code: " + code);
    }
}
