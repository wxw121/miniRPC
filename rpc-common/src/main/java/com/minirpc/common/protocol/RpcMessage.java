package com.minirpc.common.protocol;

/**
 * RPC 协议消息
 * <p>
 * 协议格式：
 * <pre>
 * +--------+--------+---------+-----------+---------+------+
 * | Magic  | Version| MsgType | SerType   | BodyLen | Body |
 * | 2bytes | 1byte  | 1byte   | 1byte     | 4bytes  | N    |
 * +--------+--------+---------+-----------+---------+------+
 * </pre>
 * <p>
 * Magic Number: 0x1234，用于标识 RPC 协议，快速过滤非法数据包
 * Version: 协议版本号，用于后续协议升级兼容
 * MessageType: 消息类型（REQUEST / RESPONSE / HEARTBEAT）
 * SerializerType: 序列化方式（JDK / HESSIAN2）
 * BodyLength: 消息体长度，用于解决 TCP 粘包/拆包问题
 * Body: 序列化后的业务数据（RpcRequest 或 RpcResponse）
 */
public class RpcMessage {

    /**
     * 协议魔数，用于标识 RPC 协议
     */
    public static final short MAGIC_NUMBER = (short) 0x1234;

    /**
     * 协议版本
     */
    public static final byte VERSION = 2;

    public static boolean isSupportedVersion(byte version) {
        return version == 1 || version == VERSION;
    }

    /**
     * 协议头固定长度: magic(2) + version(1) + msgType(1) + serType(1) + bodyLength(4) = 9
     */
    public static final int HEADER_LENGTH = 9;

    /**
     * 消息类型
     */
    private MessageType messageType;

    /**
     * 序列化类型
     */
    private SerializerType serializerType;

    /**
     * 消息体（RpcRequest 或 RpcResponse 对象）
     */
    private Object data;

    public RpcMessage() {
    }

    public RpcMessage(MessageType messageType, SerializerType serializerType, Object data) {
        this.messageType = messageType;
        this.serializerType = serializerType;
        this.data = data;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public SerializerType getSerializerType() {
        return serializerType;
    }

    public void setSerializerType(SerializerType serializerType) {
        this.serializerType = serializerType;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
