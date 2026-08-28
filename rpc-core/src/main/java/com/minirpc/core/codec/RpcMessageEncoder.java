package com.minirpc.core.codec;

import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcCancellation;
import com.minirpc.common.serialize.Serializer;
import com.minirpc.common.serialize.SerializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.EncoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC 消息编码器
 * <p>
 * 将 RpcMessage 对象编码为字节流，写入网络。
 * <p>
 * 协议格式：
 * <pre>
 * +--------+--------+---------+-----------+---------+------+
 * | Magic  | Version| MsgType | SerType   | BodyLen | Body |
 * | 2bytes | 1byte  | 1byte   | 1byte     | 4bytes  | N    |
 * +--------+--------+---------+-----------+---------+------+
 * </pre>
 * <p>
 * 面试考点：Netty 编解码器、ByteBuf 操作、协议设计
 */
public class RpcMessageEncoder extends MessageToByteEncoder<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(RpcMessageEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage msg, ByteBuf out) {
        // 1. 写入魔数 (2 bytes)
        if (msg.getMessageType() == MessageType.CANCEL && !(msg.getData() instanceof RpcCancellation)) {
            throw new EncoderException("CANCEL body must be RpcCancellation");
        }
        out.writeShort(RpcMessage.MAGIC_NUMBER);
        // 2. 写入版本号 (1 byte)
        out.writeByte(RpcMessage.VERSION);
        // 3. 写入消息类型 (1 byte)
        out.writeByte(msg.getMessageType().getCode());
        // 4. 写入序列化类型 (1 byte)
        out.writeByte(msg.getSerializerType().getCode());

        // 5. 序列化消息体
        byte[] bodyBytes = new byte[0];
        if (msg.getData() != null) {
            Serializer serializer = SerializerFactory.getNetworkSerializer(msg.getSerializerType());
            bodyBytes = serializer.serialize(msg.getData());
        }

        // 6. 写入消息体长度 (4 bytes)
        out.writeInt(bodyBytes.length);
        // 7. 写入消息体
        out.writeBytes(bodyBytes);

        if (log.isDebugEnabled()) {
            log.debug("Encoded message: type={}, bodyLength={}", msg.getMessageType(), bodyBytes.length);
        }
    }
}
