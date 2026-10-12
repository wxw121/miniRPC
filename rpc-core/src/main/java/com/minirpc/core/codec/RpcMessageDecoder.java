package com.minirpc.core.codec;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcCancellation;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.common.protocol.SerializerType;
import com.minirpc.common.serialize.Serializer;
import com.minirpc.common.serialize.SerializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * RPC 消息解码器
 * <p>
 * 将网络字节流解码为 RpcMessage 对象。
 * 配合 LengthFieldBasedFrameDecoder 使用，解决 TCP 粘包/拆包问题。
 */
public class RpcMessageDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(RpcMessageDecoder.class);

    /**
     * 消息体最大长度（8MB），防止恶意报文导致 OOM
     */
    private static final int MAX_BODY_LENGTH = 8 * 1024 * 1024;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 至少需要协议头长度才能解析
        if (in.readableBytes() < RpcMessage.HEADER_LENGTH) {
            return;
        }

        // 标记当前读指针位置
        in.markReaderIndex();

        // 1. 读取并校验魔数 (2 bytes)
        short magic = in.readShort();
        if (magic != RpcMessage.MAGIC_NUMBER) {
            in.resetReaderIndex();
            throw new RpcException("Invalid magic number: " + magic + ", expected: " + RpcMessage.MAGIC_NUMBER);
        }

        // 2. 读取并校验版本号 (1 byte)
        byte version = in.readByte();
        if (!RpcMessage.isSupportedVersion(version)) {
            in.resetReaderIndex();
            throw new RpcException("Unsupported protocol version: " + version);
        }

        // 3. 读取消息类型 (1 byte)
        byte messageTypeCode = in.readByte();
        MessageType messageType = MessageType.fromCode(messageTypeCode);

        // 4. 读取序列化类型 (1 byte)
        byte serializerTypeCode = in.readByte();
        SerializerType serializerType = SerializerType.fromCode(serializerTypeCode);

        // 5. 读取消息体长度 (4 bytes)
        int bodyLength = in.readInt();

        // 安全校验：防止恶意报文伪造超大 bodyLength 导致 OOM
        if (bodyLength < 0 || bodyLength > MAX_BODY_LENGTH) {
            throw new RpcException("Invalid body length: " + bodyLength + ", max allowed: " + MAX_BODY_LENGTH);
        }

        // 6. 判断消息体是否完整（解决拆包问题）
        if (in.readableBytes() < bodyLength) {
            // 数据不完整，重置读指针，等待更多数据
            in.resetReaderIndex();
            return;
        }

        // 7. 读取消息体
        byte[] bodyBytes = new byte[bodyLength];
        in.readBytes(bodyBytes);

        // 8. 反序列化消息体
        Object data = null;
        if (bodyLength > 0) {
            Serializer serializer = SerializerFactory.getNetworkSerializer(serializerType);
            if (messageType == MessageType.REQUEST) {
                RpcRequest request = serializer.deserialize(bodyBytes, RpcRequest.class);
                if (request.getContext() != null) {
                    request.getContext().seal();
                }
                data = request;
            } else if (messageType == MessageType.RESPONSE) {
                data = serializer.deserialize(bodyBytes, RpcResponse.class);
            } else if (messageType == MessageType.CANCEL) {
                data = serializer.deserialize(bodyBytes, RpcCancellation.class);
            }
        }

        // 9. 构建 RpcMessage
        RpcMessage rpcMessage = new RpcMessage(messageType, serializerType, data);
        out.add(rpcMessage);

        if (log.isDebugEnabled()) {
            log.debug("Decoded message: type={}, bodyLength={}", messageType, bodyLength);
        }
    }
}
