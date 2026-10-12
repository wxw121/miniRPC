package com.minirpc.core.client;

import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC 客户端响应处理器
 * <p>
 * 处理服务端返回的 RpcResponse，通过 requestId 匹配对应的 CompletableFuture 并完成回调。
 */
public class RpcClientHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(RpcClientHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
        if (msg.getMessageType() != MessageType.RESPONSE) {
            log.warn("Received non-response message: {}", msg.getMessageType());
            return;
        }

        RpcResponse response = (RpcResponse) msg.getData();
        log.debug("Received RPC response: requestId={}", response.getRequestId());

        // 通过 requestId 找到对应的 CompletableFuture 并完成
        RequestFutureHolder.complete(response.getRequestId(), response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Client handler exception", cause);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("Server connection closed: {}", ctx.channel().remoteAddress());
    }
}
