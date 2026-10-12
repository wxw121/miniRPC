package com.minirpc.core.client;

import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.SerializerType;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端心跳处理器
 * <p>
 * 当写空闲（一段时间没有发送数据）时，自动发送心跳包给服务端，
 * 保持连接活跃，防止被服务端误判为死连接而关闭。
 */
public class HeartbeatHandler extends ChannelDuplexHandler {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatHandler.class);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleEvent) {
            if (idleEvent.state() == IdleState.WRITER_IDLE) {
                // 写空闲，发送心跳
                RpcMessage heartbeat = new RpcMessage(MessageType.HEARTBEAT, SerializerType.JDK, null);
                ctx.writeAndFlush(heartbeat);
                log.debug("Sent heartbeat to server: {}", ctx.channel().remoteAddress());
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
