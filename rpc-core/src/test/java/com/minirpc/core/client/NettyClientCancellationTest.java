package com.minirpc.core.client;

import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcCancellation;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.SerializerType;
import com.minirpc.core.codec.RpcMessageDecoder;
import com.minirpc.core.codec.RpcMessageEncoder;
import com.minirpc.core.proxy.RpcClientProxy;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class NettyClientCancellationTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sendsOneCancellationFrameAfterAnAsyncRequestIsCancelled(boolean throughProxy) throws Exception {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup workers = new NioEventLoopGroup(1);
        NettyClient client = new NettyClient();
        try {
            CaptureHandler capture = new CaptureHandler();
            Channel server = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(8 * 1024 * 1024, 5, 4, 0, 0))
                                    .addLast(new RpcMessageDecoder())
                                    .addLast(new RpcMessageEncoder())
                                    .addLast(capture);
                        }
                    })
                    .bind(0)
                    .sync()
                    .channel();
            try {
                RpcRequest request = new RpcRequest();
                request.setRequestId("client-cancel-request");
                int port = ((java.net.InetSocketAddress) server.localAddress()).getPort();
                CompletableFuture<?> future;
                if (throughProxy) {
                    RpcClientProxy proxy = new RpcClientProxy(client, "127.0.0.1", port);
                    proxy.setAsyncMode(true);
                    future = proxy.createProxy(AsyncService.class).echo();
                } else {
                    future = client.sendAsync("127.0.0.1", port, request);
                }

                assertTrue(capture.requestReceived.await(3, TimeUnit.SECONDS));
                assertTrue(future.cancel(true));
                String requestId = capture.request.getRequestId();
                assertFalse(RequestFutureHolder.contains(requestId));
                assertTrue(capture.cancellationReceived.await(3, TimeUnit.SECONDS));
                assertEquals(requestId, ((RpcCancellation) capture.cancellation.getData()).getRequestId());
            } finally {
                server.close().sync();
            }
        } finally {
            client.shutdown();
            boss.shutdownGracefully().sync();
            workers.shutdownGracefully().sync();
        }
    }

    interface AsyncService {
        CompletableFuture<String> echo();
    }

    private static final class CaptureHandler extends ChannelInboundHandlerAdapter {
        private final CountDownLatch requestReceived = new CountDownLatch(1);
        private final CountDownLatch cancellationReceived = new CountDownLatch(1);
        private volatile RpcMessage cancellation;
        private volatile RpcRequest request;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            RpcMessage rpcMessage = (RpcMessage) message;
            if (rpcMessage.getMessageType() == MessageType.REQUEST) {
                request = (RpcRequest) rpcMessage.getData();
                requestReceived.countDown();
            } else if (rpcMessage.getMessageType() == MessageType.CANCEL) {
                cancellation = rpcMessage;
                cancellationReceived.countDown();
            }
        }
    }
}
