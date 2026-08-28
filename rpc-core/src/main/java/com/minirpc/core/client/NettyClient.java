package com.minirpc.core.client;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcCancellation;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.common.protocol.SerializerType;
import com.minirpc.core.codec.RpcMessageDecoder;
import com.minirpc.core.codec.RpcMessageEncoder;
import com.minirpc.core.config.RpcConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Netty RPC 客户端
 * <p>
 * 负责与服务端建立连接、发送 RPC 请求、接收响应。
 * 支持连接复用：同一地址的连接会被缓存复用。
 * <p>
 * 面试考点：
 * - Netty 客户端 Bootstrap 配置
 * - 连接复用与 Channel 管理
 * - 同步/异步调用实现
 * - CompletableFuture 超时控制
 */
public class NettyClient {

    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);

    /**
     * 默认超时时间（毫秒）
     */
    private static final long DEFAULT_TIMEOUT_MS = 5000;

    /**
     * 序列化类型
     */
    private final SerializerType serializerType;

    /**
     * 请求超时时间（毫秒，从 RpcConfig 读取）
     */
    private final long timeoutMs;

    /**
     * 连接池缓存：address -> ChannelPool
     * <p>
     * 面试考点：为什么用连接池而不是单连接？
     * - 单 Channel 在高并发下写入竞争导致 RT 抖动
     * - 多连接轮询选取，分散写入压力
     * - 某个连接断开时自动剔除并重建，不影响其他请求
     */
    private final Map<String, ChannelPool> poolCache = new ConcurrentHashMap<>();

    /**
     * 每个地址的连接池大小
     */
    private final int poolSize;

    private final EventLoopGroup group;
    private final Bootstrap bootstrap;
    private final SslContext sslContext;

    public NettyClient() {
        this(SerializerType.HESSIAN2);
    }

    public NettyClient(SerializerType serializerType) {
        if (serializerType == SerializerType.JDK) {
            throw new IllegalArgumentException("JDK serialization is disabled for RPC transport");
        }
        this.serializerType = serializerType;
        this.timeoutMs = DEFAULT_TIMEOUT_MS;
        this.poolSize = 4;
        this.group = new NioEventLoopGroup();
        this.sslContext = null;
        this.bootstrap = new Bootstrap();
        initBootstrap(5000, 30);
    }

    public NettyClient(RpcConfig config) {
        if (config.getSerializerType() == SerializerType.JDK) {
            throw new IllegalArgumentException("JDK serialization is disabled for RPC transport");
        }
        if (config.getConnectionPoolSize() < 1) {
            throw new IllegalArgumentException("connectionPoolSize must be positive");
        }
        this.serializerType = config.getSerializerType();
        this.timeoutMs = config.getTimeoutMs();
        this.poolSize = config.getConnectionPoolSize();
        this.group = new NioEventLoopGroup();
        this.sslContext = config.getTlsConfig().enabled() ? config.getTlsConfig().createClientSslContext() : null;
        this.bootstrap = new Bootstrap();
        initBootstrap(config.getConnectTimeoutMs(), config.getHeartbeatIntervalSeconds());
    }

    /**
     * 初始化 Bootstrap 配置
     */
    private void initBootstrap(int connectTimeoutMs, int heartbeatIntervalSeconds) {

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        if (sslContext != null) {
                            ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc()));
                        }
                        ch.pipeline()
                                // 写空闲检测：配置的心跳间隔无写操作则触发心跳
                                .addLast(new IdleStateHandler(0, heartbeatIntervalSeconds, 0, java.util.concurrent.TimeUnit.SECONDS))
                                .addLast(new LengthFieldBasedFrameDecoder(
                                        8 * 1024 * 1024, 5, 4, 0, 0))
                                .addLast(new RpcMessageDecoder())
                                .addLast(new RpcMessageEncoder())
                                // 心跳发送处理器
                                .addLast(new HeartbeatHandler())
                                .addLast(new RpcClientHandler());
                    }
                });
    }

    /**
     * 获取或创建 ChannelPool（连接池复用）
     */
    private ChannelPool getPool(String host, int port) {
        String address = host + ":" + port;
        return poolCache.computeIfAbsent(address, addr ->
                new ChannelPool(addr, poolSize, this::createChannel));
    }

    /**
     * 创建单个 Channel 连接
     */
    private Channel createChannel(String host, int port) {
        try {
            ChannelFuture future = bootstrap.connect(host, port).sync();
            Channel channel = future.channel();
            log.info("Connected to server: {}:{}", host, port);
            return channel;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("Failed to connect to server: " + host + ":" + port, e);
        }
    }

    /**
     * 同步发送 RPC 请求
     *
     * @param host    服务端地址
     * @param port    服务端端口
     * @param request RPC 请求
     * @return RPC 响应
     */
    public RpcResponse sendSync(String host, int port, RpcRequest request) {
        try {
            long remainingTimeoutMs = remainingTimeoutMs(request);
            if (remainingTimeoutMs <= 0) {
                throw new RpcException(RpcErrorCode.DEADLINE_EXCEEDED, "RPC deadline already exceeded");
            }
            CompletableFuture<RpcResponse> future = sendAsync(host, port, request);
            return future.get(remainingTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (RpcException e) {
            throw e;
        } catch (TimeoutException e) {
            RequestFutureHolder.remove(request.getRequestId());
            throw new RpcException(RpcErrorCode.DEADLINE_EXCEEDED, "RPC deadline exceeded", e);
        } catch (Exception e) {
            RequestFutureHolder.remove(request.getRequestId());
            throw new RpcException("RPC call failed: " + e.getMessage(), e);
        }
    }

    /**
     * 异步发送 RPC 请求
     *
     * @param host    服务端地址
     * @param port    服务端端口
     * @param request RPC 请求
     * @return CompletableFuture
     */
    public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
        long remainingTimeoutMs = remainingTimeoutMs(request);
        if (remainingTimeoutMs <= 0) {
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.DEADLINE_EXCEEDED, "RPC deadline already exceeded"));
        }
        if (request.getContext() != null) {
            request.getContext().seal();
        }
        // 先注册 Future（保证无论后续哪个环节失败，都能被清理）
        CompletableFuture<RpcResponse> future = RequestFutureHolder.register(request.getRequestId(), remainingTimeoutMs);
        AtomicReference<Channel> requestChannel = new AtomicReference<>();
        AtomicBoolean requestWasWritten = new AtomicBoolean();
        AtomicBoolean cancellationSent = new AtomicBoolean();
        future.whenComplete((response, error) -> {
            if (future.isCancelled()) {
                RequestFutureHolder.remove(request.getRequestId());
                sendCancellationIfNeeded(request.getRequestId(), requestChannel, requestWasWritten, cancellationSent);
            }
        });

        try {
            // 从连接池获取 Channel（轮询选取）
            ChannelPool pool = getPool(host, port);
            Channel channel = pool.acquire();

            requestChannel.set(channel);
            if (future.isCancelled()) {
                return future;
            }

            // 构建消息并发送
            RpcMessage message = new RpcMessage(MessageType.REQUEST, serializerType, request);
            ChannelFuture writeFuture = channel.writeAndFlush(message);
            requestWasWritten.set(true);
            if (future.isCancelled()) {
                sendCancellationIfNeeded(request.getRequestId(), requestChannel, requestWasWritten, cancellationSent);
            }
            writeFuture.addListener(f -> {
                if (!f.isSuccess()) {
                    RequestFutureHolder.completeExceptionally(
                            request.getRequestId(),
                            new RpcException("Failed to send request: " + f.cause().getMessage(), f.cause())
                    );
                }
            });
        } catch (Exception e) {
            // 连接失败时立即完成 Future，避免泄漏等待 TTL 清理
            RequestFutureHolder.completeExceptionally(
                    request.getRequestId(),
                    new RpcException("Failed to acquire channel: " + e.getMessage(), e)
            );
        }

        return future;
    }

    private void sendCancellationIfNeeded(String requestId, AtomicReference<Channel> requestChannel, AtomicBoolean requestWasWritten, AtomicBoolean cancellationSent) {
        Channel channel = requestChannel.get();
        if (channel != null && channel.isActive() && requestWasWritten.get() && cancellationSent.compareAndSet(false, true)) {
            channel.writeAndFlush(new RpcMessage(MessageType.CANCEL, serializerType, new RpcCancellation(requestId)));
        }
    }
    private long remainingTimeoutMs(RpcRequest request) {
        if (request.getContext() == null || request.getContext().getDeadlineEpochMillis() <= 0) {
            return timeoutMs;
        }
        return request.getContext().getDeadlineEpochMillis() - System.currentTimeMillis();
    }

    /**
     * 关闭客户端（优雅关闭：等待连接池中的连接释放）
     */
    public int getActiveConnectionCount() {
        return poolCache.values().stream().mapToInt(ChannelPool::getActiveCount).sum();
    }

    public void shutdown() {
        // 关闭所有连接池
        poolCache.values().forEach(ChannelPool::close);
        poolCache.clear();
        group.shutdownGracefully();
        log.info("RPC Client shutdown");
    }
}
