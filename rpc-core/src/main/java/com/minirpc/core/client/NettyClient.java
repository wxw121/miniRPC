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
import io.netty.handler.codec.EncoderException;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Netty RPC 客户端
 * <p>
 * 负责与服务端建立连接、发送 RPC 请求、接收响应。
 * 支持连接复用：同一地址的连接会被缓存复用。
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
     */
    private final Map<String, ChannelPool> poolCache = new ConcurrentHashMap<>();

    /**
     * 每个地址的连接池大小
     */
    private final int poolSize;

    /**
     * 客户端的网络事件循环组，当前使用 NioEventLoopGroup。
     * 每条 Channel 注册到其中一个 EventLoop，由其处理网络读写事件、消息处理器和定时任务；
     * 一个 EventLoop 可以处理多条连接。客户端关闭时通过 shutdownGracefully() 释放线程资源。
     */
    private final EventLoopGroup group;

    /**
     * 客户端连接引导器，保存事件循环组、Channel 类型、连接超时和处理器链等配置。
     * 调用 connect(host, port) 时按这些配置创建并连接 Channel；同一个 Bootstrap 可用于创建多条连接。
     */
    private final Bootstrap bootstrap;

    /**
     * TLS 上下文，保存证书、信任规则等安全配置；未启用 TLS 时为 null。
     * 启用时为每条连接创建独立的 SslHandler，安装在编解码器之前，负责 TLS 握手和数据加解密。
     * SslContext 在当前客户端的连接之间复用，具体连接的 TLS 处理状态由各自的 SslHandler 管理。
     */
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
        } catch (ExecutionException e) {
            RequestFutureHolder.remove(request.getRequestId());
            // Future.get 只增加等待包装层，不能把底层已有的错误类别改成 INTERNAL。
            throw transportFailure("RPC call failed", e.getCause());
        } catch (Exception e) {
            RequestFutureHolder.remove(request.getRequestId());
            throw new RpcException("RPC call failed: " + e.getMessage(), e);
        }
    }

    /**
     * 发起一次 RPC 调用，返回承载服务端响应的 Future，让调用方自行选择回调处理或阻塞等待。
     *
     * 执行流程：
     * 1. 检查剩余时间：请求已过期则返回失败 Future；未过期则封存请求上下文。
     * 2. 登记响应 Future：保存 requestId 与 Future 的映射，准备发送状态，并登记取消回调。
     * 3. 获取连接并发送：从连接池取得 Channel，发送前后检查取消状态，提交请求并监听写操作失败；
     *    取连接或提交发送时直接抛出的异常也会使响应 Future 异常完成，并清理映射。
     * 4. 返回响应 Future：本方法不等待业务响应；响应到达后，由 RpcClientHandler
     *    通过 RequestFutureHolder 按 requestId 完成这个 Future。
     *
     * 设计原因：
     * 1. 先登记再发送：避免响应过快，接收线程收到响应时却找不到对应的 Future。
     * 2. 分开发送结果与响应结果：ChannelFuture 只表示写操作结果，写成功不代表服务端已执行业务；
     *    写失败则立即清理映射并让响应 Future 异常完成。
     * 3. 复用同一套调用逻辑：异步调用方登记结果回调，sendSync() 通过 get() 等待，
     *    共用这里的发送和响应匹配逻辑。
     * 4. 协调取消与发送：取消 Future 时清理本地映射；请求已提交且原连接有效时尝试发送 CANCEL，
     *    发送前后的状态检查用于补偿取消与发送的交错，原子标志用于避免重复尝试发送取消消息。
     *
     * 行为边界：
     * 1. 不等待业务响应，但获取连接时可能同步等待建连，因此整个方法并非完全不阻塞。
     * 2. 过期时间由 RequestFutureHolder 定期检查，不是精确到截止时刻的超时调度。
     * 3. 发送 CANCEL 不保证远端业务被中断；当前服务端仅取消尚未开始执行的排队任务。
     *
     * @param host    服务端地址
     * @param port    服务端端口
     * @param request 要发送的 RPC 请求，其 requestId 用于关联后续响应
     * @return RPC 响应的 Future；请求已过期时返回已经异常完成的 Future
     */
    public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
        // 已过期时返回失败 Future，统一交由调用方通过 Future 处理，不再获取连接或发送请求。
        long remainingTimeoutMs = remainingTimeoutMs(request);
        if (remainingTimeoutMs <= 0) {
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.DEADLINE_EXCEEDED, "RPC deadline already exceeded"));
        }
        // 封存调用元数据，阻止发送后继续通过 setter 修改 deadline、traceId 或附件。
        if (request.getContext() != null) {
            request.getContext().seal();
        }
        // 先建立 requestId -> Future 映射，既能接住快速返回的响应，也能按编号清理后续发送失败的请求。
        CompletableFuture<RpcResponse> future = RequestFutureHolder.register(request.getRequestId(), remainingTimeoutMs);

        // 登记取消回调时还没有取得连接，用原子容器保存后续选中的 Channel，供回调访问。
        AtomicReference<Channel> requestChannel = new AtomicReference<>();
        // 只表示已调用 writeAndFlush 提交请求，不代表写操作成功或服务端已经收到请求。
        AtomicBoolean requestWasWritten = new AtomicBoolean();
        // 由取消发送方法通过 CAS 更新，多个触发路径只能有一个尝试发送 CANCEL。
        AtomicBoolean cancellationSent = new AtomicBoolean();

        // 此处只是登记回调，不等待响应，也不自动创建线程；仅在 Future 被取消时执行额外清理。
        future.whenComplete((response, error) -> {
            if (future.isCancelled()) {
                RequestFutureHolder.remove(request.getRequestId());
                sendCancellationIfNeeded(request.getRequestId(), requestChannel, requestWasWritten, cancellationSent);
            }
        });

        try {
            // 复用该地址的连接池并轮询选取连接；选中的连接不可用时，acquire() 可能同步等待重建。
            ChannelPool pool = getPool(host, port);
            Channel channel = pool.acquire();

            // 保存实际发送连接，使后续 CANCEL 与原请求使用同一条连接。
            requestChannel.set(channel);
            // 发送前发现已取消，直接返回，避免继续提交这次请求。
            if (future.isCancelled()) {
                return future;
            }

            // writeFuture 只反映本次写操作的结果；上面的 future 才承载完整的 RPC 响应。
            RpcMessage message = new RpcMessage(MessageType.REQUEST, serializerType, request);
            ChannelFuture writeFuture = channel.writeAndFlush(message);
            requestWasWritten.set(true);

            // 取消可能发生在前一次检查与标志更新之间，回调当时看到“尚未提交”就不会发 CANCEL。
            // 提交后再检查一次以补偿这种交错；取消回调和此处重复触发时，由 cancellationSent 的 CAS 去重。
            if (future.isCancelled()) {
                sendCancellationIfNeeded(request.getRequestId(), requestChannel, requestWasWritten, cancellationSent);
            }

            // 异步写失败时立即通知调用方并清理映射，避免仅等待响应或定时清理。
            // 写成功不完成响应 Future，仍由响应处理器按 requestId 匹配结果。
            writeFuture.addListener(f -> {
                if (!f.isSuccess()) {
                    RequestFutureHolder.completeExceptionally(
                            request.getRequestId(),
                            transportFailure("Failed to send request", f.cause())
                    );
                }
            });
        } catch (Exception e) {
            // 获取连接或提交发送时直接抛出的异常也转为响应 Future 的失败，并立即移除映射，无需等待定时清理。
            RequestFutureHolder.completeExceptionally(
                    request.getRequestId(),
                    transportFailure("Failed to acquire channel", e)
            );
        }

        // 返回的仍是最初登记的响应 Future，此处不调用 get() 等待服务端响应。
        return future;
    }

    private RpcException transportFailure(String message, Throwable cause) {
        if (cause instanceof RpcException rpcException) {
            return rpcException;
        }
        RpcErrorCode code = cause instanceof EncoderException
                ? RpcErrorCode.SERIALIZATION_ERROR : RpcErrorCode.UNAVAILABLE;
        return new RpcException(code, message, cause);
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
