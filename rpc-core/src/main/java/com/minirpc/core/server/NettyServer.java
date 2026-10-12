package com.minirpc.core.server;

import com.minirpc.core.codec.RpcMessageDecoder;
import com.minirpc.core.codec.RpcMessageEncoder;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.observability.RpcInvocationListener;
import com.minirpc.core.security.RpcAuthenticator;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty RPC 服务端
 * <p>
 * 基于 Netty NIO 实现的 RPC 服务端，采用 Reactor 线程模型：
 * - BossGroup: 负责接收客户端连接（Accept）
 * - WorkerGroup: 负责处理已建立连接的 I/O 读写
 * <p>
 * ChannelPipeline 设计：
 * LengthFieldBasedFrameDecoder -> RpcMessageDecoder -> RpcMessageEncoder -> RpcServerHandler
 */
public class NettyServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    /**
     * 配置对象（支持通过 RpcConfig 统一配置）
     */
    private final RpcConfig config;

    /**
     * 优雅停机管理器
     */
    private final GracefulShutdownManager shutdownManager = new GracefulShutdownManager();

    /**
     * 保证 stop() 幂等：防止 finally + 外部重复调用
     */
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ChannelGroup childChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /**
     * 业务线程池：将反射调用从 Netty IO 线程剥离
     */
    private SslContext sslContext;
    private ThreadPoolExecutor businessExecutor;
    private volatile RpcAuthenticator authenticator = RpcAuthenticator.allowAll();
    private final List<RpcInvocationListener> invocationListeners = new CopyOnWriteArrayList<>();

    public NettyServer(int port) {
        this.port = port;
        this.config = RpcConfig.builder().port(port).build();
    }

    public NettyServer(RpcConfig config) {
        this.config = config;
        this.port = config.getPort();
    }

    public void setAuthenticator(RpcAuthenticator authenticator) {
        this.authenticator = authenticator == null ? RpcAuthenticator.allowAll() : authenticator;
    }

    public void addInvocationListener(RpcInvocationListener listener) {
        if (listener != null) {
            invocationListeners.add(listener);
        }
    }

    /**
     * 启动服务端
     */
    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        sslContext = config.getTlsConfig().enabled() ? config.getTlsConfig().createServerSslContext() : null;
        if (sslContext == null) {
            log.warn("TLS is disabled while binding to a non-loopback address; RPC traffic may be exposed");
        }
        workerGroup = new NioEventLoopGroup();

        // 创建业务线程池：从 RpcConfig 读取配置
        int corePoolSize = config.getBizCorePoolSize();
        int maxPoolSize = config.getBizMaxPoolSize();
        int queueCapacity = config.getBizQueueCapacity();
        businessExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "rpc-biz-" + counter.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.AbortPolicy()
        );

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    // TCP 连接队列大小
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // 开启 TCP 底层心跳
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    // 关闭 Nagle 算法，降低延迟
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            childChannels.add(ch);
                            if (sslContext != null) {
                                ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc()));
                            }
                            ch.pipeline()
                                    // 心跳检测：60秒读空闲则关闭连接（客户端应每30秒发一次心跳）
                                    .addLast(new IdleStateHandler(60, 0, 0, java.util.concurrent.TimeUnit.SECONDS))
                                    // 解决 TCP 粘包/拆包：基于长度字段的帧解码器
                                    .addLast(new LengthFieldBasedFrameDecoder(
                                            8 * 1024 * 1024, 5, 4, 0, 0))
                                    // 自定义协议解码器
                                    .addLast(new RpcMessageDecoder())
                                    // 自定义协议编码器
                                    .addLast(new RpcMessageEncoder())
                                    // 业务处理器（使用独立业务线程池 + 优雅停机管理器）
                                    .addLast(new RpcServerHandler(businessExecutor, shutdownManager, authenticator, invocationListeners));
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            running.set(true);
            log.info("RPC Server started on port {}", port);
            future.channel().closeFuture().sync();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Server interrupted", e);
        } finally {
            stop();
        }
    }

    /**
     * 获取优雅停机管理器（由 RpcServer 编排停机流程）
     */
    public GracefulShutdownManager getShutdownManager() {
        return shutdownManager;
    }

    public boolean isRunning() {
        return running.get();
    }

    public int getActiveConnectionCount() {
        return childChannels.size();
    }

    public int getActiveBusinessThreads() {
        return businessExecutor == null ? 0 : businessExecutor.getActiveCount();
    }

    public int getQueuedBusinessTasks() {
        return businessExecutor == null ? 0 : businessExecutor.getQueue().size();
    }

    public int getBusinessQueueCapacity() {
        return businessExecutor == null ? config.getBizQueueCapacity()
                : businessExecutor.getQueue().size() + businessExecutor.getQueue().remainingCapacity();
    }

    public boolean isBusinessExecutorSaturated() {
        return businessExecutor != null
                && businessExecutor.getActiveCount() >= businessExecutor.getMaximumPoolSize()
                && businessExecutor.getQueue().remainingCapacity() == 0;
    }

    /**
     * 停止服务端（幂等：多次调用只执行一次）
     */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return; // 已经停止过，直接返回
        }
        running.set(false);
        childChannels.close().awaitUninterruptibly();
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (businessExecutor != null) {
            businessExecutor.shutdown();
            try {
                if (!businessExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    businessExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                businessExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("RPC Server stopped");
    }
}
