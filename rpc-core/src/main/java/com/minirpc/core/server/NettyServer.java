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
 * RPC 服务端的网络入口，负责监听端口、配置连接 Pipeline，以及管理网络和业务线程资源。
 *
 * 线程分工：
 * 1. bossGroup 接收新连接，并将连接交给 workerGroup。
 * 2. workerGroup 处理网络读写、协议编解码，以及 RpcServerHandler 中提交业务任务前的检查。
 * 3. businessExecutor 执行反射调用等业务任务，避免耗时业务占用 Netty IO 线程。
 *
 * 接收请求：SslHandler（可选，解密）→ IdleStateHandler → 长度帧解码 → RpcMessageDecoder → RpcServerHandler。
 * 返回响应：RpcServerHandler 发起写操作 → RpcMessageEncoder → SslHandler（可选，加密）→ 网络。
 * Pipeline 的入站、出站事件按各自方向传播，编码器只处理出站消息。
 */
public class NettyServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    // 服务端监听端口，构造时确定，实际绑定在 start() 中执行。
    private final int port;

    // 接收新连接的线程组，与处理已建立连接的 IO 线程组分开。
    private EventLoopGroup bossGroup;

    // 处理客户端连接读写的线程组，同一个 EventLoop 可以服务多条连接。
    private EventLoopGroup workerGroup;

    /**
     * 配置对象（支持通过 RpcConfig 统一配置）
     */
    private final RpcConfig config;

    /**
     * 记录停机标志和在途请求数；RpcServer 编排等待流程，RpcServerHandler 配合接收、释放请求。
     */
    private final GracefulShutdownManager shutdownManager = new GracefulShutdownManager();

    /**
     * 保证 stop() 幂等：防止 finally + 外部重复调用
     */
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    // bind 成功后设为 true，stop() 开始时设为 false；健康检查还需结合停机和业务池状态判断。
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 维护已接受的客户端连接，连接关闭后会自动移除，停机时用于批量关闭。
    private final ChannelGroup childChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /**
     * 服务端 TLS 上下文，未启用时为 null；每条连接基于它创建独立的 SslHandler。
     */
    private SslContext sslContext;

    // 执行反射业务调用的线程池，使用有界队列和拒绝策略控制积压。
    private ThreadPoolExecutor businessExecutor;

    // 新建连接时传给 RpcServerHandler 的鉴权器，默认允许所有请求。
    private volatile RpcAuthenticator authenticator = RpcAuthenticator.allowAll();

    // 线程安全的监听器列表，供多个连接的 Handler 报告调用结果。
    private final List<RpcInvocationListener> invocationListeners = new CopyOnWriteArrayList<>();

    /** 使用指定端口和其余默认配置；构造时尚未启动网络监听。 */
    public NettyServer(int port) {
        this.port = port;
        this.config = RpcConfig.builder().port(port).build();
    }

    /** 使用统一配置中的端口、线程池及 TLS 等参数。 */
    public NettyServer(RpcConfig config) {
        this.config = config;
        this.port = config.getPort();
    }

    /** 设置请求鉴权器，null 表示默认放行；通常在启动前配置，新建连接使用当时的鉴权器。 */
    public void setAuthenticator(RpcAuthenticator authenticator) {
        this.authenticator = authenticator == null ? RpcAuthenticator.allowAll() : authenticator;
    }

    /** 注册调用结果监听器，用于指标统计、链路追踪等，忽略 null。 */
    public void addInvocationListener(RpcInvocationListener listener) {
        if (listener != null) {
            invocationListeners.add(listener);
        }
    }

    /**
     * 启动服务端，并阻塞当前线程等待监听 Channel 关闭。
     *
     * 1. 创建 Boss、Worker 线程组，按需初始化 TLS，再创建独立业务线程池。
     * 2. 配置 ServerBootstrap，为每条新连接安装协议处理 Pipeline。
     * 3. 绑定端口，成功后标记运行，再等待监听 Channel 的关闭事件。
     * 4. 绑定、等待阶段退出时，在 finally 中调用 stop() 释放资源。
     */
    public void start() {
        // 单个 Boss 线程负责接收连接；TLS 上下文供各连接复用，Worker 数量使用 Netty 默认配置。
        bossGroup = new NioEventLoopGroup(1);
        sslContext = config.getTlsConfig().enabled() ? config.getTlsConfig().createServerSslContext() : null;
        if (sslContext == null) {
            log.warn("TLS is disabled while binding to a non-loopback address; RPC traffic may be exposed");
        }
        workerGroup = new NioEventLoopGroup();

        int corePoolSize = config.getBizCorePoolSize();
        int maxPoolSize = config.getBizMaxPoolSize();
        int queueCapacity = config.getBizQueueCapacity();
        businessExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                // 非核心线程空闲 60 秒后可以回收，默认不会因空闲回收核心线程。
                60L, TimeUnit.SECONDS,
                // 有界队列限制排队任务数，避免请求积压无限增长。
                new LinkedBlockingQueue<>(queueCapacity),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        // 统一线程名称便于定位业务堆栈；守护线程不会单独阻止 JVM 退出。
                        Thread t = new Thread(r, "rpc-biz-" + counter.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                },
                // 满载或关闭时抛出 RejectedExecutionException，由 RpcServerHandler 返回 OVERLOADED。
                new ThreadPoolExecutor.AbortPolicy()
        );

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    // NIO 服务端监听 Channel；每条已接受连接使用对应的 SocketChannel。
                    .channel(NioServerSocketChannel.class)
                    // option 配置监听 Channel，childOption 配置每条客户端连接。
                    // SO_BACKLOG 是待接收连接队列的长度提示，实际效果受操作系统限制。
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // 开启操作系统 TCP 保活探测；探测周期由系统配置，RPC 心跳另行处理。
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    // 关闭 Nagle 算法，减少小报文因等待合并而产生的延迟。
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 每条新连接都安装自己的 Handler，并加入连接组供停机时统一关闭。
                            childChannels.add(ch);
                            if (sslContext != null) {
                                // TLS 放在前面：入站先解密再解析 RPC，出站在 RPC 编码后加密。
                                ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc()));
                            }
                            ch.pipeline()
                                    // 60 秒未收到入站数据时触发读空闲事件，由 RpcServerHandler 关闭连接。
                                    .addLast(new IdleStateHandler(60, 0, 0, java.util.concurrent.TimeUnit.SECONDS))
                                    // 先按长度字段收齐、切分完整消息：帧上限 8MB，长度字段偏移 5、占 4 字节。
                                    // 后两个 0 表示长度调整值为 0、不剥离协议头，整条消息交给后面的解码器。
                                    .addLast(new LengthFieldBasedFrameDecoder(
                                            8 * 1024 * 1024, 5, 4, 0, 0))
                                    // 自定义协议解码器
                                    .addLast(new RpcMessageDecoder())
                                    // 自定义协议编码器
                                    .addLast(new RpcMessageEncoder())
                                    // Handler 先检查请求、鉴权和停机状态，再将反射调用提交到业务线程池。
                                    .addLast(new RpcServerHandler(businessExecutor, shutdownManager, authenticator, invocationListeners));
                        }
                    });

            // bind 本身是异步操作，sync() 等待绑定完成，失败时不会设置 running。
            ChannelFuture future = bootstrap.bind(port).sync();
            running.set(true);
            log.info("RPC Server started on port {}", port);
            // 此处等待的是监听 Channel 关闭，期间 Netty 线程仍会正常接收连接和处理请求。
            future.channel().closeFuture().sync();

        } catch (InterruptedException e) {
            // 恢复中断标志，让上层知道当前线程收到过中断请求。
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

    /** 返回启动、停机流程维护的运行标记。 */
    public boolean isRunning() {
        return running.get();
    }

    /** 获取当前连接组中维护的客户端连接数。 */
    public int getActiveConnectionCount() {
        return childChannels.size();
    }

    /** 获取正在执行业务任务的线程数，为线程池提供的近似统计值。 */
    public int getActiveBusinessThreads() {
        return businessExecutor == null ? 0 : businessExecutor.getActiveCount();
    }

    /** 获取业务队列中尚未被线程取走的任务数。 */
    public int getQueuedBusinessTasks() {
        return businessExecutor == null ? 0 : businessExecutor.getQueue().size();
    }

    /** 获取业务队列容量，线程池尚未创建时返回配置值。 */
    public int getBusinessQueueCapacity() {
        return businessExecutor == null ? config.getBizQueueCapacity()
                : businessExecutor.getQueue().size() + businessExecutor.getQueue().remainingCapacity();
    }

    /** 判断业务线程是否已达到最大活跃数量且队列已满，供就绪状态检查使用。 */
    public boolean isBusinessExecutorSaturated() {
        return businessExecutor != null
                && businessExecutor.getActiveCount() >= businessExecutor.getMaximumPoolSize()
                && businessExecutor.getQueue().remainingCapacity() == 0;
    }

    /**
     * 释放服务端资源，重复调用时只有一个线程执行关闭流程。
     *
     * 1. 标记停止，并关闭当前维护的客户端连接。
     * 2. 发起 Boss、Worker 线程组的优雅关闭。
     * 3. 关闭业务池，最多等待 10 秒，超时或中断时尝试中断任务并移除排队任务。
     *
     * 注册中心摘流量、等待在途请求等步骤，由 RpcServer.stop() 在调用本方法前编排。
     */
    public void stop() {
        // CAS 保证外部调用和 start() 的 finally 同时触发时，仅执行一次资源释放。
        if (!stopped.compareAndSet(false, true)) {
            return; // 已经停止过，直接返回
        }
        running.set(false);
        // 批量关闭客户端连接并等待完成；该等待不会因当前线程被中断而提前退出。
        childChannels.close().awaitUninterruptibly();
        // shutdownGracefully() 发起异步关闭，这里没有同步等待线程组完全终止。
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (businessExecutor != null) {
            // 不再接收新任务，已接收的任务仍可继续执行，直到完成或等待超时。
            businessExecutor.shutdown();
            try {
                if (!businessExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    // 超时后请求中断正在执行的任务；业务是否及时退出取决于其对中断的响应。
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
