package com.minirpc.core;

import com.minirpc.core.annotation.RpcService;
import com.minirpc.core.registry.LocalServiceRegistry;
import com.minirpc.core.server.NettyServer;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.observability.RpcInvocationListener;
import com.minirpc.core.security.RpcAuthenticator;
import com.minirpc.registry.Registry;
import com.minirpc.registry.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RPC 服务端门面类
 * <p>
 * 提供服务端的一站式启动入口：
 * 1. 注册服务实现到本地注册表
 * 2. 注册服务到注册中心（可选）
 * 3. 启动 Netty 服务端监听端口
 * 4. JVM ShutdownHook 优雅关闭
 * <p>
 * 使用示例：
 * <pre>
 * RpcServer server = new RpcServer(9090);
 * server.setRegistry(new ZookeeperRegistry("127.0.0.1:2181"));
 * server.registerService(new HelloServiceImpl());
 * server.start();
 * </pre>
 */
public class RpcServer {

    private static final Logger log = LoggerFactory.getLogger(RpcServer.class);

    private final int port;
    private final NettyServer nettyServer;
    private Registry registry;

    /**
     * 已注册的服务实例列表（用于优雅关闭时注销）
     * 使用 CopyOnWriteArrayList 保证 ShutdownHook 线程与注册线程的并发安全
     */
    private final List<ServiceInstance> registeredInstances = new CopyOnWriteArrayList<>();
    private final AtomicBoolean stopInitiated = new AtomicBoolean(false);

    public RpcServer(int port) {
        this.port = port;
        this.nettyServer = new NettyServer(port);
    }

    public RpcServer(RpcConfig config) {
        this.port = config.getPort();
        this.nettyServer = new NettyServer(config);
    }

    public void setAuthenticator(RpcAuthenticator authenticator) {
        nettyServer.setAuthenticator(authenticator);
    }

    public void addInvocationListener(RpcInvocationListener listener) {
        nettyServer.addInvocationListener(listener);
    }

    /**
     * 设置注册中心（可选，不设置则仅本地注册）
     */
    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    /**
     * 注册服务实现
     */
    public void registerService(Object service) {
        Class<?> serviceClass = service.getClass();
        RpcService annotation = serviceClass.getAnnotation(RpcService.class);

        String interfaceName;
        String version = "1.0.0";
        String group = "default";

        if (annotation != null && annotation.interfaceClass() != void.class) {
            interfaceName = annotation.interfaceClass().getName();
            version = annotation.version();
            group = annotation.group();
        } else {
            Class<?>[] interfaces = serviceClass.getInterfaces();
            if (interfaces.length == 0) {
                throw new IllegalArgumentException(
                        "Service class must implement at least one interface: " + serviceClass.getName());
            }
            interfaceName = interfaces[0].getName();
        }

        // 注册到本地注册表
        LocalServiceRegistry.registerService(interfaceName, service);

        // 注册到注册中心
        if (registry != null) {
            String host = getLocalHost();
            ServiceInstance instance = new ServiceInstance(interfaceName, host, port, version, group);
            registry.register(instance);
            registeredInstances.add(instance);
        }
    }

    /**
     * 启动 RPC 服务端（阻塞）
     */
    public void start() {
        // 注册 ShutdownHook 实现优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        log.info("Starting RPC Server on port {}, registered services: {}",
                port, LocalServiceRegistry.getServiceCount());
        nettyServer.start();
    }

    /**
     * 优雅停止 RPC 服务端
     * <p>
     * 完整停机流程（五步）：
     * <pre>
     * ① 标记停机 → 新请求立即拒绝（返回 "server shutting down" 错误）
     * ② 摘流量   → 从注册中心注销，消费者不再发现本节点
     * ③ 等待缓存刷新 → sleep 2s，让消费者更新本地实例列表
     * ④ 等待 in-flight → 等待已接收的请求处理完毕（最多 30s）
     * ⑤ 关闭连接   → 关闭 Netty EventLoop + 业务线程池
     * </pre>
     */
    public void stop() {
        if (!stopInitiated.compareAndSet(false, true)) {
            return;
        }
        log.info("=== Graceful shutdown initiated ===");

        // ① 标记停机：新请求立即拒绝
        nettyServer.getShutdownManager().markShuttingDown();

        // ② 从注册中心注销所有服务（摘流量）
        if (registry != null) {
            for (ServiceInstance instance : registeredInstances) {
                try {
                    registry.unregister(instance);
                    log.info("Unregistered service: {}", instance);
                } catch (Exception e) {
                    log.error("Failed to unregister service: {}", instance, e);
                }
            }
            registry.close();
        }

        // ③ 等待消费者刷新本地缓存（消费者缓存 TTL 通常 1-5s）
        try {
            log.info("Waiting 2s for consumers to refresh service cache...");
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ④ 等待 in-flight 请求处理完毕（最多等 30 秒）
        boolean allCompleted = nettyServer.getShutdownManager().awaitInFlight(30, TimeUnit.SECONDS);
        if (!allCompleted) {
            log.warn("Forcing shutdown: {} requests still in-flight",
                    nettyServer.getShutdownManager().getInFlightCount());
        }

        // ⑤ 关闭 Netty 服务端 + 业务线程池
        nettyServer.stop();
        log.info("=== Graceful shutdown complete ===");
    }

    /** Current in-process readiness state for monitoring integrations. */
    public RpcServerStatus getStatus() {
        int registeredServiceCount = registry == null
                ? LocalServiceRegistry.getServiceCount()
                : registeredInstances.size();
        return new RpcServerStatus(nettyServer.isRunning(),
                nettyServer.getShutdownManager().isShuttingDown(),
                nettyServer.getShutdownManager().getInFlightCount(),
                registry != null,
                registeredServiceCount,
                nettyServer.getActiveConnectionCount(),
                nettyServer.getActiveBusinessThreads(),
                nettyServer.getQueuedBusinessTasks(),
                nettyServer.getBusinessQueueCapacity(),
                nettyServer.isBusinessExecutorSaturated());
    }

    /**
     * 获取本机 IP
     */
    private String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}
