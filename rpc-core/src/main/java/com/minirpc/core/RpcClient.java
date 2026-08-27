package com.minirpc.core;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.generic.GenericService;
import com.minirpc.common.protocol.SerializerType;
import com.minirpc.core.client.NettyClient;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.loadbalance.LoadBalancer;
import com.minirpc.core.observability.RpcInvocationListener;
import com.minirpc.core.loadbalance.RandomLoadBalancer;
import com.minirpc.core.proxy.RpcClientProxy;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * RPC 客户端门面类
 * <p>
 * 提供客户端的一站式使用入口：
 * 1. 管理 Netty 连接
 * 2. 通过服务发现获取服务实例列表
 * 3. 通过负载均衡选择目标节点
 * 4. 通过动态代理生成服务接口的代理对象
 * <p>
 * 使用示例（直连模式，V1.0 兼容）：
 * <pre>
 * RpcClient client = new RpcClient("127.0.0.1", 9090);
 * HelloService helloService = client.createProxy(HelloService.class);
 * </pre>
 * <p>
 * 使用示例（注册中心模式，V2.0）：
 * <pre>
 * RpcClient client = new RpcClient(serviceDiscovery);
 * HelloService helloService = client.createProxy(HelloService.class);
 * </pre>
 */
public class RpcClient {

    private static final Logger log = LoggerFactory.getLogger(RpcClient.class);

    private final NettyClient nettyClient;
    private final ServiceDiscovery serviceDiscovery;
    private final LoadBalancer loadBalancer;
    private final RpcConfig config;
    private final List<RpcInvocationListener> invocationListeners = new CopyOnWriteArrayList<>();

    // 直连模式
    private String directHost;
    private int directPort;
    private boolean directMode = false;

    /**
     * 直连模式构造（V1.0 兼容）
     */
    public RpcClient(String host, int port) {
        this(host, port, SerializerType.HESSIAN2);
    }

    public RpcClient(String host, int port, SerializerType serializerType) {
        this(host, port, RpcConfig.builder().serializerType(serializerType).build());
    }

    public RpcClient(String host, int port, RpcConfig config) {
        this.directHost = host;
        this.directPort = port;
        this.directMode = true;
        this.config = config;
        this.nettyClient = new NettyClient(config);
        this.serviceDiscovery = null;
        this.loadBalancer = new RandomLoadBalancer();
    }

    /**
     * 注册中心模式构造（V2.0）
     */
    public RpcClient(ServiceDiscovery serviceDiscovery) {
        this(serviceDiscovery, new RandomLoadBalancer(), SerializerType.HESSIAN2);
    }

    public RpcClient(ServiceDiscovery serviceDiscovery, RpcConfig config) {
        this(serviceDiscovery, new RandomLoadBalancer(), config);
    }

    public RpcClient(ServiceDiscovery serviceDiscovery, LoadBalancer loadBalancer) {
        this(serviceDiscovery, loadBalancer, SerializerType.HESSIAN2);
    }

    public RpcClient(ServiceDiscovery serviceDiscovery, LoadBalancer loadBalancer, SerializerType serializerType) {
        this(serviceDiscovery, loadBalancer, RpcConfig.builder().serializerType(serializerType).build());
    }

    public RpcClient(ServiceDiscovery serviceDiscovery, LoadBalancer loadBalancer, RpcConfig config) {
        this.serviceDiscovery = serviceDiscovery;
        this.loadBalancer = loadBalancer;
        this.config = config;
        this.nettyClient = new NettyClient(config);
        this.directMode = false;
    }

    /**
     * 创建服务接口的代理对象（同步调用）
     */
    public <T> T createProxy(Class<T> interfaceClass) {
        return createProxy(interfaceClass, "1.0.0", "default");
    }

    public <T> T createProxy(Class<T> interfaceClass, String version, String group) {
        RpcClientProxy proxy;
        if (directMode) {
            proxy = new RpcClientProxy(nettyClient, directHost, directPort, config);
        } else {
            proxy = new RpcClientProxy(nettyClient, serviceDiscovery, loadBalancer, config);
        }
        proxy.setInvocationListeners(invocationListeners);
        proxy.setServiceVersion(version);
        proxy.setServiceGroup(group);
        log.info("Created RPC proxy for: {}", interfaceClass.getName());
        return proxy.createProxy(interfaceClass);
    }

    /**
     * 创建异步代理：方法返回值为 CompletableFuture<T>
     * <p>
     * 使用示例：
     * <pre>
     * HelloServiceAsync asyncProxy = client.createAsyncProxy(HelloServiceAsync.class);
     * CompletableFuture&lt;String&gt; future = asyncProxy.sayHello("World");
     * future.thenAccept(result -&gt; System.out.println(result));
     * </pre>
     * <p>
     * 面试考点：异步非阻塞调用，可并行调用多个服务提升吐量
     */
    public <T> T createAsyncProxy(Class<T> interfaceClass) {
        RpcClientProxy proxy;
        if (directMode) {
            proxy = new RpcClientProxy(nettyClient, directHost, directPort, config);
        } else {
            proxy = new RpcClientProxy(nettyClient, serviceDiscovery, loadBalancer, config);
        }
        proxy.setInvocationListeners(invocationListeners);
        proxy.setAsyncMode(true);
        log.info("Created async RPC proxy for: {}", interfaceClass.getName());
        return proxy.createProxy(interfaceClass);
    }

    /**
     * 创建泛化调用代理：无需依赖服务接口 JAR
     * <p>
     * 使用示例：
     * <pre>
     * GenericService generic = client.createGenericProxy("com.example.EchoService");
     * Object result = generic.$invoke("echo", new String[]{"java.lang.String"}, new Object[]{"hello"});
     * </pre>
     * <p>
     * 面试考点：网关/测试平台不依赖接口 JAR 动态调用任意服务
     *
     * @param interfaceName 服务接口全限定名
     */
    public GenericService createGenericProxy(String interfaceName) {
        RpcClientProxy proxy;
        if (directMode) {
            proxy = new RpcClientProxy(nettyClient, directHost, directPort, config);
        } else {
            proxy = new RpcClientProxy(nettyClient, serviceDiscovery, loadBalancer, config);
        }
        proxy.setInvocationListeners(invocationListeners);
        proxy.setGenericMode(true);
        proxy.setGenericInterfaceName(interfaceName);
        log.info("Created generic RPC proxy for: {}", interfaceName);
        return proxy.createProxy(GenericService.class);
    }

    public void addInvocationListener(RpcInvocationListener listener) {
        if (listener != null) {
            invocationListeners.add(listener);
        }
    }

    /**
     * 优雅关闭客户端
     * <p>
     * 等待 in-flight 请求完成后再关闭连接
     */
    public void shutdown() {
        log.info("Shutting down RPC Client gracefully...");
        // 等待进行中的请求完成（通过 RequestFutureHolder 的 TTL 机制保证最终清理）
        // 给已发送但未收到响应的请求留出合理的完成时间
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        nettyClient.shutdown();
        if (serviceDiscovery != null) {
            serviceDiscovery.close();
        }
        log.info("RPC Client shutdown complete");
    }
}
