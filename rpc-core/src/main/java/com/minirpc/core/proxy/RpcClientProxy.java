package com.minirpc.core.proxy;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcContext;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.common.util.RpcUtil;
import com.minirpc.core.client.NettyClient;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.cluster.CircuitBreaker;
import com.minirpc.core.cluster.ClusterStrategy;
import com.minirpc.core.cluster.FailfastClusterStrategy;
import com.minirpc.core.cluster.FailoverClusterStrategy;
import com.minirpc.core.cluster.EndpointHealthTracker;
import com.minirpc.core.cluster.TokenBucketRateLimiter;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.filter.FilterChain;
import com.minirpc.core.filter.MetricsFilter;
import com.minirpc.core.filter.TraceFilter;
import com.minirpc.core.loadbalance.LoadBalancer;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RPC 客户端动态代理
 * <p>
 * 通过 JDK 动态代理为服务接口生成代理对象。
 * 支持两种模式：
 * 1. 直连模式（V1.0）：直接连接指定的 host:port
 * 2. 注册中心模式（V2.0）：通过服务发现 + 负载均衡选择目标节点
 * <p>
 * 完整调用链路（V3.0/V4.0）：
 * 限流检查 → FilterChain（Trace + Metrics）→ 熔断器判断 → 容错策略 → 实际 RPC 调用
 * <p>
 * 面试考点：
 * - JDK 动态代理原理（Proxy + InvocationHandler）
 * - 责任链模式（FilterChain）
 * - 熔断器、限流器、容错策略的串联设计
 */
public class RpcClientProxy implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(RpcClientProxy.class);

    private final NettyClient nettyClient;

    // 直连模式
    private String host;
    private int port;
    private boolean directMode;

    // 注册中心模式
    private ServiceDiscovery serviceDiscovery;
    private LoadBalancer loadBalancer;

    // V3.0 容错组件
    private final FilterChain filterChain;
    private final CircuitBreaker circuitBreaker;
    private final ClusterStrategy failoverStrategy;
    private final ClusterStrategy failfastStrategy;
    private final TokenBucketRateLimiter rateLimiter;
    private final long requestTimeoutMs;
    private final EndpointHealthTracker endpointHealthTracker;

    // 异步模式：方法返回 CompletableFuture<T> 而非 T
    private boolean asyncMode = false;

    // 泛化调用模式：无需依赖服务接口 JAR
    private boolean genericMode = false;
    private String genericInterfaceName;

    private String serviceVersion = "1.0.0";
    private String serviceGroup = "default";

    /**
     * 记录当前请求重试过程中失败的节点地址
     * <p>
     * 面试考点：Failover 重试时为什么要换节点？
     * - 重试同一个故障节点没有意义，大概率还会失败
     * - 排除故障节点后重新负载均衡，提高重试成功率
     */
    private static final ThreadLocal<Set<String>> FAILED_NODES = ThreadLocal.withInitial(HashSet::new);

    /**
     * 直连模式构造
     */
    public RpcClientProxy(NettyClient nettyClient, String host, int port) {
        this(nettyClient, host, port, RpcConfig.defaultConfig());
    }

    /**
     * 直连模式构造（支持 RpcConfig 配置）
     */
    public RpcClientProxy(NettyClient nettyClient, String host, int port, RpcConfig config) {
        this.nettyClient = nettyClient;
        this.host = host;
        this.port = port;
        this.directMode = true;
        this.serviceDiscovery = null;
        this.loadBalancer = null;

        // 从 RpcConfig 初始化容错组件
        this.filterChain = buildDefaultFilterChain();
        this.circuitBreaker = new CircuitBreaker(
                config.getCircuitBreakerFailureThreshold(),
                config.getCircuitBreakerOpenTimeoutMs(), 3);
        this.failoverStrategy = new FailoverClusterStrategy(
                config.getFailoverMaxRetries(), config.getRetryBaseBackoffMs(),
                config.getRetryMaxBackoffMs());
        this.failfastStrategy = new FailfastClusterStrategy();
        this.rateLimiter = new TokenBucketRateLimiter(
                config.getRateLimiterCapacity(), config.getRateLimiterTokensPerSecond());
        this.requestTimeoutMs = config.getTimeoutMs();
        this.endpointHealthTracker = new EndpointHealthTracker();
    }

    /**
     * 注册中心模式构造
     */
    public RpcClientProxy(NettyClient nettyClient, ServiceDiscovery serviceDiscovery, LoadBalancer loadBalancer) {
        this(nettyClient, serviceDiscovery, loadBalancer, RpcConfig.defaultConfig());
    }

    /**
     * 注册中心模式构造（支持 RpcConfig 配置）
     */
    public RpcClientProxy(NettyClient nettyClient, ServiceDiscovery serviceDiscovery, LoadBalancer loadBalancer, RpcConfig config) {
        this(nettyClient, serviceDiscovery, loadBalancer, config, new EndpointHealthTracker());
    }

    RpcClientProxy(NettyClient nettyClient, ServiceDiscovery serviceDiscovery, LoadBalancer loadBalancer,
                   RpcConfig config, EndpointHealthTracker endpointHealthTracker) {
        this.nettyClient = nettyClient;
        this.serviceDiscovery = serviceDiscovery;
        this.loadBalancer = loadBalancer;
        this.directMode = false;

        // 从 RpcConfig 初始化容错组件
        this.filterChain = buildDefaultFilterChain();
        this.circuitBreaker = new CircuitBreaker(
                config.getCircuitBreakerFailureThreshold(),
                config.getCircuitBreakerOpenTimeoutMs(), 3);
        this.failoverStrategy = new FailoverClusterStrategy(
                config.getFailoverMaxRetries(), config.getRetryBaseBackoffMs(),
                config.getRetryMaxBackoffMs());
        this.failfastStrategy = new FailfastClusterStrategy();
        this.rateLimiter = new TokenBucketRateLimiter(
                config.getRateLimiterCapacity(), config.getRateLimiterTokensPerSecond());
        this.requestTimeoutMs = config.getTimeoutMs();
        this.endpointHealthTracker = endpointHealthTracker;
    }

    /**
     * 设置异步模式
     */
    public void setAsyncMode(boolean asyncMode) {
        this.asyncMode = asyncMode;
    }

    /**
     * 设置泛化调用模式
     */
    public void setGenericMode(boolean genericMode) {
        this.genericMode = genericMode;
    }

    /**
     * 设置泛化调用的目标接口名
     */
    public void setGenericInterfaceName(String genericInterfaceName) {
        this.genericInterfaceName = genericInterfaceName;
    }

    public void setServiceVersion(String serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    public void setServiceGroup(String serviceGroup) {
        this.serviceGroup = serviceGroup;
    }

    /**
     * 构建默认 Filter 链：TraceFilter + MetricsFilter
     */
    private FilterChain buildDefaultFilterChain() {
        FilterChain chain = new FilterChain();
        chain.addFilter(new TraceFilter());
        chain.addFilter(new MetricsFilter());
        return chain;
    }

    /**
     * 创建服务接口的代理对象
     */
    @SuppressWarnings("unchecked")
    public <T> T createProxy(Class<T> interfaceClass) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                this
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 处理 Object 类的方法
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }

        // 1. 构建 RPC 请求
        RpcRequest request = new RpcRequest();
        request.setRequestId(RpcUtil.generateRequestId());
        request.setVersion(serviceVersion);
        request.setGroup(serviceGroup);
        RpcContext context = new RpcContext();
        context.setDeadlineEpochMillis(System.currentTimeMillis() + requestTimeoutMs);
        request.setContext(context);

        if (genericMode) {
            // 泛化调用：从 $invoke 参数中提取方法名、参数类型、参数值
            // args[0] = methodName, args[1] = parameterTypes(String[]), args[2] = parameters(Object[])
            if (args == null || args.length < 3) {
                throw new RpcException("Generic invocation requires 3 arguments: methodName, parameterTypes, parameters");
            }
            String methodName = (String) args[0];
            String[] parameterTypeNames = (String[]) args[1];
            Object[] parameters = (Object[]) args[2];

            request.setInterfaceName(genericInterfaceName);
            request.setMethodName(methodName);
            request.setParameterTypeNames(parameterTypeNames);
            request.setParameters(parameters);
        } else {
            // 普通调用：从反射 Method 中提取信息
            request.setInterfaceName(method.getDeclaringClass().getName());
            request.setMethodName(method.getName());
            request.setParameterTypes(method.getParameterTypes());
            request.setParameters(args);
        }

        log.debug("Sending RPC request: {}", request);

        // 异步模式：返回 CompletableFuture
        if (asyncMode) {
            return invokeAsync(request);
        }

        // 同步模式
        return invokeSync(request, method.isAnnotationPresent(RpcIdempotent.class));
    }

    /**
     * 同步调用：阻塞等待结果
     */
    private Object invokeSync(RpcRequest request, boolean retryable) {
        // 限流检查
        if (!rateLimiter.tryAcquire()) {
            throw new RpcException(RpcErrorCode.OVERLOADED, "Request rate limited, please try again later");
        }

        // 熔断器检查
        if (!circuitBreaker.allowRequest()) {
            throw new RpcException(RpcErrorCode.UNAVAILABLE, "Circuit breaker is OPEN, request rejected for: "
                    + request.getInterfaceName());
        }

        // 清理上次重试的失败节点记录
        FAILED_NODES.get().clear();

        // 通过 FilterChain + 容错策略执行调用
        RpcResponse response;
        AtomicInteger attempts = new AtomicInteger();
        try {
            ClusterStrategy strategy = retryable ? failoverStrategy : failfastStrategy;
            response = filterChain.execute(request, req -> strategy.invoke(req, () -> {
                req.getContext().putAttachment("retryAttempt", Integer.toString(attempts.getAndIncrement()));
                return doInvoke(req);
            }));
        } catch (Exception e) {
            circuitBreaker.recordFailure();
            throw e;
        } finally {
            FAILED_NODES.get().clear();
        }

        // 处理响应
        if (response.getStatus() == RpcResponse.STATUS_FAIL) {
            circuitBreaker.recordFailure();
            throw new RpcException(response.getErrorCode(), "RPC call failed: " + response.getErrorMessage());
        }

        circuitBreaker.recordSuccess();
        return response.getData();
    }

    /**
     * 异步调用：返回 CompletableFuture，不阻塞当前线程
     * <p>
     * 面试考点：
     * - 异步非阻塞：调用方不需要等待结果，可以继续做其他事
     * - 并行调用多个服务，提升吐量
     */
    private CompletableFuture<Object> invokeAsync(RpcRequest request) {
        // 限流检查
        if (!rateLimiter.tryAcquire()) {
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.OVERLOADED, "Request rate limited"));
        }

        // 熔断器检查
        if (!circuitBreaker.allowRequest()) {
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.UNAVAILABLE, "Circuit breaker is OPEN"));
        }

        // 异步发送，不阻塞
        String targetHost;
        int targetPort;
        if (directMode) {
            targetHost = host;
            targetPort = port;
        } else {
            String serviceName = request.getInterfaceName();
            List<ServiceInstance> instances = serviceDiscovery.getInstances(
                    serviceName, request.getVersion(), request.getGroup());
            if (instances == null || instances.isEmpty()) {
                return CompletableFuture.failedFuture(
                        new RpcException(RpcErrorCode.UNAVAILABLE, "No available service instance for: " + serviceName));
            }
            ServiceInstance instance = loadBalancer.select(instances, request.getRequestId());
            if (instance == null) {
                return CompletableFuture.failedFuture(
                        new RpcException(RpcErrorCode.UNAVAILABLE,
                                "No available service instance after load balance for: " + serviceName));
            }
            targetHost = instance.getHost();
            targetPort = instance.getPort();
        }

        return nettyClient.sendAsync(targetHost, targetPort, request)
                .thenApply(response -> {
                    if (response.getStatus() == RpcResponse.STATUS_FAIL) {
                        circuitBreaker.recordFailure();
                        throw new RpcException(response.getErrorCode(),
                                "RPC call failed: " + response.getErrorMessage());
                    }
                    circuitBreaker.recordSuccess();
                    return response.getData();
                });
    }

    /**
     * 实际的 RPC 调用：服务发现 + 负载均衡 + 网络发送
     * <p>
     * 重试时自动排除上次失败的节点
     */
    private RpcResponse doInvoke(RpcRequest request) {
        String targetHost;
        int targetPort;

        if (directMode) {
            targetHost = host;
            targetPort = port;
        } else {
            // 通过服务发现 + 负载均衡选择节点
            String serviceName = request.getInterfaceName();
            List<ServiceInstance> instances = serviceDiscovery.getInstances(
                    serviceName, request.getVersion(), request.getGroup());
            if (instances == null || instances.isEmpty()) {
                throw new RpcException(RpcErrorCode.UNAVAILABLE,
                        "No available service instance for: " + serviceName);
            }

            // 排除上次失败的节点（Failover 重试换节点）
            Set<String> failedNodes = FAILED_NODES.get();
            List<ServiceInstance> availableInstances = instances;
            if (!failedNodes.isEmpty()) {
                availableInstances = new ArrayList<>();
                for (ServiceInstance inst : instances) {
                    if (!failedNodes.contains(inst.getAddress())) {
                        availableInstances.add(inst);
                    }
                }
                // 如果所有节点都失败过，则不过滤（避免无节点可用）
                if (availableInstances.isEmpty()) {
                    availableInstances = instances;
                }
            }

            List<ServiceInstance> healthyInstances = new ArrayList<>();
            ServiceInstance recoveryProbe = null;
            for (ServiceInstance inst : availableInstances) {
                if (endpointHealthTracker.isAvailable(inst.getAddress())) {
                    healthyInstances.add(inst);
                } else if (recoveryProbe == null
                        && endpointHealthTracker.tryAcquireRecoveryProbe(inst.getAddress())) {
                    recoveryProbe = inst;
                }
            }
            if (recoveryProbe != null) {
                healthyInstances = List.of(recoveryProbe);
            }
            if (healthyInstances.isEmpty()) {
                throw new RpcException(RpcErrorCode.UNAVAILABLE,
                        "All discovered service instances are temporarily ejected: " + serviceName);
            }

            ServiceInstance instance = loadBalancer.select(healthyInstances, request.getRequestId());
            if (instance == null) {
                throw new RpcException(RpcErrorCode.UNAVAILABLE,
                        "No available service instance after load balance for: " + serviceName);
            }
            targetHost = instance.getHost();
            targetPort = instance.getPort();
            log.debug("Selected instance: {} for service: {}", instance.getAddress(), serviceName);
        }

        try {
            RpcResponse response = nettyClient.sendSync(targetHost, targetPort, request);
            if (response.getStatus() == RpcResponse.STATUS_SUCCESS) {
                endpointHealthTracker.recordSuccess(targetHost + ":" + targetPort);
            } else {
                endpointHealthTracker.recordFailure(targetHost + ":" + targetPort);
            }
            return response;
        } catch (Exception e) {
            // 记录失败节点，下次重试时排除
            FAILED_NODES.get().add(targetHost + ":" + targetPort);
            endpointHealthTracker.recordFailure(targetHost + ":" + targetPort);
            throw e;
        }
    }
}
