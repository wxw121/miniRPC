package com.minirpc.core.proxy;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcContext;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.common.util.RpcUtil;
import com.minirpc.core.client.NettyClient;
import com.minirpc.core.client.FutureSupport;
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
import com.minirpc.core.observability.RpcInvocationEvent;
import com.minirpc.core.observability.RpcInvocationListener;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;
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
    private long requestTimeoutMs;
    private ClusterStrategy clusterStrategy;
    private final EndpointHealthTracker endpointHealthTracker;
    private volatile List<RpcInvocationListener> invocationListeners = List.of();

    // 异步模式：方法返回 CompletableFuture<T> 而非 T
    private boolean asyncMode = false;

    // 泛化调用模式：无需依赖服务接口 JAR
    private boolean genericMode = false;
    private String genericInterfaceName;

    private String serviceVersion = "1.0.0";
    private String serviceGroup = "default";

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

    public void setInvocationListeners(List<RpcInvocationListener> listeners) {
        this.invocationListeners = listeners == null ? List.of() : List.copyOf(listeners);
    }

    public void setClusterStrategy(ClusterStrategy clusterStrategy) {
        this.clusterStrategy = clusterStrategy;
    }

    public void setRequestTimeoutMs(long requestTimeoutMs) {
        if (requestTimeoutMs <= 0) {
            throw new IllegalArgumentException("request timeout must be positive");
        }
        this.requestTimeoutMs = requestTimeoutMs;
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

        boolean retryable = !genericMode && method.isAnnotationPresent(RpcIdempotent.class);

        // 异步模式：返回 CompletableFuture
        if (asyncMode) {
            return invokeAsync(request, retryable);
        }

        // 同步模式
        return invokeSync(request, retryable);
    }

    /** 同步入口只负责等待共享调用链的业务结果，保留原有异常类型。 */
    private Object invokeSync(RpcRequest request, boolean retryable) {
        CompletableFuture<Object> result = invokeAsync(request, retryable);
        try {
            return result.get();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            result.cancel(false);
            throw new RpcException(RpcErrorCode.CANCELLED, "RPC wait interrupted", failure);
        } catch (ExecutionException failure) {
            Throwable cause = FutureSupport.unwrap(failure);
            if (cause instanceof RpcException rpcException) throw rpcException;
            throw new RpcException("RPC call failed: " + cause.getMessage(), cause);
        }
    }

    /**
     * 同步和异步共用：准入 → FilterChain → 容错策略 → 请求副本 → 选址与异步发送。
     * 内置策略不阻塞等待响应或退避；服务发现和获取连接仍可能同步等待。
     */
    private CompletableFuture<Object> invokeAsync(RpcRequest request, boolean retryable) {
        if (!rateLimiter.tryAcquire()) {
            emitRejectedInvocation(request, RpcErrorCode.OVERLOADED, "rate_limiter");
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.OVERLOADED, "Request rate limited"));
        }
        if (!circuitBreaker.allowRequest()) {
            emitRejectedInvocation(request, RpcErrorCode.UNAVAILABLE, "circuit_breaker");
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.UNAVAILABLE, "Circuit breaker is OPEN"));
        }
        Set<String> failedNodes = ConcurrentHashMap.newKeySet();
        AtomicInteger attempts = new AtomicInteger();
        ClusterStrategy strategy = retryable
                ? (clusterStrategy == null ? failoverStrategy : clusterStrategy) : failfastStrategy;
        CompletableFuture<RpcResponse> response = filterChain.executeAsync(request, req -> {
            CompletableFuture<RpcResponse> invocation = strategy.invokeAsync(req,
                    () -> doInvokeAsync(copyForAttempt(req, attempts.getAndIncrement()), failedNodes));
            long remaining = req.getContext().getDeadlineEpochMillis() - System.currentTimeMillis();
            return invocation.orTimeout(Math.max(0, remaining), TimeUnit.MILLISECONDS);
        });
        return FutureSupport.handle(response, (value, failure) -> {
            if (failure != null) {
                if (failure instanceof CancellationException) circuitBreaker.recordCancellation();
                else circuitBreaker.recordFailure();
                if (failure instanceof TimeoutException) {
                    throw new RpcException(RpcErrorCode.DEADLINE_EXCEEDED, "RPC deadline exceeded", failure);
                }
                throw FutureSupport.propagate(failure);
            }
            if (value.getStatus() == RpcResponse.STATUS_FAIL) {
                circuitBreaker.recordFailure();
                throw new RpcException(value.getErrorCode(), "RPC call failed: " + value.getErrorMessage());
            }
            circuitBreaker.recordSuccess();
            return value.getData();
        });
    }

    /**
     * 为一次调用尝试创建请求及上下文副本，并记录本次尝试编号。
     */
    private RpcRequest copyForAttempt(RpcRequest request, int attempt) {
        RpcRequest copy = new RpcRequest();
        copy.setRequestId(request.getRequestId());
        copy.setInterfaceName(request.getInterfaceName());
        copy.setVersion(request.getVersion());
        copy.setGroup(request.getGroup());
        copy.setMethodName(request.getMethodName());
        copy.setParameterTypeNames(request.getParameterTypeNames());
        copy.setParameters(request.getParameters());
        RpcContext context = request.getContext() == null ? new RpcContext() : request.getContext();
        copy.setContext(context.withRetryAttempt(attempt));
        return copy;
    }

    /**
     * 执行一次 RPC 尝试：选址、异步发送，在完成回调中记录事件和节点健康状态。
     *
     * 1. 获取目标：直连使用配置地址，注册中心模式按接口、版本和分组获取候选节点。
     * 2. 筛选节点：优先避开本次逻辑调用中抛过异常的节点，再检查隔离状态或申请恢复探测。
     * 3. 发送请求：通过负载均衡确定目标节点，调用 NettyClient.sendAsync() 获取响应 Future。
     * 4. 记录结果：回报调用事件、更新节点健康，并返回响应或继续抛出异常。
     *
     * 注册中心模式：ServiceDiscovery → 失败节点过滤 → EndpointHealthTracker → LoadBalancer → NettyClient.sendAsync。
     * 直连模式：配置地址 → NettyClient.sendAsync，跳过服务发现和节点筛选。
     * 本方法只执行一次尝试，是否重试由外层容错策略决定。
     */
    private CompletableFuture<RpcResponse> doInvokeAsync(RpcRequest request, Set<String> failedNodes) {
        String targetHost;
        int targetPort;
        boolean recoveryAttempt = false;

        if (directMode) {
            // 直连时始终使用指定地址，重试也不会切换到其他节点。
            targetHost = host;
            targetPort = port;
        } else {
            // 先通过服务发现获取与接口、版本和分组匹配的实例列表。
            String serviceName = request.getInterfaceName();
            List<ServiceInstance> instances = serviceDiscovery.getInstances(
                    serviceName, request.getVersion(), request.getGroup());
            if (instances == null || instances.isEmpty()) {
                throw new RpcException(RpcErrorCode.UNAVAILABLE,
                        "No available service instance for: " + serviceName);
            }

            // 本次逻辑调用保存抛过异常的节点，后续前台或后台重试选址时优先绕开它们。
            List<ServiceInstance> availableInstances = instances;
            if (!failedNodes.isEmpty()) {
                availableInstances = new ArrayList<>();
                for (ServiceInstance inst : instances) {
                    if (!failedNodes.contains(inst.getAddress())) {
                        availableInstances.add(inst);
                    }
                }
                // 若候选节点都抛过异常，则放宽这层过滤；下面仍会检查节点隔离状态。
                if (availableInstances.isEmpty()) {
                    availableInstances = instances;
                }
            }

            // 普通可用节点加入候选列表；对已隔离且到期的节点，最多为本次尝试申请一个恢复探测。
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
                // 探测节点成为唯一候选，避免申请了探测资格后，负载均衡却选中其他健康节点。
                // 探测使用这次真实 RPC 请求，其成功或失败会在下面回报给健康记录器。
                healthyInstances = List.of(recoveryProbe);
                recoveryAttempt = true;
            }
            if (healthyInstances.isEmpty()) {
                // 没有普通可用节点，也未取得探测资格，向上层报告本次选址失败。
                throw new RpcException(RpcErrorCode.UNAVAILABLE,
                        "All discovered service instances are temporarily ejected: " + serviceName);
            }

            // 在筛选后的列表中选择一个目标节点，恢复探测时列表里只有探测节点。
            ServiceInstance instance = loadBalancer.select(healthyInstances, request.getRequestId());
            if (instance == null) {
                throw new RpcException(RpcErrorCode.UNAVAILABLE,
                        "No available service instance after load balance for: " + serviceName);
            }
            targetHost = instance.getHost();
            targetPort = instance.getPort();
            log.debug("Selected instance: {} for service: {}", instance.getAddress(), serviceName);
        }

        long startedNanos = System.nanoTime();
        String endpoint = targetHost + ":" + targetPort;
        boolean isRecoveryAttempt = recoveryAttempt;
        CompletableFuture<RpcResponse> transport;
        try {
            transport = nettyClient.sendAsync(targetHost, targetPort, request);
            long remaining = request.getContext().getDeadlineEpochMillis() - System.currentTimeMillis();
            transport.orTimeout(Math.max(0, remaining), TimeUnit.MILLISECONDS);
        } catch (Exception failure) {
            transport = CompletableFuture.failedFuture(failure);
        }
        return FutureSupport.handle(transport, (response, failure) -> {
            if (failure instanceof TimeoutException) {
                failure = new RpcException(RpcErrorCode.DEADLINE_EXCEEDED, "RPC deadline exceeded", failure);
            }
            if (failure != null) {
                if (failure instanceof CancellationException) {
                    if (isRecoveryAttempt) endpointHealthTracker.releaseRecoveryProbe(endpoint);
                } else {
                    failedNodes.add(endpoint);
                    endpointHealthTracker.recordFailure(endpoint);
                }
                RpcErrorCode code = failure instanceof RpcException rpcException ? rpcException.getErrorCode()
                        : failure instanceof CancellationException ? RpcErrorCode.CANCELLED : RpcErrorCode.UNAVAILABLE;
                emitInvocation(request, code, startedNanos, endpoint);
                throw FutureSupport.propagate(failure);
            }
            boolean success = response.getStatus() == RpcResponse.STATUS_SUCCESS;
            if (success) {
                endpointHealthTracker.recordSuccess(endpoint);
            } else {
                failedNodes.add(endpoint);
                endpointHealthTracker.recordFailure(endpoint);
            }
            emitInvocation(request, success ? null : response.getErrorCode(), startedNanos, endpoint);
            return response;
        });
    }

    private void emitRejectedInvocation(RpcRequest request, RpcErrorCode errorCode, String reason) {
        String targetEndpoint = directMode ? host + ":" + port : "";
        emitInvocation(request, errorCode, System.nanoTime(), targetEndpoint,
                Map.of("rejection_reason", reason));
    }

    /**
     * 报告不带附加属性的调用结果，复用下面的完整实现。
     */
    private void emitInvocation(RpcRequest request, RpcErrorCode errorCode, long startedNanos, String targetEndpoint) {
        emitInvocation(request, errorCode, startedNanos, targetEndpoint, Map.of());
    }

    /**
     * 将本次调用结果封装为事件，交给监听器进行指标统计、链路追踪等处理。
     * errorCode 为 null 表示成功；startedNanos 是使用 System.nanoTime() 记录的计时起点。
     * attributes 用于携带拒绝原因等补充信息；监听器在当前线程中依次执行。
     */
    private void emitInvocation(RpcRequest request, RpcErrorCode errorCode, long startedNanos, String targetEndpoint,
                                Map<String, String> attributes) {
        // 没有监听器时直接返回，避免创建无人消费的事件对象。
        if (invocationListeners.isEmpty()) {
            return;
        }
        // 从上下文附件读取尝试编号：0 为首次调用，1 为第一次重试；缺失或格式错误时使用默认值 0。
        int attempt = 0;
        String retryAttempt = request.getContext() == null ? null
                : request.getContext().getAttachments().get("retryAttempt");
        try {
            attempt = retryAttempt == null ? 0 : Integer.parseInt(retryAttempt);
        } catch (NumberFormatException ignored) {
            // 统计附件格式错误时保留默认值，不让解析失败改变 RPC 调用结果。
        }
        // 记录客户端视角的结果；当前 nanoTime 减去计时起点得到耗时，单位为纳秒。
        RpcInvocationEvent event = new RpcInvocationEvent("client", request.getInterfaceName(), errorCode,
                System.nanoTime() - startedNanos, attempt, targetEndpoint,
                request.getContext() == null ? "" : request.getContext().getTraceId(), attributes);
        // 同一份事件交给各监听器，分别完成指标统计、追踪记录等工作。
        for (RpcInvocationListener listener : invocationListeners) {
            try {
                listener.onComplete(event);
            } catch (RuntimeException e) {
                // 隔离监听器的运行时异常，记录日志后继续通知其他监听器。
                log.warn("RPC invocation listener failed", e);
            }
        }
    }
}
