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

        // 异步模式：返回 CompletableFuture
        if (asyncMode) {
            return invokeAsync(request);
        }

        // 同步模式
        return invokeSync(request, method.isAnnotationPresent(RpcIdempotent.class));
    }

    /**
     * 同步执行 RPC，等待最终结果后返回业务数据，失败时抛出异常。
     *
     * 1. 先执行限流、熔断检查，不允许调用时直接拒绝请求。
     * 2. 幂等方法选择自定义策略（未配置则 Failover），其他方法选择 Failfast，再进入默认 Filter 链。
     * 3. 容错策略每发起一次尝试，都创建本次请求副本，再选址、发送并等待响应。
     * 4. 根据前台最终结果更新熔断器；失败节点集合属于本次调用，供后台重试继续使用。
     *
     * @param request 本次逻辑调用的原始请求
     * @param retryable 是否允许使用失败重试策略，由接口方法上的 RpcIdempotent 注解决定
     */
    private Object invokeSync(RpcRequest request, boolean retryable) {
        // 每次逻辑调用先获取一个令牌，策略内部重试时不会重新经过此检查。
        if (!rateLimiter.tryAcquire()) {
            emitRejectedInvocation(request, RpcErrorCode.OVERLOADED, "rate_limiter");
            throw new RpcException(RpcErrorCode.OVERLOADED, "Request rate limited, please try again later");
        }

        // 熔断器决定是否放行本次逻辑调用，最终成功或失败会在下面回报。
        if (!circuitBreaker.allowRequest()) {
            emitRejectedInvocation(request, RpcErrorCode.UNAVAILABLE, "circuit_breaker");
            throw new RpcException(RpcErrorCode.UNAVAILABLE, "Circuit breaker is OPEN, request rejected for: "
                    + request.getInterfaceName());
        }

        // 失败节点跟随本次逻辑调用，后台线程不会读到其他请求的节点记录。
        Set<String> failedNodes = ConcurrentHashMap.newKeySet();

        RpcResponse response;
        // 由内层 Lambda 共享：首次尝试编号为 0，后续重试依次为 1、2……
        AtomicInteger attempts = new AtomicInteger();
        try {
            // 幂等方法允许按策略重试；其他方法只尝试一次，避免重试造成重复业务操作。
            ClusterStrategy strategy = retryable
                    ? (clusterStrategy == null ? failoverStrategy : clusterStrategy)
                    : failfastStrategy;

            // 前台组件执行顺序（Failback 可在后台继续执行内层 Lambda）：
            // FilterChain → TraceFilter（设置 traceId）→ MetricsFilter（开始统计）
            // → ClusterStrategy.invoke → Supplier.get()（执行内层 Lambda）
            // → copyForAttempt（创建本次请求副本）→ doInvoke（选址）→ NettyClient.sendSync（发送并等待响应）。
            // doInvoke 的选址步骤：ServiceDiscovery → 失败节点过滤 / EndpointHealthTracker → LoadBalancer；
            // 直连模式跳过这些选址组件，直接向配置地址发送。
            // 正常返回：doInvoke → 容错策略 → MetricsFilter（记录最终结果）→ TraceFilter（收尾）→ 本方法。
            // Failover 重试时，只再次执行内层 Lambda：copyForAttempt → doInvoke → sendSync。
            // 因此 Filter 链只进入一次，MetricsFilter 的耗时包含整个重试过程和退避等待。
            response = filterChain.execute(request, req -> strategy.invoke(req, () -> {
                // getAndIncrement() 先返回当前编号再加一；副本保留原 deadline、traceId，并记录本次重试编号。
                RpcRequest attemptRequest = copyForAttempt(req, attempts.getAndIncrement());
                return doInvoke(attemptRequest, failedNodes);
            }));
        } catch (Exception e) {
            // 调用链以异常结束时，向熔断器记录一次失败，并将异常继续交给调用方。
            circuitBreaker.recordFailure();
            throw e;
        }

        // 容错策略也可能正常返回失败响应，需要在这里将其转换为调用异常。
        if (response.getStatus() == RpcResponse.STATUS_FAIL) {
            circuitBreaker.recordFailure();
            throw new RpcException(response.getErrorCode(), "RPC call failed: " + response.getErrorMessage());
        }

        // 熔断器按整个逻辑调用的最终结果记账，成功后将响应中的业务数据返回给调用方。
        circuitBreaker.recordSuccess();
        return response.getData();
    }

    /**
     * 发起异步 RPC，返回承载业务结果的 Future，本方法不主动等待远端业务响应。
     *
     * 1. 执行限流、熔断检查；不允许调用时返回已经异常完成的 Future。
     * 2. 确定目标地址：直连模式使用配置地址，注册中心模式通过服务发现和负载均衡选择节点。
     * 3. 调用 NettyClient.sendAsync() 获取响应 Future，再用 thenApply() 转换为业务结果 Future。
     * 4. 收到正常响应后检查状态，记录熔断器结果；成功提取业务数据，失败让业务结果 Future 异常完成。
     *
     * 当前异步路径直接选址和发送，未接入同步路径的 FilterChain、容错重试和 EndpointHealthTracker。
     * 前置服务发现、获取连接等操作仍可能同步等待，因此整个方法并非完全不阻塞。
     *
     * @param request 本次调用的请求
     * @return 业务数据的 Future；检查失败或调用失败时，可通过异常回调处理
     */
    private CompletableFuture<Object> invokeAsync(RpcRequest request) {
        // 限流失败时保留拒绝事件，并通过失败 Future 将错误交给调用方。
        if (!rateLimiter.tryAcquire()) {
            emitRejectedInvocation(request, RpcErrorCode.OVERLOADED, "rate_limiter");
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.OVERLOADED, "Request rate limited"));
        }

        // 熔断器未放行时立即返回失败 Future，不再选址或发送请求。
        if (!circuitBreaker.allowRequest()) {
            emitRejectedInvocation(request, RpcErrorCode.UNAVAILABLE, "circuit_breaker");
            return CompletableFuture.failedFuture(
                    new RpcException(RpcErrorCode.UNAVAILABLE, "Circuit breaker is OPEN"));
        }

        // 正常发起顺序：TokenBucketRateLimiter → CircuitBreaker → ServiceDiscovery → LoadBalancer → NettyClient.sendAsync。
        // 直连模式跳过服务发现和负载均衡；这里的选址在当前调用线程中执行。
        String targetHost;
        int targetPort;
        if (directMode) {
            targetHost = host;
            targetPort = port;
        } else {
            String serviceName = request.getInterfaceName();
            // 获取与接口、版本和分组匹配的实例；无候选节点时以失败 Future 结束本次调用。
            List<ServiceInstance> instances = serviceDiscovery.getInstances(
                    serviceName, request.getVersion(), request.getGroup());
            if (instances == null || instances.isEmpty()) {
                return CompletableFuture.failedFuture(
                        new RpcException(RpcErrorCode.UNAVAILABLE, "No available service instance for: " + serviceName));
            }
            // 从候选列表中选择一个节点；当前异步路径没有额外过滤被 EndpointHealthTracker 隔离的节点。
            ServiceInstance instance = loadBalancer.select(instances, request.getRequestId());
            if (instance == null) {
                return CompletableFuture.failedFuture(
                        new RpcException(RpcErrorCode.UNAVAILABLE,
                                "No available service instance after load balance for: " + serviceName));
            }
            targetHost = instance.getHost();
            targetPort = instance.getPort();
        }

        // 响应处理顺序：RpcClientHandler → RequestFutureHolder.complete → 响应 Future 完成 → thenApply → 业务结果 Future 完成。
        // sendAsync 返回 CompletableFuture<RpcResponse>，thenApply 返回一个新的 CompletableFuture<Object>。
        // thenApply 只在响应 Future 正常完成时执行；底层异常完成会直接传播给新 Future，不进入下面的回调。
        return nettyClient.sendAsync(targetHost, targetPort, request)
                .thenApply(response -> {
                    // Future 正常完成只代表拿到了响应，响应自身仍可能表示服务端处理失败。
                    if (response.getStatus() == RpcResponse.STATUS_FAIL) {
                        circuitBreaker.recordFailure();
                        // 在回调内抛异常会让业务结果 Future 异常完成，交给调用方的异常处理逻辑。
                        throw new RpcException(response.getErrorCode(),
                                "RPC call failed: " + response.getErrorMessage());
                    }
                    // 成功时记录熔断器结果，并将响应中的业务数据作为新 Future 的完成值。
                    circuitBreaker.recordSuccess();
                    return response.getData();
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
     * 执行一次 RPC 尝试：选址、发送并等待响应，记录本次尝试的事件和节点健康状态。
     *
     * 1. 获取目标：直连使用配置地址，注册中心模式按接口、版本和分组获取候选节点。
     * 2. 筛选节点：优先避开本次逻辑调用中抛过异常的节点，再检查隔离状态或申请恢复探测。
     * 3. 发送请求：通过负载均衡确定目标节点，调用 NettyClient.sendSync() 等待响应。
     * 4. 记录结果：回报调用事件、更新节点健康，并返回响应或继续抛出异常。
     *
     * 注册中心模式：ServiceDiscovery → 失败节点过滤 → EndpointHealthTracker → LoadBalancer → NettyClient.sendSync。
     * 直连模式：配置地址 → NettyClient.sendSync，跳过服务发现和节点筛选。
     * 本方法只执行一次尝试，是否重试由外层容错策略决定。
     */
    private RpcResponse doInvoke(RpcRequest request, Set<String> failedNodes) {
        String targetHost;
        int targetPort;

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

        // 从发送前开始计时，本次事件的耗时不包含上面的服务发现和节点筛选。
        long startedNanos = System.nanoTime();
        try {
            // 同步发送并等待响应；内部由 Future 将网络接收结果交回当前调用线程。
            RpcResponse response = nettyClient.sendSync(targetHost, targetPort, request);
            // 收到响应后报告本次尝试：成功时错误码为 null，失败响应保留服务端错误码。
            emitInvocation(request, response.getStatus() == RpcResponse.STATUS_FAIL ? response.getErrorCode() : null,
                    startedNanos, targetHost + ":" + targetPort);
            if (response.getStatus() == RpcResponse.STATUS_SUCCESS) {
                // 清除该节点的失败和隔离记录；如果本次是恢复探测，节点由此恢复可用。
                endpointHealthTracker.recordSuccess(targetHost + ":" + targetPort);
            } else {
                // 失败响应与异常一样避开当前节点，供后续前台或后台重试选址。
                failedNodes.add(targetHost + ":" + targetPort);
                endpointHealthTracker.recordFailure(targetHost + ":" + targetPort);
            }
            // 保留完整响应，交给外层策略决定是否重试，再由 invokeSync() 提取业务数据。
            return response;
        } catch (Exception e) {
            // 将抛出异常的节点加入本次调用的失败集合，供下一次尝试优先排除，并更新节点健康。
            failedNodes.add(targetHost + ":" + targetPort);
            endpointHealthTracker.recordFailure(targetHost + ":" + targetPort);
            // RpcException 使用自身错误码，其他异常按服务不可用报告，然后将原异常继续向上抛出。
            RpcErrorCode errorCode = e instanceof RpcException rpcException
                    ? rpcException.getErrorCode() : RpcErrorCode.UNAVAILABLE;
            emitInvocation(request, errorCode, startedNanos, targetHost + ":" + targetPort);
            throw e;
        }
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
