package com.minirpc.core.server;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.common.protocol.RpcCancellation;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.core.observability.RpcInvocationEvent;
import com.minirpc.core.observability.RpcInvocationListener;
import com.minirpc.core.registry.LocalServiceRegistry;
import com.minirpc.core.security.RpcAuthenticator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.RejectedExecutionException;

/**
 * RPC 服务端请求处理器
 * <p>
 * 处理客户端发来的 RPC 请求：
 * 1. 从本地服务注册表中查找服务实现
 * 2. 将业务逻辑提交到独立业务线程池执行（避免阻塞 Netty IO 线程）
 * 3. 通过反射调用目标方法
 * 4. 将结果封装为 RpcResponse 返回
 */
public class RpcServerHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(RpcServerHandler.class);

    /**
     * Method 缓存：避免每次请求都执行反射 getMethod()
     * key = 实现类名 + 方法名 + 参数类型列表，value = 解析后的 Method 对象
     */
    private static final Map<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();

    // 当前连接的未结束业务任务：requestId → PendingInvocation，供 CANCEL 消息查找任务。
    private final Map<String, PendingInvocation> pendingInvocations = new ConcurrentHashMap<>();

    /**
     * 业务线程池：将耗时的反射调用从 Netty IO 线程剥离
     */
    private final ExecutorService businessExecutor;

    /**
     * 优雅停机管理器：追踪 in-flight 请求，停机时拒绝新请求
     */
    private final GracefulShutdownManager shutdownManager;
    private final RpcAuthenticator authenticator;
    private final List<RpcInvocationListener> invocationListeners;

    public RpcServerHandler(ExecutorService businessExecutor, GracefulShutdownManager shutdownManager) {
        this(businessExecutor, shutdownManager, RpcAuthenticator.allowAll(), List.of());
    }

    public RpcServerHandler(ExecutorService businessExecutor,
                            GracefulShutdownManager shutdownManager,
                            RpcAuthenticator authenticator,
                            List<RpcInvocationListener> invocationListeners) {
        this.businessExecutor = businessExecutor;
        this.shutdownManager = shutdownManager;
        this.authenticator = authenticator == null ? RpcAuthenticator.allowAll() : authenticator;
        this.invocationListeners = List.copyOf(invocationListeners);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
        // 处理心跳消息（心跳在 IO 线程直接处理，无需提交到业务线程池）
        if (msg.getMessageType() == MessageType.HEARTBEAT) {
            log.debug("Received heartbeat from: {}", ctx.channel().remoteAddress());
            return;
        }

        if (msg.getMessageType() == MessageType.CANCEL) {
            if (msg.getData() instanceof RpcCancellation cancellation) {
                cancelQueuedInvocation(cancellation.getRequestId());
            } else {
                log.warn("Ignoring malformed cancellation frame from {}", ctx.channel().remoteAddress());
            }
            return;
        }
        if (msg.getMessageType() != MessageType.REQUEST) {

            log.warn("Received non-request message: {}", msg.getMessageType());
            return;
        }

        RpcRequest request = (RpcRequest) msg.getData();
        log.debug("Received RPC request: {}", request);
        long startedNanos = System.nanoTime();

        if (request.getContext() != null && request.getContext().isExpired(System.currentTimeMillis())) {
            RpcResponse expired = RpcResponse.fail(request.getRequestId(), RpcErrorCode.DEADLINE_EXCEEDED,
                    "RPC deadline exceeded before server execution");
            completeRequest(ctx, msg, request, expired, startedNanos);
            return;
        }

        RpcAuthenticator.AuthenticationResult authentication;
        try {
            authentication = authenticator.authenticate(request);
        } catch (RuntimeException e) {
            log.warn("Request authentication failed: requestId={}", request.getRequestId(), e);
            authentication = RpcAuthenticator.AuthenticationResult.deny("Authentication failed");
        }
        if (!authentication.authenticated()) {
            RpcResponse unauthorized = RpcResponse.fail(request.getRequestId(), RpcErrorCode.UNAUTHORIZED,
                    authentication.reason().isBlank() ? "Authentication failed" : authentication.reason());
            completeRequest(ctx, msg, request, unauthorized, startedNanos);
            return;
        }

        // 优雅停机：拒绝新请求
        if (!shutdownManager.acquireRequest()) {
            log.warn("Rejecting request during shutdown: requestId={}", request.getRequestId());
            RpcResponse rejectResponse = RpcResponse.fail(
                    request.getRequestId(), RpcErrorCode.UNAVAILABLE,
                    "Server is shutting down, please retry on another node");
            RpcMessage rejectMessage = new RpcMessage(MessageType.RESPONSE, msg.getSerializerType(), rejectResponse);
            completeRequest(ctx, msg, request, rejectResponse, startedNanos);
            return;
        }

        // 将业务逻辑提交到独立线程池，避免阻塞 Netty IO 线程
        PendingInvocation pending = new PendingInvocation(ctx, request, startedNanos, () -> {
            RpcResponse response;
            try {
                if (request.getContext() != null && request.getContext().isExpired(System.currentTimeMillis())) {
                    response = RpcResponse.fail(request.getRequestId(), RpcErrorCode.DEADLINE_EXCEEDED,
                            "RPC deadline exceeded before business execution");
                    completeRequest(ctx, msg, request, response, startedNanos);
                    return;
                }
                // 1. 从注册表获取服务实现
                Object service = LocalServiceRegistry.getService(request.getInterfaceName());
                if (service == null) {
                    throw new RpcException(RpcErrorCode.UNAVAILABLE,
                            "Service not found: " + request.getInterfaceName());
                }

                // 2. 解析参数类型并反射调用目标方法（使用 Method 缓存提升性能）
                Class<?>[] paramTypes = request.getParameterTypes();
                String methodKey = service.getClass().getName() + "#" + request.getMethodName()
                        + "#" + Arrays.toString(paramTypes);
                Method method = METHOD_CACHE.computeIfAbsent(methodKey, k -> {
                    try {
                        return service.getClass().getMethod(request.getMethodName(), paramTypes);
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException("Method not found: " + request.getMethodName(), e);
                    }
                });
                Object result = method.invoke(service, request.getParameters());

                // 3. 构建成功响应
                response = RpcResponse.success(request.getRequestId(), result);
                log.info("RPC request processed successfully: requestId={}", request.getRequestId());

            } catch (Exception e) {
                log.error("RPC request processing failed: requestId={}", request.getRequestId(), e);
                RpcErrorCode errorCode = e instanceof RpcException rpcException
                        ? rpcException.getErrorCode() : RpcErrorCode.INTERNAL;
                response = RpcResponse.fail(request.getRequestId(), errorCode, e.getMessage());

            }
            // 4. 发送响应（检查 Channel 是否仍活跃，避免客户端已断开时写入报错）
            completeRequest(ctx, msg, request, response, startedNanos);
            });
        if (pendingInvocations.putIfAbsent(request.getRequestId(), pending) != null) {
            shutdownManager.releaseRequest();
            RpcResponse duplicate = RpcResponse.fail(request.getRequestId(), RpcErrorCode.INVALID_REQUEST,
                    "Duplicate request ID is already pending");
            completeRequest(ctx, msg, request, duplicate, startedNanos);
            return;
        }
        try {
            businessExecutor.execute(pending);
        } catch (RejectedExecutionException e) {
            pendingInvocations.remove(request.getRequestId(), pending);
            shutdownManager.releaseRequest();
            RpcResponse overload = RpcResponse.fail(request.getRequestId(), RpcErrorCode.OVERLOADED,
                    "Server is overloaded or shutting down");
            completeRequest(ctx, msg, request, overload, startedNanos, Map.of("rejection_reason", "business_executor"));
        }
    }

    /** 按请求 ID 查找任务并尝试取消，只有仍处于 QUEUED 状态的任务可以取消成功。 */
    boolean cancelQueuedInvocation(String requestId) {
        PendingInvocation pending = pendingInvocations.get(requestId);
        if (pending != null && pending.cancelQueued()) {
            log.debug("Cancelled queued RPC request: requestId={}", requestId);
            return true;
        }
        return false;
    }
    // 正常执行：QUEUED → RUNNING → COMPLETED；排队取消：QUEUED → CANCELLED。
    private enum InvocationState { QUEUED, RUNNING, CANCELLED, COMPLETED }

    /**
     * 提交到业务线程池的任务包装器，协调“开始执行”和“排队取消”之间的竞争。
     *
     * 1. 初始状态为 QUEUED，内部 FutureTask 包装实际的反射调用及响应处理逻辑。
     * 2. 业务线程通过 CAS 将 QUEUED 改为 RUNNING，取得执行资格后才运行实际任务。
     * 3. 取消线程通过 CAS 将 QUEUED 改为 CANCELLED，成功后取消任务、清理记录并报告取消事件。
     * 4. 执行和取消两条路径互斥，各自负责释放一次在途请求计数，避免重复释放。
     *
     * 已经 RUNNING 的业务不在这里中断；已取消的包装任务仍可能留在工作队列，取出时会直接跳过。
     */
    private final class PendingInvocation implements Runnable {
        // 保存原连接、请求和计时起点，供排队取消时构造服务端调用事件。
        private final ChannelHandlerContext ctx;
        private final RpcRequest request;
        private final long startedNanos;

        // 提供实际任务的执行和取消能力；提交到线程池队列的是外层 PendingInvocation。
        private final FutureTask<Void> task;

        // 用同一个原子状态协调业务线程的 run() 与取消路径的 cancelQueued()。
        private final AtomicReference<InvocationState> state = new AtomicReference<>(InvocationState.QUEUED);

        /** 包装业务逻辑但暂不执行，等待线程池调度或排队取消。 */
        private PendingInvocation(ChannelHandlerContext ctx, RpcRequest request,
                                  long startedNanos, Runnable invocation) {
            this.ctx = ctx;
            this.request = request;
            this.startedNanos = startedNanos;
            // invocation 是无返回值的 Runnable，因此 FutureTask 的完成值设为 null。
            this.task = new FutureTask<>(invocation, null);
        }

        /** 业务线程取出任务后执行，取得运行资格才进入业务逻辑，结束时统一清理。 */
        @Override
        public void run() {
            // 取消已成功或任务已被执行过时，CAS 失败，直接返回，也不会重复释放请求计数。
            if (!state.compareAndSet(InvocationState.QUEUED, InvocationState.RUNNING)) {
                return;
            }
            try {
                task.run();
            } finally {
                // 无论实际任务如何结束，都标记完成并移除当前任务；按对象匹配可避免误删其他任务。
                state.set(InvocationState.COMPLETED);
                pendingInvocations.remove(request.getRequestId(), this);
                shutdownManager.releaseRequest();
            }
        }

        /** 尝试取消尚未开始的任务，成功返回 true；已运行、已完成或已取消时返回 false。 */
        private boolean cancelQueued() {
            // 与 run() 竞争 QUEUED 状态，只有取得取消资格的线程才负责后续清理。
            if (!state.compareAndSet(InvocationState.QUEUED, InvocationState.CANCELLED)) {
                return false;
            }
            // false 表示不请求中断线程；这里仅支持排队取消，不打断已经开始的业务。
            task.cancel(false);
            // 移除的是请求映射，不会立即移除线程池队列中的包装任务，也不会立即腾出队列容量。
            pendingInvocations.remove(request.getRequestId(), this);
            // 取消路径负责释放计数；以后 run() 因状态不匹配直接返回，不会再次释放。
            shutdownManager.releaseRequest();
            emitCancellationEvent(ctx, request, startedNanos);
            return true;
        }
    }

    /**
     * 报告排队请求已经取消：读取尝试编号 → 构造 CANCELLED 事件 → 通知本地监听器。
     * 本方法不发送取消确认响应，也不释放在途计数；计数已由 cancelQueued() 释放。
     */
    private void emitCancellationEvent(ChannelHandlerContext ctx, RpcRequest request, long startedNanos) {
        // 尝试编号默认 0，表示首次调用；重试请求从上下文附件中读取编号。
        int attempt = 0;
        if (request.getContext() != null) {
            String value = request.getContext().getAttachments().get("retryAttempt");
            try {
                attempt = value == null ? 0 : Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
                // 附件格式错误时保留默认编号，不影响已经确定的取消结果。
            }
        }
        // 记录服务端视角的取消结果，耗时包含取消前已等待的时间，节点地址使用连接的本地地址。
        RpcInvocationEvent event = new RpcInvocationEvent(
                "server", request.getInterfaceName(), RpcErrorCode.CANCELLED,
                System.nanoTime() - startedNanos, attempt, String.valueOf(ctx.channel().localAddress()),
                request.getContext() == null ? "" : request.getContext().getTraceId(), Map.of());
        // 在当前线程依次回调，同一份事件可交给指标统计、链路追踪等多个监听器处理。
        for (RpcInvocationListener listener : invocationListeners) {
            try {
                listener.onComplete(event);
            } catch (RuntimeException e) {
                // 监听器的运行时异常只记录日志，继续通知其他监听器。
                log.warn("RPC invocation listener failed", e);
            }
        }
    }

    /** 提交响应并通知监听器，默认不携带附加事件属性，复用下面的完整实现。 */
    private void completeRequest(ChannelHandlerContext ctx, RpcMessage requestMessage, RpcRequest request,
                                 RpcResponse response, long startedNanos) {
        completeRequest(ctx, requestMessage, request, response, startedNanos, Map.of());
    }

    /**
     * 统一处理业务响应和拒绝响应，并报告服务端调用结果。
     *
     * 1. returnResponse() 在连接有效时提交响应写入，连接已关闭时丢弃响应。
     * 2. 根据响应状态、耗时、尝试编号、traceId 和附加属性构造调用事件。
     * 3. 在当前线程依次通知监听器，隔离监听器抛出的运行时异常。
     *
     * 事件表示服务端处理结果，不代表网络写入已经完成或客户端已经收到响应。
     * 本方法不清理任务映射或释放在途计数，这些工作由 PendingInvocation 或前面的拒绝分支负责。
     */
    private void completeRequest(ChannelHandlerContext ctx, RpcMessage requestMessage, RpcRequest request,
                                 RpcResponse response, long startedNanos, Map<String, String> attributes) {
        // 提交写操作后继续记录事件，不等待网络发送完成；已关闭连接的结果仍会被记录。
        returnResponse(ctx, requestMessage, response);
        // 事件约定：错误码为 null 表示成功，失败响应保留其错误码。
        RpcErrorCode errorCode = response.getStatus() == RpcResponse.STATUS_FAIL ? response.getErrorCode() : null;
        // 读取客户端携带的重试编号，缺失时按首次调用记录。
        int attempt = 0;
        if (request.getContext() != null) {
            String value = request.getContext().getAttachments().get("retryAttempt");
            try {
                attempt = value == null ? 0 : Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
                // 统计附件格式错误不影响响应处理，继续使用默认编号 0。
            }
        }
        // 从 Handler 开始处理请求时计时，包含前置检查、排队和业务执行中实际经历的时间。
        RpcInvocationEvent event = new RpcInvocationEvent(
                "server", request.getInterfaceName(), errorCode, System.nanoTime() - startedNanos, attempt,
                String.valueOf(ctx.channel().localAddress()),
                request.getContext() == null ? "" : request.getContext().getTraceId(), attributes);
        // 将同一份结果事件同步交给各监听器，例如写入 Micrometer 指标或生成追踪记录。
        for (RpcInvocationListener listener : invocationListeners) {
            try {
                listener.onComplete(event);
            } catch (RuntimeException e) {
                // 一个监听器失败不会阻止其余监听器接收事件。
                log.warn("RPC invocation listener failed", e);
            }
        }
    }

    private void returnResponse(ChannelHandlerContext ctx, RpcMessage requestMessage, RpcResponse response) {
        if (ctx.channel().isActive()) {
            RpcMessage responseMessage = new RpcMessage(MessageType.RESPONSE,
                    requestMessage.getSerializerType(), response);
            ctx.writeAndFlush(responseMessage);
        } else {
            log.warn("Channel already closed, discarding response for requestId={}", response.getRequestId());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleEvent) {
            if (idleEvent.state() == IdleState.READER_IDLE) {
                // 读空闲：客户端长时间未发送心跳，关闭连接
                log.warn("Client heartbeat timeout, closing connection: {}", ctx.channel().remoteAddress());
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Server handler exception", cause);
        ctx.close();
    }
}
