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
 * <p>
 * 面试考点：
 * - 为什么不能在 Netty IO 线程执行业务逻辑？（阻塞 Worker 线程导致所有连接卡死）
 * - IO 线程与业务线程分离（Reactor 模型的最佳实践）
 * - Dubbo 的线程模型：IO 线程只负责编解码，业务线程池负责反射调用
 */
public class RpcServerHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(RpcServerHandler.class);

    /**
     * Method 缓存：避免每次请求都执行反射 getMethod()
     * key = 实现类名 + 方法名 + 参数类型列表，value = 解析后的 Method 对象
     */
    private static final Map<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();
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

    boolean cancelQueuedInvocation(String requestId) {
        PendingInvocation pending = pendingInvocations.get(requestId);
        if (pending != null && pending.cancelQueued()) {
            log.debug("Cancelled queued RPC request: requestId={}", requestId);
            return true;
        }
        return false;
    }
    private enum InvocationState { QUEUED, RUNNING, CANCELLED, COMPLETED }

    private final class PendingInvocation implements Runnable {
        private final ChannelHandlerContext ctx;
        private final RpcRequest request;
        private final long startedNanos;
        private final FutureTask<Void> task;
        private final AtomicReference<InvocationState> state = new AtomicReference<>(InvocationState.QUEUED);

        private PendingInvocation(ChannelHandlerContext ctx, RpcRequest request,
                                  long startedNanos, Runnable invocation) {
            this.ctx = ctx;
            this.request = request;
            this.startedNanos = startedNanos;
            this.task = new FutureTask<>(invocation, null);
        }

        @Override
        public void run() {
            if (!state.compareAndSet(InvocationState.QUEUED, InvocationState.RUNNING)) {
                return;
            }
            try {
                task.run();
            } finally {
                state.set(InvocationState.COMPLETED);
                pendingInvocations.remove(request.getRequestId(), this);
                shutdownManager.releaseRequest();
            }
        }

        private boolean cancelQueued() {
            if (!state.compareAndSet(InvocationState.QUEUED, InvocationState.CANCELLED)) {
                return false;
            }
            task.cancel(false);
            pendingInvocations.remove(request.getRequestId(), this);
            shutdownManager.releaseRequest();
            emitCancellationEvent(ctx, request, startedNanos);
            return true;
        }
    }
    private void emitCancellationEvent(ChannelHandlerContext ctx, RpcRequest request, long startedNanos) {
        int attempt = 0;
        if (request.getContext() != null) {
            String value = request.getContext().getAttachments().get("retryAttempt");
            try {
                attempt = value == null ? 0 : Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
                // A malformed optional attachment must not change cancellation semantics.
            }
        }
        RpcInvocationEvent event = new RpcInvocationEvent(
                "server", request.getInterfaceName(), RpcErrorCode.CANCELLED,
                System.nanoTime() - startedNanos, attempt, String.valueOf(ctx.channel().localAddress()),
                request.getContext() == null ? "" : request.getContext().getTraceId(), Map.of());
        for (RpcInvocationListener listener : invocationListeners) {
            try {
                listener.onComplete(event);
            } catch (RuntimeException e) {
                log.warn("RPC invocation listener failed", e);
            }
        }
    }
    private void completeRequest(ChannelHandlerContext ctx, RpcMessage requestMessage, RpcRequest request,
                                 RpcResponse response, long startedNanos) {
        completeRequest(ctx, requestMessage, request, response, startedNanos, Map.of());
    }

    private void completeRequest(ChannelHandlerContext ctx, RpcMessage requestMessage, RpcRequest request,
                                 RpcResponse response, long startedNanos, Map<String, String> attributes) {
        returnResponse(ctx, requestMessage, response);
        RpcErrorCode errorCode = response.getStatus() == RpcResponse.STATUS_FAIL ? response.getErrorCode() : null;
        int attempt = 0;
        if (request.getContext() != null) {
            String value = request.getContext().getAttachments().get("retryAttempt");
            try {
                attempt = value == null ? 0 : Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
                // A malformed optional attachment must not change request completion semantics.
            }
        }
        RpcInvocationEvent event = new RpcInvocationEvent(
                "server", request.getInterfaceName(), errorCode, System.nanoTime() - startedNanos, attempt,
                String.valueOf(ctx.channel().localAddress()),
                request.getContext() == null ? "" : request.getContext().getTraceId(), attributes);
        for (RpcInvocationListener listener : invocationListeners) {
            try {
                listener.onComplete(event);
            } catch (RuntimeException e) {
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
