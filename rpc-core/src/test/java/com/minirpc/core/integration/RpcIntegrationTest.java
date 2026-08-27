package com.minirpc.core.integration;

import com.minirpc.common.generic.GenericService;
import com.minirpc.core.RpcClient;
import com.minirpc.core.RpcServer;
import com.minirpc.core.registry.LocalServiceRegistry;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RPC 端到端集成测试
 * <p>
 * 启动真实的 Server + Client，完成完整的 RPC 调用链路验证：
 * 客户端代理 → 限流 → 熔断 → FilterChain → 序列化 → Netty传输 → 反序列化 → 业务线程池 → 反射调用 → 响应返回
 * <p>
 * 面试考点：
 * - 集成测试 vs 单元测试的区别
 * - 如何验证分布式系统的端到端正确性
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RpcIntegrationTest {

    private static RpcServer server;
    private static Thread serverThread;
    private static final int TEST_PORT = 19090;

    @BeforeAll
    static void startServer() throws InterruptedException {
        server = new RpcServer(TEST_PORT);
        server.registerService(new EchoServiceImpl());

        // 异步接口也注册同一个实现（服务端不区分同步/异步，只是客户端返回方式不同）
        LocalServiceRegistry.registerService(
                EchoServiceAsync.class.getName(), new EchoServiceImpl());

        // 在后台线程启动服务端
        serverThread = new Thread(() -> server.start(), "test-rpc-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // 等待服务端启动
        Thread.sleep(1000);
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    @Order(1)
    @DisplayName("同步 RPC 调用：echo 方法")
    void testSyncRpcCall() {
        RpcClient client = new RpcClient("127.0.0.1", TEST_PORT);
        try {
            EchoService echoService = client.createProxy(EchoService.class);
            String result = echoService.echo("Hello MiniRPC");
            assertEquals("Echo: Hello MiniRPC", result);
        } finally {
            client.shutdown();
        }
    }

    @Test
    @Order(2)
    @DisplayName("同步 RPC 调用：多参数方法")
    void testMultiParamCall() {
        RpcClient client = new RpcClient("127.0.0.1", TEST_PORT);
        try {
            EchoService echoService = client.createProxy(EchoService.class);
            int result = echoService.add(3, 7);
            assertEquals(10, result);
        } finally {
            client.shutdown();
        }
    }

    @Test
    @Order(3)
    @DisplayName("异步 RPC 调用：CompletableFuture")
    void testAsyncRpcCall() throws Exception {
        RpcClient client = new RpcClient("127.0.0.1", TEST_PORT);
        try {
            // 异步代理：接口方法返回 CompletableFuture
            EchoServiceAsync asyncService = client.createAsyncProxy(EchoServiceAsync.class);
            CompletableFuture<String> future = asyncService.echo("Async Hello");
            String value = future.get(5, TimeUnit.SECONDS);
            assertEquals("Echo: Async Hello", value);
        } finally {
            client.shutdown();
        }
    }

    @Test
    @Order(4)
    @DisplayName("泛化调用：无需依赖接口 JAR")
    void testGenericInvocation() {
        RpcClient client = new RpcClient("127.0.0.1", TEST_PORT);
        try {
            // 泛化调用：不需要 EchoService.class，只需知道接口名
            GenericService genericService = client.createGenericProxy(
                    EchoService.class.getName());

            // 调用 echo 方法
            Object result = genericService.$invoke(
                    "echo",
                    new String[]{"java.lang.String"},
                    new Object[]{"Generic Hello"});
            assertEquals("Echo: Generic Hello", result);

            // 调用 add 方法
            Object sum = genericService.$invoke(
                    "add",
                    new String[]{"int", "int"},
                    new Object[]{5, 8});
            assertEquals(13, sum);
        } finally {
            client.shutdown();
        }
    }

    @Test
    @Order(5)
    @DisplayName("多次调用验证连接池复用")
    void testConnectionPoolReuse() {
        RpcClient client = new RpcClient("127.0.0.1", TEST_PORT);
        try {
            EchoService echoService = client.createProxy(EchoService.class);
            for (int i = 0; i < 20; i++) {
                String result = echoService.echo("msg-" + i);
                assertEquals("Echo: msg-" + i, result);
            }
        } finally {
            client.shutdown();
        }
    }

    // ========== 测试用服务接口和实现 ==========

    public interface EchoService {
        String echo(String message);
        int add(int a, int b);
    }

    /**
     * 异步接口：方法返回 CompletableFuture
     */
    public interface EchoServiceAsync {
        CompletableFuture<String> echo(String message);
    }

    public static class EchoServiceImpl implements EchoService {
        @Override
        public String echo(String message) {
            return "Echo: " + message;
        }

        @Override
        public int add(int a, int b) {
            return a + b;
        }
    }
}
