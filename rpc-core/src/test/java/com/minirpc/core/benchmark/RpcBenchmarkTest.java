package com.minirpc.core.benchmark;

import com.minirpc.common.protocol.SerializerType;
import com.minirpc.core.RpcServer;
import com.minirpc.core.client.NettyClient;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.proxy.RpcClientProxy;
import com.minirpc.core.registry.LocalServiceRegistry;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mini RPC 框架性能压测
 * <p>
 * 测试场景：
 * 1. 单线程基准 —— 建立延迟基线，测量 P50/P95/P99
 * 2. 多并发梯度 —— 4/8/16/32 并发线程，测量最大 TPS
 * 3. 序列化协议对比 —— JDK / Hessian2 / Protostuff
 * 4. 负载大小对比 —— 轻负载(String) vs 中负载(复合对象)
 * <p>
 * 结果会通过 BenchmarkResult 汇总打印，
 * 外部脚本读取 stdout 写入报告文档。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("benchmark")
class RpcBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(RpcBenchmarkTest.class);

    private static final int TEST_PORT = 19099;
    // 预热次数
    private static final int WARMUP_CALLS = 500;
    // 单线程基准调用次数
    private static final int BASELINE_CALLS = 5000;
    // 并发压测每线程调用次数
    private static final int CALLS_PER_THREAD = 2000;

    private static RpcServer server;

    // 全局压测结果汇总
    private static final List<BenchmarkResult> RESULTS = new ArrayList<>();

    // ========== 服务接口与实现 ==========

    public interface EchoService {
        String echo(String message);
        int add(int a, int b);
        OrderInfo getOrder(String id);
    }

    /**
     * 模拟中等复杂度的业务对象（用于测试序列化开销）
     */
    public static class OrderInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private String orderId;
        private String productName;
        private int quantity;
        private double price;
        private String status;
        private long createTime;

        public OrderInfo() {}
        public OrderInfo(String orderId, String productName, int qty, double price) {
            this.orderId = orderId;
            this.productName = productName;
            this.quantity = qty;
            this.price = price;
            this.status = "CREATED";
            this.createTime = System.currentTimeMillis();
        }
        public String getOrderId() { return orderId; }
        public void setOrderId(String v) { this.orderId = v; }
        public String getProductName() { return productName; }
        public void setProductName(String v) { this.productName = v; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int v) { this.quantity = v; }
        public double getPrice() { return price; }
        public void setPrice(double v) { this.price = v; }
        public String getStatus() { return status; }
        public void setStatus(String v) { this.status = v; }
        public long getCreateTime() { return createTime; }
        public void setCreateTime(long v) { this.createTime = v; }
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
        @Override
        public OrderInfo getOrder(String id) {
            return new OrderInfo(id, "MiniRPC Widget", 3, 99.9);
        }
    }

    // ========== JUnit 生命周期 ==========

    @BeforeAll
    static void startServer() throws InterruptedException {
        LocalServiceRegistry.clear();
        server = new RpcServer(TEST_PORT);
        server.registerService(new EchoServiceImpl());

        Thread t = new Thread(() -> server.start(), "bench-server");
        t.setDaemon(true);
        t.start();
        Thread.sleep(1500);
        log.info("=== Benchmark Server started on port {} ===", TEST_PORT);
    }

    @AfterAll
    static void stopServerAndPrintResults() {
        if (server != null) {
            server.stop();
        }
        printSummary();
    }

    // ========== 压测场景 ==========

    /**
     * 场景 1：Hessian2 序列化，单线程基准测试
     */
    @Test
    @Order(1)
    @DisplayName("场景1: Hessian2 单线程基准")
    void bench_singleThread_hessian2() throws Exception {
        BenchmarkResult result = runSingleThread(
                "Hessian2 单线程 echo", SerializerType.HESSIAN2, BASELINE_CALLS);
        RESULTS.add(result);
    }

    /**
     * 场景 2：JDK 序列化，单线程基准测试
     */
    @Test
    @Order(2)
    @DisplayName("场景2: JDK 序列化单线程基准")
    void bench_singleThread_jdk() throws Exception {
        BenchmarkResult result = runSingleThread(
                "JDK 单线程 echo", SerializerType.JDK, BASELINE_CALLS);
        RESULTS.add(result);
    }

    /**
     * 场景 3：Protostuff 序列化，单线程基准测试
     */
    @Test
    @Order(3)
    @DisplayName("场景3: Protostuff 序列化单线程基准")
    void bench_singleThread_protostuff() throws Exception {
        BenchmarkResult result = runSingleThread(
                "Protostuff 单线程 echo", SerializerType.PROTOSTUFF, BASELINE_CALLS);
        RESULTS.add(result);
    }

    /**
     * 场景 4：Hessian2，4/8/16/32 线程并发梯度
     */
    @Test
    @Order(4)
    @DisplayName("场景4: Hessian2 并发梯度 4→32 线程")
    void bench_concurrent_gradient() throws Exception {
        int[] threads = {4, 8, 16, 32};
        for (int t : threads) {
            BenchmarkResult result = runConcurrent(
                    "Hessian2 并发 " + t + " 线程", SerializerType.HESSIAN2, t, CALLS_PER_THREAD);
            RESULTS.add(result);
        }
    }

    /**
     * 场景 5：复合对象负载（中等大小 payload）
     */
    @Test
    @Order(5)
    @DisplayName("场景5: 复合对象负载对比（Hessian2 vs Protostuff）")
    void bench_complexPayload() throws Exception {
        BenchmarkResult r1 = runComplexPayload(
                "Hessian2 复合对象 8线程", SerializerType.HESSIAN2, 8, CALLS_PER_THREAD);
        BenchmarkResult r2 = runComplexPayload(
                "Protostuff 复合对象 8线程", SerializerType.PROTOSTUFF, 8, CALLS_PER_THREAD);
        RESULTS.add(r1);
        RESULTS.add(r2);
    }

    // ========== 压测执行引擎 ==========

    /**
     * 单线程压测：先预热，再正式计时
     */
    private BenchmarkResult runSingleThread(String name, SerializerType serType, int calls)
            throws Exception {
        RpcConfig config = buildBenchConfig(serType);
        NettyClient nettyClient = new NettyClient(config);
        try {
            RpcClientProxy proxy = new RpcClientProxy(nettyClient, "127.0.0.1", TEST_PORT, config);
            EchoService service = proxy.createProxy(EchoService.class);

            // 预热
            for (int i = 0; i < WARMUP_CALLS; i++) {
                service.echo("warmup-" + i);
            }

            // 正式压测
            long[] latencies = new long[calls];
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < calls; i++) {
                long t0 = System.nanoTime();
                service.echo("benchmark-" + i);
                latencies[i] = (System.nanoTime() - t0) / 1000; // 微秒
            }
            long totalMs = System.currentTimeMillis() - startTime;

            return BenchmarkResult.of(name, 1, calls, totalMs, latencies);
        } finally {
            nettyClient.shutdown();
        }
    }

    /**
     * 多线程并发压测（echo 方法）
     */
    private BenchmarkResult runConcurrent(String name, SerializerType serType,
                                           int threadCount, int callsPerThread) throws Exception {
        RpcConfig config = buildBenchConfig(serType);
        NettyClient nettyClient = new NettyClient(config);
        try {
            // 预热（单线程预热，不计入统计）
            RpcClientProxy warmupProxy = new RpcClientProxy(
                    nettyClient, "127.0.0.1", TEST_PORT, config);
            EchoService warmupSvc = warmupProxy.createProxy(EchoService.class);
            for (int i = 0; i < WARMUP_CALLS; i++) {
                warmupSvc.echo("warmup");
            }

            // 正式压测
            int totalCalls = threadCount * callsPerThread;
            long[] latencies = new long[totalCalls];
            LongAdder successCount = new LongAdder();
            AtomicInteger latencyIndex = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    RpcClientProxy proxy = new RpcClientProxy(
                            nettyClient, "127.0.0.1", TEST_PORT, config);
                    EchoService svc = proxy.createProxy(EchoService.class);
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            long t0 = System.nanoTime();
                            svc.echo("bench");
                            int idx = latencyIndex.getAndIncrement();
                            if (idx < latencies.length) {
                                latencies[idx] = (System.nanoTime() - t0) / 1000;
                            }
                            successCount.increment();
                        }
                    } catch (Exception e) {
                        log.warn("Bench error: {}", e.getMessage());
                    } finally {
                        done.countDown();
                    }
                });
            }

            ready.await();
            long startMs = System.currentTimeMillis();
            start.countDown();
            done.await();
            long totalMs = System.currentTimeMillis() - startMs;

            executor.shutdown();

            // 只取实际记录的延迟
            int recorded = Math.min(latencyIndex.get(), latencies.length);
            long[] actualLatencies = Arrays.copyOf(latencies, recorded);

            return BenchmarkResult.of(name, threadCount, successCount.intValue(), totalMs, actualLatencies);
        } finally {
            nettyClient.shutdown();
        }
    }

    /**
     * 复合对象负载压测（getOrder 方法）
     */
    private BenchmarkResult runComplexPayload(String name, SerializerType serType,
                                               int threadCount, int callsPerThread) throws Exception {
        RpcConfig config = buildBenchConfig(serType);
        NettyClient nettyClient = new NettyClient(config);
        try {
            // 预热
            RpcClientProxy warmupProxy = new RpcClientProxy(
                    nettyClient, "127.0.0.1", TEST_PORT, config);
            EchoService warmupSvc = warmupProxy.createProxy(EchoService.class);
            for (int i = 0; i < WARMUP_CALLS; i++) {
                warmupSvc.getOrder("warmup-" + i);
            }

            int totalCalls = threadCount * callsPerThread;
            long[] latencies = new long[totalCalls];
            LongAdder successCount = new LongAdder();
            AtomicInteger latencyIndex = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    RpcClientProxy proxy = new RpcClientProxy(
                            nettyClient, "127.0.0.1", TEST_PORT, config);
                    EchoService svc = proxy.createProxy(EchoService.class);
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            long t0 = System.nanoTime();
                            svc.getOrder("ORDER-" + i);
                            int idx = latencyIndex.getAndIncrement();
                            if (idx < latencies.length) {
                                latencies[idx] = (System.nanoTime() - t0) / 1000;
                            }
                            successCount.increment();
                        }
                    } catch (Exception e) {
                        log.warn("Bench error: {}", e.getMessage());
                    } finally {
                        done.countDown();
                    }
                });
            }

            ready.await();
            long startMs = System.currentTimeMillis();
            start.countDown();
            done.await();
            long totalMs = System.currentTimeMillis() - startMs;

            executor.shutdown();

            int recorded = Math.min(latencyIndex.get(), latencies.length);
            long[] actualLatencies = Arrays.copyOf(latencies, recorded);

            return BenchmarkResult.of(name, threadCount, successCount.intValue(), totalMs, actualLatencies);
        } finally {
            nettyClient.shutdown();
        }
    }

    // ========== 工具方法 ==========

    /**
     * 构建压测专用 RpcConfig：关闭限流、禁用重试、超长超时
     */
    private RpcConfig buildBenchConfig(SerializerType serType) {
        return RpcConfig.builder()
                .serializerType(serType)
                .timeoutMs(30000)
                .rateLimiterCapacity(10_000_000L)
                .rateLimiterTokensPerSecond(10_000_000.0)
                .circuitBreakerFailureThreshold(100_000)
                .failoverMaxRetries(0)
                .build();
    }

    /**
     * 打印汇总表
     */
    private static void printSummary() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                        Mini RPC 性能压测结果汇总                                          ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════════════════════════════╣");
        System.out.printf("║ %-28s │ %5s │ %8s │ %8s │ %7s │ %7s │ %7s ║%n",
                "场景", "线程", "TPS", "P50(ms)", "P95(ms)", "P99(ms)", "Max(ms)");
        System.out.println("╠══════════════════════════════════════════════════════════════════════════════════════════╣");
        for (BenchmarkResult r : RESULTS) {
            System.out.printf("║ %-28s │ %5d │ %8d │ %8.2f │ %7.2f │ %7.2f │ %7.2f ║%n",
                    r.name, r.threads, r.tps,
                    r.p50Ms, r.p95Ms, r.p99Ms, r.maxMs);
        }
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();

        // 同时输出便于解析的 CSV 格式（供报告生成脚本读取）
        System.out.println("=== BENCHMARK_CSV_START ===");
        System.out.println("scenario,threads,total_calls,total_ms,tps,avg_ms,p50_ms,p75_ms,p95_ms,p99_ms,p999_ms,max_ms,min_ms");
        for (BenchmarkResult r : RESULTS) {
            System.out.printf("%s,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                    r.name, r.threads, r.totalCalls, r.totalMs, r.tps,
                    r.avgMs, r.p50Ms, r.p75Ms, r.p95Ms, r.p99Ms, r.p999Ms, r.maxMs, r.minMs);
        }
        System.out.println("=== BENCHMARK_CSV_END ===");
    }

    // ========== 结果数据模型 ==========

    static class BenchmarkResult {
        final String name;
        final int threads;
        final int totalCalls;
        final long totalMs;
        final long tps;
        final double avgMs;
        final double p50Ms;
        final double p75Ms;
        final double p95Ms;
        final double p99Ms;
        final double p999Ms;
        final double maxMs;
        final double minMs;

        BenchmarkResult(String name, int threads, int totalCalls, long totalMs,
                        long tps, double avgMs, double p50Ms, double p75Ms,
                        double p95Ms, double p99Ms, double p999Ms,
                        double maxMs, double minMs) {
            this.name = name;
            this.threads = threads;
            this.totalCalls = totalCalls;
            this.totalMs = totalMs;
            this.tps = tps;
            this.avgMs = avgMs;
            this.p50Ms = p50Ms;
            this.p75Ms = p75Ms;
            this.p95Ms = p95Ms;
            this.p99Ms = p99Ms;
            this.p999Ms = p999Ms;
            this.maxMs = maxMs;
            this.minMs = minMs;
        }

        /**
         * 从原始延迟数组（微秒）计算统计指标
         */
        static BenchmarkResult of(String name, int threads, int totalCalls,
                                   long totalMs, long[] latenciesUs) {
            if (latenciesUs == null || latenciesUs.length == 0) {
                return new BenchmarkResult(name, threads, totalCalls, totalMs,
                        0, 0, 0, 0, 0, 0, 0, 0, 0);
            }

            long[] sorted = Arrays.copyOf(latenciesUs, latenciesUs.length);
            Arrays.sort(sorted);

            long tps = totalMs > 0 ? (totalCalls * 1000L / totalMs) : 0;
            double avgUs = Arrays.stream(sorted).average().orElse(0);

            return new BenchmarkResult(
                    name, threads, totalCalls, totalMs, tps,
                    avgUs / 1000.0,
                    percentile(sorted, 50) / 1000.0,
                    percentile(sorted, 75) / 1000.0,
                    percentile(sorted, 95) / 1000.0,
                    percentile(sorted, 99) / 1000.0,
                    percentile(sorted, 99.9) / 1000.0,
                    sorted[sorted.length - 1] / 1000.0,
                    sorted[0] / 1000.0
            );
        }

        static long percentile(long[] sorted, double p) {
            if (sorted.length == 0) return 0;
            int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
            return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
        }
    }
}
