package com.minirpc.core.filter;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 调用统计 Filter（简易 Metrics）
 * <p>
 * 统计每个服务的：
 * - 总调用次数
 * - 成功次数 / 失败次数
 * - 总耗时（用于计算平均 RT）
 * <p>
 * 面试考点：
 * - 监控指标设计（QPS、RT、成功率、P99）
 * - 无锁计数（AtomicLong）
 * - 滑动窗口统计（进阶）
 */
public class MetricsFilter implements RpcFilter {

    private static final Logger log = LoggerFactory.getLogger(MetricsFilter.class);

    /**
     * 服务级别统计：serviceName -> Metrics
     */
    private static final Map<String, ServiceMetrics> METRICS_MAP = new ConcurrentHashMap<>();

    @Override
    public RpcResponse filter(RpcRequest request, FilterInvoker invoker) {
        String serviceName = request.getInterfaceName();
        ServiceMetrics metrics = METRICS_MAP.computeIfAbsent(serviceName, k -> new ServiceMetrics());

        long startTime = System.currentTimeMillis();
        metrics.totalCalls.incrementAndGet();

        try {
            RpcResponse response = invoker.invoke(request);
            long rt = System.currentTimeMillis() - startTime;
            metrics.totalRt.addAndGet(rt);

            if (response.getStatus() == RpcResponse.STATUS_SUCCESS) {
                metrics.successCalls.incrementAndGet();
            } else {
                metrics.failedCalls.incrementAndGet();
            }

            log.debug("[Metrics] {}.{} RT={}ms", serviceName, request.getMethodName(), rt);
            return response;

        } catch (Exception e) {
            metrics.failedCalls.incrementAndGet();
            long rt = System.currentTimeMillis() - startTime;
            metrics.totalRt.addAndGet(rt);
            throw e;
        }
    }

    @Override
    public int getOrder() {
        return -50; // 在 TraceFilter 之后
    }

    /**
     * 获取指定服务的统计信息
     */
    public static ServiceMetrics getMetrics(String serviceName) {
        return METRICS_MAP.get(serviceName);
    }

    /**
     * 打印所有服务的统计信息
     */
    public static void printAllMetrics() {
        METRICS_MAP.forEach((name, metrics) -> {
            long total = metrics.totalCalls.get();
            long success = metrics.successCalls.get();
            long failed = metrics.failedCalls.get();
            long totalRt = metrics.totalRt.get();
            double avgRt = total > 0 ? (double) totalRt / total : 0;
            double successRate = total > 0 ? (double) success / total * 100 : 0;

            log.info("[Metrics] {} | total={} | success={} | failed={} | avgRT={}ms | successRate={}%",
                    name, total, success, failed, String.format("%.2f", avgRt), String.format("%.1f", successRate));
        });
    }

    /**
     * 服务级别统计指标
     */
    public static class ServiceMetrics {
        public final AtomicLong totalCalls = new AtomicLong(0);
        public final AtomicLong successCalls = new AtomicLong(0);
        public final AtomicLong failedCalls = new AtomicLong(0);
        public final AtomicLong totalRt = new AtomicLong(0);

        public double getAvgRt() {
            long total = totalCalls.get();
            return total > 0 ? (double) totalRt.get() / total : 0;
        }

        public double getSuccessRate() {
            long total = totalCalls.get();
            return total > 0 ? (double) successCalls.get() / total * 100 : 0;
        }
    }
}
