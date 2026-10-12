package com.minirpc.core.loadbalance;

import com.minirpc.registry.ServiceInstance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 加权随机负载均衡（支持服务预热/慢启动）
 * <p>
 * 根据服务实例的有效权重进行加权随机选择。
 * 有效权重 = 配置权重 × 预热进度（新节点权重从 1 线性增长到配置值）
 */
public class RandomLoadBalancer implements LoadBalancer {

    /**
     * 默认预热时长：10 分钟（与 Dubbo 一致）
     */
    private static final long DEFAULT_WARMUP_PERIOD = 10 * 60 * 1000L;

    private final long warmupPeriod;

    public RandomLoadBalancer() {
        this(DEFAULT_WARMUP_PERIOD);
    }

    public RandomLoadBalancer(long warmupPeriod) {
        this.warmupPeriod = warmupPeriod;
    }

    @Override
    public ServiceInstance select(List<ServiceInstance> instances, String requestKey) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        if (instances.size() == 1) {
            return instances.get(0);
        }

        // 计算每个实例的预热有效权重
        int totalWeight = 0;
        boolean allSameWeight = true;
        int firstWeight = instances.get(0).getWarmupWeight(warmupPeriod);

        for (ServiceInstance instance : instances) {
            int weight = instance.getWarmupWeight(warmupPeriod);
            totalWeight += weight;
            if (weight != firstWeight) {
                allSameWeight = false;
            }
        }

        // 所有实例权重相同，退化为普通随机
        if (allSameWeight || totalWeight <= 0) {
            int index = ThreadLocalRandom.current().nextInt(instances.size());
            return instances.get(index);
        }

        // 加权随机：生成 [0, totalWeight) 的随机数
        int randomWeight = ThreadLocalRandom.current().nextInt(totalWeight);
        for (ServiceInstance instance : instances) {
            randomWeight -= instance.getWarmupWeight(warmupPeriod);
            if (randomWeight < 0) {
                return instance;
            }
        }

        // 容错：理论上不会到达这里
        return instances.get(0);
    }
}
