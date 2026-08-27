package com.minirpc.core.loadbalance;

import com.minirpc.registry.ServiceInstance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询负载均衡
 * <p>
 * 按顺序依次选择服务实例，保证每个实例被均匀调用。
 * 使用 AtomicInteger 保证多线程安全。
 * <p>
 * 面试考点：AtomicInteger CAS 无锁操作、取模运算防溢出
 */
public class RoundRobinLoadBalancer implements LoadBalancer {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public ServiceInstance select(List<ServiceInstance> instances, String requestKey) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        // 使用 & 0x7FFFFFFF 防止负数（int 溢出后变负）
        int index = (counter.getAndIncrement() & 0x7FFFFFFF) % instances.size();
        return instances.get(index);
    }
}
