package com.minirpc.core.loadbalance;

import com.minirpc.registry.ServiceInstance;

import java.util.List;

/**
 * 负载均衡器 SPI 接口
 * <p>
 * 从多个服务实例中选择一个进行调用。
 * 内置 Random、RoundRobin、ConsistentHash 三种策略。
 */
public interface LoadBalancer {

    /**
     * 从服务实例列表中选择一个实例
     *
     * @param instances 服务实例列表
     * @param requestKey 请求标识（用于一致性哈希等策略）
     * @return 选中的服务实例
     */
    ServiceInstance select(List<ServiceInstance> instances, String requestKey);
}
