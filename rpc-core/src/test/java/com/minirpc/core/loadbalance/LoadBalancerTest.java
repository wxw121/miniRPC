package com.minirpc.core.loadbalance;

import com.minirpc.registry.ServiceInstance;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 负载均衡器单元测试
 * <p>
 * 验证 Random、RoundRobin、ConsistentHash 三种策略
 */
class LoadBalancerTest {

    private List<ServiceInstance> buildInstances() {
        List<ServiceInstance> instances = new ArrayList<>();
        instances.add(new ServiceInstance("TestService", "192.168.1.1", 9090, "1.0", "default"));
        instances.add(new ServiceInstance("TestService", "192.168.1.2", 9090, "1.0", "default"));
        instances.add(new ServiceInstance("TestService", "192.168.1.3", 9090, "1.0", "default"));
        return instances;
    }

    @Test
    @DisplayName("RandomLoadBalancer: 返回的实例在列表中")
    void testRandomLoadBalancer() {
        LoadBalancer lb = new RandomLoadBalancer();
        List<ServiceInstance> instances = buildInstances();

        for (int i = 0; i < 100; i++) {
            ServiceInstance selected = lb.select(instances, "req-" + i);
            assertNotNull(selected);
            assertTrue(instances.contains(selected));
        }
    }

    @Test
    @DisplayName("RandomLoadBalancer: 多次选择应覆盖所有节点")
    void testRandomDistribution() {
        LoadBalancer lb = new RandomLoadBalancer();
        List<ServiceInstance> instances = buildInstances();
        Map<String, Integer> countMap = new HashMap<>();

        for (int i = 0; i < 300; i++) {
            ServiceInstance selected = lb.select(instances, "req-" + i);
            countMap.merge(selected.getAddress(), 1, Integer::sum);
        }

        // 300次选择，3个节点，每个节点至少应被选中一些次数
        assertTrue(countMap.size() == 3, "All 3 instances should be selected");
    }

    @Test
    @DisplayName("RoundRobinLoadBalancer: 轮询顺序正确")
    void testRoundRobinLoadBalancer() {
        LoadBalancer lb = new RoundRobinLoadBalancer();
        List<ServiceInstance> instances = buildInstances();

        ServiceInstance first = lb.select(instances, "req-1");
        ServiceInstance second = lb.select(instances, "req-2");
        ServiceInstance third = lb.select(instances, "req-3");
        ServiceInstance fourth = lb.select(instances, "req-4");

        // 轮询：1->2->3->1
        assertEquals(first.getAddress(), fourth.getAddress());
        assertNotEquals(first.getAddress(), second.getAddress());
        assertNotEquals(second.getAddress(), third.getAddress());
    }

    @Test
    @DisplayName("ConsistentHashLoadBalancer: 相同 key 选择相同节点")
    void testConsistentHashSameKey() {
        LoadBalancer lb = new ConsistentHashLoadBalancer();
        List<ServiceInstance> instances = buildInstances();

        ServiceInstance first = lb.select(instances, "user-123");
        ServiceInstance second = lb.select(instances, "user-123");
        ServiceInstance third = lb.select(instances, "user-123");

        assertEquals(first.getAddress(), second.getAddress());
        assertEquals(second.getAddress(), third.getAddress());
    }

    @Test
    @DisplayName("ConsistentHashLoadBalancer: 不同 key 可能选择不同节点")
    void testConsistentHashDifferentKeys() {
        LoadBalancer lb = new ConsistentHashLoadBalancer();
        List<ServiceInstance> instances = buildInstances();
        Map<String, Integer> countMap = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            ServiceInstance selected = lb.select(instances, "key-" + i);
            countMap.merge(selected.getAddress(), 1, Integer::sum);
        }

        // 100个不同key，应该分散到多个节点
        assertTrue(countMap.size() >= 2, "Keys should be distributed to multiple nodes");
    }

    @Test
    @DisplayName("单节点时所有策略都返回该节点")
    void testSingleNode() {
        List<ServiceInstance> instances = new ArrayList<>();
        instances.add(new ServiceInstance("TestService", "192.168.1.1", 9090, "1.0", "default"));

        assertEquals("192.168.1.1:9090", new RandomLoadBalancer().select(instances, "k").getAddress());
        assertEquals("192.168.1.1:9090", new RoundRobinLoadBalancer().select(instances, "k").getAddress());
        assertEquals("192.168.1.1:9090", new ConsistentHashLoadBalancer().select(instances, "k").getAddress());
    }

    @Test
    @DisplayName("服务预热: 新节点权重低于老节点")
    void testWarmupWeight() {
        long warmupPeriod = 10 * 60 * 1000L; // 10分钟

        // 老节点：启动已超过预热期
        ServiceInstance oldInstance = new ServiceInstance("Svc", "10.0.0.1", 9090);
        oldInstance.setWeight(100);
        oldInstance.setStartTime(System.currentTimeMillis() - warmupPeriod - 1000);

        // 新节点：刚启动 1 分钟（预热期 10 分钟的 10%）
        ServiceInstance newInstance = new ServiceInstance("Svc", "10.0.0.2", 9090);
        newInstance.setWeight(100);
        newInstance.setStartTime(System.currentTimeMillis() - 60 * 1000L);

        // 老节点有效权重应为全量 100
        assertEquals(100, oldInstance.getWarmupWeight(warmupPeriod));

        // 新节点有效权重应约为 10（100 * 1min / 10min）
        int newWeight = newInstance.getWarmupWeight(warmupPeriod);
        assertTrue(newWeight >= 8 && newWeight <= 12,
                "New instance warmup weight should be ~10, got: " + newWeight);
    }

    @Test
    @DisplayName("服务预热: 新节点被选中概率显著低于老节点")
    void testWarmupAffectsSelection() {
        long warmupPeriod = 10 * 60 * 1000L;

        // 老节点：权重 100，已过预热期
        ServiceInstance oldInstance = new ServiceInstance("Svc", "10.0.0.1", 9090);
        oldInstance.setWeight(100);
        oldInstance.setStartTime(System.currentTimeMillis() - warmupPeriod - 1000);

        // 新节点：权重 100，但刚启动（预热期内，有效权重很低）
        ServiceInstance newInstance = new ServiceInstance("Svc", "10.0.0.2", 9090);
        newInstance.setWeight(100);
        newInstance.setStartTime(System.currentTimeMillis());

        List<ServiceInstance> instances = List.of(oldInstance, newInstance);
        LoadBalancer lb = new RandomLoadBalancer(warmupPeriod);

        int oldCount = 0;
        int total = 1000;
        for (int i = 0; i < total; i++) {
            ServiceInstance selected = lb.select(instances, "req-" + i);
            if ("10.0.0.1:9090".equals(selected.getAddress())) {
                oldCount++;
            }
        }

        // 老节点应获得绑大多数流量（>90%）
        assertTrue(oldCount > total * 0.9,
                "Old instance should get >90% traffic during warmup, got: " + (oldCount * 100.0 / total) + "%");
    }
}
