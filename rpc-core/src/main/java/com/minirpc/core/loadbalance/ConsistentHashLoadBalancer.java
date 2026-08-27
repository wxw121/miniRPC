package com.minirpc.core.loadbalance;

import com.minirpc.registry.ServiceInstance;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 一致性哈希负载均衡
 * <p>
 * 原理：
 * 1. 将哈希值空间组织成一个环（0 ~ 2^32-1）
 * 2. 将服务节点映射到环上（使用虚拟节点解决数据倾斜）
 * 3. 请求通过 key 哈希后，顺时针找到最近的节点
 * <p>
 * 优点：节点增减时，只影响相邻节点的数据，不会导致大规模迁移
 * <p>
 * 面试考点：
 * - 一致性哈希原理与虚拟节点
 * - TreeMap 的 ceilingEntry 实现顺时针查找
 * - 与简单取模哈希的对比
 */
public class ConsistentHashLoadBalancer implements LoadBalancer {

    /**
     * 每个物理节点的虚拟节点数量
     */
    private static final int VIRTUAL_NODES = 160;

    /**
     * 哈希环缓存：避免每次请求都重建哈希环
     * key = 实例列表的指纹（地址集合的 hashCode），value = 对应的哈希环
     */
    private volatile TreeMap<Long, ServiceInstance> cachedRing;
    private volatile int cachedFingerprint;

    @Override
    public ServiceInstance select(List<ServiceInstance> instances, String requestKey) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }

        // 计算实例列表指纹，判断是否需要重建哈希环
        int fingerprint = computeFingerprint(instances);
        TreeMap<Long, ServiceInstance> ring = cachedRing;
        if (ring == null || fingerprint != cachedFingerprint) {
            ring = buildHashRing(instances);
            cachedRing = ring;
            cachedFingerprint = fingerprint;
        }

        // 计算请求 key 的哈希值，顺时针找到最近的节点
        long requestHash = hash(requestKey);
        SortedMap<Long, ServiceInstance> tailMap = ring.tailMap(requestHash);
        Long targetHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        return ring.get(targetHash);
    }

    /**
     * 构建一致性哈希环
     */
    private TreeMap<Long, ServiceInstance> buildHashRing(List<ServiceInstance> instances) {
        TreeMap<Long, ServiceInstance> hashRing = new TreeMap<>();
        for (ServiceInstance instance : instances) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                String virtualKey = instance.getAddress() + "#VN" + i;
                long hash = hash(virtualKey);
                hashRing.put(hash, instance);
            }
        }
        return hashRing;
    }

    /**
     * 计算实例列表指纹（用于判断是否需要重建哈希环）
     */
    private int computeFingerprint(List<ServiceInstance> instances) {
        int result = 1;
        for (ServiceInstance instance : instances) {
            result = 31 * result + instance.getAddress().hashCode();
        }
        return result;
    }

    /**
     * 使用 MD5 计算哈希值（取前 8 字节作为 long）
     */
    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // 取前 8 字节组成 long
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            return hash;
        } catch (NoSuchAlgorithmException e) {
            // fallback to simple hash
            return key.hashCode() & 0x7FFFFFFFL;
        }
    }
}
