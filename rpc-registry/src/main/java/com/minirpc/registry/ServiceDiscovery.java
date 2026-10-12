package com.minirpc.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务发现（带本地缓存）
 * <p>
 * 在 Registry 之上增加本地缓存层：
 * 1. 首次查询时从注册中心拉取服务列表并缓存
 * 2. 订阅服务变更，实时更新本地缓存
 * 3. 注册中心宕机时，仍可使用本地缓存进行调用（降级策略）
 */
public class ServiceDiscovery {

    private static final Logger log = LoggerFactory.getLogger(ServiceDiscovery.class);

    private final Registry registry;

    /**
     * 本地服务缓存：serviceName -> instances
     */
    private final Map<String, List<ServiceInstance>> localCache = new ConcurrentHashMap<>();

    /**
     * 已订阅的服务集合：防止重复订阅（竞态条件防护）
     */
    private final Set<String> subscribedServices = ConcurrentHashMap.newKeySet();

    public ServiceDiscovery(Registry registry) {
        this.registry = registry;
    }

    /**
     * 获取服务实例列表（优先从缓存获取）
     *
     * @param serviceName 服务名
     * @return 服务实例列表
     */
    public List<ServiceInstance> getInstances(String serviceName) {
        // 1. 先查本地缓存
        List<ServiceInstance> cached = localCache.get(serviceName);
        if (cached != null && !cached.isEmpty()) {
            return cached;
        }

        // 2. 缓存未命中，从注册中心拉取
        try {
            List<ServiceInstance> instances = registry.lookup(serviceName);
            if (instances != null && !instances.isEmpty()) {
                localCache.put(serviceName, Collections.unmodifiableList(new ArrayList<>(instances)));
                // 订阅变更（保证只订阅一次，避免多线程重复订阅）
                if (subscribedServices.add(serviceName)) {
                    registry.subscribe(serviceName, (name, newInstances) -> {
                        log.info("Service instances changed: {} -> {} instances", name, newInstances.size());
                        localCache.put(name, Collections.unmodifiableList(new ArrayList<>(newInstances)));
                    });
                }
                return localCache.get(serviceName);
            }
        } catch (Exception e) {
            log.error("Failed to lookup service from registry: {}", serviceName, e);
        }

        return Collections.emptyList();
    }

    /**
     * Gets instances for one compatible service version and group.
     */
    public List<ServiceInstance> getInstances(String serviceName, String version, String group) {
        List<ServiceInstance> instances = getInstances(serviceName);
        if (instances.isEmpty()) {
            return instances;
        }
        List<ServiceInstance> compatible = new ArrayList<>();
        for (ServiceInstance instance : instances) {
            if (java.util.Objects.equals(version, instance.getVersion())
                    && java.util.Objects.equals(group, instance.getGroup())) {
                compatible.add(instance);
            }
        }
        return compatible;
    }

    /**
     * 刷新指定服务的缓存
     */
    public void refresh(String serviceName) {
        try {
            List<ServiceInstance> instances = registry.lookup(serviceName);
            if (instances != null) {
                localCache.put(serviceName, Collections.unmodifiableList(new ArrayList<>(instances)));
            }
        } catch (Exception e) {
            log.error("Failed to refresh cache for service: {}", serviceName, e);
        }
    }

    /**
     * 关闭服务发现
     */
    public void close() {
        localCache.clear();
        registry.close();
    }
}
