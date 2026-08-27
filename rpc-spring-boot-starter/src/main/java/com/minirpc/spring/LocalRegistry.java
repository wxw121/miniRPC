package com.minirpc.spring;

import com.minirpc.registry.Registry;
import com.minirpc.registry.ServiceChangeListener;
import com.minirpc.registry.ServiceInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地内存注册中心（单机/测试用）
 * <p>
 * 当 minirpc.registry.type=local 时使用，无需外部依赖
 */
public class LocalRegistry implements Registry {

    private final Map<String, List<ServiceInstance>> services = new ConcurrentHashMap<>();

    @Override
    public void register(ServiceInstance instance) {
        services.computeIfAbsent(instance.getServiceName(), k -> Collections.synchronizedList(new ArrayList<>()))
                .add(instance);
    }

    @Override
    public void unregister(ServiceInstance instance) {
        List<ServiceInstance> instances = services.get(instance.getServiceName());
        if (instances != null) {
            instances.removeIf(i -> i.getAddress().equals(instance.getAddress()));
        }
    }

    @Override
    public List<ServiceInstance> lookup(String serviceName) {
        List<ServiceInstance> instances = services.get(serviceName);
        return instances != null ? new ArrayList<>(instances) : Collections.emptyList();
    }

    @Override
    public void subscribe(String serviceName, ServiceChangeListener listener) {
        // 本地注册中心无需订阅（单 JVM 内直接可见）
    }

    @Override
    public void unsubscribe(String serviceName, ServiceChangeListener listener) {
        // no-op
    }

    @Override
    public void close() {
        services.clear();
    }
}
