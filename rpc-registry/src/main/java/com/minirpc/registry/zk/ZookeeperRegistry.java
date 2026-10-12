package com.minirpc.registry.zk;

import com.minirpc.common.exception.RpcException;
import com.minirpc.registry.Registry;
import com.minirpc.registry.ServiceChangeListener;
import com.minirpc.registry.ServiceInstance;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ZooKeeper 注册中心实现
 * <p>
 * 基于 Apache Curator 实现，ZK 节点设计：
 * <pre>
 * /mini-rpc
 *   /{serviceName}           -- 持久节点（服务目录）
 *     /{host:port}           -- 临时节点（服务实例，会话断开自动删除）
 * </pre>
 */
public class ZookeeperRegistry implements Registry {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperRegistry.class);

    /**
     * ZK 根路径
     */
    private static final String ROOT_PATH = "/mini-rpc";

    private final String rootPath;

    private final CuratorFramework client;

    /**
     * 服务监听缓存：serviceName -> CuratorCache
     */
    private final Map<String, CuratorCache> cacheMap = new ConcurrentHashMap<>();

    /**
     * 监听器映射：serviceName -> listeners
     * 使用 CopyOnWriteArrayList 保证并发读写安全（订阅/通知可能在不同线程）
     */
    private final Map<String, List<ServiceChangeListener>> listenerMap = new ConcurrentHashMap<>();

    public ZookeeperRegistry(String zkAddress) {
        this(zkAddress, ROOT_PATH);
    }

    public ZookeeperRegistry(String zkAddress, String basePath) {
        this.rootPath = normalizeBasePath(basePath);
        this.client = CuratorFrameworkFactory.builder()
                .connectString(zkAddress)
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.client.start();
        log.info("ZooKeeper registry connected: {}", zkAddress);
    }

    @Override
    public void register(ServiceInstance instance) {
        String path = buildInstancePath(instance);
        try {
            // 确保父路径存在（creatingParentsIfNeeded 是幂等的，并发安全）
            String servicePath = buildServicePath(instance.getServiceName());
            try {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(servicePath);
            } catch (KeeperException.NodeExistsException e) {
                // 父路径已存在，忽略（并发注册时的正常情况）
            }

            // 创建临时节点（EPHEMERAL），服务宕机后自动删除
            // 使用 try-catch 代替 checkExists+create，避免竞态条件
            String data = serializeInstance(instance);
            try {
                client.create()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(path, data.getBytes(StandardCharsets.UTF_8));
            } catch (KeeperException.NodeExistsException e) {
                // 节点已存在，更新数据（并发注册时的正常情况）
                client.setData().forPath(path, data.getBytes(StandardCharsets.UTF_8));
            }
            log.info("Service registered to ZK: {}", instance);
        } catch (Exception e) {
            throw new RpcException("Failed to register service to ZK: " + instance, e);
        }
    }

    @Override
    public void unregister(ServiceInstance instance) {
        String path = buildInstancePath(instance);
        try {
            if (client.checkExists().forPath(path) != null) {
                client.delete().forPath(path);
                log.info("Service unregistered from ZK: {}", instance);
            }
        } catch (Exception e) {
            throw new RpcException("Failed to unregister service from ZK: " + instance, e);
        }
    }

    @Override
    public List<ServiceInstance> lookup(String serviceName) {
        String servicePath = buildServicePath(serviceName);
        List<ServiceInstance> instances = new ArrayList<>();
        try {
            if (client.checkExists().forPath(servicePath) == null) {
                return instances;
            }
            List<String> children = client.getChildren().forPath(servicePath);
            for (String child : children) {
                byte[] data = client.getData().forPath(servicePath + "/" + child);
                ServiceInstance instance = deserializeInstance(new String(data, StandardCharsets.UTF_8));
                if (instance != null) {
                    instances.add(instance);
                }
            }
        } catch (Exception e) {
            throw new RpcException("Failed to lookup service from ZK: " + serviceName, e);
        }
        return instances;
    }

    @Override
    public void subscribe(String serviceName, ServiceChangeListener listener) {
        listenerMap.computeIfAbsent(serviceName, k -> new CopyOnWriteArrayList<>()).add(listener);

        // 如果还没有创建 CuratorCache，则创建并启动监听
        cacheMap.computeIfAbsent(serviceName, k -> {
            String servicePath = buildServicePath(serviceName);
            CuratorCache cache = CuratorCache.build(client, servicePath);
            cache.listenable().addListener(CuratorCacheListener.builder()
                    .forChanges((oldNode, node) -> notifyListeners(serviceName))
                    .forCreates(node -> notifyListeners(serviceName))
                    .forDeletes(oldNode -> notifyListeners(serviceName))
                    .build());
            cache.start();
            log.info("Subscribed to service: {}", serviceName);
            return cache;
        });
    }

    @Override
    public void unsubscribe(String serviceName, ServiceChangeListener listener) {
        List<ServiceChangeListener> listeners = listenerMap.get(serviceName);
        if (listeners != null) {
            listeners.remove(listener);
            if (listeners.isEmpty()) {
                listenerMap.remove(serviceName);
                CuratorCache cache = cacheMap.remove(serviceName);
                if (cache != null) {
                    cache.close();
                    log.info("Unsubscribed from service: {}", serviceName);
                }
            }
        }
    }

    @Override
    public void close() {
        cacheMap.values().forEach(CuratorCache::close);
        cacheMap.clear();
        listenerMap.clear();
        client.close();
        log.info("ZooKeeper registry closed");
    }

    /**
     * 通知所有监听器
     */
    private void notifyListeners(String serviceName) {
        List<ServiceChangeListener> listeners = listenerMap.get(serviceName);
        if (listeners != null && !listeners.isEmpty()) {
            List<ServiceInstance> instances = lookup(serviceName);
            for (ServiceChangeListener listener : listeners) {
                try {
                    listener.onChanged(serviceName, instances);
                } catch (Exception e) {
                    log.error("Error notifying listener for service: {}", serviceName, e);
                }
            }
        }
    }

    /**
     * 构建服务路径: /{serviceName}
     */
    private String buildServicePath(String serviceName) {
        return rootPath + "/" + serviceName;
    }

    /**
     * 构建实例路径: /{serviceName}/{host:port}
     */
    private String buildInstancePath(ServiceInstance instance) {
        return buildServicePath(instance.getServiceName()) + "/" + instance.getHost() + ":" + instance.getPort();
    }

    private static String normalizeBasePath(String basePath) {
        if (basePath == null || basePath.isBlank() || "/".equals(basePath.trim())) {
            return ROOT_PATH;
        }
        String normalized = basePath.trim();
        return normalized.startsWith("/") ? normalized : "/" + normalized;
    }

    /**
     * 序列化服务实例为字符串（简单 JSON 格式）
     * 包含 startTime 以保证消费者端预热/慢启动机制正确生效
     */
    private String serializeInstance(ServiceInstance instance) {
        return instance.getServiceName() + "|" +
                instance.getHost() + "|" +
                instance.getPort() + "|" +
                instance.getVersion() + "|" +
                instance.getGroup() + "|" +
                instance.getWeight() + "|" +
                instance.getStartTime();
    }

    /**
     * 反序列化字符串为服务实例
     */
    private ServiceInstance deserializeInstance(String data) {
        try {
            String[] parts = data.split("\\|");
            ServiceInstance instance = new ServiceInstance();
            instance.setServiceName(parts[0]);
            instance.setHost(parts[1]);
            instance.setPort(Integer.parseInt(parts[2]));
            instance.setVersion(parts[3]);
            instance.setGroup(parts[4]);
            instance.setWeight(Integer.parseInt(parts[5]));
            // 恢复服务提供者的真实启动时间，保证预热权重计算正确
            if (parts.length > 6) {
                instance.setStartTime(Long.parseLong(parts[6]));
            }
            return instance;
        } catch (Exception e) {
            log.error("Failed to deserialize service instance: {}", data, e);
            return null;
        }
    }
}
