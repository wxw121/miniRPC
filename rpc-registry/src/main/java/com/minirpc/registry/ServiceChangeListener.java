package com.minirpc.registry;

import java.util.List;

/**
 * 服务变更监听器
 * <p>
 * 当注册中心的服务实例列表发生变化时（新增/下线），回调此接口。
 * <p>
 * 面试考点：观察者模式、ZooKeeper Watcher 机制
 */
@FunctionalInterface
public interface ServiceChangeListener {

    /**
     * 服务实例列表变更回调
     *
     * @param serviceName 服务名
     * @param instances   最新的服务实例列表
     */
    void onChanged(String serviceName, List<ServiceInstance> instances);
}
