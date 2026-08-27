package com.minirpc.registry;

import java.util.List;

/**
 * 注册中心 SPI 接口
 * <p>
 * 定义服务注册与发现的统一抽象，支持可插拔的注册中心实现。
 * 内置 ZooKeeper 实现，后续可扩展 Nacos、Etcd、Consul 等。
 * <p>
 * 面试考点：
 * - SPI 扩展机制
 * - 服务注册与发现的核心流程
 * - CAP 理论在注册中心选型中的应用（ZK=CP, Nacos=AP/CP可切换）
 */
public interface Registry {

    /**
     * 注册服务实例
     *
     * @param instance 服务实例
     */
    void register(ServiceInstance instance);

    /**
     * 注销服务实例
     *
     * @param instance 服务实例
     */
    void unregister(ServiceInstance instance);

    /**
     * 查找服务实例列表
     *
     * @param serviceName 服务接口全限定名
     * @return 服务实例列表
     */
    List<ServiceInstance> lookup(String serviceName);

    /**
     * 订阅服务变更
     *
     * @param serviceName 服务接口全限定名
     * @param listener    变更监听器
     */
    void subscribe(String serviceName, ServiceChangeListener listener);

    /**
     * 取消订阅
     *
     * @param serviceName 服务接口全限定名
     * @param listener    变更监听器
     */
    void unsubscribe(String serviceName, ServiceChangeListener listener);

    /**
     * 关闭注册中心连接
     */
    void close();
}
