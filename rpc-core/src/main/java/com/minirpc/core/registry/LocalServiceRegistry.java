package com.minirpc.core.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地服务注册表
 * <p>
 * 维护 接口名 -> 服务实现实例 的映射关系。
 * 服务端启动时，将 @RpcService 标注的实现类注册到此表中。
 * 收到 RPC 请求时，根据接口名从此表中查找对应的实现。
 */
public class LocalServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(LocalServiceRegistry.class);

    /**
     * 服务注册表：接口全限定名 -> 服务实现实例
     */
    private static final Map<String, Object> SERVICE_MAP = new ConcurrentHashMap<>();

    /**
     * 注册服务
     *
     * @param interfaceName 接口全限定名
     * @param service       服务实现实例
     */
    public static void registerService(String interfaceName, Object service) {
        SERVICE_MAP.put(interfaceName, service);
        log.info("Service registered: {} -> {}", interfaceName, service.getClass().getName());
    }

    /**
     * 获取服务实现
     *
     * @param interfaceName 接口全限定名
     * @return 服务实现实例，不存在则返回 null
     */
    public static Object getService(String interfaceName) {
        return SERVICE_MAP.get(interfaceName);
    }

    /**
     * 判断服务是否已注册
     */
    public static boolean hasService(String interfaceName) {
        return SERVICE_MAP.containsKey(interfaceName);
    }

    /**
     * 获取所有已注册的服务数量
     */
    public static int getServiceCount() {
        return SERVICE_MAP.size();
    }

    /**
     * 清空注册表（测试用）
     */
    public static void clear() {
        SERVICE_MAP.clear();
    }
}
