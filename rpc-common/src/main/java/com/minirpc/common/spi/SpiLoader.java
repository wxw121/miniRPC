package com.minirpc.common.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SPI（Service Provider Interface）扩展加载器
 * <p>
 * 基于 Java ServiceLoader 实现组件的可插拔加载。
 * 通过 META-INF/services/ 下的配置文件声明实现类。
 */
public class SpiLoader {

    private static final Logger log = LoggerFactory.getLogger(SpiLoader.class);

    /**
     * 缓存已加载的 SPI 实例
     */
    private static final Map<Class<?>, Object> INSTANCE_CACHE = new ConcurrentHashMap<>();

    /**
     * 加载 SPI 实现（单例）
     * <p>
     * 通过 ServiceLoader 加载接口的第一个实现类。
     * 如果没有找到实现，返回 null。
     *
     * @param interfaceClass SPI 接口类
     * @return 实现类实例
     */
    @SuppressWarnings("unchecked")
    public static <T> T load(Class<T> interfaceClass) {
        Object cached = INSTANCE_CACHE.get(interfaceClass);
        if (cached != null) {
            return (T) cached;
        }

        ServiceLoader<T> loader = ServiceLoader.load(interfaceClass);
        for (T instance : loader) {
            log.info("Loaded SPI implementation: {} -> {}", interfaceClass.getSimpleName(),
                    instance.getClass().getName());
            INSTANCE_CACHE.put(interfaceClass, instance);
            return instance;
        }
        log.debug("No SPI implementation found for: {}", interfaceClass.getSimpleName());
        return null;
    }

    /**
     * 加载 SPI 实现，如果不存在则使用默认实现
     *
     * @param interfaceClass   SPI 接口类
     * @param defaultInstance  默认实现
     * @return 实现类实例
     */
    @SuppressWarnings("unchecked")
    public static <T> T loadOrDefault(Class<T> interfaceClass, T defaultInstance) {
        T instance = load(interfaceClass);
        return instance != null ? instance : defaultInstance;
    }

    /**
     * 加载所有 SPI 实现
     */
    public static <T> Iterable<T> loadAll(Class<T> interfaceClass) {
        return ServiceLoader.load(interfaceClass);
    }
}
