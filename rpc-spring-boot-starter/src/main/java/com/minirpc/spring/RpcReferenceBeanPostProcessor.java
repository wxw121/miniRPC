package com.minirpc.spring;

import com.minirpc.core.RpcClient;
import com.minirpc.spring.annotation.RpcReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * 扫描 @RpcReference 注解字段，自动注入 RPC 代理对象
 */
public class RpcReferenceBeanPostProcessor implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(RpcReferenceBeanPostProcessor.class);

    private final RpcClient rpcClient;

    public RpcReferenceBeanPostProcessor(RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        // 遍历当前类及所有父类的字段（支持继承场景）
        List<Field> allFields = getAllFields(bean.getClass());
        for (Field field : allFields) {
            RpcReference annotation = field.getAnnotation(RpcReference.class);
            if (annotation == null) {
                continue;
            }

            Class<?> serviceInterface = field.getType();
            if (!serviceInterface.isInterface()) {
                log.warn("@RpcReference 只能标注在接口类型的字段上: {}.{}", bean.getClass().getSimpleName(), field.getName());
                continue;
            }

            // 创建代理
            Object proxy = rpcClient.createProxy(serviceInterface,
                    annotation.version(), annotation.group());

            // 注入
            field.setAccessible(true);
            try {
                field.set(bean, proxy);
                log.info("注入 RPC 代理: {}.{} -> {}", bean.getClass().getSimpleName(), field.getName(), serviceInterface.getName());
            } catch (IllegalAccessException e) {
                throw new BeansException("注入 RPC 代理失败: " + field.getName(), e) {};
            }
        }
        return bean;
    }

    /**
     * 获取类及其所有父类的字段（直到 Object）
     */
    private List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        while (clazz != null && clazz != Object.class) {
            for (Field field : clazz.getDeclaredFields()) {
                fields.add(field);
            }
            clazz = clazz.getSuperclass();
        }
        return fields;
    }
}
