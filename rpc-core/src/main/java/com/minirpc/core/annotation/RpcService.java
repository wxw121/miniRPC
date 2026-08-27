package com.minirpc.core.annotation;

import java.lang.annotation.*;

/**
 * RPC 服务注解
 * <p>
 * 标注在服务实现类上，表示该类是一个 RPC 服务提供者。
 * 框架启动时会扫描此注解，将服务注册到本地注册表并暴露端口。
 * <p>
 * 面试考点：自定义注解、注解的 RetentionPolicy 和 Target
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RpcService {

    /**
     * 服务接口（默认取实现类的第一个接口）
     */
    Class<?> interfaceClass() default void.class;

    /**
     * 服务版本（V2.0 支持）
     */
    String version() default "1.0.0";

    /**
     * 服务分组（V2.0 支持）
     */
    String group() default "default";
}
