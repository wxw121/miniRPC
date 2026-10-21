package com.minirpc.core.annotation;

import java.lang.annotation.*;

/**
 * RPC 服务引用注解
 * <p>
 * 标注在字段上，表示该字段需要注入一个远程服务的代理对象。
 * Spring Boot Starter 的注解处理器会通过 JDK 动态代理生成接口的代理实例并注入该字段。
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface RpcReference {

    /**
     * 服务版本（V2.0 支持）
     */
    String version() default "1.0.0";

    /**
     * 服务分组（V2.0 支持）
     */
    String group() default "default";

    /**
     * 超时时间（毫秒，0 表示使用客户端全局配置）
     */
    long timeout() default 0;
}
