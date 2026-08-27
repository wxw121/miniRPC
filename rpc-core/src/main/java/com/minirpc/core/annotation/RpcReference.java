package com.minirpc.core.annotation;

import java.lang.annotation.*;

/**
 * RPC 服务引用注解
 * <p>
 * 标注在字段上，表示该字段需要注入一个远程服务的代理对象。
 * 框架会通过 JDK 动态代理生成接口的代理实例，实现透明的远程调用。
 * <p>
 * 面试考点：动态代理、依赖注入、AOP 思想
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
     * 超时时间（毫秒）
     */
    long timeout() default 5000;
}
