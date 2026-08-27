package com.minirpc.spring.annotation;

import java.lang.annotation.*;

/**
 * 标注在字段上，自动注入 RPC 服务代理
 * 
 * 使用示例：
 * <pre>
 * @RpcReference
 * private EchoService echoService;
 * </pre>
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcReference {

    /** 服务版本（默认 default） */
    String version() default "1.0.0";

    String group() default "default";

    /** 序列化方式（默认使用全局配置） */

    /** 容错策略: failover / failfast（默认 failover） */

    /** 超时时间（毫秒，0 表示使用全局配置） */
}
