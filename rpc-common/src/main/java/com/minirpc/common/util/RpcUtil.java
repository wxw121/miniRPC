package com.minirpc.common.util;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RPC 工具类
 */
public class RpcUtil {

    private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong(0);

    /**
     * 生成请求ID（基于 UUID，保证全局唯一）
     */
    public static String generateRequestId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 生成自增序列号（用于日志追踪等场景）
     */
    public static long nextSequence() {
        return REQUEST_ID_GENERATOR.incrementAndGet();
    }

    /**
     * 判断字符串是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * 判断字符串是否非空
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }
}
