package com.minirpc.common.util;

import java.util.UUID;

/**
 * RPC 工具类
 */
public class RpcUtil {

    /**
     * 生成请求ID（基于 UUID，保证全局唯一）
     */
    public static String generateRequestId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
