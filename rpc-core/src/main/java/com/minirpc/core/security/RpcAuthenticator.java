package com.minirpc.core.security;

import com.minirpc.common.protocol.RpcRequest;

/**
 * Server-side authentication boundary invoked before a request enters the
 * business executor.
 */
@FunctionalInterface
public interface RpcAuthenticator {

    AuthenticationResult authenticate(RpcRequest request);

    static RpcAuthenticator allowAll() {
        return request -> AuthenticationResult.allow();
    }

    /**
     * 一次请求的鉴权结果，供 RpcServerHandler 判断是否允许请求进入业务执行流程。
     *
     * @param authenticated true 表示鉴权通过，false 表示拒绝请求
     * @param reason 拒绝原因，服务端会将其放入 UNAUTHORIZED 响应；为空白时使用默认提示
     */
    record AuthenticationResult(boolean authenticated, String reason) {
        /**
         * record 的紧凑构造器：先规范化参数，随后由编译器自动将参数赋给对应字段。
         */
        public AuthenticationResult {
            // 将 null 统一为空字符串，避免服务端调用 reason().isBlank() 时出现空指针。
            reason = reason == null ? "" : reason;
        }

        /** 创建鉴权通过的结果，不包含拒绝原因。 */
        public static AuthenticationResult allow() {
            return new AuthenticationResult(true, "");
        }

        /** 创建鉴权拒绝的结果，并携带拒绝原因。 */
        public static AuthenticationResult deny(String reason) {
            return new AuthenticationResult(false, reason);
        }
    }
}
