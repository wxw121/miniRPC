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

    record AuthenticationResult(boolean authenticated, String reason) {
        public AuthenticationResult {
            reason = reason == null ? "" : reason;
        }

        public static AuthenticationResult allow() {
            return new AuthenticationResult(true, "");
        }

        public static AuthenticationResult deny(String reason) {
            return new AuthenticationResult(false, reason);
        }
    }
}
