package com.minirpc.spring;

import com.minirpc.core.RpcServer;
import com.minirpc.core.RpcServerStatus;

/**
 * Framework-neutral health bean. Applications may adapt it to Actuator, Kubernetes probes,
 * or an existing management endpoint without bringing a web dependency into rpc-core.
 */
public class MiniRpcHealth {
    private final RpcServer rpcServer;

    public MiniRpcHealth(RpcServer rpcServer) {
        this.rpcServer = rpcServer;
    }

    public RpcServerStatus readiness() {
        return rpcServer.getStatus();
    }
}
