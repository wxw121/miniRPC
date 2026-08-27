package com.minirpc.core.observability;

/** Consumer SPI for completed RPC invocation attempts. */
@FunctionalInterface
public interface RpcInvocationListener {

    void onComplete(RpcInvocationEvent event);
}
