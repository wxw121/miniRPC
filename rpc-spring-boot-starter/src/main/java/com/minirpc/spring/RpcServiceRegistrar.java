package com.minirpc.spring;

import com.minirpc.core.RpcServer;
import com.minirpc.core.annotation.RpcService;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;

import java.util.Map;

/** Registers Spring-managed RPC providers before starting the listener. */
public class RpcServiceRegistrar implements SmartInitializingSingleton {

    private final ApplicationContext applicationContext;
    private final RpcServer rpcServer;

    public RpcServiceRegistrar(ApplicationContext applicationContext, RpcServer rpcServer) {
        this.applicationContext = applicationContext;
        this.rpcServer = rpcServer;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, Object> services = applicationContext.getBeansWithAnnotation(RpcService.class);
        services.values().forEach(rpcServer::registerService);
        Thread serverThread = new Thread(rpcServer::start, "minirpc-server");
        serverThread.setDaemon(true);
        serverThread.start();
    }
}
