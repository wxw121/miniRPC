package com.minirpc.sample.server;

import com.minirpc.core.annotation.RpcService;
import com.minirpc.sample.api.HelloService;

/**
 * HelloService 服务实现
 * <p>
 * 使用 @RpcService 注解标记为 RPC 服务提供者。
 */
@RpcService(interfaceClass = HelloService.class)
public class HelloServiceImpl implements HelloService {

    @Override
    public String sayHello(String name) {
        return "Hello, " + name + "! Welcome to Mini RPC Framework.";
    }

    @Override
    public int add(int a, int b) {
        return a + b;
    }
}
