package com.minirpc.sample.client;

import com.minirpc.core.RpcClient;
import com.minirpc.core.loadbalance.RoundRobinLoadBalancer;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.zk.ZookeeperRegistry;
import com.minirpc.sample.api.HelloService;

/**
 * RPC 服务消费者启动类
 * <p>
 * 支持两种模式：
 * 1. 直连模式（V1.0）：直接连接服务端 IP:Port
 * 2. 注册中心模式（V2.0）：通过 ZooKeeper 服务发现 + 负载均衡
 */
public class ClientApplication {

    public static void main(String[] args) {
        RpcClient client;

        String zkAddress = System.getProperty("zk.address");
        if (zkAddress != null && !zkAddress.isEmpty()) {
            // V2.0 注册中心模式
            System.out.println("Mode: Registry (ZooKeeper @ " + zkAddress + ")");
            ServiceDiscovery discovery = new ServiceDiscovery(new ZookeeperRegistry(zkAddress));
            client = new RpcClient(discovery, new RoundRobinLoadBalancer());
        } else {
            // V1.0 直连模式
            System.out.println("Mode: Direct (127.0.0.1:9090)");
            client = new RpcClient("127.0.0.1", 9090);
        }

        try {
            // 通过动态代理获取服务接口
            HelloService helloService = client.createProxy(HelloService.class);

            // 调用远程方法（对调用者透明，就像调用本地方法一样）
            String greeting = helloService.sayHello("Interview");
            System.out.println("sayHello result: " + greeting);

            int sum = helloService.add(3, 5);
            System.out.println("add result: 3 + 5 = " + sum);

            System.out.println("\n=== RPC Call Success! ===");

        } finally {
            client.shutdown();
        }
    }
}
