package com.minirpc.sample.server;

import com.minirpc.core.RpcServer;
import com.minirpc.registry.zk.ZookeeperRegistry;

/**
 * RPC 服务提供者启动类
 * <p>
 * 支持两种模式：
 * 1. 直连模式（无需 ZK）：直接启动，客户端通过 IP:Port 连接
 * 2. 注册中心模式（需要 ZK）：启动时注册到 ZooKeeper
 */
public class ServerApplication {

    public static void main(String[] args) {
        RpcServer server = new RpcServer(9090);

        // 注册服务实现
        server.registerService(new HelloServiceImpl());

        // 如果指定了 ZK 地址，则注册到 ZooKeeper（V2.0 模式）
        String zkAddress = System.getProperty("zk.address");
        if (zkAddress != null && !zkAddress.isEmpty()) {
            server.setRegistry(new ZookeeperRegistry(zkAddress));
            System.out.println("Registry: ZooKeeper @ " + zkAddress);
        } else {
            System.out.println("Registry: None (direct mode)");
        }

        System.out.println("=== Mini RPC Server Starting ===");
        System.out.println("Port: 9090");
        System.out.println("Service: com.minirpc.sample.api.HelloService");
        System.out.println("================================");

        // 启动服务端（阻塞）
        server.start();
    }
}
