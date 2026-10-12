package com.minirpc.sample.client;

import com.minirpc.core.RpcClient;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.loadbalance.RoundRobinLoadBalancer;
import com.minirpc.core.security.TlsConfig;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.zk.ZookeeperRegistry;
import com.minirpc.sample.api.HelloService;

import java.nio.file.Path;
import java.util.Locale;

/**
 * RPC 服务消费者启动类。
 *
 * <p>Use {@code -Drpc.security.mode=plaintext|tls|mtls}. TLS material is supplied through
 * {@code rpc.tls.client.*} properties; see {@code docs/examples/transport-samples.md}.</p>
 */
public class ClientApplication {

    public static void main(String[] args) {
        String securityMode = System.getProperty("rpc.security.mode", "plaintext");
        RpcConfig config = RpcConfig.builder().tlsConfig(tlsConfig(securityMode, "client")).build();
        RpcClient client;

        String zkAddress = System.getProperty("zk.address");
        if (zkAddress != null && !zkAddress.isEmpty()) {
            System.out.println("Mode: Registry (ZooKeeper @ " + zkAddress + ")");
            ServiceDiscovery discovery = new ServiceDiscovery(new ZookeeperRegistry(zkAddress));
            client = new RpcClient(discovery, new RoundRobinLoadBalancer(), config);
        } else {
            String host = System.getProperty("rpc.host", "127.0.0.1");
            int port = Integer.getInteger("rpc.port", 9090);
            System.out.println("Mode: Direct (" + host + ":" + port + ")");
            client = new RpcClient(host, port, config);
        }

        try {
            HelloService helloService = client.createProxy(HelloService.class);
            // 调用远程方法（对调用者透明，就像调用本地方法一样）
            String greeting = helloService.sayHello("World");
            System.out.println("sayHello result: " + greeting);
            int sum = helloService.add(3, 5);
            System.out.println("add result: 3 + 5 = " + sum);
            System.out.println("\n=== RPC Call Success! ===");
        } finally {
            client.shutdown();
        }
    }

    private static TlsConfig tlsConfig(String modeValue, String role) {
        String mode = modeValue.toLowerCase(Locale.ROOT);
        return switch (mode) {
            case "plaintext" -> TlsConfig.disabled();
            case "tls", "mtls" -> TlsConfig.enabled(
                    Path.of(requiredProperty("rpc.tls." + role + ".key-store")),
                    requiredProperty("rpc.tls." + role + ".key-store-password"),
                    Path.of(requiredProperty("rpc.tls." + role + ".trust-store")),
                    requiredProperty("rpc.tls." + role + ".trust-store-password"),
                    mode.equals("mtls"));
            default -> throw new IllegalArgumentException(
                    "rpc.security.mode must be plaintext, tls, or mtls: " + modeValue);
        };
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required system property: " + name);
        }
        return value;
    }
}
