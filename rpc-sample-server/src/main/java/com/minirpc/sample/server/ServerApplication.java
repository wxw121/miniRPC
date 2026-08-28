package com.minirpc.sample.server;

import com.minirpc.core.RpcServer;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.security.TlsConfig;
import com.minirpc.registry.zk.ZookeeperRegistry;

import java.nio.file.Path;
import java.util.Locale;

/**
 * RPC 服务提供者启动类。
 *
 * <p>Use {@code -Drpc.security.mode=plaintext|tls|mtls}. TLS material is supplied through
 * {@code rpc.tls.server.*} properties; see {@code docs/examples/transport-samples.md}.</p>
 */
public class ServerApplication {

    public static void main(String[] args) {
        int port = Integer.getInteger("rpc.port", 9090);
        String securityMode = System.getProperty("rpc.security.mode", "plaintext");
        RpcServer server = new RpcServer(RpcConfig.builder()
                .port(port)
                .tlsConfig(tlsConfig(securityMode, "server"))
                .build());

        server.registerService(new HelloServiceImpl());

        String zkAddress = System.getProperty("zk.address");
        if (zkAddress != null && !zkAddress.isEmpty()) {
            server.setRegistry(new ZookeeperRegistry(zkAddress));
            System.out.println("Registry: ZooKeeper @ " + zkAddress);
        } else {
            System.out.println("Registry: None (direct mode)");
        }

        System.out.println("=== Mini RPC Server Starting ===");
        System.out.println("Port: " + port);
        System.out.println("Transport: " + securityMode);
        System.out.println("Service: com.minirpc.sample.api.HelloService");
        System.out.println("================================");
        server.start();
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
