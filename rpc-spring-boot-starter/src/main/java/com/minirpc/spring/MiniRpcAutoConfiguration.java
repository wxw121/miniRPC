package com.minirpc.spring;

import com.minirpc.core.RpcClient;
import com.minirpc.core.RpcServer;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.common.protocol.SerializerType;
import com.minirpc.registry.Registry;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.zk.ZookeeperRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationContext;

/**
 * Mini RPC 自动配置类
 * 当 minirpc.enabled=true（默认）时生效
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "minirpc", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(MiniRpcProperties.class)
public class MiniRpcAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MiniRpcAutoConfiguration.class);

    /**
     * 注册中心
     */
    @Bean
    @ConditionalOnMissingBean(Registry.class)
    public Registry registry(MiniRpcProperties properties) {
        MiniRpcProperties.Registry registryProps = properties.getRegistry();
        if ("zookeeper".equalsIgnoreCase(registryProps.getType())) {
            log.info("Mini RPC 使用 ZooKeeper 注册中心: {}", registryProps.getAddress());
            return new ZookeeperRegistry(registryProps.getAddress(), registryProps.getBasePath());
        }
        log.info("Mini RPC 使用本地内存注册中心");
        return new LocalRegistry();
    }

    /**
     * 服务发现（带本地缓存）
     */
    @Bean
    @ConditionalOnMissingBean(ServiceDiscovery.class)
    public ServiceDiscovery serviceDiscovery(Registry registry) {
        return new ServiceDiscovery(registry);
    }

    /**
     * RPC 服务端（异步启动，不阻塞 Spring 容器初始化）
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean(RpcServer.class)
    @ConditionalOnProperty(prefix = "minirpc.server", name = "enabled", havingValue = "true")
    public RpcServer rpcServer(MiniRpcProperties properties, Registry registry) {
        MiniRpcProperties.Server serverProps = properties.getServer();
        log.info("Mini RPC Server 启动端口: {}", serverProps.getPort());
        RpcServer server = new RpcServer(RpcConfig.builder()
                .port(serverProps.getPort())
                .bizCorePoolSize(serverProps.getCorePoolSize())
                .bizMaxPoolSize(serverProps.getMaxPoolSize())
                .bizQueueCapacity(serverProps.getQueueCapacity())
                .build());
        server.setRegistry(registry);
        // 在守护线程中启动，避免阻塞 Spring 容器
        return server;
    }

    @Bean
    @ConditionalOnProperty(prefix = "minirpc.server", name = "enabled", havingValue = "true")
    public RpcServiceRegistrar rpcServiceRegistrar(ApplicationContext applicationContext, RpcServer rpcServer) {
        return new RpcServiceRegistrar(applicationContext, rpcServer);
    }

    @Bean
    @ConditionalOnProperty(prefix = "minirpc.server", name = "enabled", havingValue = "true")
    public MiniRpcHealth miniRpcHealth(RpcServer rpcServer) {
        return new MiniRpcHealth(rpcServer);
    }

    /**
     * RPC 客户端
     */
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(RpcClient.class)
    public RpcClient rpcClient(ServiceDiscovery serviceDiscovery, MiniRpcProperties properties) {
        log.info("Mini RPC Client 初始化");
        MiniRpcProperties.Client client = properties.getClient();
        RpcConfig config = RpcConfig.builder()
                .serializerType(SerializerType.valueOf(client.getSerializer().toUpperCase()))
                .connectTimeoutMs(client.getConnectTimeout())
                .timeoutMs(client.getRequestTimeout())
                .connectionPoolSize(client.getPoolSize())
                .build();
        return new RpcClient(serviceDiscovery, config);
    }

    /**
     * @RpcReference 注解处理器
     */
    @Bean
    public RpcReferenceBeanPostProcessor rpcReferenceBeanPostProcessor(RpcClient rpcClient) {
        return new RpcReferenceBeanPostProcessor(rpcClient);
    }
}
