package com.minirpc.spring;

import com.minirpc.core.RpcClient;
import com.minirpc.core.RpcServer;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.observability.RpcInvocationListener;
import com.minirpc.core.security.TlsConfig;
import com.minirpc.common.protocol.SerializerType;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import com.minirpc.registry.Registry;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.zk.ZookeeperRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationContext;

import java.nio.file.Path;
/**
 * Mini RPC 自动配置类
 * 当 minirpc.enabled=true（默认）时生效
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "minirpc", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({MiniRpcProperties.class, MiniRpcReliabilityProperties.class})
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
    public RpcServer rpcServer(MiniRpcProperties properties, MiniRpcReliabilityProperties reliabilityProperties, Registry registry, ObjectProvider<RpcInvocationListener> invocationListeners) {
        MiniRpcProperties.Server serverProps = properties.getServer();
        log.info("Mini RPC Server 启动端口: {}", serverProps.getPort());
        RpcServer server = new RpcServer(RpcConfig.builder()
                .port(serverProps.getPort())
                .bizCorePoolSize(serverProps.getCorePoolSize())
                .bizMaxPoolSize(serverProps.getMaxPoolSize())
                .bizQueueCapacity(serverProps.getQueueCapacity())
                .tlsConfig(tlsConfig(reliabilityProperties.getServer().getTls()))
                .build());
        server.setRegistry(registry);
        invocationListeners.orderedStream().forEach(server::addInvocationListener);
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
    public RpcClient rpcClient(ServiceDiscovery serviceDiscovery, MiniRpcProperties properties, MiniRpcReliabilityProperties reliabilityProperties, ObjectProvider<RpcInvocationListener> invocationListeners) {
        log.info("Mini RPC Client 初始化");
        MiniRpcProperties.Client client = properties.getClient();
        RpcConfig config = RpcConfig.builder()
                .serializerType(SerializerType.valueOf(client.getSerializer().toUpperCase()))
                .connectTimeoutMs(client.getConnectTimeout())
                .timeoutMs(client.getRequestTimeout())
                .connectionPoolSize(client.getPoolSize())
                .failoverMaxRetries(reliabilityProperties.getClient().getRetry().getMaxRetries())
                .retryBaseBackoffMs(reliabilityProperties.getClient().getRetry().getBaseBackoffMs())
                .retryMaxBackoffMs(reliabilityProperties.getClient().getRetry().getMaxBackoffMs())
                .tlsConfig(tlsConfig(reliabilityProperties.getClient().getTls()))
                .build();
        RpcClient rpcClient = new RpcClient(serviceDiscovery, config);
        invocationListeners.orderedStream().forEach(rpcClient::addInvocationListener);
        return rpcClient;
    }

    @Bean
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnProperty(prefix = "minirpc.observability", name = "micrometer-enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(MiniRpcMeterRegistryListener.class)
    public RpcInvocationListener miniRpcMeterRegistryListener(MeterRegistry meterRegistry) {
        return new MiniRpcMeterRegistryListener(meterRegistry);
    }

    @Bean
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean({MeterRegistry.class, RpcClient.class})
    @ConditionalOnProperty(prefix = "minirpc.observability", name = "micrometer-enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(MiniRpcConnectionPoolMetrics.class)
    public MiniRpcConnectionPoolMetrics miniRpcConnectionPoolMetrics(MeterRegistry meterRegistry, RpcClient rpcClient) {
        return new MiniRpcConnectionPoolMetrics(meterRegistry, rpcClient);
    }

    @Bean
    @ConditionalOnClass(name = "io.opentelemetry.api.OpenTelemetry")
    @ConditionalOnMissingBean(MiniRpcOpenTelemetryListener.class)
    @ConditionalOnProperty(prefix = "minirpc.observability", name = "opentelemetry-enabled", havingValue = "true")
    public RpcInvocationListener miniRpcOpenTelemetryListener() {
        return new MiniRpcOpenTelemetryListener(GlobalOpenTelemetry.get());
    }

    @Bean("minirpcReadiness")
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnBean(RpcServer.class)
    @ConditionalOnMissingBean(name = "minirpcReadiness")
    public HealthIndicator minirpcReadiness(RpcServer rpcServer) {
        return new MiniRpcHealthIndicator(rpcServer, MiniRpcHealthIndicator.Kind.READINESS);
    }

    @Bean("minirpcLiveness")
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnBean(RpcServer.class)
    @ConditionalOnMissingBean(name = "minirpcLiveness")
    public HealthIndicator minirpcLiveness(RpcServer rpcServer) {
        return new MiniRpcHealthIndicator(rpcServer, MiniRpcHealthIndicator.Kind.LIVENESS);
    }
    /**
     * @RpcReference 注解处理器
     */
    @Bean
    public RpcReferenceBeanPostProcessor rpcReferenceBeanPostProcessor(RpcClient rpcClient) {
        return new RpcReferenceBeanPostProcessor(rpcClient);
    }
    private static TlsConfig tlsConfig(MiniRpcReliabilityProperties.Tls properties) {
        if (!properties.isEnabled()) {
            return TlsConfig.disabled();
        }
        return TlsConfig.enabled(
                properties.getKeyStorePath() == null ? null : Path.of(properties.getKeyStorePath()),
                properties.getKeyStorePassword(),
                properties.getTrustStorePath() == null ? null : Path.of(properties.getTrustStorePath()),
                properties.getTrustStorePassword(),
                properties.isMutualAuthentication());
    }
}
