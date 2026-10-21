package com.minirpc.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Mini RPC 配置属性
 * 对应 application.yml 中 minirpc.* 前缀的配置
 */
@ConfigurationProperties(prefix = "minirpc")
public class MiniRpcProperties {

    /** 是否启用 Mini RPC */
    private boolean enabled = true;

    /** 服务端配置 */
    private Server server = new Server();

    /** 客户端配置 */
    private Client client = new Client();

    /** 注册中心配置 */
    private Registry registry = new Registry();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public Registry getRegistry() {
        return registry;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    /**
     * 服务端配置
     */
    public static class Server {
        private boolean enabled = false;

        /** 监听端口 */
        private int port = 9090;

        /** 业务线程池核心线程数 */
        private int corePoolSize = Runtime.getRuntime().availableProcessors() * 2;

        /** 业务线程池最大线程数 */
        private int maxPoolSize = 256;

        private int queueCapacity = 1024;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getCorePoolSize() {
            return corePoolSize;
        }

        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }

        public int getMaxPoolSize() {
            return maxPoolSize;
        }

        public void setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }
    }

    /**
     * 客户端配置
     */
    public static class Client {
        /** 连接超时（毫秒） */
        private int connectTimeout = 5000;

        /** 请求超时（毫秒） */
        private int requestTimeout = 10000;

        /** 连接池大小 */
        private int poolSize = 4;

        /** 序列化方式: hessian2 / jdk */
        private String serializer = "hessian2";

        public int getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public int getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }

        public String getSerializer() {
            return serializer;
        }

        public void setSerializer(String serializer) {
            this.serializer = serializer;
        }
    }

    /**
     * 注册中心配置
     */
    public static class Registry {
        /** 注册中心类型: local / zookeeper / spi */
        private String type = "local";

        /** ZooKeeper 连接地址 */
        private String address = "localhost:2181";

        /** 服务根路径 */
        private String basePath = "/minirpc";

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getBasePath() {
            return basePath;
        }

        public void setBasePath(String basePath) {
            this.basePath = basePath;
        }
    }
}
