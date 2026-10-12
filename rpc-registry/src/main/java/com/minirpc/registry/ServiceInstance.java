package com.minirpc.registry;

import java.io.Serializable;
import java.util.Objects;

/**
 * 服务实例
 * <p>
 * 描述一个注册到注册中心的服务节点信息。
 */
public class ServiceInstance implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 服务接口全限定名
     */
    private String serviceName;

    /**
     * 服务提供者 IP
     */
    private String host;

    /**
     * 服务提供者端口
     */
    private int port;

    /**
     * 服务版本
     */
    private String version;

    /**
     * 服务分组
     */
    private String group;

    /**
     * 权重（用于加权负载均衡）
     */
    private int weight = 1;

    /**
     * 服务启动时间戳（用于服务预热/慢启动）
     */
    private long startTime = System.currentTimeMillis();

    public ServiceInstance() {
    }

    public ServiceInstance(String serviceName, String host, int port) {
        this.serviceName = serviceName;
        this.host = host;
        this.port = port;
        this.version = "1.0.0";
        this.group = "default";
    }

    public ServiceInstance(String serviceName, String host, int port, String version, String group) {
        this.serviceName = serviceName;
        this.host = host;
        this.port = port;
        this.version = version;
        this.group = group;
    }

    /**
     * 获取服务地址标识
     */
    public String getAddress() {
        return host + ":" + port;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * 计算预热后的有效权重
     * <p>
     * 算法：在 warmupPeriod 内，权重从 1 线性增长到配置权重。
     * 公式：effectiveWeight = max(1, weight * uptime / warmupPeriod)
     *
     * @param warmupPeriod 预热时长（毫秒），默认 10 分钟
     * @return 有效权重
     */
    public int getWarmupWeight(long warmupPeriod) {
        long uptime = System.currentTimeMillis() - startTime;
        if (uptime >= warmupPeriod) {
            return weight; // 预热完成，返回全量权重
        }
        // 线性增长：至少为 1，避免权重为 0 导致永远不被选中
        int effectiveWeight = (int) (weight * uptime / warmupPeriod);
        return Math.max(effectiveWeight, 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return port == that.port &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(host, that.host) &&
                Objects.equals(version, that.version) &&
                Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, host, port, version, group);
    }

    @Override
    public String toString() {
        return "ServiceInstance{" +
                "serviceName='" + serviceName + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", version='" + version + '\'' +
                ", group='" + group + '\'' +
                ", weight=" + weight +
                '}';
    }
}
