package com.minirpc.core.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消费端的节点健康记录器，按节点地址（host:port）记录失败情况，帮助选址逻辑暂时避开异常节点。
 *
 * 工作流程：
 * 1. 节点没有健康记录时，默认可以参与负载均衡。
 * 2. 调用失败时累计次数；达到阈值后将该节点标记为隔离，普通请求暂时不能选择它。
 * 3. 隔离时间到期后，允许一个请求取得恢复探测资格，通过一次真实 RPC 调用检查节点是否恢复。
 * 4. 收到成功结果后清除记录，节点恢复可用；探测阶段收到失败结果则重新开始隔离计时。
 *
 * 使用边界：
 * 1. 状态只保存在当前记录器实例中，不修改注册中心，也不会通知其他消费者摘除节点。
 * 2. 本类不主动发送心跳或定时探测；RpcClientProxy 在选择节点、收到调用结果时调用这些方法。
 * 3. 隔离到期只代表可以申请探测，普通流量仍需等待成功结果后才能恢复。
 * 4. 与 CircuitBreaker 按代理对象控制调用不同，本类按地址分别隔离节点，其他健康节点仍可被选择。
 */
public class EndpointHealthTracker {

    // key 为节点地址，value 为该节点的失败和隔离状态
    private final Map<String, EndpointHealth> endpoints = new ConcurrentHashMap<>();

    // 失败次数阈值；成功时会删除整条记录，因此不是节点从启动以来的累计失败次数。
    private final int failureThreshold;

    // 每次隔离持续的时间，单位为毫秒。
    private final long ejectDurationMs;

    /**
     * 默认失败达到 3 次后隔离 30 秒；成功回报会清除此前的失败记录。
     */
    public EndpointHealthTracker() {
        this(3, 30_000L);
    }

    public EndpointHealthTracker(int failureThreshold, long ejectDurationMs) {
        this.failureThreshold = failureThreshold;
        this.ejectDurationMs = ejectDurationMs;
    }

    /**
     * 判断节点是否可以承接普通请求，供调用方筛选负载均衡候选节点。
     * 无记录视为可用；已隔离的节点即使隔离时间到期也返回 false，需另行申请恢复探测。
     */
    public boolean isAvailable(String address) {
        EndpointHealth health = endpoints.get(address);
        return health == null || !health.ejected;
    }

    /**
     * 记录一次调用成功：删除失败、隔离和探测状态，后续查询会将该节点视为可用。
     * 普通调用成功时可清除失败计数，恢复探测成功时可解除隔离。
     */
    public void recordSuccess(String address) {
        endpoints.remove(address);
    }

    /** A cancelled recovery attempt must not hold the sole probe slot indefinitely. */
    public void releaseRecoveryProbe(String address) {
        EndpointHealth health = endpoints.get(address);
        if (health != null) {
            synchronized (health) {
                health.recoveryProbeInFlight = false;
            }
        }
    }

    /**
     * 记录一次调用失败，并根据节点所处阶段决定是否隔离。
     *
     * 1. 未隔离：累计失败次数，达到阈值后隔离。
     * 2. 已隔离且有恢复探测正在进行：重新隔离，等待下一轮探测。
     * 3. 已隔离但没有恢复探测：忽略本次失败，避免旧请求的失败不断延长隔离期。
     *
     * 是否属于失败由调用方判断；本类不分析异常类型、响应状态或错误码。
     */
    public void recordFailure(String address) {
        // 该地址没有记录时才创建状态对象，已有记录则直接复用。
        EndpointHealth health = endpoints.computeIfAbsent(address, ignored -> new EndpointHealth());
        // 按节点加锁，协调失败计数、隔离标记和探测标记的修改，不同节点使用各自的锁。
        synchronized (health) {
            if (health.ejected) {
                if (health.recoveryProbeInFlight) {
                    // 当前处于探测阶段，失败后重新计算隔离截止时间。
                    eject(health);
                }
                return;
            }
            if (health.failures.incrementAndGet() >= failureThreshold) {
                eject(health);
            }
        }
    }

    /**
     * 尝试取得该节点本轮唯一的恢复探测资格，本方法本身不会发送 RPC。
     *
     * 1. 节点必须已隔离、隔离时间已到期，并且当前没有其他恢复探测占用资格。
     * 2. 在同一节点的锁内完成检查和占用，避免多个线程同时取得本轮探测资格。
     * 3. 返回 true 后，由调用方将一次真实请求发往该节点，再回报成功或失败。
     *
     * 占用探测资格不会解除隔离，其他普通请求仍会在 isAvailable() 处被排除。
     * 本类没有独立的探测超时回收任务，调用方需要回报结果才能结束该探测状态。
     *
     * @return true 表示取得探测资格；false 表示本次不能作为恢复探测，不代表节点一定异常
     */
    public boolean tryAcquireRecoveryProbe(String address) {
        EndpointHealth health = endpoints.get(address);
        if (health == null) {
            return false;
        }
        synchronized (health) {
            // 正常节点无需探测；探测已被占用或隔离尚未到期时，也不能放行新的探测。
            if (!health.ejected
                    || health.recoveryProbeInFlight
                    || System.currentTimeMillis() < health.ejectedUntilMillis) {
                return false;
            }
            // 先占用资格再返回，后来的线程会看到探测正在进行。
            health.recoveryProbeInFlight = true;
            return true;
        }
    }

    /**
     * 开始或重新开始一轮隔离；调用方需持有该 health 对象的锁。
     * 清除本轮探测标记，设置新的截止时间，并将失败计数归零。
     */
    private void eject(EndpointHealth health) {
        health.ejected = true;
        health.recoveryProbeInFlight = false;
        health.ejectedUntilMillis = System.currentTimeMillis() + ejectDurationMs;
        health.failures.set(0);
    }

    /**
     * 单个节点的本地状态；隔离和探测状态需要组合判断，相关修改由 synchronized (health) 协调。
     */
    private static final class EndpointHealth {
        // 当前记录中的失败次数，隔离时归零；成功时整个 EndpointHealth 会被移除。
        private final AtomicInteger failures = new AtomicInteger();

        // 隔离截止时间（毫秒时间戳）；到期只允许申请探测，不会自动清除隔离标记。
        private volatile long ejectedUntilMillis;

        // 是否已隔离；volatile 让未加锁的 isAvailable() 能看到该标记的更新。
        private volatile boolean ejected;

        // 恢复探测资格是否已被占用；只在持有当前健康状态对象的锁时读取和修改。
        private boolean recoveryProbeInFlight;
    }
}
