package com.minirpc.core.client;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Channel 连接池
 * <p>
 * 每个地址维护多个 Channel 连接，发送时轮询选取。
 * <p>
 * 面试考点：
 * - 为什么需要连接池？单 Channel 在高并发下写入竞争导致 RT 抖动
 * - Dubbo 默认每个地址 1 个连接（因为 Netty 多路复用），但高吞吐场景需要多连接
 * - 轮询选取保证负载均匀
 * - 连接断开时自动剔除，获取时惰性重建
 */
public class ChannelPool {

    private static final Logger log = LoggerFactory.getLogger(ChannelPool.class);

    /**
     * 默认每个地址的连接数
     */
    private static final int DEFAULT_POOL_SIZE = 4;

    private final String address;
    private final String host;
    private final int port;
    private final int poolSize;
    private final Channel[] channels;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 连接创建器（由 NettyClient 提供）
     */
    private final ChannelCreator channelCreator;

    @FunctionalInterface
    public interface ChannelCreator {
        Channel create(String host, int port);
    }

    public ChannelPool(String address, ChannelCreator channelCreator) {
        this(address, DEFAULT_POOL_SIZE, channelCreator);
    }

    public ChannelPool(String address, int poolSize, ChannelCreator channelCreator) {
        this.address = address;
        this.poolSize = poolSize;
        this.channels = new Channel[poolSize];
        this.channelCreator = channelCreator;
        // 预解析 host/port，避免每次 reconnect 都 split 字符串
        String[] parts = address.split(":");
        this.host = parts[0];
        this.port = Integer.parseInt(parts[1]);
    }

    /**
     * 获取一个可用的 Channel（轮询选取）
     * <p>
     * 如果选中的 Channel 不可用，则尝试重建
     */
    public Channel acquire() {
        // 轮询选取（位运算避免 Math.abs(Integer.MIN_VALUE) 溢出问题）
        int index = (roundRobinIndex.getAndIncrement() & Integer.MAX_VALUE) % poolSize;
        Channel channel = channels[index];

        if (channel != null && channel.isActive()) {
            return channel;
        }

        // Channel 不可用，尝试重建
        return reconnect(index);
    }

    /**
     * 重建指定位置的连接
     */
    private Channel reconnect(int index) {
        lock.lock();
        try {
            // Double-check
            Channel channel = channels[index];
            if (channel != null && channel.isActive()) {
                return channel;
            }

            // 关闭旧连接，释放文件描述符资源（防止半关闭状态的 Channel 泄漏）
            if (channel != null) {
                channel.close();
            }

            Channel newChannel = channelCreator.create(host, port);
            channels[index] = newChannel;
            log.debug("Reconnected pool channel [{}] for address: {}", index, address);
            return newChannel;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取活跃连接数
     */
    public int getActiveCount() {
        int count = 0;
        for (Channel ch : channels) {
            if (ch != null && ch.isActive()) {
                count++;
            }
        }
        return count;
    }

    /**
     * 关闭所有连接
     */
    public void close() {
        for (int i = 0; i < channels.length; i++) {
            Channel ch = channels[i];
            if (ch != null) {
                ch.close();
                channels[i] = null;
            }
        }
        log.debug("ChannelPool closed for address: {}", address);
    }
}
