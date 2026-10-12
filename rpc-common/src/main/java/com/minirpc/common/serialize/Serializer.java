package com.minirpc.common.serialize;

/**
 * 序列化器 SPI 接口
 * <p>
 * 定义序列化/反序列化的统一抽象，支持可插拔的序列化实现。
 * 内置 JDK 原生序列化和 Hessian2 两种实现，后续可扩展 Protobuf、Kryo 等。
 */
public interface Serializer {

    /**
     * 将对象序列化为字节数组
     *
     * @param obj 待序列化对象
     * @return 字节数组
     */
    byte[] serialize(Object obj);

    /**
     * 将字节数组反序列化为对象
     *
     * @param bytes 字节数组
     * @param clazz 目标类型
     * @param <T>   泛型
     * @return 反序列化后的对象
     */
    <T> T deserialize(byte[] bytes, Class<T> clazz);
}
