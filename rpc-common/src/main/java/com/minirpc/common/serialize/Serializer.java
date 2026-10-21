package com.minirpc.common.serialize;

import com.minirpc.common.protocol.SerializerType;

/**
 * 序列化器 SPI 接口
 * <p>
 * 定义序列化/反序列化的统一抽象，支持可插拔的序列化实现。
 * 内置 JDK 原生序列化、Hessian2 和 Protostuff；JDK 原生序列化仅供本地显式使用。
 */
public interface Serializer {

    /**
     * 返回实现支持的协议类型。自定义实现必须与该类型的线上字节格式兼容。
     */
    SerializerType getType();

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
