package com.minirpc.common.serialize;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protostuff 序列化器（Protobuf 兼容）
 * <p>
 * 基于 Protostuff 实现，使用 Protobuf  wire format，但无需编写 .proto 文件。
 * 性能优于 Hessian2 和 JDK 序列化，适合对性能敏感的场景。
 * <p>
 * 实现原理：
 * 1. 通过 RuntimeSchema 在运行时动态生成类的 Schema（类似 Protobuf 的 Message 定义）
 * 2. 使用 ProtostuffIOUtil 进行高效的二进制序列化/反序列化
 * 3. Schema 缓存避免重复生成，LinkedBuffer 复用减少 GC
 * <p>
 * 面试考点：
 * - Protobuf vs Hessian2 vs JDK 序列化的性能对比
 * - Protostuff 如何做到无需 .proto 文件（运行时 Schema 生成）
 * - Schema 缓存 + ThreadLocal Buffer 的性能优化
 * - 为什么 Protobuf 比 JSON 快？（二进制编码 + varint 压缩 + 字段编号代替字段名）
 * <p>
 * 已知限制：
 * - 不支持循环引用对象（会导致 StackOverflow）
 * - 对多态 Object[] 字段（如 RpcRequest.parameters）可能需要 Hessian2 回退
 * - 要求目标类有无参构造函数
 */
public class ProtostuffSerializer implements Serializer {

    /**
     * Schema 缓存：避免每次序列化都通过反射生成 Schema
     */
    private static final Map<Class<?>, Schema<?>> SCHEMA_CACHE = new ConcurrentHashMap<>();

    /**
     * ThreadLocal 复用 LinkedBuffer，减少频繁分配/回收带来的 GC 压力
     * 默认 512 字节，大多数 RPC 消息足够
     */
    private static final ThreadLocal<LinkedBuffer> BUFFER_POOL =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(512));

    @SuppressWarnings("unchecked")
    private static <T> Schema<T> getSchema(Class<T> clazz) {
        Schema<T> schema = (Schema<T>) SCHEMA_CACHE.get(clazz);
        if (schema == null) {
            schema = RuntimeSchema.createFrom(clazz);
            SCHEMA_CACHE.put(clazz, schema);
        }
        return schema;
    }

    @Override
    public byte[] serialize(Object obj) {
        if (obj == null) {
            return new byte[0];
        }
        @SuppressWarnings("unchecked")
        Class<Object> clazz = (Class<Object>) obj.getClass();
        Schema<Object> schema = getSchema(clazz);
        LinkedBuffer buffer = BUFFER_POOL.get();
        try {
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        Schema<T> schema = getSchema(clazz);
        T obj = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, obj, schema);
        return obj;
    }
}
