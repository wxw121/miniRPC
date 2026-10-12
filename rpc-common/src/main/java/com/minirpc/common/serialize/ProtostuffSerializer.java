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
 * Schema 会被缓存，LinkedBuffer 按线程复用以减少分配。
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
