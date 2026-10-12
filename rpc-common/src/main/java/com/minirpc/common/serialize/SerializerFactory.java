package com.minirpc.common.serialize;

import com.minirpc.common.protocol.SerializerType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 序列化器工厂
 * <p>
 * 根据 SerializerType 获取对应的 Serializer 实现。
 * 使用单例模式缓存已创建的序列化器实例，避免重复创建。
 */
public class SerializerFactory {

    private static final Map<SerializerType, Serializer> SERIALIZER_MAP = new ConcurrentHashMap<>();

    static {
        SERIALIZER_MAP.put(SerializerType.JDK, new JdkSerializer());
        SERIALIZER_MAP.put(SerializerType.HESSIAN2, new Hessian2Serializer());
        SERIALIZER_MAP.put(SerializerType.PROTOSTUFF, new ProtostuffSerializer());
    }

    /**
     * 根据序列化类型获取序列化器
     *
     * @param type 序列化类型
     * @return 序列化器实例
     */
    public static Serializer getSerializer(SerializerType type) {
        Serializer serializer = SERIALIZER_MAP.get(type);
        if (serializer == null) {
            throw new IllegalArgumentException("Unsupported serializer type: " + type);
        }
        return serializer;
    }

    /**
     * Returns a serializer that is safe to select from an RPC frame.
     * <p>
     * JDK serialization is deliberately unavailable on the wire.  Its object
     * graph can be controlled by a remote peer and it has no type filter here,
     * so accepting it would turn an unauthenticated RPC listener into a
     * deserialization attack surface.  It remains available through
     * {@link #getSerializer(SerializerType)} for explicit local-only use.
     */
    public static Serializer getNetworkSerializer(SerializerType type) {
        if (type == SerializerType.JDK) {
            throw new IllegalArgumentException("JDK serialization is disabled for RPC transport");
        }
        return getSerializer(type);
    }

    /**
     * 获取默认序列化器（Hessian2）
     */
    public static Serializer getDefaultSerializer() {
        return SERIALIZER_MAP.get(SerializerType.HESSIAN2);
    }
}
