package com.minirpc.common.serialize;

import com.minirpc.common.protocol.SerializerType;
import com.minirpc.common.spi.SpiLoader;

import java.util.EnumMap;
import java.util.Map;

/**
 * 序列化器工厂
 * <p>
 * 根据 SerializerType 获取对应的 Serializer 实现。
 * 使用单例模式缓存已创建的序列化器实例，避免重复创建。
 */
public class SerializerFactory {

    private static final Map<SerializerType, Serializer> SERIALIZER_MAP =
            loadSerializers(SpiLoader.loadAll(Serializer.class));

    static Map<SerializerType, Serializer> loadSerializers(Iterable<Serializer> providers) {
        Map<SerializerType, Serializer> serializers = new EnumMap<>(SerializerType.class);
        serializers.put(SerializerType.JDK, new JdkSerializer());
        serializers.put(SerializerType.HESSIAN2, new Hessian2Serializer());
        serializers.put(SerializerType.PROTOSTUFF, new ProtostuffSerializer());

        Map<SerializerType, Serializer> extensions = new EnumMap<>(SerializerType.class);
        for (Serializer provider : providers) {
            SerializerType type = provider.getType();
            if (type == null) {
                throw new IllegalArgumentException("Serializer has no wire type: " + provider.getClass().getName());
            }
            // 内置服务声明只提供回退值，不覆盖用户扩展，避免依赖资源发现顺序。
            if (provider.getClass() != serializers.get(type).getClass()) {
                Serializer previous = extensions.putIfAbsent(type, provider);
                if (previous != null && previous.getClass() != provider.getClass()) {
                    throw new IllegalStateException("Multiple serializer providers for " + type + ": "
                            + previous.getClass().getName() + ", " + provider.getClass().getName());
                }
            }
        }
        serializers.putAll(extensions);
        return serializers;
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
     * 获取网络协议允许使用的序列化器，统一限制报文可选择的序列化方式。
     * <p>
     * 禁用 JDK 原生序列化的原因：
     * {@link java.io.ObjectInputStream#readObject()} 在恢复对象时，可能自动调用本地类定义的
     * {@code readObject}、{@code readExternal}、{@code readResolve} 等方法。
     * 如果接收端依赖中存在可利用的类组合，攻击者可能通过构造对象及其字段，
     * 让这些方法串联执行，最终造成远程代码执行、拒绝服务等问题。
     * <p>
     * 当前 {@link JdkSerializer} 没有显式配置 {@link java.io.ObjectInputFilter}，
     * 而应用层认证发生在请求体反序列化之后，无法阻止恢复对象时已经触发的行为。
     * 因此网络入口直接拒绝 JDK 类型，不执行其反序列化，从而阻断依赖 JDK 原生恢复机制的利用路径。
     * 这是一项针对该入口的限制，不表示当前项目已被证实存在某个具体的可利用漏洞。
     * <p>
     * 允许 Hessian2、Protostuff 不代表它们已经安全：它们使用不同的对象恢复机制，
     * 仍可能触发本地方法，例如 Hessian2 的 {@code readResolve}、重建 HashMap 时键的
     * {@code hashCode}。它们仍需各自的类型限制、资源限制和安全评估；
     * 禁用 JDK 不会改变先反序列化、后应用层认证的顺序，也不能代替这些防护。
     * <p>
     * JDK 序列化仍可通过 {@link #getSerializer(SerializerType)} 在本地显式获取，
     * 调用方仍须确保输入可信或配置合适的过滤规则；本地使用本身不构成安全保证。
     *
     * @param type 报文声明或发送端配置的序列化类型
     * @return 网络协议允许使用的序列化器实例
     * @throws IllegalArgumentException 类型为 JDK 或没有对应的序列化器
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
