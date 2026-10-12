package com.minirpc.common.serialize;

import com.minirpc.common.exception.RpcException;

import java.io.*;

/**
 * JDK 原生序列化实现
 * <p>
 * 优点：无需额外依赖，Java 原生支持
 * 缺点：序列化后体积大、性能较差、要求对象实现 Serializable 接口
 */
public class JdkSerializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RpcException("JDK serialize failed: " + e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RpcException("JDK deserialize failed: " + e.getMessage(), e);
        }
    }
}
