package com.minirpc.common.serialize;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.minirpc.common.exception.RpcException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Hessian2 序列化实现
 * <p>
 * 优点：跨语言、体积小、速度快、无需实现 Serializable 接口
 * 缺点：需要额外依赖
 * <p>
 * 面试考点：Hessian2 与 JDK 序列化的对比、为什么 Dubbo 默认使用 Hessian2
 */
public class Hessian2Serializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            Hessian2Output out = new Hessian2Output(bos);
            out.writeObject(obj);
            out.flush();
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RpcException("Hessian2 serialize failed: " + e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            Hessian2Input in = new Hessian2Input(bis);
            Object obj = in.readObject(clazz);
            in.close();
            return (T) obj;
        } catch (IOException e) {
            throw new RpcException("Hessian2 deserialize failed: " + e.getMessage(), e);
        }
    }
}
