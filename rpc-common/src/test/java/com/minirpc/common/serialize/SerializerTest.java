package com.minirpc.common.serialize;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.SerializerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 序列化器单元测试
 * <p>
 * 验证 JDK 序列化和 Hessian2 序列化的正确性
 */
class SerializerTest {

    @Test
    @DisplayName("JDK 序列化/反序列化 RpcRequest")
    void testJdkSerializer() {
        Serializer serializer = SerializerFactory.getSerializer(SerializerType.JDK);

        RpcRequest request = buildTestRequest();
        byte[] bytes = serializer.serialize(request);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        RpcRequest deserialized = serializer.deserialize(bytes, RpcRequest.class);
        assertNotNull(deserialized);
        assertEquals(request.getRequestId(), deserialized.getRequestId());
        assertEquals(request.getInterfaceName(), deserialized.getInterfaceName());
        assertEquals(request.getMethodName(), deserialized.getMethodName());
        assertArrayEquals(request.getParameterTypeNames(), deserialized.getParameterTypeNames());
        assertArrayEquals(request.getParameters(), deserialized.getParameters());
    }

    @Test
    @DisplayName("Hessian2 序列化/反序列化 RpcRequest")
    void testHessian2Serializer() {
        Serializer serializer = SerializerFactory.getSerializer(SerializerType.HESSIAN2);

        RpcRequest request = buildTestRequest();
        byte[] bytes = serializer.serialize(request);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        RpcRequest deserialized = serializer.deserialize(bytes, RpcRequest.class);
        assertNotNull(deserialized);
        assertEquals(request.getRequestId(), deserialized.getRequestId());
        assertEquals(request.getInterfaceName(), deserialized.getInterfaceName());
        assertEquals(request.getMethodName(), deserialized.getMethodName());
        assertArrayEquals(request.getParameterTypeNames(), deserialized.getParameterTypeNames());
        assertArrayEquals(request.getParameters(), deserialized.getParameters());
    }

    @Test
    @DisplayName("Hessian2 序列化体积应小于 JDK 序列化")
    void testHessian2SmallerThanJdk() {
        RpcRequest request = buildTestRequest();

        Serializer jdkSerializer = SerializerFactory.getSerializer(SerializerType.JDK);
        Serializer hessian2Serializer = SerializerFactory.getSerializer(SerializerType.HESSIAN2);

        byte[] jdkBytes = jdkSerializer.serialize(request);
        byte[] hessian2Bytes = hessian2Serializer.serialize(request);

        assertTrue(hessian2Bytes.length < jdkBytes.length,
                "Hessian2 (" + hessian2Bytes.length + ") should be smaller than JDK (" + jdkBytes.length + ")");
    }

    @Test
    @DisplayName("SerializerFactory 返回正确的序列化器类型")
    void testSerializerFactory() {
        Serializer jdk = SerializerFactory.getSerializer(SerializerType.JDK);
        Serializer hessian2 = SerializerFactory.getSerializer(SerializerType.HESSIAN2);
        Serializer protostuff = SerializerFactory.getSerializer(SerializerType.PROTOSTUFF);

        assertInstanceOf(JdkSerializer.class, jdk);
        assertInstanceOf(Hessian2Serializer.class, hessian2);
        assertInstanceOf(ProtostuffSerializer.class, protostuff);
    }

    @Test
    @DisplayName("JDK 序列化不能由 RPC 报文选择")
    void testJdkSerializerIsBlockedForNetworkTransport() {
        assertThrows(IllegalArgumentException.class,
                () -> SerializerFactory.getNetworkSerializer(SerializerType.JDK));
        assertNotNull(SerializerFactory.getNetworkSerializer(SerializerType.HESSIAN2));
    }

    @Test
    @DisplayName("Protostuff 序列化/反序列化 RpcRequest")
    void testProtostuffSerializer() {
        Serializer serializer = SerializerFactory.getSerializer(SerializerType.PROTOSTUFF);

        RpcRequest request = buildTestRequest();
        byte[] bytes = serializer.serialize(request);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        RpcRequest deserialized = serializer.deserialize(bytes, RpcRequest.class);
        assertNotNull(deserialized);
        assertEquals(request.getRequestId(), deserialized.getRequestId());
        assertEquals(request.getInterfaceName(), deserialized.getInterfaceName());
        assertEquals(request.getMethodName(), deserialized.getMethodName());
        assertArrayEquals(request.getParameterTypeNames(), deserialized.getParameterTypeNames());
        assertArrayEquals(request.getParameters(), deserialized.getParameters());
    }

    @Test
    @DisplayName("Protostuff 序列化体积应小于 Hessian2")
    void testProtostuffSmallerThanHessian2() {
        RpcRequest request = buildTestRequest();

        Serializer hessian2Serializer = SerializerFactory.getSerializer(SerializerType.HESSIAN2);
        Serializer protostuffSerializer = SerializerFactory.getSerializer(SerializerType.PROTOSTUFF);

        byte[] hessian2Bytes = hessian2Serializer.serialize(request);
        byte[] protostuffBytes = protostuffSerializer.serialize(request);

        assertTrue(protostuffBytes.length < hessian2Bytes.length,
                "Protostuff (" + protostuffBytes.length + ") should be smaller than Hessian2 (" + hessian2Bytes.length + ")");
    }

    private RpcRequest buildTestRequest() {
        RpcRequest request = new RpcRequest();
        request.setRequestId("test-req-001");
        request.setInterfaceName("com.minirpc.sample.api.HelloService");
        request.setMethodName("sayHello");
        request.setParameterTypes(new Class<?>[]{String.class});
        request.setParameters(new Object[]{"World"});
        return request;
    }
}
