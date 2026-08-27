package com.minirpc.core.config;

import com.minirpc.common.protocol.SerializerType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcConfigTest {

    @Test
    void acceptsDefaultNetworkConfiguration() {
        assertDoesNotThrow(() -> RpcConfig.defaultConfig());
    }

    @Test
    void rejectsJdkSerializationForNetworkConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> RpcConfig.builder().serializerType(SerializerType.JDK).build());
    }

    @Test
    void rejectsInvalidRuntimeLimits() {
        assertThrows(IllegalArgumentException.class,
                () -> RpcConfig.builder().timeoutMs(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> RpcConfig.builder().port(70000).build());
        assertThrows(IllegalArgumentException.class,
                () -> RpcConfig.builder().bizCorePoolSize(4).bizMaxPoolSize(2).build());
        assertThrows(IllegalArgumentException.class,
                () -> RpcConfig.builder().connectionPoolSize(0).build());
    }
}
