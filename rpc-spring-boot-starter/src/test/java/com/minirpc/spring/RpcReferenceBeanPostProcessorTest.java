package com.minirpc.spring;

import com.minirpc.core.RpcClient;
import com.minirpc.core.annotation.RpcReference;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RpcReferenceBeanPostProcessorTest {

    @Test
    void injectsInheritedCoreReferenceWithVersionGroupAndTimeout() {
        CapturingClient client = new CapturingClient();
        try {
            CoreConsumer consumer = new CoreConsumer();
            new RpcReferenceBeanPostProcessor(client).postProcessBeforeInitialization(consumer, "consumer");

            assertThat(consumer.echo).isNotNull();
            assertThat(client.version).isEqualTo("2.0");
            assertThat(client.group).isEqualTo("blue");
            assertThat(client.timeout).isEqualTo(1234);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void injectsReferenceUsingDefaultVersionGroupAndGlobalTimeout() {
        CapturingClient client = new CapturingClient();
        try {
            DefaultConsumer consumer = new DefaultConsumer();
            new RpcReferenceBeanPostProcessor(client).postProcessBeforeInitialization(consumer, "consumer");
            assertThat(consumer.echo).isNotNull();
            assertThat(client.version).isEqualTo("1.0.0");
            assertThat(client.group).isEqualTo("default");
            assertThat(client.timeout).isZero();
        } finally {
            client.shutdown();
        }
    }

    @Test
    void rejectsNegativeReferenceTimeouts() {
        CapturingClient client = new CapturingClient();
        try {
            assertThatThrownBy(() -> new RpcReferenceBeanPostProcessor(client)
                    .postProcessBeforeInitialization(new InvalidConsumer(), "consumer"))
                    .hasMessageContaining("timeout");
        } finally {
            client.shutdown();
        }
    }

    interface EchoService {
        String echo();
    }

    static class CoreParent {
        @RpcReference(version = "2.0", group = "blue", timeout = 1234)
        EchoService echo;
    }

    static class CoreConsumer extends CoreParent { }

    static class DefaultConsumer {
        @RpcReference
        EchoService echo;
    }

    static class InvalidConsumer {
        @RpcReference(timeout = -1)
        EchoService echo;
    }

    static class CapturingClient extends RpcClient {
        String version;
        String group;
        long timeout;

        CapturingClient() {
            super("127.0.0.1", 1);
        }

        @Override
        public <T> T createProxy(Class<T> type, String version, String group) {
            return createProxy(type, version, group, 0);
        }

        @Override
        public <T> T createProxy(Class<T> type, String version, String group, long timeout) {
            this.version = version;
            this.group = group;
            this.timeout = timeout;
            return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                    (proxy, method, args) -> null));
        }
    }
}
