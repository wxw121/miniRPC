package com.minirpc.spring;

import com.minirpc.common.exception.RpcException;
import com.minirpc.common.protocol.RpcResponse;
import com.minirpc.core.RpcClient;
import com.minirpc.core.annotation.RpcIdempotent;
import com.minirpc.core.cluster.ClusterStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniRpcClientExtensionsTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(MiniRpcAutoConfiguration.class);

    @Test
    void usesClusterStrategyBeanForIdempotentCalls() {
        runner.withBean(ClusterStrategy.class,
                () -> (request, invoker) -> RpcResponse.success(request.getRequestId(), "custom"))
                .run(context -> assertThat(context.getBean(RpcClient.class)
                        .createProxy(EchoService.class).echo()).isEqualTo("custom"));
    }

    @Test
    void nonIdempotentCallsStillFailFastWithACustomStrategy() {
        AtomicInteger calls = new AtomicInteger();
        runner.withBean(ClusterStrategy.class, () -> (request, invoker) -> {
            calls.incrementAndGet();
            return RpcResponse.success(request.getRequestId(), "custom");
        }).run(context -> {
            WriteService service = context.getBean(RpcClient.class).createProxy(WriteService.class);
            assertThatThrownBy(service::write).isInstanceOf(RpcException.class);
            assertThat(calls).hasValue(0);
        });
    }

    interface EchoService {
        @RpcIdempotent String echo();
    }

    interface WriteService {
        String write();
    }
}
