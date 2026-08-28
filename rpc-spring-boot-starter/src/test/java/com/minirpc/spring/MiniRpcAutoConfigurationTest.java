package com.minirpc.spring;

import com.minirpc.core.RpcServer;
import com.minirpc.core.observability.RpcInvocationListener;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class MiniRpcAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(MiniRpcAutoConfiguration.class);

    @Test
    void configuresMicrometerInvocationListenerWhenMeterRegistryIsAvailable() {
        contextRunner.withBean(MeterRegistry.class, SimpleMeterRegistryConfiguration::meterRegistry)
                .run(context -> assertThat(context).hasSingleBean(RpcInvocationListener.class));
    }


    @Test
    void doesNotConfigureMicrometerListenerWhenDisabled() {
        contextRunner.withBean(MeterRegistry.class, SimpleMeterRegistryConfiguration::meterRegistry)
                .withPropertyValues("minirpc.observability.micrometer-enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(MiniRpcMeterRegistryListener.class));
    }

    @Test
    void startsWithoutMicrometerOnTheClasspath() {
        contextRunner.withClassLoader(new FilteredClassLoader("io.micrometer"))
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void exposesMiniRpcHealthIndicatorWithExpectedName() {
        contextRunner.withBean(RpcServer.class, () -> new RpcServer(0))
                .run(context -> {
                    Health readiness = context.getBean("minirpcReadiness", HealthIndicator.class).health();
                    Health liveness = context.getBean("minirpcLiveness", HealthIndicator.class).health();
                    assertThat(readiness.getStatus()).isEqualTo(org.springframework.boot.actuate.health.Status.DOWN);
                    assertThat(liveness.getStatus()).isEqualTo(org.springframework.boot.actuate.health.Status.UP);
                });
    }

    static class SimpleMeterRegistryConfiguration {
        static MeterRegistry meterRegistry() {
            return new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        }
    }
}
