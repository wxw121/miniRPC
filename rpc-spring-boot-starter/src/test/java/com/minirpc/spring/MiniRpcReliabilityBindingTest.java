package com.minirpc.spring;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class MiniRpcReliabilityBindingTest {

    @Test
    void bindsClientTlsEnabledProperty() {
        new ApplicationContextRunner().withUserConfiguration(BindingConfiguration.class)
                .withPropertyValues("minirpc.client.tls.enabled=true")
                .run(context -> assertThat(context.getBean(MiniRpcReliabilityProperties.class)
                        .getClient().getTls().isEnabled()).isTrue());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(MiniRpcReliabilityProperties.class)
    static class BindingConfiguration {
    }
}
