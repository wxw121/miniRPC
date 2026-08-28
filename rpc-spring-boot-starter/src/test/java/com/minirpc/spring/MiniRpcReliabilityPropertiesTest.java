package com.minirpc.spring;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class MiniRpcReliabilityPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(MiniRpcAutoConfiguration.class);

    @Test
    void failsDuringContextCreationWhenEnabledClientTlsHasNoKeyStore() {
        contextRunner.withPropertyValues("minirpc.client.tls.enabled=true")
                .run(context -> assertThat(context.getStartupFailure())
                        .hasMessageContaining("TLS key store path is required"));
    }
}
