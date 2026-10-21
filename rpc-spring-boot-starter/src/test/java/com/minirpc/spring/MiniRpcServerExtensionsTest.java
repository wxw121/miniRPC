package com.minirpc.spring;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.core.RpcServer;
import com.minirpc.core.security.RpcAuthenticator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import static org.assertj.core.api.Assertions.assertThat;

class MiniRpcServerExtensionsTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ServerConfiguration.class)
            .withPropertyValues("minirpc.server.enabled=true", "minirpc.server.port=0");

    @Test
    void installsUserAuthenticatorOnTheAutoConfiguredServer() {
        RpcAuthenticator authenticator = request -> RpcAuthenticator.AuthenticationResult.deny("test denial");
        runner.withBean(RpcAuthenticator.class, () -> authenticator).run(context -> {
            assertThat(context).hasNotFailed();
            RpcAuthenticator installed = installedAuthenticator(context.getBean(RpcServer.class));
            assertThat(installed).isSameAs(authenticator);
            assertThat(installed.authenticate(new RpcRequest()).authenticated()).isFalse();
        });
    }

    @Test
    void keepsDefaultAuthenticationWhenNoBeanIsProvided() {
        runner.run(context -> assertThat(installedAuthenticator(context.getBean(RpcServer.class))
                .authenticate(new RpcRequest()).authenticated()).isTrue());
    }

    @Test
    void failsStartupForAmbiguousAuthenticators() {
        runner.withBean("first", RpcAuthenticator.class, RpcAuthenticator::allowAll)
                .withBean("second", RpcAuthenticator.class, RpcAuthenticator::allowAll)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("RpcAuthenticator");
                });
    }

    @Test
    void choosesPrimaryAuthenticatorWhenMultipleBeansExist() {
        RpcAuthenticator selected = request -> RpcAuthenticator.AuthenticationResult.deny("primary");
        runner.withBean("fallback", RpcAuthenticator.class, RpcAuthenticator::allowAll)
                .withBean("selected", RpcAuthenticator.class, () -> selected, definition -> definition.setPrimary(true))
                .run(context -> assertThat(installedAuthenticator(context.getBean(RpcServer.class))).isSameAs(selected));
    }

    private RpcAuthenticator installedAuthenticator(RpcServer server) {
        return (RpcAuthenticator) new DirectFieldAccessor(server)
                .getPropertyValue("nettyServer.authenticator");
    }

    // Exercise the actual server factory without opening a listener during these wiring tests.
    static class ServerConfiguration extends MiniRpcAutoConfiguration {
        @Bean
        @Override
        public RpcServiceRegistrar rpcServiceRegistrar(ApplicationContext context, RpcServer server) {
            return new RpcServiceRegistrar(context, server) {
                @Override public void afterSingletonsInstantiated() { }
            };
        }
    }
}
