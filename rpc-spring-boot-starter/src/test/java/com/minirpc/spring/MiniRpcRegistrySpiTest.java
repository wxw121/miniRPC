package com.minirpc.spring;

import com.minirpc.registry.Registry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class MiniRpcRegistrySpiTest {
    private static final String SERVICE_FILE = "META-INF/services/" + Registry.class.getName();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(MiniRpcAutoConfiguration.class);

    @Test
    void loadsRegistryProviderWhenSpiIsSelected() {
        runner.withPropertyValues("minirpc.registry.type=spi")
                .run(context -> assertThat(context.getBean(Registry.class)).isInstanceOf(SpiTestRegistry.class));
    }

    @Test
    void createsSeparateRegistryInstancesForSeparateContexts() {
        AtomicReference<Registry> first = new AtomicReference<>();
        runner.withPropertyValues("minirpc.registry.type=spi")
                .run(context -> first.set(context.getBean(Registry.class)));
        runner.withPropertyValues("minirpc.registry.type=spi")
                .run(context -> assertThat(context.getBean(Registry.class)).isNotSameAs(first.get()));
    }

    @Test
    void explicitRegistryBeanTakesPrecedenceOverSpi() {
        LocalRegistry registry = new LocalRegistry();
        runner.withPropertyValues("minirpc.registry.type=spi").withBean(Registry.class, () -> registry)
                .run(context -> assertThat(context.getBean(Registry.class)).isSameAs(registry));
    }

    @Test
    void rejectsUnknownRegistryTypes() {
        runner.withPropertyValues("minirpc.registry.type=typo")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void failsWhenSpiIsSelectedWithoutAProvider() {
        ClassLoader noRegistryProviders = new ClassLoader(getClass().getClassLoader()) {
            @Override
            public Enumeration<URL> getResources(String name) throws IOException {
                return SERVICE_FILE.equals(name) ? Collections.emptyEnumeration() : super.getResources(name);
            }
        };
        runner.withClassLoader(noRegistryProviders).withPropertyValues("minirpc.registry.type=spi")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("No Registry SPI provider");
                });
    }

    @Test
    void failsWhenMultipleRegistryProvidersAreDiscovered(@TempDir Path directory) throws Exception {
        Path descriptor = directory.resolve(SERVICE_FILE);
        Files.createDirectories(descriptor.getParent());
        Files.writeString(descriptor, OtherRegistry.class.getName());
        try (URLClassLoader loader = new URLClassLoader(new URL[]{directory.toUri().toURL()},
                getClass().getClassLoader())) {
            runner.withClassLoader(loader).withPropertyValues("minirpc.registry.type=spi")
                    .run(context -> {
                        assertThat(context).hasFailed();
                        assertThat(context.getStartupFailure()).hasStackTraceContaining("Multiple Registry SPI providers");
                    });
        }
    }

    public static class OtherRegistry extends LocalRegistry { }
}
