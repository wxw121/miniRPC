package com.minirpc.spring;

import com.minirpc.core.RpcServer;
import com.minirpc.core.annotation.RpcService;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcServiceRegistrarTest {

    @Test
    void registersRpcServiceBeansBeforeStartingServer() throws Exception {
        CapturingServer server = new CapturingServer();
        ApplicationContext context = (ApplicationContext) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{ApplicationContext.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getBeansWithAnnotation")) {
                        return Map.of("echo", new EchoServiceImpl());
                    }
                    throw new UnsupportedOperationException(method.getName());
                });

        new RpcServiceRegistrar(context, server).afterSingletonsInstantiated();

        assertTrue(server.started.await(2, TimeUnit.SECONDS));
        assertEquals(1, server.services.size());
        assertTrue(server.services.get(0) instanceof EchoServiceImpl);
    }

    interface EchoService {
        String echo(String value);
    }

    @RpcService(interfaceClass = EchoService.class)
    static class EchoServiceImpl implements EchoService {
        @Override
        public String echo(String value) {
            return value;
        }
    }

    static class CapturingServer extends RpcServer {
        private final List<Object> services = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);

        CapturingServer() {
            super(0);
        }

        @Override
        public void registerService(Object service) {
            services.add(service);
        }

        @Override
        public void start() {
            started.countDown();
        }
    }
}
