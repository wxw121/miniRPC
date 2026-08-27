package com.minirpc.core.registry;

import com.minirpc.registry.Registry;
import com.minirpc.registry.ServiceChangeListener;
import com.minirpc.registry.ServiceDiscovery;
import com.minirpc.registry.ServiceInstance;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ServiceDiscoveryTest {

    @Test
    void selectsOnlyMatchingVersionAndGroup() {
        ServiceInstance v1 = new ServiceInstance("service", "127.0.0.1", 9001, "1.0.0", "default");
        ServiceInstance v2 = new ServiceInstance("service", "127.0.0.1", 9002, "2.0.0", "blue");
        ServiceDiscovery discovery = new ServiceDiscovery(new StaticRegistry(List.of(v1, v2)));

        List<ServiceInstance> selected = discovery.getInstances("service", "2.0.0", "blue");

        assertEquals(List.of(v2), selected);
    }

    private record StaticRegistry(List<ServiceInstance> instances) implements Registry {
        @Override public void register(ServiceInstance instance) { }
        @Override public void unregister(ServiceInstance instance) { }
        @Override public List<ServiceInstance> lookup(String serviceName) { return instances; }
        @Override public void subscribe(String serviceName, ServiceChangeListener listener) { }
        @Override public void unsubscribe(String serviceName, ServiceChangeListener listener) { }
        @Override public void close() { }
    }
}
