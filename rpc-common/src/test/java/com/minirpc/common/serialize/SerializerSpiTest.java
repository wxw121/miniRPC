package com.minirpc.common.serialize;

import com.minirpc.common.protocol.SerializerType;
import com.minirpc.common.spi.SpiLoader;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class SerializerSpiTest {

    @Test
    void factoryUsesProviderFromServiceDescriptor() {
        assertTrue(StreamSupport.stream(SpiLoader.loadAll(Serializer.class).spliterator(), false)
                .anyMatch(provider -> provider.getClass() == SpiHessian2Serializer.class));

        Serializer provider = SerializerFactory.getSerializer(SerializerType.HESSIAN2);
        assertInstanceOf(SpiHessian2Serializer.class, provider);
        assertSame(provider, SerializerFactory.getDefaultSerializer());
        assertSame(provider, SerializerFactory.getNetworkSerializer(SerializerType.HESSIAN2));
    }

    @Test
    void loadAllCreatesProvidersForEachCall() {
        Serializer first = findTestProvider(SpiLoader.loadAll(Serializer.class));
        Serializer second = findTestProvider(SpiLoader.loadAll(Serializer.class));

        assertNotSame(first, second);
    }

    @Test
    void missingProvidersKeepBuiltinsAvailable() {
        Map<SerializerType, Serializer> serializers = SerializerFactory.loadSerializers(List.of());

        assertEquals(JdkSerializer.class, serializers.get(SerializerType.JDK).getClass());
        assertEquals(Hessian2Serializer.class, serializers.get(SerializerType.HESSIAN2).getClass());
        assertEquals(ProtostuffSerializer.class, serializers.get(SerializerType.PROTOSTUFF).getClass());
    }

    @Test
    void customProviderWinsRegardlessOfBuiltinDiscoveryOrder() {
        Serializer custom = new SpiHessian2Serializer();
        Serializer builtin = new Hessian2Serializer();

        assertSame(custom, SerializerFactory.loadSerializers(List.of(custom, builtin))
                .get(SerializerType.HESSIAN2));
        assertSame(custom, SerializerFactory.loadSerializers(List.of(builtin, custom))
                .get(SerializerType.HESSIAN2));
    }

    @Test
    void conflictingCustomProvidersFailWithTheirTypeAndClassNames() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> SerializerFactory.loadSerializers(List.of(
                        new SpiHessian2Serializer(), new OtherHessian2Serializer())));

        assertTrue(error.getMessage().contains("HESSIAN2"));
        assertTrue(error.getMessage().contains(SpiHessian2Serializer.class.getName()));
        assertTrue(error.getMessage().contains(OtherHessian2Serializer.class.getName()));
    }

    @Test
    void repeatedProviderClassKeepsFirstInstance() {
        Serializer first = new SpiHessian2Serializer();

        assertSame(first, SerializerFactory.loadSerializers(List.of(first, new SpiHessian2Serializer()))
                .get(SerializerType.HESSIAN2));
    }

    @Test
    void providerMustDeclareItsWireType() {
        Serializer provider = new Hessian2Serializer() {
            @Override
            public SerializerType getType() {
                return null;
            }
        };

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SerializerFactory.loadSerializers(List.of(provider)));

        assertTrue(error.getMessage().contains(provider.getClass().getName()));
    }

    private static class OtherHessian2Serializer extends Hessian2Serializer {
    }

    private Serializer findTestProvider(Iterable<Serializer> providers) {
        return StreamSupport.stream(providers.spliterator(), false)
                .filter(provider -> provider.getClass() == SpiHessian2Serializer.class)
                .findFirst().orElseThrow();
    }
}
