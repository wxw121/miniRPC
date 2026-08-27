package com.minirpc.core.security;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TlsConfigTest {

    @Test
    void rejectsEnabledTlsWithoutKeyStoreMaterial() {
        assertThrows(IllegalArgumentException.class,
                () -> TlsConfig.enabled(null, null, null, null, false));
    }

    @Test
    void preservesValidatedTlsAndMutualAuthenticationSettings() {
        TlsConfig config = TlsConfig.enabled(
                Path.of("server.p12"), "changeit", Path.of("trust.p12"), "changeit", true);

        assertTrue(config.enabled());
        assertTrue(config.mutualAuthentication());
    }

    @Test
    void disablesTransportSecurityByDefault() {
        assertFalse(TlsConfig.disabled().enabled());
    }
}
