package com.minirpc.core.security;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;

/** Immutable transport-security material supplied by the deployment environment. */
public record TlsConfig(
        boolean enabled,
        Path keyStorePath,
        String keyStorePassword,
        Path trustStorePath,
        String trustStorePassword,
        boolean mutualAuthentication) {

    public TlsConfig {
        if (enabled) {
            require(keyStorePath, "TLS key store path is required");
            require(keyStorePassword, "TLS key store password is required");
            if (mutualAuthentication) {
                require(trustStorePath, "TLS trust store path is required for mutual authentication");
                require(trustStorePassword, "TLS trust store password is required for mutual authentication");
            }
        }
    }

    public static TlsConfig disabled() {
        return new TlsConfig(false, null, null, null, null, false);
    }

    public static TlsConfig enabled(Path keyStorePath, String keyStorePassword,
                                    Path trustStorePath, String trustStorePassword,
                                    boolean mutualAuthentication) {
        return new TlsConfig(true, keyStorePath, keyStorePassword, trustStorePath,
                trustStorePassword, mutualAuthentication);
    }

    public SslContext createServerSslContext() {
        try {
            SslContextBuilder builder = SslContextBuilder.forServer(keyManagerFactory());
            if (trustStorePath != null) {
                builder.trustManager(trustManagerFactory());
            }
            if (mutualAuthentication) {
                builder.clientAuth(ClientAuth.REQUIRE);
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create TLS server context", e);
        }
    }

    public SslContext createClientSslContext() {
        try {
            SslContextBuilder builder = SslContextBuilder.forClient();
            if (trustStorePath != null) {
                builder.trustManager(trustManagerFactory());
            }
            if (mutualAuthentication) {
                builder.keyManager(keyManagerFactory());
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create TLS client context", e);
        }
    }

    private KeyManagerFactory keyManagerFactory() throws Exception {
        KeyStore store = loadStore(keyStorePath, keyStorePassword);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(store, keyStorePassword.toCharArray());
        return factory;
    }

    private TrustManagerFactory trustManagerFactory() throws Exception {
        KeyStore store = loadStore(trustStorePath, trustStorePassword);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(store);
        return factory;
    }

    private static KeyStore loadStore(Path path, String password) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = java.nio.file.Files.newInputStream(path)) {
            store.load(input, password.toCharArray());
        }
        return store;
    }

    private static void require(Object value, String message) {
        if (value == null || (value instanceof String text && text.isBlank())) {
            throw new IllegalArgumentException(message);
        }
    }
}
