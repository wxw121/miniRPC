package com.minirpc.core.security;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;

/**
 * RPC 连接的 TLS 配置，保存部署环境提供的证书库、信任库路径及密码。
 * 调用方先检查 enabled，再创建 SslContext，为连接生成处理握手和加解密的 SslHandler。
 *
 * @param enabled 是否启用 TLS
 * @param keyStorePath 本端证书及私钥所在的 PKCS12 文件路径，用于证明“我是谁”
 * @param keyStorePassword 证书库密码，本实现也将其用作库中私钥的密码
 * @param trustStorePath 信任的证书或 CA 所在的 PKCS12 文件路径，用于确定“我信任谁”
 * @param trustStorePassword 信任库密码
 * @param mutualAuthentication 是否启用 mTLS：客户端也提供证书，服务端校验客户端证书
 */
public record TlsConfig(
        boolean enabled,
        Path keyStorePath,
        String keyStorePassword,
        Path trustStorePath,
        String trustStorePassword,
        boolean mutualAuthentication) {

    /** 启用 TLS 时校验必填配置；文件内容在创建 SslContext 时加载。 */
    public TlsConfig {
        if (enabled) {
            // 当前配置统一要求提供证书库；普通 TLS 客户端创建上下文时不会加载它，mTLS 客户端才会加载。
            require(keyStorePath, "TLS key store path is required");
            require(keyStorePassword, "TLS key store password is required");
            if (mutualAuthentication) {
                // 双向认证时，必须明确提供用于校验对端证书的信任库。
                require(trustStorePath, "TLS trust store path is required for mutual authentication");
                require(trustStorePassword, "TLS trust store password is required for mutual authentication");
            }
        }
    }

    /** 创建关闭 TLS 的配置，无需提供证书和密码。 */
    public static TlsConfig disabled() {
        return new TlsConfig(false, null, null, null, null, false);
    }

    /** 创建启用 TLS 的配置，必填项由构造器统一检查。 */
    public static TlsConfig enabled(Path keyStorePath, String keyStorePassword,
                                    Path trustStorePath, String trustStorePassword,
                                    boolean mutualAuthentication) {
        return new TlsConfig(true, keyStorePath, keyStorePassword, trustStorePath,
                trustStorePassword, mutualAuthentication);
    }

    /**
     * 创建服务端 TLS 上下文：加载本端证书和私钥 → 配置信任规则 → 按需要求客户端证书。
     * 同一个上下文可供多条连接使用，各连接再创建自己的 SslHandler。
     */
    public SslContext createServerSslContext() {
        try {
            // 服务端需要提供证书和私钥，供 TLS 握手进行身份认证。
            SslContextBuilder builder = SslContextBuilder.forServer(keyManagerFactory());
            if (trustStorePath != null) {
                // 指定校验对端证书的信任规则，mTLS 时用于校验客户端证书。
                builder.trustManager(trustManagerFactory());
            }
            if (mutualAuthentication) {
                // REQUIRE 表示客户端必须提供证书并通过校验，否则 TLS 握手失败。
                builder.clientAuth(ClientAuth.REQUIRE);
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create TLS server context", e);
        }
    }

    /**
     * 创建客户端 TLS 上下文：配置信任规则 → mTLS 时加载本端证书和私钥 → 构建上下文。
     */
    public SslContext createClientSslContext() {
        try {
            SslContextBuilder builder = SslContextBuilder.forClient();
            if (trustStorePath != null) {
                // 使用指定信任库校验服务端证书；未指定时沿用 TLS 实现的默认信任配置。
                builder.trustManager(trustManagerFactory());
            }
            if (mutualAuthentication) {
                // mTLS 客户端也需要提供自己的证书和私钥，让服务端校验其身份。
                builder.keyManager(keyManagerFactory());
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create TLS client context", e);
        }
    }

    /** 加载本端证书和私钥，交给 TLS 实现用于握手时的身份认证。 */
    private KeyManagerFactory keyManagerFactory() throws Exception {
        KeyStore store = loadStore(keyStorePath, keyStorePassword);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        // 本实现使用证书库密码读取库中的私钥。
        factory.init(store, keyStorePassword.toCharArray());
        return factory;
    }

    /** 加载受信任的证书或 CA，为校验对端证书提供信任规则。 */
    private TrustManagerFactory trustManagerFactory() throws Exception {
        KeyStore store = loadStore(trustStorePath, trustStorePassword);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(store);
        return factory;
    }

    /** 按 PKCS12 格式读取证书库或信任库，两者复用相同的文件加载逻辑。 */
    private static KeyStore loadStore(Path path, String password) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        // try-with-resources 在读取结束或抛出异常时自动关闭文件流。
        try (InputStream input = java.nio.file.Files.newInputStream(path)) {
            store.load(input, password.toCharArray());
        }
        return store;
    }

    /** 检查必填项，拒绝 null 和空白字符串。 */
    private static void require(Object value, String message) {
        if (value == null || (value instanceof String text && text.isBlank())) {
            throw new IllegalArgumentException(message);
        }
    }
}
