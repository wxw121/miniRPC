package com.minirpc.core.integration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.minirpc.common.exception.RpcException;
import com.minirpc.core.RpcClient;
import com.minirpc.core.RpcServer;
import com.minirpc.core.config.RpcConfig;
import com.minirpc.core.security.TlsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcTlsIntegrationTest {

    private static final String PASSWORD = "changeit";
    private static Path certificateDirectory;

    @BeforeAll
    static void createCertificateMaterial() throws Exception {
        certificateDirectory = Files.createTempDirectory("minirpc-tls-");
        generateKeyStore("server", "CN=127.0.0.1", "-ext", "SAN=ip:127.0.0.1");
        generateKeyStore("client", "CN=minirpc-client");
        exportCertificate("server");
        exportCertificate("client");
        importCertificate("server", "server", "client-trust");
        importCertificate("client", "client", "server-trust");
    }

    @AfterAll
    static void removeCertificateMaterial() throws IOException {
        if (certificateDirectory == null) {
            return;
        }
        try (var files = Files.walk(certificateDirectory)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to delete TLS test material", e);
                }
            });
        }
    }

    @Test
    void tlsClientInvokesTlsServerAndPlaintextClientIsRejected() throws Exception {
        RunningServer server = startServer(tlsConfig("server", "server-trust", false));
        RpcClient secureClient = new RpcClient("127.0.0.1", server.port(),
                clientConfig(tlsConfig("client", "client-trust", false)));
        RpcClient plaintextClient = new RpcClient("127.0.0.1", server.port(), clientConfig(TlsConfig.disabled()));

        try {
            assertEquals("Echo: secure", secureClient.createProxy(EchoService.class).echo("secure"));
            assertThrows(RpcException.class, () -> plaintextClient.createProxy(EchoService.class).echo("secure"));
        } finally {
            secureClient.shutdown();
            plaintextClient.shutdown();
            server.server().stop();
        }
    }

    @Test
    void mutualTlsRejectsClientWithoutCertificateAndAcceptsClientCertificate() throws Exception {
        RunningServer server = startServer(tlsConfig("server", "server-trust", true));
        RpcClient clientWithoutCertificate = new RpcClient("127.0.0.1", server.port(),
                clientConfig(tlsConfig("client", "client-trust", false)));
        RpcClient mutualTlsClient = new RpcClient("127.0.0.1", server.port(),
                clientConfig(tlsConfig("client", "client-trust", true)));

        try {
            assertThrows(RpcException.class,
                    () -> clientWithoutCertificate.createProxy(EchoService.class).echo("blocked"));
            assertEquals("Echo: mutual", mutualTlsClient.createProxy(EchoService.class).echo("mutual"));
        } finally {
            clientWithoutCertificate.shutdown();
            mutualTlsClient.shutdown();
            server.server().stop();
        }
    }

    @Test
    void warnsWhenPlaintextServerBindsAllInterfaces() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger("com.minirpc.core.server.NettyServer");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        RunningServer server = startServer(TlsConfig.disabled());
        try {
            assertTrue(appender.list.stream().anyMatch(event -> event.getLevel() == Level.WARN
                    && event.getFormattedMessage().contains("TLS is disabled while binding to a non-loopback address")));
        } finally {
            server.server().stop();
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    private static RunningServer startServer(TlsConfig tlsConfig) throws Exception {
        int port = freePort();
        RpcServer server = new RpcServer(RpcConfig.builder().port(port).tlsConfig(tlsConfig).build());
        server.registerService(new EchoServiceImpl());
        Thread serverThread = new Thread(server::start, "tls-integration-server");
        serverThread.setDaemon(true);
        serverThread.start();
        for (int attempt = 0; attempt < 100; attempt++) {
            if (server.getStatus().running()) {
                return new RunningServer(server, port);
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        server.stop();
        throw new IllegalStateException("TLS test server did not start");
    }

    private static RpcConfig clientConfig(TlsConfig tlsConfig) {
        return RpcConfig.builder()
                .timeoutMs(1_000)
                .connectTimeoutMs(1_000)
                .tlsConfig(tlsConfig)
                .build();
    }

    private static TlsConfig tlsConfig(String keyStoreName, String trustStoreName, boolean mutualAuthentication) {
        return TlsConfig.enabled(certificateDirectory.resolve(keyStoreName + ".p12"), PASSWORD,
                certificateDirectory.resolve(trustStoreName + ".p12"), PASSWORD, mutualAuthentication);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void generateKeyStore(String alias, String distinguishedName, String... extraArgs) throws Exception {
        List<String> arguments = new ArrayList<>(List.of(
                "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048",
                "-storetype", "PKCS12", "-keystore", certificateDirectory.resolve(alias + ".p12").toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD, "-dname", distinguishedName,
                "-validity", "2", "-noprompt"));
        arguments.addAll(List.of(extraArgs));
        runKeytool(arguments);
    }

    private static void exportCertificate(String alias) throws Exception {
        runKeytool(List.of("-exportcert", "-rfc", "-alias", alias,
                "-keystore", certificateDirectory.resolve(alias + ".p12").toString(),
                "-storepass", PASSWORD, "-file", certificateDirectory.resolve(alias + ".crt").toString()));
    }

    private static void importCertificate(String alias, String certificateName, String trustStoreName) throws Exception {
        runKeytool(List.of("-importcert", "-noprompt", "-alias", alias,
                "-file", certificateDirectory.resolve(certificateName + ".crt").toString(),
                "-storetype", "PKCS12", "-keystore", certificateDirectory.resolve(trustStoreName + ".p12").toString(),
                "-storepass", PASSWORD));
    }

    private static void runKeytool(List<String> arguments) throws Exception {
        String executable = Path.of(System.getProperty("java.home"), "bin", "keytool.exe").toString();
        List<String> command = new ArrayList<>();
        command.add(executable);
        command.addAll(arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        if (process.waitFor() != 0) {
            throw new IllegalStateException("keytool failed: " + new String(process.getInputStream().readAllBytes()));
        }
    }

    interface EchoService {
        String echo(String value);
    }

    public static final class EchoServiceImpl implements EchoService {
        @Override
        public String echo(String value) {
            return "Echo: " + value;
        }
    }
    private record RunningServer(RpcServer server, int port) {
    }

}
