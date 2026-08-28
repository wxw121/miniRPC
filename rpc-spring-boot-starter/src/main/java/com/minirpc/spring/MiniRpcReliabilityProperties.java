package com.minirpc.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Additional reliability and transport properties kept separate from legacy starter settings. */
@ConfigurationProperties(prefix = "minirpc")
public class MiniRpcReliabilityProperties {

    private Client client = new Client();
    private Server server = new Server();
    private Observability observability = new Observability();

    public Client getClient() { return client; }
    public Server getServer() { return server; }
    public Observability getObservability() { return observability; }
    public void setClient(Client client) { this.client = client; }
    public void setServer(Server server) { this.server = server; }
    public void setObservability(Observability observability) { this.observability = observability; }

    public static class Client {
        private Retry retry = new Retry();
        private Tls tls = new Tls();
        public Retry getRetry() { return retry; }
        public Tls getTls() { return tls; }
        public void setRetry(Retry retry) { this.retry = retry; }
        public void setTls(Tls tls) { this.tls = tls; }
    }

    public static class Server {
        private Tls tls = new Tls();
        public Tls getTls() { return tls; }
        public void setTls(Tls tls) { this.tls = tls; }
    }

    public static class Retry {
        private int maxRetries = 2;
        private long baseBackoffMs = 100;
        private long maxBackoffMs = 1_000;
        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
        public long getBaseBackoffMs() { return baseBackoffMs; }
        public void setBaseBackoffMs(long baseBackoffMs) { this.baseBackoffMs = baseBackoffMs; }
        public long getMaxBackoffMs() { return maxBackoffMs; }
        public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }
    }

    public static class Tls {
        private boolean enabled;
        private String keyStorePath;
        private String keyStorePassword;
        private String trustStorePath;
        private String trustStorePassword;
        private boolean mutualAuthentication;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getKeyStorePath() { return keyStorePath; }
        public void setKeyStorePath(String keyStorePath) { this.keyStorePath = keyStorePath; }
        public String getKeyStorePassword() { return keyStorePassword; }
        public void setKeyStorePassword(String keyStorePassword) { this.keyStorePassword = keyStorePassword; }
        public String getTrustStorePath() { return trustStorePath; }
        public void setTrustStorePath(String trustStorePath) { this.trustStorePath = trustStorePath; }
        public String getTrustStorePassword() { return trustStorePassword; }
        public void setTrustStorePassword(String trustStorePassword) { this.trustStorePassword = trustStorePassword; }
        public boolean isMutualAuthentication() { return mutualAuthentication; }
        public void setMutualAuthentication(boolean mutualAuthentication) { this.mutualAuthentication = mutualAuthentication; }
    }

    public static class Observability {
        private boolean micrometerEnabled = true;
        private boolean opentelemetryEnabled;
        public boolean isMicrometerEnabled() { return micrometerEnabled; }
        public void setMicrometerEnabled(boolean micrometerEnabled) { this.micrometerEnabled = micrometerEnabled; }
        public boolean isOpentelemetryEnabled() { return opentelemetryEnabled; }
        public void setOpentelemetryEnabled(boolean opentelemetryEnabled) { this.opentelemetryEnabled = opentelemetryEnabled; }
    }
}
