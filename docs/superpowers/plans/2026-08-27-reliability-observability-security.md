# Reliability, Observability, and Transport Security Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the remaining production-baseline capabilities from `docs/EVOLUTION_DESIGN.md`: diagnosable calls, bounded retries, and TLS/mTLS-protected transport.

**Architecture:** `rpc-core` publishes dependency-free invocation events and transport/security SPI contracts; it never imports Micrometer, OpenTelemetry, Spring, or certificate-management libraries. The Spring Boot starter binds the optional integrations and health contributors. The Netty pipelines install TLS before framing when enabled, and reject unauthenticated requests before business-executor submission.

**Tech Stack:** Java 17, Netty 4.1, JUnit 5, Spring Boot 3.2, Micrometer core (optional starter integration), OpenTelemetry API (optional starter integration).

**Spec:** `docs/EVOLUTION_DESIGN.md`

## Global Constraints

- Network JDK serialization remains forbidden; certificate material is supplied by the deployment environment, never generated or persisted by MiniRPC.
- Retry only methods annotated `@RpcIdempotent`, only `UNAVAILABLE`/`OVERLOADED` failures, only while deadline budget and retry budget remain.
- Retry delay uses bounded full jitter and must not start an attempt after the absolute request deadline.
- Core metric labels are fixed and low-cardinality: `side`, `service`, `outcome`, and `error_code`; do not tag method arguments, response values, request IDs, trace IDs, or endpoints.
- TLS is opt-in. A server bound to a non-loopback address without TLS logs an explicit warning. mTLS requires client certificates and validates them before business execution.
- Update `README.md`, `docs/ARCHITECTURE.md`, `docs/PROTOCOL.md`, `docs/FEATURES.md`, and `CHANGELOG.md` whenever the public protocol, configuration, annotations, or defaults change.

---

## File Structure

- `rpc-core/.../observability/RpcInvocationEvent.java` — immutable dependency-free call outcome event.
- `rpc-core/.../observability/RpcInvocationListener.java` — SPI for consumers of those events.
- `rpc-core/.../security/RpcAuthenticator.java` — server-side request authentication SPI.
- `rpc-core/.../security/TlsConfig.java` — validated key/trust store transport settings.
- `rpc-core/.../cluster/RetryPolicy.java` — pure retry admission/backoff policy.
- `rpc-core/.../cluster/EndpointHealthTracker.java` — ejection plus single recovery probe state.
- `rpc-core/.../config/RpcConfig.java` — immutable configuration roots and validation for those components.
- `rpc-core/.../client/NettyClient.java`, `rpc-core/.../server/NettyServer.java` — pipeline TLS insertion and listener/config plumbing.
- `rpc-core/.../server/RpcServerHandler.java` — authentication and server invocation events before executor submission.
- `rpc-core/.../proxy/RpcClientProxy.java` — endpoint-aware retry loop and client invocation events.
- `rpc-spring-boot-starter/.../MiniRpcProperties.java` — binding for retries, TLS, and observability toggles.
- `rpc-spring-boot-starter/.../MiniRpcMeterRegistryListener.java` — optional Micrometer adapter.
- `rpc-spring-boot-starter/.../MiniRpcOpenTelemetryListener.java` — optional OpenTelemetry span adapter.
- `rpc-spring-boot-starter/.../MiniRpcHealthIndicator.java` — Actuator readiness/liveness adapter.

### Task 1: Core invocation-event contract and endpoint recovery state

**Files:**
- Create: `rpc-core/src/main/java/com/minirpc/core/observability/RpcInvocationEvent.java`
- Create: `rpc-core/src/main/java/com/minirpc/core/observability/RpcInvocationListener.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/cluster/EndpointHealthTracker.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/observability/RpcInvocationEventTest.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/cluster/EndpointHealthTrackerTest.java`

**Interfaces:**
- Produces: `RpcInvocationListener#onComplete(RpcInvocationEvent event)` and `EndpointHealthTracker#tryAcquireRecoveryProbe(String endpoint)`.

- [ ] **Step 1: Write failing event-immutability and single-probe tests.**

```java
assertEquals(RpcErrorCode.UNAVAILABLE, event.errorCode());
assertThrows(UnsupportedOperationException.class, () -> event.attributes().put("x", "y"));
assertTrue(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));
assertFalse(tracker.tryAcquireRecoveryProbe("127.0.0.1:9090"));
```

- [ ] **Step 2: Run tests and confirm they fail because the contract methods do not exist.**

Run: `mvnw.cmd -pl rpc-core -Dtest=RpcInvocationEventTest,EndpointHealthTrackerTest test`

- [ ] **Step 3: Add immutable event records and recovery state.**

```java
public record RpcInvocationEvent(String side, String service, RpcErrorCode errorCode,
                                 long durationNanos, int attempt, String targetEndpoint) { }
public interface RpcInvocationListener { void onComplete(RpcInvocationEvent event); }
```

The tracker must keep an ejected endpoint unavailable until its ejection deadline, grant only one probe afterwards, clear failure state on a probe success, and re-eject on a probe failure.

- [ ] **Step 4: Re-run the focused tests.**

Run: `mvnw.cmd -pl rpc-core -Dtest=RpcInvocationEventTest,EndpointHealthTrackerTest test`

- [ ] **Step 5: Commit if Git metadata is available.**

Run: `git add rpc-core/src/main/java/com/minirpc/core/{observability,cluster} rpc-core/src/test/java/com/minirpc/core/{observability,cluster} && git commit -m "feat(core): add invocation events and endpoint recovery probes"`

### Task 2: Deadline-safe retry policy and proxy integration

**Files:**
- Create: `rpc-core/src/main/java/com/minirpc/core/cluster/RetryPolicy.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/cluster/FailoverClusterStrategy.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/config/RpcConfig.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/proxy/RpcClientProxy.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/cluster/RetryPolicyTest.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/proxy/RpcRetryPolicyTest.java`

**Interfaces:**
- Consumes: `RpcInvocationEvent`, `EndpointHealthTracker#tryAcquireRecoveryProbe`.
- Produces: `RetryPolicy#nextDelayMillis(RpcRequest, int, RpcErrorCode, long nowMillis)` returning `OptionalLong`.

- [ ] **Step 1: Write failing admission and deadline tests.**

```java
assertTrue(policy.nextDelayMillis(requestWithDeadline(1_000), 1, UNAVAILABLE, 100).isPresent());
assertTrue(policy.nextDelayMillis(requestWithDeadline(150), 1, UNAVAILABLE, 100).isEmpty());
assertTrue(policy.nextDelayMillis(requestWithDeadline(1_000), 1, UNAUTHORIZED, 100).isEmpty());
assertEquals(1, nonIdempotentClient.calls);
```

- [ ] **Step 2: Run the new tests and verify expected failures.**

Run: `mvnw.cmd -pl rpc-core -Dtest=RetryPolicyTest,RpcRetryPolicyTest test`

- [ ] **Step 3: Implement bounded full-jitter retry admission.**

```java
public OptionalLong nextDelayMillis(RpcRequest request, int retryIndex,
                                    RpcErrorCode errorCode, long nowMillis) {
    if (!retryableCodes.contains(errorCode) || retryIndex > maxRetries) return OptionalLong.empty();
    long cap = Math.min(maxBackoffMs, baseBackoffMs << Math.min(retryIndex - 1, 20));
    long delay = ThreadLocalRandom.current().nextLong(cap + 1);
    return deadlineAllows(request, nowMillis, delay) ? OptionalLong.of(delay) : OptionalLong.empty();
}
```

Make `FailoverClusterStrategy` consult this policy for failed responses and exceptions mapped to stable error codes; `RpcClientProxy` must report attempts, never retry after the deadline, and treat a recovery probe as the only candidate while an endpoint is recovering.

- [ ] **Step 4: Run focused retry and existing deadline tests.**

Run: `mvnw.cmd -pl rpc-core -Dtest=RetryPolicyTest,RpcRetryPolicyTest,RpcDeadlineTest test`

- [ ] **Step 5: Commit if Git metadata is available.**

Run: `git add rpc-core/src/main/java/com/minirpc/core/{cluster,config,proxy} rpc-core/src/test/java/com/minirpc/core/{cluster,proxy} && git commit -m "feat(core): govern retries by deadline and error code"`

### Task 3: Server overload/authentication and core observability wiring

**Files:**
- Create: `rpc-core/src/main/java/com/minirpc/core/security/RpcAuthenticator.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/RpcServer.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/server/NettyServer.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/server/RpcServerHandler.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/proxy/RpcClientProxy.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/server/RpcServerHandlerTest.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/observability/RpcInvocationListenerTest.java`

**Interfaces:**
- Produces: `RpcAuthenticator#authenticate(RpcRequest request)` returning `AuthenticationResult`, and listener registration on `RpcServer`/`RpcClient`.

- [ ] **Step 1: Write failing handler tests for rejection before executor submission.**

```java
assertEquals(RpcErrorCode.UNAUTHORIZED, response.getErrorCode());
assertEquals(0, businessExecutor.submissionCount());
assertEquals(RpcErrorCode.OVERLOADED, overloadResponse.getErrorCode());
```

- [ ] **Step 2: Run handler tests and confirm the missing SPI/wiring failure.**

Run: `mvnw.cmd -pl rpc-core -Dtest=RpcServerHandlerTest,RpcInvocationListenerTest test`

- [ ] **Step 3: Add authentication and event emission.**

```java
public interface RpcAuthenticator {
    AuthenticationResult authenticate(RpcRequest request);
}
public record AuthenticationResult(boolean authenticated, String reason) {
    public static AuthenticationResult allow() { return new AuthenticationResult(true, ""); }
}
```

Invoke authentication after decode/deadline validation but before `businessExecutor.execute`. Map denials to `UNAUTHORIZED`; catch `RejectedExecutionException` and return `OVERLOADED`; emit exactly one event per completed client/server attempt with elapsed duration and stable error code.

- [ ] **Step 4: Run focused core tests.**

Run: `mvnw.cmd -pl rpc-core -Dtest=RpcServerHandlerTest,RpcInvocationListenerTest,RpcRetryPolicyTest test`

- [ ] **Step 5: Commit if Git metadata is available.**

Run: `git add rpc-core/src/main/java/com/minirpc/core/{security,server,proxy} rpc-core/src/test/java/com/minirpc/core/{server,observability} && git commit -m "feat(core): add authenticated and observable invocation lifecycle"`

### Task 4: TLS and mTLS transport configuration

**Files:**
- Create: `rpc-core/src/main/java/com/minirpc/core/security/TlsConfig.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/config/RpcConfig.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/client/NettyClient.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/server/NettyServer.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/security/TlsConfigTest.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/integration/RpcTlsIntegrationTest.java`

**Interfaces:**
- Produces: `TlsConfig#enabled()`, `TlsConfig#mutualAuthentication()`, and `SslContext` factories internal to the Netty transport.

- [ ] **Step 1: Write failing config-validation and TLS handshake tests.**

```java
assertThrows(IllegalArgumentException.class, () -> TlsConfig.enabled(null, null, null, null, false));
assertEquals("Echo: secure", secureClient.createProxy(EchoService.class).echo("secure"));
assertThrows(Exception.class, () -> plaintextClient.createProxy(EchoService.class).echo("secure"));
```

- [ ] **Step 2: Run TLS config test and verify the feature is absent.**

Run: `mvnw.cmd -pl rpc-core -Dtest=TlsConfigTest test`

- [ ] **Step 3: Implement Netty SSL-context factories and pipeline insertion.**

```java
if (tlsConfig.enabled()) {
    pipeline.addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
}
pipeline.addLast(new LengthFieldBasedFrameDecoder(...));
```

Build contexts from configured key/trust stores using the JDK `KeyStore` API and Netty `SslContextBuilder`. Server mTLS uses `ClientAuth.REQUIRE`; client mTLS loads both key and trust material. Keep TLS ahead of heartbeat, frame decoding, and RPC codecs. Warn when a non-loopback server binds without TLS.

- [ ] **Step 4: Run local TLS and mTLS integration tests.**

Run: `mvnw.cmd -pl rpc-core -Dtest=TlsConfigTest,RpcTlsIntegrationTest test`

- [ ] **Step 5: Commit if Git metadata is available.**

Run: `git add rpc-core/src/main/java/com/minirpc/core/{security,config,client,server} rpc-core/src/test/java/com/minirpc/core/{security,integration} && git commit -m "feat(core): secure RPC transport with TLS and mTLS"`

### Task 5: Spring Boot observability, health, and property binding

**Files:**
- Modify: `pom.xml`
- Modify: `rpc-spring-boot-starter/pom.xml`
- Modify: `rpc-spring-boot-starter/src/main/java/com/minirpc/spring/MiniRpcProperties.java`
- Modify: `rpc-spring-boot-starter/src/main/java/com/minirpc/spring/MiniRpcAutoConfiguration.java`
- Create: `rpc-spring-boot-starter/src/main/java/com/minirpc/spring/MiniRpcMeterRegistryListener.java`
- Create: `rpc-spring-boot-starter/src/main/java/com/minirpc/spring/MiniRpcOpenTelemetryListener.java`
- Create: `rpc-spring-boot-starter/src/main/java/com/minirpc/spring/MiniRpcHealthIndicator.java`
- Test: `rpc-spring-boot-starter/src/test/java/com/minirpc/spring/MiniRpcAutoConfigurationTest.java`

**Interfaces:**
- Consumes: core listener/authenticator/TLS configuration contracts.
- Produces: optional `MeterRegistry` counters/timer, optional OpenTelemetry spans, and named `minirpcReadiness` / `minirpcLiveness` health indicators.

- [ ] **Step 1: Write failing auto-configuration tests.**

```java
assertNotNull(context.getBean(MiniRpcMeterRegistryListener.class));
assertEquals(Health.up().build().getStatus(), context.getBean("minirpcLiveness", HealthIndicator.class).health().getStatus());
assertEquals("TLS key store path is required", exception.getMessage());
```

- [ ] **Step 2: Run starter tests and verify expected missing beans/properties.**

Run: `mvnw.cmd -pl rpc-spring-boot-starter -Dtest=MiniRpcAutoConfigurationTest test`

- [ ] **Step 3: Bind and adapt optional integrations.**

Add `minirpc.client.retry.*`, `minirpc.client.tls.*`, `minirpc.server.tls.*`, and `minirpc.observability.*` properties. Use `@ConditionalOnClass` for Micrometer, Actuator, and OpenTelemetry. Bind only fixed metric labels; create an OpenTelemetry span per event without exporting request metadata as attributes. Use `RpcServerStatus` for readiness and liveness, including `running`, `shuttingDown`, and `inFlightRequests` details.

- [ ] **Step 4: Re-run starter tests.**

Run: `mvnw.cmd -pl rpc-spring-boot-starter -Dtest=MiniRpcAutoConfigurationTest,RpcServiceRegistrarTest test`

- [ ] **Step 5: Commit if Git metadata is available.**

Run: `git add pom.xml rpc-spring-boot-starter && git commit -m "feat(starter): expose RPC metrics traces health and TLS settings"`

### Task 6: Executable examples, documentation, and full regression

**Files:**
- Modify: `rpc-sample-server/src/main/resources/application.yml`
- Modify: `rpc-sample-client/src/main/resources/application.yml`
- Create: `docs/examples/tls/application.yml`
- Create: `docs/examples/mtls/application.yml`
- Modify: `README.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/PROTOCOL.md`
- Modify: `docs/FEATURES.md`
- Modify: `CHANGELOG.md`
- Test: `rpc-core/src/test/java/com/minirpc/core/integration/RpcIntegrationTest.java`

- [ ] **Step 1: Write example-validation tests that load every documented properties file.**

```java
assertDoesNotThrow(() -> new YamlPropertySourceLoader().load("tls", resource));
assertTrue(exampleText.contains("minirpc.server.tls.enabled"));
```

- [ ] **Step 2: Run the validation test and confirm it fails before examples exist.**

Run: `mvnw.cmd -pl rpc-spring-boot-starter -Dtest=DocumentedConfigurationTest test`

- [ ] **Step 3: Add redacted plaintext/TLS/mTLS examples and contract documentation.**

Document certificate generation as a local-development prerequisite only; omit private keys, secrets, payload logs, and unsafe `trustAll` settings. Specify field names for metrics/logs/traces, migration behavior, retry eligibility, security errors, and configuration validation.

- [ ] **Step 4: Run documentation/example validation plus the complete reactor test suite.**

Run: `mvnw.cmd test`

- [ ] **Step 5: Commit if Git metadata is available.**

Run: `git add README.md CHANGELOG.md docs rpc-sample-* && git commit -m "docs: document secure and observable RPC operation"`

## Plan Self-Review

- Spec coverage: Task 1–3 deliver core events, trace/metric correlation, overload rejection, authentication, readiness inputs, endpoint recovery, and retry governance. Task 4 delivers TLS/mTLS. Task 5 binds optional starter integrations and health. Task 6 provides the three executable configuration modes and required documentation.
- Placeholder scan: no task defers behavior or leaves an unspecified public interface; external integration is explicitly conditional.
- Type consistency: core public contracts are introduced before retry/server/starter consumers, and all transport behavior remains behind `RpcConfig`.
