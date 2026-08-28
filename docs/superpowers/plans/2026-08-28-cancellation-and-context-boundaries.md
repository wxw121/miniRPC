# Cancellation and Context Boundaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver cross-process cancellation for queued async calls and sealed, bounded request metadata.

**Architecture:** A version-2 `CANCEL` frame is decoded as a minimal `RpcCancellation`. The client emits it only after a returned async Future is cancelled, and the server owns a pending `FutureTask` map so it can cancel only work that has not started. Contexts are copied per attempt and sealed before a network write.

**Tech Stack:** Java 17, Netty 4.1, Hessian2/Protostuff, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-08-28-cancellation-and-context-boundaries-design.md`

## Global Constraints

- Do not interrupt business code that has started.
- Do not include arguments, response data, credentials, request IDs, trace IDs, or endpoints in metrics tags.
- Maintain the existing 9-byte frame layout; version 2 is the current encoder version and decoder accepts version 1 normal frames.
- `CANCELLED` is never retryable.

---

### Task 1: Cancellation protocol contract and codec

**Files:**
- Create: `rpc-common/src/main/java/com/minirpc/common/protocol/RpcCancellation.java`
- Modify: `rpc-common/src/main/java/com/minirpc/common/protocol/MessageType.java`
- Modify: `rpc-common/src/main/java/com/minirpc/common/protocol/RpcMessage.java`
- Modify: `rpc-common/src/main/java/com/minirpc/common/protocol/RpcErrorCode.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/codec/RpcMessageDecoder.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/codec/RpcMessageEncoder.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/codec/RpcCancellationCodecTest.java`

**Interfaces:**
- Produces `new RpcCancellation(String requestId)` and `MessageType.CANCEL`.
- Produces an encoder that rejects a cancel frame whose body is not `RpcCancellation`.

- [ ] **Step 1: Write failing codec tests**

```java
assertEquals("req-1", decode(encode(new RpcMessage(MessageType.CANCEL,
        SerializerType.HESSIAN2, new RpcCancellation("req-1")))).getData().requestId());
assertThrows(EncoderException.class, () -> encode(new RpcMessage(MessageType.CANCEL,
assertEquals(2, encodedVersion(encode(new RpcMessage(MessageType.CANCEL,
        SerializerType.HESSIAN2, new RpcCancellation("req-1")))));
        SerializerType.HESSIAN2, new RpcRequest())));
```

- [ ] **Step 2: Run the test and observe missing cancellation support**

Run: `mvn -pl rpc-core -am -Dtest=RpcCancellationCodecTest -Dsurefire.failIfNoSpecifiedTests=false test`

- [ ] **Step 3: Implement the protocol mapping**

```java
CANCEL((byte) 4);
if (messageType == MessageType.CANCEL) {
public static final byte VERSION = 2;
if (version != 1 && version != VERSION) {
    throw new RpcException("Unsupported protocol version: " + version);
}
    data = serializer.deserialize(bodyBytes, RpcCancellation.class);
}
if (msg.getMessageType() == MessageType.CANCEL && !(msg.getData() instanceof RpcCancellation)) {
    throw new EncoderException("CANCEL body must be RpcCancellation");
}
```

- [ ] **Step 4: Re-run the codec test and commit**

Run: `mvn -pl rpc-core -am -Dtest=RpcCancellationCodecTest -Dsurefire.failIfNoSpecifiedTests=false test`

Commit: `feat(protocol): add cancellation control frame`

### Task 2: Client/server queued cancellation

**Files:**
- Modify: `rpc-core/src/main/java/com/minirpc/core/client/NettyClient.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/server/RpcServerHandler.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/client/NettyClientCancellationTest.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/server/RpcServerHandlerCancellationTest.java`

**Interfaces:**
- Client returns the same pending `CompletableFuture<RpcResponse>` and writes at most one cancellation frame when it is cancelled after sending.
- Server tracks pending `FutureTask` by request ID and offers `cancelQueuedInvocation(String requestId)` internally.

- [ ] **Step 1: Write failing lifecycle tests**

```java
assertTrue(future.cancel(true));
assertFalse(RequestFutureHolder.contains("req-1"));
assertEquals(MessageType.CANCEL, outboundMessage.getMessageType());
assertFalse(serviceWasInvoked.get());
assertEquals(0, shutdownManager.getInFlightCount());
```

- [ ] **Step 2: Run client and handler tests to verify queued work still runs**

Run: `mvn -pl rpc-core -am -Dtest=NettyClientCancellationTest,RpcServerHandlerCancellationTest -Dsurefire.failIfNoSpecifiedTests=false test`

- [ ] **Step 3: Implement client channel ownership and server FutureTask tracking**

```java
future.whenComplete((response, error) -> {
    if (future.isCancelled() && requestWasWritten.compareAndSet(true, false)) {
        channel.writeAndFlush(new RpcMessage(MessageType.CANCEL, serializerType,
                new RpcCancellation(request.getRequestId())));
    }
});
```

```java
FutureTask<Void> task = new FutureTask<>(invocation, null);
pendingInvocations.put(requestId, task);
businessExecutor.execute(task);
```

- [ ] **Step 4: Verify queued cancellation and started-work non-interruption, then commit**

Run: `mvn -pl rpc-core -am -Dtest=NettyClientCancellationTest,RpcServerHandlerCancellationTest,RpcServerHandlerTest -Dsurefire.failIfNoSpecifiedTests=false test`

Commit: `feat(core): propagate cancellation before execution`

### Task 3: Sealed context and retry copies

**Files:**
- Modify: `rpc-common/src/main/java/com/minirpc/common/protocol/RpcContext.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/proxy/RpcClientProxy.java`
- Modify: `rpc-core/src/main/java/com/minirpc/core/codec/RpcMessageDecoder.java`
- Test: `rpc-common/src/test/java/com/minirpc/common/protocol/RpcContextTest.java`
- Test: `rpc-core/src/test/java/com/minirpc/core/proxy/RpcRetryPolicyTest.java`
- Modify: `docs/PROTOCOL.md`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- `RpcContext.seal()` makes all setters and `putAttachment` reject mutation.
- `RpcContext.withRetryAttempt(int)` returns a sealed per-attempt copy.

- [ ] **Step 1: Write failing context tests**

```java
RpcContext context = new RpcContext();
context.putAttachment("retryAttempt", "0");
context.seal();
assertThrows(IllegalStateException.class, () -> context.putAttachment("retryAttempt", "1"));
assertThrows(IllegalArgumentException.class, () -> new RpcContext().putAttachment("unknown", "value"));
assertThrows(UnsupportedOperationException.class, () -> context.getAttachments().put("retryAttempt", "1"));
```

- [ ] **Step 2: Run tests and observe mutation/unknown-key acceptance**

Run: `mvn -pl rpc-common,rpc-core -am -Dtest=RpcContextTest,RpcRetryPolicyTest -Dsurefire.failIfNoSpecifiedTests=false test`

- [ ] **Step 3: Implement sealing, whitelist, decoder sealing, and immutable retry copies**

```java
private static final Set<String> ALLOWED_ATTACHMENT_KEYS = Set.of("retryAttempt", "auth.token");
public RpcContext withRetryAttempt(int attempt) { return copyWith("retryAttempt", Integer.toString(attempt)).seal(); }
```

- [ ] **Step 4: Re-run lifecycle tests, update public docs, and commit**

Run: `mvn -pl rpc-common,rpc-core -am -Dtest=RpcContextTest,RpcRetryPolicyTest,RpcCancellationCodecTest,NettyClientCancellationTest,RpcServerHandlerCancellationTest -Dsurefire.failIfNoSpecifiedTests=false test`

Commit: `feat(context): seal bounded request metadata`
