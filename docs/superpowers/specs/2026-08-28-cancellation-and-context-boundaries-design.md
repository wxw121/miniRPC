# Cancellation and Context Boundaries Design

## Goal

Complete the request-lifecycle design in `docs/EVOLUTION_DESIGN.md`: the Future returned directly by `NettyClient.sendAsync` can request cancellation of a queued remote invocation, and request contexts are sealed with bounded attachments. Implementation notes below reflect the current code as of 2026-10-22.

## Protocol

`MessageType.CANCEL` carries a serializable `RpcCancellation` containing only `requestId`. It uses the existing frame layout and serializer. Version 2 introduced cancellation and is the current outbound version. `isSupportedVersion` currently accepts 1 or 2; the encoder always writes 2. There is no version negotiation or response downgrade, so accepting an incoming version-1 frame does not establish full bidirectional compatibility with a version-1 endpoint. Both endpoints must understand the cancellation message to use that capability.

The current decoder validates the version and message type independently. It does not reject the combination of version 1 and `CANCEL`; a strict per-version message-capability check is not implemented. See `docs/PROTOCOL.md` for the current wire fields and compatibility scope.

The decoder maps `CANCEL` bodies only to `RpcCancellation`; encoder rejects a cancellation message with another body type. The control frame never reaches service lookup, authentication, or the business executor.

## Client lifecycle

`NettyClient.sendAsync` owns a request's pending future and selected channel. Cancelling its returned future removes it from `RequestFutureHolder` immediately. If the request frame was submitted to an active channel, the client attempts to write at most one `CANCEL` frame on that same channel. It checks for cancellation before writing the request and again afterwards to handle a concurrent cancellation. Timeout and ordinary completion never emit a cancellation frame.

The shared proxy pipeline preserves cancellation through its business-result, filter, strategy and attempt Futures to the original `NettyClient.sendAsync` Future. Cancelling the public async proxy Future therefore also suppresses later failover attempts and can emit the same transport CANCEL frame. Deadline expiration completes exceptionally without cancelling the transport Future.

## Server lifecycle

`RpcServerHandler` places each accepted request into a concurrent pending-invocation map before calling `Executor.execute`. The map value tracks a `FutureTask` and an atomic state: `QUEUED`, `RUNNING`, `CANCELLED`, or `COMPLETED`. Execution and cancellation compete to transition from `QUEUED`; only a queued task can be cancelled. Cancellation releases the graceful-shutdown in-flight count and emits a `CANCELLED` completion event without sending a normal response. A task that has started is left running. Duplicate or late cancellation frames are harmless no-ops.

Cancelling a queued task removes its pending-map entry but does not explicitly remove the Runnable wrapper from the executor queue. The wrapper can remain queued until a worker takes it and observes that execution is no longer allowed; immediate queue-slot reclamation is not guaranteed.

## Context boundary

`RpcContext` is constructed with its no-argument constructor and setters, then sealed before network transmission. Its exposed attachment map is an unmodifiable snapshot. The allowed attachment keys are `retryAttempt` and `auth.token`; trace ID and idempotency key are first-class fields. Existing count, key-length, value-length, and total attachment-length limits remain enforced. `withRetryAttempt(int)` returns a sealed copy, so retry state never mutates a request that may already be in flight. The local `sealed` flag is transient, and the decoder seals a deserialized context before it is delivered to the handler.

## Errors, observability, and compatibility

`RpcErrorCode.CANCELLED` identifies a locally cancelled queued invocation in events; it is never retryable. Request IDs and trace IDs are not metric tags; default request/response string representations retain request IDs for correlation while omitting business arguments and response data. Context credentials are also omitted from those string representations. The protocol document describes version acceptance and the absence of negotiation and per-version message-capability enforcement.

## Verification

- Codec tests round-trip a cancellation frame and reject an invalid body.
- Client tests prove cancellation removes the pending Future and writes at most one control frame after a sent request.
- Handler tests prove a queued invocation is never executed after cancellation, in-flight is released, and a started invocation is not interrupted.
- Context tests prove unknown keys, mutation after sealing, oversized values, and mutable-map exposure are rejected.
- Client cancellation tests exercise both `NettyClient` directly and the public async proxy, verifying pending-map cleanup and matching CANCEL request IDs. Current test results require a fresh test run rather than a status claim in this design document.
