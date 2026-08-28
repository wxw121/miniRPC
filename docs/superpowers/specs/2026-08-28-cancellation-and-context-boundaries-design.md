# Cancellation and Context Boundaries Design

## Goal

Complete the request-lifecycle guarantees in `docs/EVOLUTION_DESIGN.md`: an async caller can cancel a queued remote invocation, and request metadata has an immutable, bounded wire representation.

## Protocol

`MessageType.CANCEL` carries a serializable `RpcCancellation` containing only `requestId`. It uses the existing frame layout and serializer. Version 2 is the current protocol version; the decoder accepts version 1 request/response/heartbeat frames for rolling server upgrades, but cancellation is available only between version-2 peers. A version-1 peer receiving a version-2 frame fails closed rather than misinterpreting a control frame.

The decoder maps `CANCEL` bodies only to `RpcCancellation`; encoder rejects a cancellation message with another body type. The control frame never reaches service lookup, authentication, or the business executor.

## Client lifecycle

`NettyClient.sendAsync` owns a request's pending future and selected channel. Cancelling its returned future removes it from `RequestFutureHolder` immediately. If the request frame was accepted by the channel, the client writes one `CANCEL` frame on that same channel. Cancellation before request transmission prevents transmission. Timeout and ordinary completion never emit a cancellation frame.

## Server lifecycle

`RpcServerHandler` places each accepted request into a concurrent pending-invocation map before calling `Executor.execute`. The map value tracks a `FutureTask` and a `started` flag. A `CANCEL` removes and cancels only a task whose flag is still false; it releases the graceful-shutdown in-flight count and emits a `CANCELLED` completion event without sending a normal response. A task that has started is left running and will complete normally. Duplicate or late cancellation frames are harmless no-ops.

## Context boundary

`RpcContext` uses a builder/factory for normal construction and is sealed before every network send. Its exposed attachment map is unmodifiable. The allowed attachment keys are `retryAttempt` and `auth.token`; trace ID and idempotency key are first-class fields. Existing count, key-length, value-length, and total-size limits remain enforced. `withRetryAttempt(int)` returns a sealed copy, so retry state never mutates a request that may already be in flight. The decoder seals a deserialized context before it is delivered to the handler.

## Errors, observability, and compatibility

`RpcErrorCode.CANCELLED` identifies a locally cancelled queued invocation in events; it is never retryable. Request IDs, trace IDs, credentials, arguments, and response data remain absent from metric tags and default string representations. The protocol document will describe the version-2 migration and explicit cancellation limitation for version-1 peers.

## Verification

- Codec tests round-trip a cancellation frame and reject an invalid body.
- Client tests prove cancellation removes the pending Future and writes at most one control frame after a sent request.
- Handler tests prove a queued invocation is never executed after cancellation, in-flight is released, and a started invocation is not interrupted.
- Context tests prove unknown keys, mutation after sealing, oversized values, and mutable-map exposure are rejected.
- The complete Maven reactor remains green.
