# Unified client invocation

The synchronous and asynchronous proxies share admission, filters, cluster strategy,
endpoint selection, attempt events and health updates. Both inspect `@RpcIdempotent`;
generic and non-idempotent calls remain single-attempt. Synchronous callers wait for
the shared business-result Future and asynchronous callers receive it directly.

Java 17 and existing dependencies are sufficient. Preserve the synchronous SPI
methods and lambda compatibility. Add async default bridges for legacy extensions,
executed on the JDK common pool; built-in filters and strategies override them and
do not block while waiting for responses or retry backoff. Discovery and connection
acquisition can still block as before.

Failover uses the existing RetryPolicy, attempt copies, shared absolute deadline and
failed-endpoint set. Delayed retries are scheduled, never slept on a response thread.
Failback preserves its foreground-error/background-compensation contract in both
modes; its existing close method stops queued compensation.

Filter metrics cover one logical invocation, including retries. Attempt events and
endpoint health cover each actual send. Circuit breaker accounting covers the final
foreground result, including exceptional Future completion. Cancellation propagates
to the active transport Future and suppresses future failover attempts. Caller cancellation does not mark the endpoint or circuit unhealthy, and releases
recovery probe permits. A deadline is a typed failure, not cancellation, and does
not send CANCEL. Cached or synthetic
custom strategy responses remain valid.

Tests cover response and transport failures, annotation admission, request context,
endpoint ejection/recovery, trace/metrics/events, custom SPI compatibility, deadline,
cancellation and retry callback thread behavior. Existing transport cancellation,
failback, starter and real-network integration tests remain regression coverage.

Cancellation during already-started discovery/connection preflight reaches that
transport Future once preflight returns; new attempts are suppressed immediately.
