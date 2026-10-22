# Unified Client Invocation Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Give sync and async proxies identical retry admission and endpoint governance.
**Architecture:** Future-based shared proxy pipeline; asynchronous SPI entry points with
legacy synchronous adapters and native nonblocking built-ins.
**Tech Stack:** Java 17, CompletableFuture, Maven, JUnit 5; no new dependencies.
**Spec:** docs/superpowers/specs/2026-10-22-unified-client-invocation-design.md

## Global Constraints

- Preserve synchronous SPI methods and lambda compatibility.
- Generic and non-idempotent calls remain single-attempt.
- No blocking response waits or backoff on the Netty callback thread in built-ins.
- Preserve the absolute deadline and cancellation/timeout distinction.

## Review Focus

- Cancel during a pending attempt or retry delay: no later sends.
- Exceptional completions update health and the circuit breaker.
- Reused trace/deadline and independent attempt contexts across retries.
- Legacy custom strategy/filter methods remain usable without blocking the caller.
- Failback background work and close preserve their existing behavior.

## Task 1: Pin the shared governance contract

- [x] Add RpcAsyncGovernanceTest for retries, annotation admission, endpoint selection,
  trace/events/metrics, cancellation, deadlines and callback-thread behavior.
- [x] Run the new tests against the old implementation and confirm behavioral failures.

## Task 2: Implement Future-based execution

- [x] Add cancellation-preserving Future transformations and legacy SPI adapters.
- [x] Add ClusterStrategy.invokeAsync and RpcFilter.filterAsync/FilterChain.executeAsync;
  implement built-in Failover, Failfast, Failback, Trace and Metrics async paths.
- [x] Route both proxy modes through one Future pipeline and one endpoint selector.
- [x] Adapt existing transport test doubles to the common sendAsync boundary.
- [x] Run focused proxy/cluster/filter tests, then the complete Maven reactor tests.

## Task 3: Verify and document

- [x] Review the final diff for cancellation races, error wrapping and deadline limits.
- [x] Update README, FEATURES, PROTOCOL and cancellation notes to match actual behavior.
- [x] Record final test results and leave reviewable working-tree changes.

## Verification

- Initial async governance tests reproduced seven behavioral failures in the old path.
- Focused shared proxy/cluster/filter checks: 28 tests passed.
- Cancellation, timeout classification and real-network proxy cancellation checks: 24 tests passed.
- Final `mvn -o -Dio.netty.eventLoopThreads=2 clean test`: BUILD SUCCESS;
  Surefire XML totals: 171 tests, 0 failures, 0 errors, 0 skipped.
- Independent review found holder-sweep deadline classification; its new regression
  failed before the fix and passed afterwards with the complete reactor.
- Implementation and verification used the existing workspace; pre-existing `.idea/` is preserved outside the change.
