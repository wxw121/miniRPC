package com.minirpc.spring;

import com.minirpc.core.observability.RpcInvocationEvent;
import com.minirpc.core.observability.RpcInvocationListener;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

import java.util.Objects;

/** Emits one span per completed RPC attempt with bounded RPC correlation attributes. */
public final class MiniRpcOpenTelemetryListener implements RpcInvocationListener {

    private final OpenTelemetry openTelemetry;

    public MiniRpcOpenTelemetryListener(OpenTelemetry openTelemetry) {
        this.openTelemetry = Objects.requireNonNull(openTelemetry, "openTelemetry must not be null");
    }

    @Override
    public void onComplete(RpcInvocationEvent event) {
        Span span = openTelemetry.getTracer("minirpc").spanBuilder("minirpc.rpc").startSpan();
        try {
            span.setAttribute("rpc.side", event.side());
            span.setAttribute("rpc.service", event.service());
            span.setAttribute("rpc.retry_attempt", event.attempt());
            if (!event.traceId().isBlank()) {
                span.setAttribute("rpc.trace_id", event.traceId());
            }
            if (!event.targetEndpoint().isBlank()) {
                span.setAttribute("rpc.target_endpoint", event.targetEndpoint());
            }
            if (event.errorCode() != null) {
                span.setAttribute("rpc.error_code", event.errorCode().name());
                span.setStatus(StatusCode.ERROR);
            }
        } finally {
            span.end();
        }
    }
}
