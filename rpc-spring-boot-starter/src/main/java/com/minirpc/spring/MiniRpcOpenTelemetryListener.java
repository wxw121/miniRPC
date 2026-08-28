package com.minirpc.spring;

import com.minirpc.core.observability.RpcInvocationEvent;
import com.minirpc.core.observability.RpcInvocationListener;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

import java.util.Objects;

/** Emits one span per completed RPC attempt without request metadata attributes. */
public final class MiniRpcOpenTelemetryListener implements RpcInvocationListener {

    private final OpenTelemetry openTelemetry;

    public MiniRpcOpenTelemetryListener(OpenTelemetry openTelemetry) {
        this.openTelemetry = Objects.requireNonNull(openTelemetry, "openTelemetry must not be null");
    }

    @Override
    public void onComplete(RpcInvocationEvent event) {
        Span span = openTelemetry.getTracer("minirpc").spanBuilder("minirpc.rpc").startSpan();
        try {
            if (event.errorCode() != null) {
                span.setStatus(StatusCode.ERROR);
            }
        } finally {
            span.end();
        }
    }
}
