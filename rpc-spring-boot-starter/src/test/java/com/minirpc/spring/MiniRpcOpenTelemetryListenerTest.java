package com.minirpc.spring;

import com.minirpc.common.protocol.RpcErrorCode;
import com.minirpc.core.observability.RpcInvocationEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MiniRpcOpenTelemetryListenerTest {

    @Test
    void exportsBoundedCorrelationAttributesWhenAuthorized() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
        try {
            MiniRpcOpenTelemetryListener listener = new MiniRpcOpenTelemetryListener(openTelemetry);

            listener.onComplete(new RpcInvocationEvent("client", "example.EchoService",
                    RpcErrorCode.DEADLINE_EXCEEDED, 1_000_000, 2,
                    "127.0.0.1:9090", "trace-123", Map.of()));

            SpanData span = exporter.getFinishedSpanItems().get(0);
            assertEquals("client", span.getAttributes().get(AttributeKey.stringKey("rpc.side")));
            assertEquals("example.EchoService", span.getAttributes().get(AttributeKey.stringKey("rpc.service")));
            assertEquals(2L, span.getAttributes().get(AttributeKey.longKey("rpc.retry_attempt")));
            assertEquals("trace-123", span.getAttributes().get(AttributeKey.stringKey("rpc.trace_id")));
            assertEquals("127.0.0.1:9090", span.getAttributes().get(AttributeKey.stringKey("rpc.target_endpoint")));
            assertEquals("DEADLINE_EXCEEDED", span.getAttributes().get(AttributeKey.stringKey("rpc.error_code")));
        } finally {
            openTelemetry.close();
        }
    }
}
