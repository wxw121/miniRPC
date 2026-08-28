package com.minirpc.spring;

import com.minirpc.core.observability.RpcInvocationEvent;
import com.minirpc.core.observability.RpcInvocationListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Objects;

/** Records completed RPC attempts with a fixed, low-cardinality tag set. */
public final class MiniRpcMeterRegistryListener implements RpcInvocationListener {

    private final MeterRegistry meterRegistry;

    public MiniRpcMeterRegistryListener(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    }

    @Override
    public void onComplete(RpcInvocationEvent event) {
        String outcome = event.errorCode() == null ? "success" : "failure";
        String errorCode = event.errorCode() == null ? "none" : event.errorCode().name();
        String[] tags = {"side", event.side(), "service", event.service(),
                "outcome", outcome, "error_code", errorCode};
        Counter.builder("minirpc.rpc.calls").tags(tags).register(meterRegistry).increment();
        Timer.builder("minirpc.rpc.duration").tags(tags).register(meterRegistry)
                .record(event.durationNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
    }
}
