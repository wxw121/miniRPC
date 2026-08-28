package com.minirpc.common.protocol;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Request-scoped metadata propagated with an RPC call.
 * Values are deliberately bounded because this object is sent on the wire.
 */
public class RpcContext implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int MAX_ATTACHMENTS = 16;
    private static final int MAX_KEY_LENGTH = 64;
    private static final int MAX_VALUE_LENGTH = 1024;
    private static final int MAX_TOTAL_ATTACHMENT_LENGTH = 4096;
    private static final Set<String> ALLOWED_ATTACHMENT_KEYS = Set.of("retryAttempt", "auth.token");

    private String traceId;
    private long deadlineEpochMillis;
    private String idempotencyKey;
    private Map<String, String> attachments = new LinkedHashMap<>();
    private transient boolean sealed;

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        ensureMutable();
        this.traceId = traceId;
    }

    public long getDeadlineEpochMillis() {
        return deadlineEpochMillis;
    }

    public void setDeadlineEpochMillis(long deadlineEpochMillis) {
        ensureMutable();
        this.deadlineEpochMillis = deadlineEpochMillis;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        ensureMutable();
        this.idempotencyKey = idempotencyKey;
    }

    public Map<String, String> getAttachments() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(attachments));
    }

    public void setAttachments(Map<String, String> attachments) {
        ensureMutable();
        this.attachments = new LinkedHashMap<>();
        if (attachments != null) {
            attachments.forEach(this::putAttachment);
        }
    }

    public void putAttachment(String key, String value) {
        ensureMutable();
        if (key == null || key.isBlank() || key.length() > MAX_KEY_LENGTH
                || value == null || value.length() > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("invalid RPC context attachment");
        }
        if (!ALLOWED_ATTACHMENT_KEYS.contains(key)) {
            throw new IllegalArgumentException("unsupported RPC context attachment: " + key);
        }
        int existingLength = attachments.containsKey(key) ? attachments.get(key).length() : 0;
        int totalLength = attachments.entrySet().stream()
                .mapToInt(entry -> entry.getKey().length() + entry.getValue().length()).sum();
        if ((!attachments.containsKey(key) && attachments.size() >= MAX_ATTACHMENTS)
                || totalLength - existingLength + key.length() + value.length() > MAX_TOTAL_ATTACHMENT_LENGTH) {
            throw new IllegalArgumentException("RPC context attachments exceed limits");
        }
        attachments.put(key, value);
    }

    public RpcContext withRetryAttempt(int attempt) {
        if (attempt < 0) {
            throw new IllegalArgumentException("retry attempt must not be negative");
        }
        RpcContext copy = new RpcContext();
        copy.setTraceId(traceId);
        copy.setDeadlineEpochMillis(deadlineEpochMillis);
        copy.setIdempotencyKey(idempotencyKey);
        copy.setAttachments(attachments);
        copy.putAttachment("retryAttempt", Integer.toString(attempt));
        return copy.seal();
    }

    public RpcContext seal() {
        sealed = true;
        return this;
    }

    public boolean isSealed() {
        return sealed;
    }

    public boolean isExpired(long nowMillis) {
        return deadlineEpochMillis > 0 && nowMillis >= deadlineEpochMillis;
    }

    private void ensureMutable() {
        if (sealed) {
            throw new IllegalStateException("RPC context is sealed");
        }
    }
}
