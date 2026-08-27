package com.minirpc.common.protocol;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

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

    private String traceId;
    private long deadlineEpochMillis;
    private String idempotencyKey;
    private Map<String, String> attachments = new LinkedHashMap<>();

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public long getDeadlineEpochMillis() {
        return deadlineEpochMillis;
    }

    public void setDeadlineEpochMillis(long deadlineEpochMillis) {
        this.deadlineEpochMillis = deadlineEpochMillis;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Map<String, String> getAttachments() {
        return new LinkedHashMap<>(attachments);
    }

    public void setAttachments(Map<String, String> attachments) {
        this.attachments = new LinkedHashMap<>();
        if (attachments != null) {
            attachments.forEach(this::putAttachment);
        }
    }

    public void putAttachment(String key, String value) {
        if (key == null || key.isBlank() || key.length() > MAX_KEY_LENGTH
                || value == null || value.length() > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("invalid RPC context attachment");
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

    public boolean isExpired(long nowMillis) {
        return deadlineEpochMillis > 0 && nowMillis >= deadlineEpochMillis;
    }
}
