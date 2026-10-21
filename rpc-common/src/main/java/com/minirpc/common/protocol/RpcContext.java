package com.minirpc.common.protocol;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 一次 RPC 请求的上下文，携带链路追踪、截止时间、幂等标识和附加信息。
 * <p>
 * 上下文保存在 {@link RpcRequest} 中，随请求一起序列化并传递给服务端。
 * 业务方法的参数由 RpcRequest 单独保存，这里保存的是调用过程需要的元数据。
 * <p>
 * 构建时允许修改；框架在网络发送前、接收端反序列化后调用 {@link #seal()} 封存。
 * 重试时通过 {@link #withRetryAttempt(int)} 创建独立副本，避免修改已经发送的上下文。
 */
public class RpcContext implements Serializable {

    private static final long serialVersionUID = 1L;

    // 附件会随请求传输，因此限制数量和字符串长度；长度按 String.length() 计算，而非字节数。
    private static final int MAX_ATTACHMENTS = 16;
    private static final int MAX_KEY_LENGTH = 64;
    private static final int MAX_VALUE_LENGTH = 1024;
    private static final int MAX_TOTAL_ATTACHMENT_LENGTH = 4096;

    /** 附件键白名单：调用尝试次数，以及供鉴权扩展读取的令牌。 */
    private static final Set<String> ALLOWED_ATTACHMENT_KEYS = Set.of("retryAttempt", "auth.token");

    private String traceId;

    private long deadlineEpochMillis;

    /** 幂等标识 实际的重复请求识别与去重需要另外实现。 */
    private String idempotencyKey;

    /** 受白名单和大小限制的字符串附件，例如 retryAttempt、auth.token。 */
    private Map<String, String> attachments = new LinkedHashMap<>();

    /** 本地封存状态，不随字段序列化传输；接收端解码器会重新封存上下文。 */
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

    /**
     * 返回附件的只读快照：调用方不能修改它，后续内部修改也不会影响已返回的快照。
     */
    public Map<String, String> getAttachments() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(attachments));
    }

    /**
     * 替换全部附件，并逐项执行与 {@link #putAttachment(String, String)} 相同的校验。
     * 复制传入 Map 的内容，避免调用方后续修改原 Map 时影响上下文；传入 null 表示清空。
     */
    public void setAttachments(Map<String, String> attachments) {
        ensureMutable();
        this.attachments = new LinkedHashMap<>();
        if (attachments != null) {
            attachments.forEach(this::putAttachment);
        }
    }

    /**
     * 添加或覆盖一个附件，仅允许白名单中的键，并检查数量、单项长度和累计长度。
     *
     * @throws IllegalStateException 上下文已经封存
     * @throws IllegalArgumentException 键值无效、键不在白名单中，或附件超出限制
     */
    public void putAttachment(String key, String value) {
        ensureMutable();
        if (key == null || key.isBlank() || key.length() > MAX_KEY_LENGTH
                || value == null || value.length() > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("invalid RPC context attachment");
        }
        if (!ALLOWED_ATTACHMENT_KEYS.contains(key)) {
            throw new IllegalArgumentException("unsupported RPC context attachment: " + key);
        }
        // 覆盖已有键时，先扣除旧值长度，再进行本次写入的容量检查。
        int existingLength = attachments.containsKey(key) ? attachments.get(key).length() : 0;
        int totalLength = attachments.entrySet().stream()
                .mapToInt(entry -> entry.getKey().length() + entry.getValue().length()).sum();
        if ((!attachments.containsKey(key) && attachments.size() >= MAX_ATTACHMENTS)
                || totalLength - existingLength + key.length() + value.length() > MAX_TOTAL_ATTACHMENT_LENGTH) {
            throw new IllegalArgumentException("RPC context attachments exceed limits");
        }
        attachments.put(key, value);
    }

    /**
     * 为本次调用尝试创建已封存的上下文副本，原上下文保持不变。
     * <p>
     * 保留原 traceId、截止时间和幂等标识，只更新副本中的 retryAttempt 附件。
     * 沿用同一截止时间，使重试共享整次调用的剩余时间，而不是每次重新计时。
     *
     * @param attempt 调用尝试序号，0 表示首次调用，1 表示第一次重试，以此类推
     * @return 已写入尝试序号并封存的独立上下文
     */
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

    /**
     * 封存当前上下文。之后通过 setter 或 putAttachment 修改时会抛出异常。
     *
     * @return 当前对象，便于链式调用；重复封存不会改变结果
     */
    public RpcContext seal() {
        sealed = true;
        return this;
    }

    /** 判断当前上下文是否已经封存。 */
    public boolean isSealed() {
        return sealed;
    }

    /**
     * 判断指定时刻是否已到达或超过截止时间；未设置截止时间时返回 false。
     *
     * @param nowMillis 当前时间，使用与 System.currentTimeMillis() 相同的毫秒时间戳
     */
    public boolean isExpired(long nowMillis) {
        return deadlineEpochMillis > 0 && nowMillis >= deadlineEpochMillis;
    }

    /** 所有写入操作共用的检查，防止上下文封存后继续被修改。 */
    private void ensureMutable() {
        if (sealed) {
            throw new IllegalStateException("RPC context is sealed");
        }
    }
}
