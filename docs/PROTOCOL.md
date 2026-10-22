# 协议规范

本文档描述 MiniRPC 自定义应用层协议的字节布局、字段含义与编解码流程。

## 一、协议帧格式

每个 RPC 消息（请求/响应/心跳/取消）都封装为一个协议帧：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          Magic Number         | Version | MsgType | SerType  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Body Length                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                            Body (N bytes)                     |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| 字段 | 长度 | 说明 |
|------|------|------|
| Magic Number | 2 字节 | 魔数 `0x1234`，标识本协议，快速过滤非法数据包 |
| Version | 1 字节 | 当前发送版本为 `2`；接收端允许 `1` 和 `2`，兼容范围见下文 |
| MsgType | 1 字节 | 消息类型（见下表） |
| SerType | 1 字节 | 序列化类型（见下表） |
| Body Length | 4 字节 | 消息体长度 N，用于解决 TCP 粘包/拆包 |
| Body | N 字节 | 序列化后的消息体（RpcRequest / RpcResponse / RpcCancellation）；心跳为空 |

**协议头固定长度 = 2 + 1 + 1 + 1 + 4 = 9 字节**

### 协议版本与兼容范围

`RpcMessage.VERSION` 当前为 `2`，编码器始终写入这个值。`isSupportedVersion(version)` 的判断是 `version == 1 || version == VERSION`，因此当前解码器接受版本 `1`、`2`，拒绝其他版本。如果将来修改 `VERSION`，需要同时检查旧版本兼容列表，不能假设历史版本会自动全部保留。

版本 `2` 引入 `CANCEL` 取消消息。接收版本 `1` 的普通请求帧，不代表框架已经实现完整的新旧端点双向兼容：当前没有版本协商，也不会因为收到版本 `1` 请求就把响应降为版本 `1`。需要使用取消能力时，应升级通信双方到支持该消息的实现。

当前版本校验只检查版本号，**没有按版本限制消息类型**，因此不会单独拒绝“版本 `1` + `CANCEL`”的组合。版本接受范围与消息能力校验是两个不同问题，文档不能将后者描述为已经实现。

这里的帧版本号与 `RpcRequest.version` 不同：帧版本表示通信协议格式，后者是业务服务版本，例如 `1.0.0`。

## 二、消息类型 (MsgType)

| 值 | 名称 | 说明 |
|----|------|------|
| `1` | REQUEST | 请求消息，Body 为 `RpcRequest` |
| `2` | RESPONSE | 响应消息，Body 为 `RpcResponse` |
| `3` | HEARTBEAT | 心跳消息，Body 为空 |
| `4` | CANCEL | 取消控制消息，Body 为 `RpcCancellation`，请求取消对应的排队任务 |

编码器要求 `CANCEL` 消息体必须是 `RpcCancellation`，否则抛出编码异常。服务端在业务请求处理之前分支处理取消消息；取消只作用于尚未开始的任务，不中断已经运行的业务。

## 三、序列化类型 (SerType)

| 值 | 名称 | 说明 |
|----|------|------|
| `0` | JDK | 保留编码值以兼容协议；网络编解码器拒绝该类型 |
| `1` | HESSIAN2 | Hessian2 序列化（默认，跨语言、性能好） |
| `2` | PROTOSTUFF | Protostuff 序列化（Protobuf 兼容，无需 .proto） |

## 四、消息体结构

### RpcRequest

| 字段 | 类型 | 说明 |
|------|------|------|
| requestId | String | 请求唯一标识，代理默认生成 UUID，用于请求-响应匹配 |
| interfaceName | String | 服务接口全限定名 |
| version | String | 业务服务版本，默认 `1.0.0`；不是协议头版本号 |
| group | String | 业务服务分组，默认 `default` |
| context | RpcContext | 追踪标识、截止时间、幂等标识和附件 |
| methodName | String | 方法名 |
| parameterTypeNames | String[] | 方法声明中的参数类型名称；普通调用与泛化调用都使用此字段 |
| parameters | Object[] | 参数值数组 |

`parameterTypes` 不是实际存储字段。普通代理从反射方法取得 `Class<?>[]`，再由 `setParameterTypes()` 调用每个类型的 `getName()` 转成字符串；泛化调用直接提供名称。服务端调用 `getParameterTypes()` 将这些名称解析回本地的 `Class<?>[]`，再用 `getMethod(methodName, parameterTypes)` 查找目标方法。

传类型名称的目的是使用明确的传输表示，减少对序列化器如何处理 Java `Class` 对象的依赖，并允许泛化调用只提供名称。`Class` 本身可以被某些序列化方式处理，不能把这里的选择解释成“Class 完全不能序列化”；传递名称也不能自动解决两端类定义不一致的问题，服务端仍需加载到对应类型。

基本类型使用 `int`、`long` 等名称，由 `resolveClass()` 显式映射；引用类型由 `Class.forName()` 解析。数组使用 `Class.getName()` 对应的名称，例如 `[I` 表示 `int[]`，`[Ljava.lang.String;` 表示 `String[]`。

类型名称和参数值承担不同职责。例如调用 `add(int, int)` 时，名称是 `{"int", "int"}`，值是 `{1, 2}`；放入 `Object[]` 的整数值会装箱为 `Integer`，但方法声明中的类型仍是 `int`。类似地，`echo(String)` 与 `echo(Object)` 可以收到相同的字符串值，单靠值无法确定要调用哪个重载，`null` 也无法提供运行时类型。

把类型名称字段改成 `Object` 或 `Object[]` 在 Java 声明上可行，但只是放宽容器类型，接收端仍要约定里面保存什么、检查类型并转换，不能替代方法签名信息。`Object.class` 只代表方法声明中的 `Object` 参数，不是查找任意参数类型的通配符。当前 `String[]` 保存类型名称、`Object[]` 保存实际值的区分更明确。

### RpcContext

`RpcContext` 随 `RpcRequest` 一起传输，保存本次调用的辅助信息。发送前和接收端反序列化后，框架会调用 `seal()` 封存上下文。

| 字段 | 类型 | 说明 |
|------|------|------|
| traceId | String | 关联调用记录的链路标识 |
| deadlineEpochMillis | long | 绝对截止时间，单位为 Unix 纪元毫秒；小于等于 0 表示未设置 |
| idempotencyKey | String | 幂等标识；当前仅携带，不自动实现业务去重 |
| attachments | Map<String, String> | 限制键和值长度的附件；当前键白名单为 `retryAttempt`、`auth.token` |

`sealed` 是本地 `transient` 状态，不作为传输字段。重试通过 `withRetryAttempt()` 创建已封存的独立副本，保留同一截止时间。

### RpcResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| requestId | String | 对应请求的 ID |
| status | byte | `0` 表示成功，`1` 表示失败 |
| data | Object | 方法返回值（成功时） |
| errorMessage | String | 错误信息（失败时） |
| errorCode | RpcErrorCode | 稳定错误类别；当前成功工厂方法设置为 `OK` |

兼容缺少 `errorCode` 的旧响应时，`getErrorCode()` 根据 `status` 返回 `OK` 或 `INTERNAL`。是否重试应依据稳定错误码，而不是异常文本。

### RpcCancellation

`RpcCancellation` 是取消控制消息的消息体，仅保存 `String requestId`，用于定位本连接上尚未开始执行的请求。

底层 `NettyClient.sendAsync()` 返回的原始 Future 被取消时，会清理本地等待记录，并在请求已写出、连接仍可用时发送取消帧。异步代理返回的业务 Future 会逐层传递取消到底层当前请求，并停止后续 Failover 尝试。普通超时以 `DEADLINE_EXCEEDED` 结束，不发送取消帧。服务端仍只取消尚未开始执行的排队任务。

## 五、编解码流程

### 编码（Encoder）

```
RpcMessage 对象
    │
    ├─ writeShort(MAGIC_NUMBER)        魔数
    ├─ writeByte(VERSION)              当前固定写入版本 2
    ├─ writeByte(messageType.code)     消息类型
    ├─ writeByte(serializerType.code)  序列化类型
    ├─ serializer.serialize(data)      序列化 Body
    ├─ writeInt(bodyBytes.length)      Body 长度
    └─ writeBytes(bodyBytes)           Body
    │
    ▼
字节流 → 网络
```

### 解码（Decoder）

配合 Netty 的 `LengthFieldBasedFrameDecoder` 使用：

```
配置：new LengthFieldBasedFrameDecoder(8MB, 5, 4, 0, 0)
                              │       │  │
                              │       │  └─ 长度字段占 4 字节
                              │       └──── 长度字段偏移 5（magic2+version1+msgType1+serType1）
                              └──────────── 最大帧长度 8MB
```

解码步骤：

```
1. 检查可读字节 ≥ 9（协议头长度）
2. 读取并校验魔数（不匹配则抛异常）
3. 读取并校验版本号（当前接受 1、2，拒绝其他值）
4. 读取消息类型、序列化类型
5. 读取 Body 长度，校验 0 ≤ length ≤ 8MB（防 OOM）
6. 可读字节不足 → resetReaderIndex 等待更多数据（解决拆包）
7. 读取 Body 字节数组
8. 按消息类型反序列化为 RpcRequest / RpcResponse / RpcCancellation
9. 封装为 RpcMessage 输出
```

请求中含有 `RpcContext` 时，在输出前封存该上下文。心跳通常没有消息体，不执行消息体反序列化。

`LengthFieldBasedFrameDecoder` 的 8 MiB 限制针对整个协议帧，`RpcMessageDecoder` 还单独检查消息体不超过 8 MiB。因此在当前标准 pipeline 中，可接受的消息体最大为 `8 * 1024 * 1024 - 9` 字节，需要计入 9 字节协议头。

## 六、粘包/拆包解决方案

TCP 是字节流协议，无消息边界，存在：

- **粘包**：多个请求黏在一起
- **拆包**：一个请求被拆成多段

MiniRPC 采用 **长度字段法**：

1. 协议头携带 4 字节 Body Length
2. `LengthFieldBasedFrameDecoder` 根据长度字段切分出完整帧
3. 数据不足时重置读指针，等待后续数据

## 七、心跳机制

```
客户端：IdleStateHandler(写空闲 30s) → 触发 HeartbeatHandler 发送 HEARTBEAT 帧
服务端：IdleStateHandler(读空闲超时) → 长时间未收到任何数据则关闭连接
```

心跳用于及时探测连接存活，剔除失效连接。

## 八、安全设计

| 风险 | 防护措施 |
|------|----------|
| 非法数据包 | 魔数校验，不匹配直接拒绝 |
| 恶意超大消息（OOM 攻击） | 8 MiB 整帧限制，以及 Body Length 非负和上限检查 |
| 不支持的协议版本 | 明确接受版本 1、2；不支持的版本抛出异常，不进行版本协商 |

## 九、传输安全与错误边界

TLS 不是应用协议帧的一部分。启用后，Netty 在 9 字节 MiniRPC 帧编解码之前完成 TLS 握手，因此帧头和消息体都由 TLS 记录层保护。mTLS 会在握手阶段要求客户端证书；握手失败的连接不会进入请求处理链。

TLS 使用部署提供的 PKCS#12 key store / trust store。框架不生成、持久化或打印私钥、密码，也不提供 `trustAll` 模式。`minirpc.*.tls.enabled=true` 缺少 key store 路径或密码会在配置创建阶段失败；mTLS 缺少 trust store 也会失败。

请求通过可选应用层认证后才会进入业务执行器。认证失败返回稳定错误码 `UNAUTHORIZED`；饱和的业务线程池返回 `OVERLOADED`；可重试的传输不可用返回 `UNAVAILABLE`。客户端只会为显式 `@RpcIdempotent` 调用按这些稳定错误码作有限重试，绝不依赖异常文本。
