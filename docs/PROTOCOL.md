# 协议规范

本文档描述 MiniRPC 自定义应用层协议的字节布局、字段含义与编解码流程。

## 一、协议帧格式

每个 RPC 消息（请求/响应/心跳）都封装为一个协议帧：

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
| Version | 1 字节 | 协议版本号，当前为 `1`，用于后续升级兼容 |
| MsgType | 1 字节 | 消息类型（见下表） |
| SerType | 1 字节 | 序列化类型（见下表） |
| Body Length | 4 字节 | 消息体长度 N，用于解决 TCP 粘包/拆包 |
| Body | N 字节 | 序列化后的业务数据（RpcRequest / RpcResponse） |

**协议头固定长度 = 2 + 1 + 1 + 1 + 4 = 9 字节**

## 二、消息类型 (MsgType)

| 值 | 名称 | 说明 |
|----|------|------|
| `1` | REQUEST | 请求消息，Body 为 `RpcRequest` |
| `2` | RESPONSE | 响应消息，Body 为 `RpcResponse` |
| `3` | HEARTBEAT | 心跳消息，Body 为空 |

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
| requestId | String | 请求唯一标识（UUID），用于请求-响应匹配 |
| interfaceName | String | 服务接口全限定名 |
| methodName | String | 方法名 |
| parameterTypes | Class[] | 参数类型数组（普通调用） |
| parameterTypeNames | String[] | 参数类型名数组（泛化调用） |
| parameters | Object[] | 参数值数组 |

### RpcResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| requestId | String | 对应请求的 ID |
| success | boolean | 是否成功 |
| result | Object | 返回值（成功时） |
| errorMessage | String | 错误信息（失败时） |

## 五、编解码流程

### 编码（Encoder）

```
RpcMessage 对象
    │
    ├─ writeShort(MAGIC_NUMBER)        魔数
    ├─ writeByte(VERSION)              版本
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
3. 读取并校验版本号
4. 读取消息类型、序列化类型
5. 读取 Body 长度，校验 0 ≤ length ≤ 8MB（防 OOM）
6. 可读字节不足 → resetReaderIndex 等待更多数据（解决拆包）
7. 读取 Body 字节数组
8. 按消息类型反序列化为 RpcRequest / RpcResponse
9. 封装为 RpcMessage 输出
```

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
| 恶意超大 Body（OOM 攻击） | Body Length 上限 8MB 校验 |
| 协议升级冲突 | Version 字段预留兼容性 |
