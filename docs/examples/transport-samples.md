# 本地明文、TLS 与 mTLS 样例

示例 Provider 与 Consumer 共用同一套启动类。`rpc.security.mode` 决定传输模式：`plaintext`、`tls` 或 `mtls`。TLS 证书由部署方提供；下面的脚本只生成 30 天有效期的本地演示材料，输出目录不会被 Git 跟踪。

在 PowerShell 中先生成材料并保存绝对路径：

```powershell
.\scripts\create-local-tls-material.ps1
$cert = (Resolve-Path .\build\local-tls).Path
```

## 明文直连

终端一：

```powershell
mvn -pl rpc-sample-server -am exec:java '-Dexec.mainClass=com.minirpc.sample.server.ServerApplication' '-Drpc.security.mode=plaintext'
```

终端二：

```powershell
mvn -pl rpc-sample-client -am exec:java '-Dexec.mainClass=com.minirpc.sample.client.ClientApplication' '-Drpc.security.mode=plaintext'
```

## TLS 直连

终端一：

```powershell
mvn -pl rpc-sample-server -am exec:java '-Dexec.mainClass=com.minirpc.sample.server.ServerApplication' '-Drpc.security.mode=tls' "-Drpc.tls.server.key-store=$cert\server.p12" '-Drpc.tls.server.key-store-password=changeit' "-Drpc.tls.server.trust-store=$cert\server-trust.p12" '-Drpc.tls.server.trust-store-password=changeit'
```

终端二：

```powershell
mvn -pl rpc-sample-client -am exec:java '-Dexec.mainClass=com.minirpc.sample.client.ClientApplication' '-Drpc.security.mode=tls' "-Drpc.tls.client.key-store=$cert\client.p12" '-Drpc.tls.client.key-store-password=changeit' "-Drpc.tls.client.trust-store=$cert\client-trust.p12" '-Drpc.tls.client.trust-store-password=changeit'
```

## mTLS 直连

使用同一组证书，只把两端模式改为 `mtls`。服务端会要求客户端在 TLS 握手中提供证书；未携带客户端证书的连接不会进入 RPC 请求处理链。

终端一：

```powershell
mvn -pl rpc-sample-server -am exec:java '-Dexec.mainClass=com.minirpc.sample.server.ServerApplication' '-Drpc.security.mode=mtls' "-Drpc.tls.server.key-store=$cert\server.p12" '-Drpc.tls.server.key-store-password=changeit' "-Drpc.tls.server.trust-store=$cert\server-trust.p12" '-Drpc.tls.server.trust-store-password=changeit'
```

终端二：

```powershell
mvn -pl rpc-sample-client -am exec:java '-Dexec.mainClass=com.minirpc.sample.client.ClientApplication' '-Drpc.security.mode=mtls' "-Drpc.tls.client.key-store=$cert\client.p12" '-Drpc.tls.client.key-store-password=changeit' "-Drpc.tls.client.trust-store=$cert\client-trust.p12" '-Drpc.tls.client.trust-store-password=changeit'
```

可选的 `-Dzk.address=127.0.0.1:2181` 仍可与三种模式组合使用；Provider 和 Consumer 必须使用一致的传输模式。
