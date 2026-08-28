param(
    [string]$OutputDirectory = "build/local-tls",
    [string]$Password = "changeit"
)

$ErrorActionPreference = "Stop"
if ((Test-Path -LiteralPath $OutputDirectory -PathType Container) -and
        @(Get-ChildItem -LiteralPath $OutputDirectory -Force).Count -gt 0) {
    throw "Output directory must be empty: $OutputDirectory"
}
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$output = (Resolve-Path -LiteralPath $OutputDirectory).Path

$keytool = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME "bin/keytool.exe" } else { $null }
if (-not $keytool -or -not (Test-Path -LiteralPath $keytool)) {
    $keytool = (Get-Command keytool -ErrorAction Stop).Source
}

function Invoke-Keytool([string[]]$Arguments) {
    & $keytool @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "keytool failed with exit code $LASTEXITCODE"
    }
}

Invoke-Keytool @("-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12", "-keystore", "$output/server.p12", "-storepass", $Password, "-keypass", $Password, "-dname", "CN=127.0.0.1", "-ext", "SAN=ip:127.0.0.1", "-validity", "30", "-noprompt")
Invoke-Keytool @("-genkeypair", "-alias", "client", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12", "-keystore", "$output/client.p12", "-storepass", $Password, "-keypass", $Password, "-dname", "CN=minirpc-client", "-validity", "30", "-noprompt")
Invoke-Keytool @("-exportcert", "-rfc", "-alias", "server", "-keystore", "$output/server.p12", "-storepass", $Password, "-file", "$output/server.crt")
Invoke-Keytool @("-exportcert", "-rfc", "-alias", "client", "-keystore", "$output/client.p12", "-storepass", $Password, "-file", "$output/client.crt")
Invoke-Keytool @("-importcert", "-noprompt", "-alias", "server", "-file", "$output/server.crt", "-storetype", "PKCS12", "-keystore", "$output/client-trust.p12", "-storepass", $Password)
Invoke-Keytool @("-importcert", "-noprompt", "-alias", "client", "-file", "$output/client.crt", "-storetype", "PKCS12", "-keystore", "$output/server-trust.p12", "-storepass", $Password)

Write-Output "Created local TLS materials in $output"
