# Generates a self-signed TLS certificate for local development.
# Uses OpenSSL from Git for Windows if available, otherwise falls back to New-SelfSignedCertificate.
# Output: certs/cert.pem + certs/key.pem

$projectDir = Split-Path $PSScriptRoot -Parent
$certsDir   = Join-Path $projectDir "certs"
New-Item -ItemType Directory -Force -Path $certsDir | Out-Null

$certFile = Join-Path $certsDir "cert.pem"
$keyFile  = Join-Path $certsDir "key.pem"

# Find openssl.exe
$opensslCmd = Get-Command openssl -ErrorAction SilentlyContinue
$opensslExe = $null
if ($opensslCmd) {
    $opensslExe = $opensslCmd.Source
}
if (-not $opensslExe) {
    $candidates = @(
        'C:\Program Files\Git\usr\bin\openssl.exe',
        'C:\Program Files\Git\mingw64\bin\openssl.exe',
        'C:\Program Files (x86)\Git\usr\bin\openssl.exe'
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { $opensslExe = $c; break }
    }
}

if ($opensslExe) {
    Write-Host "Using OpenSSL: $opensslExe"

    & $opensslExe req -x509 -newkey rsa:2048 `
        -keyout $keyFile `
        -out $certFile `
        -days 365 -nodes `
        -subj '/CN=localhost' `
        -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost'

    if ($LASTEXITCODE -ne 0) {
        Write-Error "OpenSSL failed"
        exit 1
    }
} else {
    Write-Host "OpenSSL not found - using New-SelfSignedCertificate"

    $cert = New-SelfSignedCertificate `
        -Subject 'CN=localhost' `
        -DnsName 'localhost' `
        -CertStoreLocation 'Cert:/CurrentUser/My' `
        -NotAfter (Get-Date).AddDays(365) `
        -KeyAlgorithm RSA -KeyLength 2048 `
        -KeyExportPolicy Exportable

    $pfxFile  = [System.IO.Path]::GetTempFileName() + '.pfx'
    $password = ConvertTo-SecureString 'vkbtemp' -Force -AsPlainText
    Export-PfxCertificate -Cert $cert -FilePath $pfxFile -Password $password | Out-Null

    $pfxCert = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new(
        $pfxFile, 'vkbtemp',
        [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::Exportable
    )

    # cert.pem
    $b64 = [System.Convert]::ToBase64String($pfxCert.RawData, 'InsertLineBreaks')
    Set-Content $certFile -Encoding ASCII -Value ("-----BEGIN CERTIFICATE-----`r`n" + $b64 + "`r`n-----END CERTIFICATE-----")

    # key.pem (requires .NET 5+ / PS 7)
    $rsa      = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($pfxCert)
    $keyBytes = $rsa.ExportPkcs8PrivateKey()
    $kb64     = [System.Convert]::ToBase64String($keyBytes, 'InsertLineBreaks')
    Set-Content $keyFile -Encoding ASCII -Value ("-----BEGIN PRIVATE KEY-----`r`n" + $kb64 + "`r`n-----END PRIVATE KEY-----")

    Remove-Item $pfxFile -Force
    Remove-Item ('Cert:/CurrentUser/My/' + $cert.Thumbprint) -Force
}

Write-Host ''
Write-Host 'Generated:' -ForegroundColor Green
Write-Host "  cert: $certFile"
Write-Host "  key:  $keyFile"
Write-Host ''
Write-Host 'Add to your .env:' -ForegroundColor Yellow
Write-Host '  TLS_CERT=./certs/cert.pem'
Write-Host '  TLS_KEY=./certs/key.pem'
Write-Host ''
Write-Host 'Trust the cert (run once as Administrator):' -ForegroundColor Yellow
Write-Host "  Import-Certificate -FilePath '$certFile' -CertStoreLocation Cert:/LocalMachine/Root"
