$ErrorActionPreference = "Stop"

$repo = if ($env:BOOM_REPO) { $env:BOOM_REPO } else { "Soubhik-10/boom" }
$version = if ($env:BOOM_VERSION) { $env:BOOM_VERSION.TrimStart("v") } else { "latest" }
$installDir = if ($env:BOOM_INSTALL_DIR) { $env:BOOM_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "boom\bin" }

if (-not [Environment]::Is64BitOperatingSystem) {
    throw "32-bit Windows is not supported"
}
$asset = "boom-windows-x86_64.zip"
$baseUrl = if ($version -eq "latest") {
    "https://github.com/$repo/releases/latest/download"
} else {
    "https://github.com/$repo/releases/download/v$version"
}

$tempDir = Join-Path ([IO.Path]::GetTempPath()) ("boom-install-" + [guid]::NewGuid())
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null
try {
    $archive = Join-Path $tempDir $asset
    $checksums = Join-Path $tempDir "SHA256SUMS"
    Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/$asset" -OutFile $archive
    Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/SHA256SUMS" -OutFile $checksums

    $checksumLine = Get-Content $checksums |
        Where-Object { $_ -match "\s$([regex]::Escape($asset))$" } |
        Select-Object -First 1
    if (-not $checksumLine) {
        throw "Checksum entry not found for $asset"
    }
    $expected = ($checksumLine -split "\s+")[0]
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash.ToLowerInvariant()
    if ($actual -ne $expected.ToLowerInvariant()) {
        throw "Checksum verification failed"
    }

    $expanded = Join-Path $tempDir "expanded"
    Expand-Archive -LiteralPath $archive -DestinationPath $expanded -Force
    New-Item -ItemType Directory -Force -Path $installDir | Out-Null
    Copy-Item -LiteralPath (Join-Path $expanded "boom.exe") -Destination (Join-Path $installDir "boom.exe") -Force
    Write-Host "Installed boom to $(Join-Path $installDir 'boom.exe')"
    Write-Host "Add $installDir to PATH if it is not already present."
} finally {
    Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}
