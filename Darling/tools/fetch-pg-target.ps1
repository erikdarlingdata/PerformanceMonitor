<#
.SYNOPSIS
  Fetches a real, OLD PostgreSQL major to run the Darling collectors against as a monitored TARGET (#6985).

.DESCRIPTION
  Test infrastructure, not shipped. fetch-pg-runtime.ps1 builds the PostgreSQL 18 + TimescaleDB runtime that is the
  STORE; every live test reads the store and the targets from it. This script supplies the other side: a PostgreSQL
  10, 13, 14, 16 or 17 server that a collector reads FROM, so a version gate or a NULL an old server sends turns a
  run red before the release does (#6206, #6836).

  Steps, for the one major named by -Major:
    1. Download the pinned EDB Windows x64 BINARIES zip for that major into
       <OutputDirectory>\downloads (one file per version, cached, shared by every caller) and verify its SHA256.
       A mismatch deletes the download and fails hard.
    2. Extract it and keep pgsql\{bin,lib,share} only (pgAdmin, StackBuilder, doc, include and the symbols are
       dropped: about 40-60 MB of the 280-320 MB archive) into <OutputDirectory>\<major>\pgsql.
    3. Add the Visual C++ runtime DLLs the build links, from System32, when the zip has none of its own (the PG 10
       build links msvcr120.dll, which a bare Windows does not ship; windows-latest runners and dev boxes have it).
    4. Check the files the start script needs, then write <OutputDirectory>\<major>\.script-hash.

  CACHING: .script-hash holds the SHA256 of THIS file. A tree whose stamp matches is reused with no download and no
  extraction; any edit to the script (a new pin, a changed strip list) rebuilds it. A CI cache keyed on
  hashFiles('Darling/tools/fetch-pg-target.ps1') over <OutputDirectory>\<major> does the same across runs.

  HASH PROVENANCE: EDB publishes no checksum, so each pin is the SHA256 of the zip at the exact URL pinned here, as
  computed by whoever added the row, on the date in its comment. Bumping a version means updating URL and hash
  TOGETHER from a download you hashed yourself. An old major gets security fixes only until its end of life; the
  rows track the newest minor EDB built for the major.

  The last line printed is the pgsql directory (the one holding bin\), so a caller can capture it.

.PARAMETER Major
  The PostgreSQL major to fetch: a key of the table below.

.PARAMETER OutputDirectory
  Where the stripped trees and the shared download cache go. Defaults to Darling\artifacts\pg-targets (gitignored).

.EXAMPLE
  pwsh -File Darling\tools\fetch-pg-target.ps1 -Major 10
#>
#Requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Major,
    [string]$OutputDirectory = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# ---- Pinned artifacts (update URL + SHA256 together; see HASH PROVENANCE above) -------------
# VcRuntime: DLLs copied from System32 when the zip does not carry them. EDB's PostgreSQL 10 binaries link the
# Visual C++ 2013 runtime (msvcr120.dll, found by reading the imports of postgres.exe and initdb.exe).
$targets = @{
    # 10.23 (the last PostgreSQL 10 minor), 276,310,221 bytes, hashed 2026-10-30.
    '10' = @{
        Version = '10.23'
        Url = 'https://get.enterprisedb.com/postgresql/postgresql-10.23-1-windows-x64-binaries.zip'
        Sha256 = 'A87722E090E2DCDED01C237609E9AD1673A0CA31C90BD766B63A8DFBB666AC47'
        VcRuntime = @('msvcr120.dll')
    }
    # 13.23 (the last PostgreSQL 13 minor), 318,646,219 bytes, hashed 2026-10-30. 13 and later link the Visual C++
    # 2015-2022 runtime (vcruntime140), which every supported Windows image and runner carries, so VcRuntime is empty.
    '13' = @{
        Version = '13.23'
        Url = 'https://get.enterprisedb.com/postgresql/postgresql-13.23-1-windows-x64-binaries.zip'
        Sha256 = 'B669F3E67AE20EA25D79B708E63E9237140775DACEFF092FC58A73206F63C3A7'
        VcRuntime = @()
    }
    # 14.20, 319,602,886 bytes, hashed 2026-10-30.
    '14' = @{
        Version = '14.20'
        Url = 'https://get.enterprisedb.com/postgresql/postgresql-14.20-1-windows-x64-binaries.zip'
        Sha256 = '405326EFCEEA31E39AF278F327181CD736E5F5D75179416B60506D830407CBCB'
        VcRuntime = @()
    }
    # 16.15, 332,441,502 bytes, hashed 2026-10-30.
    '16' = @{
        Version = '16.15'
        Url = 'https://get.enterprisedb.com/postgresql/postgresql-16.15-1-windows-x64-binaries.zip'
        Sha256 = '25E6FCDFB8CAEC38691BF461125E7564508760666F7B8E5DC6A5F0818F58F81E'
        VcRuntime = @()
    }
    # 17.11, 340,719,294 bytes, hashed 2026-10-30.
    '17' = @{
        Version = '17.11'
        Url = 'https://get.enterprisedb.com/postgresql/postgresql-17.11-1-windows-x64-binaries.zip'
        Sha256 = '6EABDF00D2893713B75DB4336A23C3FDF505F056E217EC6E2E95D901750CFEA3'
        VcRuntime = @()
    }
}
# ----------------------------------------------------------------------------------------------

if (-not $targets.ContainsKey($Major)) {
    throw "No pinned PostgreSQL target for major '$Major'. Known majors: $(($targets.Keys | Sort-Object { [int]$_ }) -join ', '). Add a row (version, URL, SHA256) to the table in fetch-pg-target.ps1."
}
$target = $targets[$Major]

Add-Type -AssemblyName System.IO.Compression.FileSystem
# PS 5.1 defaults to TLS 1.0/1.1 on some machines; both hosts require TLS 1.2+.
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor [System.Net.SecurityProtocolType]::Tls12

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path $PSScriptRoot '..\artifacts\pg-targets'
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
$downloadDirectory = Join-Path $OutputDirectory 'downloads'
$majorDirectory = Join-Path $OutputDirectory $Major
$pgsqlTarget = Join-Path $majorDirectory 'pgsql'
$stampPath = Join-Path $majorDirectory '.script-hash'

$scriptHash = (Get-FileHash -Algorithm SHA256 -Path $PSCommandPath).Hash
if ((Test-Path $stampPath) -and (Test-Path (Join-Path $pgsqlTarget 'bin\postgres.exe')) `
        -and ((Get-Content -Raw -Path $stampPath).Trim() -eq $scriptHash)) {
    Write-Host "PostgreSQL $($target.Version) target: cached tree for script hash $scriptHash"
    Write-Output $pgsqlTarget
    return
}

function Get-VerifiedDownload {
    param(
        [string]$Url,
        [string]$ExpectedSha256,
        [string]$Destination
    )

    if (Test-Path $Destination) {
        $cachedHash = (Get-FileHash -Algorithm SHA256 -Path $Destination).Hash
        if ($cachedHash -eq $ExpectedSha256) {
            Write-Host "  cached + verified: $Destination"
            return
        }
        Write-Warning "  cached file hash mismatch - deleting and re-downloading: $Destination"
        Remove-Item -Force $Destination
    }

    Write-Host "  downloading $Url"
    $temporary = "$Destination.partial"
    if (Test-Path $temporary) {
        Remove-Item -Force $temporary
    }
    Invoke-WebRequest -Uri $Url -OutFile $temporary -UseBasicParsing

    $actualHash = (Get-FileHash -Algorithm SHA256 -Path $temporary).Hash
    if ($actualHash -ne $ExpectedSha256) {
        Remove-Item -Force $temporary
        throw "SHA256 mismatch for $Url : expected $ExpectedSha256, got $actualHash. Refusing to use an unverified server."
    }

    Move-Item -Force $temporary $Destination
    Write-Host "  verified SHA256 $actualHash"
}

New-Item -ItemType Directory -Force -Path $downloadDirectory | Out-Null

Write-Host "PostgreSQL $($target.Version) binaries (target, Windows x64):"
$pgZip = Join-Path $downloadDirectory "postgresql-$($target.Version)-windows-x64-binaries.zip"
Get-VerifiedDownload -Url $target.Url -ExpectedSha256 $target.Sha256 -Destination $pgZip

# The tree and the extraction are rebuilt from scratch, so a half-finished earlier run leaves nothing behind.
$extractDirectory = Join-Path $OutputDirectory "extract-$Major"
foreach ($rebuilt in @($extractDirectory, $majorDirectory)) {
    if (Test-Path $rebuilt) {
        Remove-Item -Recurse -Force $rebuilt
    }
}
New-Item -ItemType Directory -Force -Path $pgsqlTarget | Out-Null

Write-Host "Extracting..."
[System.IO.Compression.ZipFile]::ExtractToDirectory($pgZip, $extractDirectory)
$pgSource = Join-Path $extractDirectory 'pgsql'
if (-not (Test-Path (Join-Path $pgSource 'bin\initdb.exe'))) {
    throw "The EDB archive did not contain pgsql\bin\initdb.exe - its layout changed; update this script."
}

foreach ($keep in @('bin', 'lib', 'share')) {
    Copy-Item -Recurse -Path (Join-Path $pgSource $keep) -Destination (Join-Path $pgsqlTarget $keep)
}
Remove-Item -Recurse -Force $extractDirectory

$binTarget = Join-Path $pgsqlTarget 'bin'
foreach ($dll in $target.VcRuntime) {
    if (Test-Path (Join-Path $binTarget $dll)) {
        continue
    }
    $vcSource = Join-Path $env:SystemRoot "System32\$dll"
    if (-not (Test-Path $vcSource)) {
        throw "MSVC runtime $dll not found at $vcSource - install the matching Visual C++ Redistributable; PostgreSQL $($target.Version) cannot start without it."
    }
    Copy-Item -Path $vcSource -Destination $binTarget
}

# Sanity: what start-test-pg.ps1 runs and loads.
$requiredFiles = @(
    'bin\initdb.exe',
    'bin\pg_ctl.exe',
    'bin\postgres.exe',
    'bin\psql.exe',
    'bin\createdb.exe',
    'bin\libpq.dll',
    'lib\pg_stat_statements.dll',
    'share\extension\pg_stat_statements.control'
) + @($target.VcRuntime | ForEach-Object { "bin\$_" })
foreach ($required in $requiredFiles) {
    if (-not (Test-Path (Join-Path $pgsqlTarget $required))) {
        throw "Assembled PostgreSQL $($target.Version) target is missing $required - refusing to use it."
    }
}

Set-Content -Path $stampPath -Value $scriptHash -NoNewline
$size = (Get-ChildItem -Recurse -File $pgsqlTarget | Measure-Object -Property Length -Sum).Sum
Write-Host ("PostgreSQL {0} target ready: {1:N1} MB stripped, script hash {2}" -f $target.Version, ($size / 1MB), $scriptHash)
Write-Output $pgsqlTarget
