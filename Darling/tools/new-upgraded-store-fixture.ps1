<#
.SYNOPSIS
  Builds the two runtimes an UPGRADED-IN-PLACE store fixture needs: a PREVIOUS-major PostgreSQL +
  TimescaleDB runtime, and the current shipped one. Emits the environment variables that unlock the
  gated store-upgrade test.

.DESCRIPTION
  #1705 proved a gap that no amount of green CI could see: darling-pg only ever creates FRESH stores,
  on the current runtime, with the current extension. A store that has been through a version change
  is a different animal - its catalog carries the history, its extension may lag its binaries, and the
  upgrade machinery that moves it (#1706) has no other way to be exercised. This script produces the
  raw material for that fixture.

  It builds the OLD side here (its own pinned artifacts, deliberately separate from the shipped pins
  in fetch-pg-runtime.ps1 so a fixture version can never be mistaken for a shipped one), and delegates
  the NEW side to fetch-pg-runtime.ps1 so the fixture always upgrades TO exactly what the package
  ships.

  Output layout under <OutputDirectory>:
    old\pg-runtime\pgsql\...       the previous-major runtime, assembled (DARLING_TEST_PGRUNTIME_OLD)
    previous\pg-runtime\pgsql\...  the previous release's runtime on the CURRENT major, assembled
                                  (DARLING_TEST_PGRUNTIME_PREVIOUS) - the same-major upgrade, #3906
    new\pg-runtime.zip             the current shipped runtime bundle (DARLING_TEST_PGRUNTIME_NEWZIP)

  Then either run the gated tests:
    $env:DARLING_TEST_PGRUNTIME_OLD      = "<OutputDirectory>\old\pg-runtime"
    $env:DARLING_TEST_PGRUNTIME_PREVIOUS = "<OutputDirectory>\previous\pg-runtime"
    $env:DARLING_TEST_PGRUNTIME_NEWZIP   = "<OutputDirectory>\new\pg-runtime.zip"
    dotnet test Darling\Darling.Tests\Darling.Tests.csproj --filter FullyQualifiedName~DarlingStoreUpgradeTests

  ...or stage a real service deployment by hand: copy old\pg-runtime beside the service binary, let it
  create a store, then drop new\pg-runtime.zip in and restart. That is the field upgrade, verbatim.

  HASH PROVENANCE: the previous-major pins below were computed 2026-07-26 from artifacts downloaded
  from the exact URLs pinned here (EDB PG 17.10 zip 333,925,750 bytes; TimescaleDB 2.28.1-for-PG17
  zip 7,792,491 bytes). The previous-release pins are 3.8.0's shipped pins, computed 2026-07-08 (EDB PG
  18.4 zip 337,444,127 bytes; TimescaleDB 2.28.1-for-PG18 zip 7,794,443 bytes). Bumping a version means
  updating URL + hash TOGETHER, from a fresh download you hashed yourself - the same discipline
  fetch-pg-runtime.ps1 documents.

.PARAMETER OutputDirectory
  Where the two runtimes land. Defaults to Darling\artifacts\upgrade-fixture (gitignored).

.PARAMETER SkipNew
  Do not build the current runtime bundle - useful when you already have a pg-runtime.zip to point at.

.EXAMPLE
  pwsh -File Darling\tools\new-upgraded-store-fixture.ps1
#>

# pwsh 7+ only, for the same reason fetch-pg-runtime.ps1 requires it: Windows PowerShell 5.1 runs on
# .NET Framework, whose ZipFile.CreateFromDirectory writes BACKSLASH entry separators.
#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$OutputDirectory = "",
    [switch]$SkipNew
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path $PSScriptRoot '..\artifacts\upgrade-fixture'
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)

# ---- Pinned PREVIOUS-major artifacts (FIXTURE ONLY - never shipped) --------------------------
# The previous major is what an existing field store was created with; the point of the fixture is
# that the upgrade has a real cluster of that vintage to move.
$oldPgVersion = '17.10'
$oldPgUrl = 'https://get.enterprisedb.com/postgresql/postgresql-17.10-1-windows-x64-binaries.zip'
$oldPgSha256 = 'F9AAFCA58E7026A1EF2CAEEE711ACF761671E57904D430ADC85F468374F5A821'

# The SAME TimescaleDB version the current bundle ships, built for the OLD PostgreSQL. That is not a
# simplification - it is the realistic shape: a store can be behind on PostgreSQL and current on the
# extension, or behind on both. Point -OldTimescaleVersion at an older release to exercise the bridge
# (the ALTER EXTENSION step) as well as pg_upgrade.
$oldTsVersion = '2.28.1'
$oldTsUrl = 'https://github.com/timescale/timescaledb/releases/download/2.28.1/timescaledb-postgresql-17-windows-amd64.zip'
$oldTsSha256 = '0B3C31A4A45B2F623E58D25F73A5980093BB920960077B369284D0B9049CF26A'

# ---- Pinned PREVIOUS-RELEASE artifacts on the CURRENT major (FIXTURE ONLY - never shipped) -----
# What the last release shipped, which is what nearly every field store runs when it upgrades: the
# bundle's own major at an older minor. The same-major store-upgrade test (#3906) builds a real store on
# it and upgrades that store to the current bundle, with no pg_upgrade. When fetch-pg-runtime.ps1 moves,
# move these to what the previous release shipped, so the fixture stays the real upgrade path.
$previousPgVersion = '18.4'
$previousPgUrl = 'https://get.enterprisedb.com/postgresql/postgresql-18.4-1-windows-x64-binaries.zip'
$previousPgSha256 = '7EFFE34C0BF89027B3F171447D351CBC460F4566C8D0F643DAEC67F140787858'

$previousTsVersion = '2.28.1'
$previousTsUrl = 'https://github.com/timescale/timescaledb/releases/download/2.28.1/timescaledb-postgresql-18-windows-amd64.zip'
$previousTsSha256 = '533D1554F3EDFF1E0E86087E8A76A9EEFF253A9FEE17392FC17FAC1C85F7CF0F'
# ----------------------------------------------------------------------------------------------

[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor [System.Net.SecurityProtocolType]::Tls12

$newRoot = Join-Path $OutputDirectory 'new'
$workDirectory = Join-Path $OutputDirectory 'work'
$downloadDirectory = Join-Path $workDirectory 'downloads'
$extractDirectory = Join-Path $workDirectory 'extract'

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
New-Item -ItemType Directory -Force -Path $downloadDirectory | Out-Null

if (Test-Path $extractDirectory) {
    Remove-Item -Recurse -Force $extractDirectory
}
New-Item -ItemType Directory -Force -Path $extractDirectory | Out-Null

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
        throw "SHA256 mismatch for $Url : expected $ExpectedSha256, got $actualHash. Refusing to build a fixture from an unverified runtime."
    }

    Move-Item -Force $temporary $Destination
    Write-Host "  verified SHA256 $actualHash"
}

# Builds one fixture runtime under <OutputDirectory>\<Name>\pg-runtime from pinned artifacts and returns
# that pg-runtime directory.
function New-FixtureRuntime {
    param(
        [string]$Name,
        [string]$Description,
        [string]$PgVersion,
        [string]$PgUrl,
        [string]$PgSha256,
        [string]$TsVersion,
        [string]$TsUrl,
        [string]$TsSha256
    )

    $pgMajor = $PgVersion.Split('.')[0]
    Write-Host "$Description runtime: PostgreSQL $PgVersion + TimescaleDB $TsVersion"
    $pgZip = Join-Path $downloadDirectory "postgresql-$PgVersion-windows-x64-binaries.zip"
    $tsZip = Join-Path $downloadDirectory "timescaledb-$TsVersion-postgresql-$pgMajor-windows-amd64.zip"
    Get-VerifiedDownload -Url $PgUrl -ExpectedSha256 $PgSha256 -Destination $pgZip
    Get-VerifiedDownload -Url $TsUrl -ExpectedSha256 $TsSha256 -Destination $tsZip

    Write-Host "Extracting..."
    $pgExtract = Join-Path $extractDirectory "$Name-pg"
    $tsExtract = Join-Path $extractDirectory "$Name-ts"
    [System.IO.Compression.ZipFile]::ExtractToDirectory($pgZip, $pgExtract)
    [System.IO.Compression.ZipFile]::ExtractToDirectory($tsZip, $tsExtract)

    $pgSource = Join-Path $pgExtract 'pgsql'
    if (-not (Test-Path (Join-Path $pgSource 'bin\initdb.exe'))) {
        throw "The EDB archive did not contain pgsql\bin\initdb.exe - its layout changed; update this script."
    }
    $tsSource = Join-Path $tsExtract 'timescaledb'
    if (-not (Test-Path (Join-Path $tsSource 'timescaledb.control'))) {
        throw "The TimescaleDB archive did not contain timescaledb\timescaledb.control - its layout changed; update this script."
    }

    # Same assembly recipe as the shipped bundle, so the fixture is a runtime this product really would
    # have installed - including the MSVC runtime, without which postgres.exe cannot launch on a clean
    # Windows Server.
    $root = Join-Path $OutputDirectory $Name
    Write-Host "Assembling $Name\pg-runtime\pgsql ..."
    if (Test-Path $root) {
        Remove-Item -Recurse -Force $root
    }
    $pgsqlTarget = Join-Path $root 'pg-runtime\pgsql'
    New-Item -ItemType Directory -Force -Path $pgsqlTarget | Out-Null

    foreach ($keep in @('bin', 'lib', 'share')) {
        Copy-Item -Recurse -Path (Join-Path $pgSource $keep) -Destination (Join-Path $pgsqlTarget $keep)
    }

    $vcRuntimeDlls = @('vcruntime140.dll', 'vcruntime140_1.dll', 'msvcp140.dll')
    $binTarget = Join-Path $pgsqlTarget 'bin'
    foreach ($dll in $vcRuntimeDlls) {
        $vcSource = Join-Path $env:SystemRoot "System32\$dll"
        if (-not (Test-Path $vcSource)) {
            throw "MSVC runtime $dll not found at $vcSource - install the Visual C++ Redistributable on this machine."
        }
        Copy-Item -Path $vcSource -Destination $binTarget
    }

    $extensionTarget = Join-Path $pgsqlTarget 'share\extension'
    New-Item -ItemType Directory -Force -Path $extensionTarget | Out-Null
    Copy-Item -Path (Join-Path $tsSource 'timescaledb*.dll') -Destination (Join-Path $pgsqlTarget 'lib')
    Copy-Item -Path (Join-Path $tsSource 'timescaledb.control') -Destination $extensionTarget
    Copy-Item -Path (Join-Path $tsSource 'timescaledb--*.sql') -Destination $extensionTarget

    # pg_upgrade lives in the NEW runtime, but a fixture runtime must still be able to START (the bridge
    # step runs ALTER EXTENSION on the old cluster, and the same-major test builds and measures its store
    # on the previous one) and be dumped from.
    $requiredFiles = @(
        'bin\initdb.exe',
        'bin\pg_ctl.exe',
        'bin\postgres.exe',
        'bin\libpq.dll',
        'bin\vcruntime140.dll',
        'lib\timescaledb.dll',
        "lib\timescaledb-$TsVersion.dll",
        'share\extension\timescaledb.control'
    )
    foreach ($required in $requiredFiles) {
        if (-not (Test-Path (Join-Path $pgsqlTarget $required))) {
            throw "Assembled $Name runtime is missing $required - refusing to build the fixture."
        }
    }

    $reported = (& (Join-Path $binTarget 'pg_ctl.exe') --version) -join ''
    Write-Host "  $Name runtime reports: $reported"
    return (Join-Path $root 'pg-runtime')
}

$oldRuntimeRoot = New-FixtureRuntime -Name 'old' -Description 'PREVIOUS-major' `
    -PgVersion $oldPgVersion -PgUrl $oldPgUrl -PgSha256 $oldPgSha256 `
    -TsVersion $oldTsVersion -TsUrl $oldTsUrl -TsSha256 $oldTsSha256

Write-Host ""
$previousRuntimeRoot = New-FixtureRuntime -Name 'previous' -Description 'PREVIOUS-release, current major' `
    -PgVersion $previousPgVersion -PgUrl $previousPgUrl -PgSha256 $previousPgSha256 `
    -TsVersion $previousTsVersion -TsUrl $previousTsUrl -TsSha256 $previousTsSha256

if (-not $SkipNew) {
    Write-Host ""
    Write-Host "CURRENT shipped runtime (delegating to fetch-pg-runtime.ps1) ..."
    New-Item -ItemType Directory -Force -Path $newRoot | Out-Null
    & (Join-Path $PSScriptRoot 'fetch-pg-runtime.ps1') -OutputDirectory $newRoot
}

$newZip = Join-Path $newRoot 'pg-runtime.zip'

Write-Host ""
Write-Host "Upgraded-in-place fixture ready:"
Write-Host "  old runtime      : $oldRuntimeRoot"
Write-Host "  previous runtime : $previousRuntimeRoot"
Write-Host "  new bundle       : $newZip"
Write-Host ""
Write-Host "Run the gated store-upgrade tests with:"
Write-Host "  `$env:DARLING_TEST_PGRUNTIME_OLD      = '$oldRuntimeRoot'"
Write-Host "  `$env:DARLING_TEST_PGRUNTIME_PREVIOUS = '$previousRuntimeRoot'"
Write-Host "  `$env:DARLING_TEST_PGRUNTIME_NEWZIP   = '$newZip'"
Write-Host "  dotnet test Darling\Darling.Tests\Darling.Tests.csproj --filter FullyQualifiedName~DarlingStoreUpgradeTests"

Remove-Item -Recurse -Force $extractDirectory
