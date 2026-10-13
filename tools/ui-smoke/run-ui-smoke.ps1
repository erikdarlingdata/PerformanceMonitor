<#
.SYNOPSIS
    Runs the web UI smoke check (#5701): service on a throwaway store, seed, then the browser walk. CI and local entry.

.DESCRIPTION
    1. Builds the Darling service (Release) unless -NoBuild.
    2. Writes darling.json from darling.ui-smoke.json (ports filled in) into -WorkDir and starts the service on it, with
       its output in -WorkDir. The store is a PostgreSQL the caller has started (CI does) or, with -StartPg, a throwaway
       cluster this script makes in -WorkDir from -PgBin and stops again.
    3. Waits for /api/ping and for the config seed, applies seed.sql with psql, then runs smoke.mjs.
    4. Stops what it started (the service by its own process id, the cluster with pg_ctl) and leaves logs and failure
       screenshots in -WorkDir.

    Locally:  tools/ui-smoke/run-ui-smoke.ps1 -PgBin <...>\pgsql\bin -StartPg -Channel msedge
    In CI:    the workflow starts the cluster on 5541 first, then runs this with -PgBin only.

.PARAMETER PgBin       Folder holding psql.exe (and, with -StartPg, initdb.exe and pg_ctl.exe) - the pgsql\bin of pg-runtime.
.PARAMETER PgPort      Port of the store (CI: 5541).
.PARAMETER WebPort     Port of the web host.
.PARAMETER WorkDir     Scratch folder for the config, logs, cluster and screenshots. Made if missing.
.PARAMETER StartPg     Make and start a throwaway cluster in WorkDir\pgdata, and stop it at the end.
.PARAMETER NoBuild     Skip dotnet build (the caller already built the Release service).
.PARAMETER Channel     Browser channel for playwright-core: chrome (the runner's) or msedge.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $PgBin,
    [int] $PgPort = 5541,
    [int] $WebPort = 5153,
    [string] $WorkDir = (Join-Path ([System.IO.Path]::GetTempPath()) 'darling-ui-smoke'),
    [switch] $StartPg,
    [switch] $NoBuild,
    [string] $Channel = 'chrome'
)

$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$repo = (Resolve-Path (Join-Path $here '..\..')).Path
$csproj = Join-Path $repo 'Darling\PerformanceMonitor.Darling.Service\PerformanceMonitor.Darling.Service.csproj'
$psql = Join-Path $PgBin 'psql.exe'
New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null
$serviceProc = $null
$startedPg = $false

function Invoke-Psql([string[]] $PsqlArgs) {
    & $psql -h 127.0.0.1 -p $PgPort -U darling -d darling -v ON_ERROR_STOP=1 -X -q -t -A @PsqlArgs
    if ($LASTEXITCODE -ne 0) { throw "psql failed ($LASTEXITCODE)" }
}

try {
    if ($StartPg) {
        $data = Join-Path $WorkDir 'pgdata'
        if (Test-Path $data) { Remove-Item -Recurse -Force $data }
        & (Join-Path $PgBin 'initdb.exe') -D $data -U darling -A trust --encoding=UTF8 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "initdb failed ($LASTEXITCODE)" }
        # The same numbers build.yml's darling-pg job writes (CiClusterWorkerSizingTests pins them there).
        foreach ($line in @("shared_preload_libraries = 'timescaledb,pg_stat_statements'", "port = $PgPort", "listen_addresses = '127.0.0.1'",
                "timescaledb.max_background_workers = 74", "max_worker_processes = 85", "timezone = 'UTC'", "log_timezone = 'UTC'")) {
            Add-Content -Path (Join-Path $data 'postgresql.conf') -Value $line
        }
        & (Join-Path $PgBin 'pg_ctl.exe') -D $data -l (Join-Path $WorkDir 'pg.log') -w start | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "pg_ctl start failed ($LASTEXITCODE)" }
        $startedPg = $true
        & (Join-Path $PgBin 'createdb.exe') -h 127.0.0.1 -p $PgPort -U darling darling
        if ($LASTEXITCODE -ne 0) { throw "createdb failed ($LASTEXITCODE)" }
    }

    if (-not $NoBuild) {
        & dotnet build $csproj -c Release --nologo -v q
        if ($LASTEXITCODE -ne 0) { throw "dotnet build failed ($LASTEXITCODE)" }
    }
    $outDir = Join-Path (Split-Path $csproj) 'bin\Release\net10.0'
    $dll = Join-Path $outDir 'PerformanceMonitor.Darling.Service.dll'
    if (-not (Test-Path $dll)) { throw "service build output not found: $dll" }

    # The config: ports filled in, written beside the files the service makes next to its config.
    $cfgDir = Join-Path $WorkDir 'cfg'
    New-Item -ItemType Directory -Force -Path $cfgDir | Out-Null
    $cfg = (Get-Content -Raw (Join-Path $here 'darling.ui-smoke.json')).Replace('__PG_PORT__', "$PgPort").Replace('__WEB_PORT__', "$WebPort")
    $cfgPath = Join-Path $cfgDir 'darling.json'
    Set-Content -Path $cfgPath -Value $cfg -Encoding utf8

    $env:DARLING_CONFIG = $cfgPath
    $serviceProc = Start-Process -FilePath 'dotnet' -ArgumentList @("`"$dll`"") -WorkingDirectory $outDir -PassThru `
        -RedirectStandardOutput (Join-Path $WorkDir 'service.out.log') -RedirectStandardError (Join-Path $WorkDir 'service.err.log') -WindowStyle Hidden
    Remove-Item Env:\DARLING_CONFIG

    # Up when /api/ping answers; seeded when the registry row for ui-sql exists (the service writes it after the migrations).
    $base = "http://127.0.0.1:$WebPort"
    $deadline = (Get-Date).AddSeconds(90)
    $up = $false
    while ((Get-Date) -lt $deadline) {
        if ($serviceProc.HasExited) { throw "the service exited early (code $($serviceProc.ExitCode)); see service.out.log" }
        try { if ((Invoke-WebRequest -Uri "$base/api/ping" -UseBasicParsing -TimeoutSec 3).StatusCode -eq 200) { $up = $true; break } } catch { }
        Start-Sleep -Seconds 1
    }
    if (-not $up) { throw "the service did not answer $base/api/ping in 90 s" }
    $registered = $false
    while ((Get-Date) -lt $deadline) {
        $n = & $psql -h 127.0.0.1 -p $PgPort -U darling -d darling -X -q -t -A -c "SELECT count(*) FROM config.config_monitored_servers WHERE name = 'ui-sql'" 2>$null
        if ($n -eq '1') { $registered = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $registered) { throw "the service did not register ui-sql in the store in time" }

    Invoke-Psql @('-f', (Join-Path $here 'seed.sql'))

    Push-Location $here
    try {
        & npm ci --no-audit --no-fund --ignore-scripts
        if ($LASTEXITCODE -ne 0) { throw "npm ci failed ($LASTEXITCODE)" }
        $env:UI_SMOKE_URL = $base
        $env:UI_SMOKE_OUT = Join-Path $WorkDir 'out'
        $env:UI_SMOKE_CHANNEL = $Channel
        & node smoke.mjs
        $smokeExit = $LASTEXITCODE
    }
    finally { Pop-Location }
    if ($smokeExit -ne 0) { throw "ui-smoke found problems (exit $smokeExit); screenshots and report.json are in $WorkDir\out" }
    Write-Host 'ui-smoke passed.'
}
finally {
    if ($serviceProc -and -not $serviceProc.HasExited) {
        # Only the process this script started, with its children.
        & taskkill /PID $serviceProc.Id /T /F | Out-Null
    }
    if ($startedPg) {
        & (Join-Path $PgBin 'pg_ctl.exe') -D (Join-Path $WorkDir 'pgdata') -m fast stop | Out-Null
    }
}
