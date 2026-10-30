<#
.SYNOPSIS
    Runs the web UI smoke check (#5701): service on a throwaway store, seed, then the browser walk. CI and local entry.

.DESCRIPTION
    1. Builds the Darling service (Release) unless -NoBuild.
    2. Writes darling.json from darling.ui-smoke.json (ports filled in) into -WorkDir and starts the service on it, with
       its output in -WorkDir. The store is a throwaway PostgreSQL the caller has started and made a database
       "darling" in (CI does; tools/ui-smoke/README.md says how to do it by hand).
    3. Waits for /api/ping and for the config seed, applies seed.sql with psql, then runs smoke.mjs.
    4. Stops the service it started (by its own process id) and leaves logs, failure screenshots and traces in -WorkDir.
       The caller stops the cluster.

    Locally:  start a throwaway cluster by hand (README.md), then
              tools/ui-smoke/run-ui-smoke.ps1 -PgBin <...>\pgsql\bin -PgPort <port> -Channel msedge
    In CI:    the workflow starts the cluster on 5541 first, then runs this with -PgBin only.

.PARAMETER PgBin       Folder holding psql.exe - the pgsql\bin of pg-runtime.
.PARAMETER PgPort      Port of the store (CI: 5541).
.PARAMETER WebPort     Port of the web host.
.PARAMETER WorkDir     Scratch folder for the config, logs, cluster and screenshots. Made if missing.
.PARAMETER NoBuild     Skip dotnet build (the caller already built the Release service).
.PARAMETER Channel     Browser channel for playwright-core: chrome (the runner's) or msedge.
.PARAMETER UpdateBaselines
                       Screenshot step (#5744): render the pages marked "screenshot" in pages.json and write new PNGs to
                       <WorkDir>\out\new-baselines, comparing nothing. Without it the step compares with tools/ui-smoke/baselines.
                       Baselines come from the CI image only (the workflow's update_baselines input), never from a laptop.
.PARAMETER NoShots     Skip the screenshot step (the smoke walk still runs).
.PARAMETER SeedOnly    Stop after seed.sql is applied: no npm, no browser walk. The WPF smoke check (#5745, wpf-smoke.yml) uses this to
                       get a migrated, seeded store for the Darling viewer; the service is stopped, and the caller keeps the cluster
                       and the config in -WorkDir\cfg\darling.json (the viewer reads it through DARLING_CONFIG).
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $PgBin,
    [int] $PgPort = 5541,
    [int] $WebPort = 5153,
    [string] $WorkDir = (Join-Path ([System.IO.Path]::GetTempPath()) 'darling-ui-smoke'),
    [switch] $NoBuild,
    [string] $Channel = 'chrome',
    [switch] $SeedOnly,
    [switch] $UpdateBaselines,
    [switch] $NoShots
)

$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$repo = (Resolve-Path (Join-Path $here '..\..')).Path
$csproj = Join-Path $repo 'Darling\PerformanceMonitor.Darling.Service\PerformanceMonitor.Darling.Service.csproj'
$psql = Join-Path $PgBin 'psql.exe'
New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null
$serviceProc = $null

function Invoke-Psql([string[]] $PsqlArgs) {
    & $psql -h 127.0.0.1 -p $PgPort -U darling -d darling -v ON_ERROR_STOP=1 -X -q -t -A @PsqlArgs
    if ($LASTEXITCODE -ne 0) { throw "psql failed ($LASTEXITCODE)" }
}

try {
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
    # The dashboard asks for its token on loopback too: a fresh one per run, never printed.
    $tokenBytes = [byte[]]::new(32)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($tokenBytes)
    $webToken = -join ($tokenBytes | ForEach-Object { $_.ToString('x2') })
    $cfg = (Get-Content -Raw (Join-Path $here 'darling.ui-smoke.json')).Replace('__PG_PORT__', "$PgPort").Replace('__WEB_PORT__', "$WebPort").Replace('__WEB_TOKEN__', $webToken)
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
        # The dashboard asks for its token on loopback, so a ping with no session answers 401: the service is up either way.
        try { if ((Invoke-WebRequest -Uri "$base/api/ping" -UseBasicParsing -TimeoutSec 3 -SkipHttpErrorCheck).StatusCode -in 200, 401) { $up = $true; break } } catch { }
        Start-Sleep -Seconds 1
    }
    if (-not $up) { throw "the service did not answer $base/api/ping in 90 s" }
    # Its own budget, not what the ping wait left over: the registry row is written only after the migrations have walked the
    # whole ladder against an empty store, which is the slower of the two waits. Probes at least once, and stops at once if the
    # service dies during the migrations rather than spending the budget and blaming registration.
    $registered = $false
    $regDeadline = (Get-Date).AddSeconds(120)
    do {
        if ($serviceProc.HasExited) { throw "the service exited early (code $($serviceProc.ExitCode)); see service.out.log" }
        $n = & $psql -h 127.0.0.1 -p $PgPort -U darling -d darling -X -q -t -A -c "SELECT count(*) FROM config.config_monitored_servers WHERE name = 'ui-sql'" 2>$null
        if ($n -eq '1') { $registered = $true; break }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $regDeadline)
    if (-not $registered) { throw "the service did not register ui-sql in the store in 120 s (migrations still running? see service.out.log)" }

    # One anchor for every seeded time, taken here so the screenshot step (smoke.mjs, shots.mjs) knows it: the store's clock, UTC, to the minute.
    $anchor = (Invoke-Psql @('-c', "SELECT to_char(date_trunc('minute', now() AT TIME ZONE 'UTC'), 'YYYY-MM-DD HH24:MI:SS')") | Select-Object -First 1).Trim()
    if ($anchor -notmatch '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') { throw "unexpected seed anchor: '$anchor'" }
    Invoke-Psql @('-v', "anchor=$anchor", '-f', (Join-Path $here 'seed.sql'))

    if ($SeedOnly) {
        Write-Host "store seeded; config is $cfgPath"
        return
    }

    Push-Location $here
    try {
        & npm ci --no-audit --no-fund --ignore-scripts
        if ($LASTEXITCODE -ne 0) { throw "npm ci failed ($LASTEXITCODE)" }
        # Sign in once, the way the sign-in form does, and hand the session cookie to the browser walks.
        $signIn = New-Object Microsoft.PowerShell.Commands.WebRequestSession
        $null = Invoke-WebRequest -Uri "$base/auth/login" -Method Post -Body @{ token = $webToken } -Headers @{ Origin = $base } `
            -WebSession $signIn -UseBasicParsing -TimeoutSec 30
        $sessionCookie = $signIn.Cookies.GetCookies([Uri]$base)['darling_web_session']
        if (-not $sessionCookie) { throw "signing in to the dashboard with the run's token returned no session cookie; see service.out.log" }
        $env:UI_SMOKE_SESSION_COOKIE = $sessionCookie.Value
        $env:UI_SMOKE_URL = $base
        $env:UI_SMOKE_OUT = Join-Path $WorkDir 'out'
        $env:UI_SMOKE_CHANNEL = $Channel
        $env:UI_SMOKE_ANCHOR = $anchor
        $env:UI_SMOKE_SHOTS = if ($NoShots) { 'off' } elseif ($UpdateBaselines) { 'update' } else { 'compare' }
        if (-not $NoShots) {
            # The screenshot pages use Playwright's own Chromium, pinned by the playwright-core version (the runner's Chrome updates with the image).
            & npx --no-install playwright-core install chromium
            if ($LASTEXITCODE -ne 0) { throw "playwright install chromium failed ($LASTEXITCODE)" }
        }
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
}
