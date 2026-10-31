<#
.SYNOPSIS
  Starts a throwaway PostgreSQL cluster (a rig) for live test classes and registers it (#5787).
.DESCRIPTION
  Unpacks the runtime zip into RigDir, runs initdb on RigDir\data with the settings CI's darling-pg job uses plus
  UTC time zones, starts the server on Port, and appends one line to C:\GitHub\worktrees\.rig-registry.tsv so
  rig-sweep.ps1 can stop it once its owner is gone. Stop it with rig-stop.ps1 -DataDir <RigDir>\data.
.EXAMPLE
  pwsh -NoProfile -File C:\GitHub\PerformanceMonitor\.claude\skills\lane-orders\rig-start.ps1 -Port 55611 `
    -RigDir C:\Users\edarl\AppData\Local\Temp\claude\<session>\scratchpad\fix-123-thing\rig -Owner C:\GitHub\PerformanceMonitor\.claude\worktrees\agent-x
#>
param(
    [Parameter(Mandatory)][int]$Port,
    # The rig folder: must not exist yet, or be empty, and must sit under an allowed root (see rig-common.ps1).
    [Parameter(Mandatory)][string]$RigDir,
    # The worktree path that uses the rig, or an agent id when there is none. The sweep stops the rig once a
    # worktree-path owner no longer exists.
    [Parameter(Mandatory)][string]$Owner,
    [string]$Zip = 'C:\GitHub\PerformanceMonitor\Darling\artifacts\pg-runtime.zip'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'rig-common.ps1')

$rig = ConvertTo-RigPath $RigDir
$data = Join-Path $rig 'data'
$refusal = Get-RigPathRefusal $data
if ($refusal) { throw "Refused: $refusal" }
if ((Test-Path -LiteralPath $rig) -and @(Get-ChildItem -LiteralPath $rig -Force).Count -gt 0) {
    throw "Refused: '$rig' already exists and is not empty. Pick a new rig folder, or stop the old rig with rig-stop.ps1."
}
if (-not (Test-Path -LiteralPath $Zip)) { throw "Runtime zip '$Zip' not found. Run Darling\tools\fetch-pg-runtime.ps1." }
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { throw "Port $Port is already in use." }
$taken = @(Read-RigRegistry | Where-Object { $_.Port -eq [string]$Port })
if ($taken.Count -gt 0) { throw "Port $Port is registered to '$($taken[0].DataDir)' (owner $($taken[0].Owner)). Pick another port, or run rig-sweep.ps1." }

$started = $false
try {
    New-Item -ItemType Directory -Force -Path $rig | Out-Null
    Expand-Archive -LiteralPath $Zip -DestinationPath $rig
    $bin = Join-Path $rig 'pgsql\bin'
    if (-not (Test-Path -LiteralPath (Join-Path $bin 'initdb.exe'))) { throw "The zip has no pgsql\bin\initdb.exe." }

    $initOut = & (Join-Path $bin 'initdb.exe') -D $data -U darling -A trust --encoding=UTF8 2>&1
    if ($LASTEXITCODE -ne 0) { throw "initdb failed ($LASTEXITCODE): $(($initOut | Select-Object -Last 5) -join ' ')" }

    # The darling-pg job's settings in .github/workflows/build.yml, plus UTC (CI's runners run in UTC) and no durability
    # (fsync, synchronous_commit, full_page_writes off, as .github/scripts/start-test-pg.ps1 does, #6972): a rig holds
    # throwaway test data and is commit-bound.
    Add-Content -LiteralPath (Join-Path $data 'postgresql.conf') -Encoding utf8NoBOM -Value @(
        "shared_preload_libraries = 'timescaledb,pg_stat_statements'",
        "port = $Port",
        "listen_addresses = '127.0.0.1'",
        "timescaledb.max_background_workers = 74",
        "max_worker_processes = 85",
        "fsync = off",
        "synchronous_commit = off",
        "full_page_writes = off",
        "log_line_prefix = '%m [%p] %c '",
        "timezone = 'UTC'",
        "log_timezone = 'UTC'"
    )

    # The postmaster inherits every inheritable handle of the process that starts it, including the caller's
    # stdout pipe, so a plain `pg_ctl -w start` (or Start-Process with redirection, which inherits too) holds a
    # tool call open until the server stops. Start-Process without -NoNewWindow or redirection goes through
    # ShellExecute in a hidden window, which passes no handles; pg.log carries everything pg_ctl would print.
    $log = Join-Path $rig 'pg.log'
    $pgCtl = Start-Process -FilePath (Join-Path $bin 'pg_ctl.exe') -PassThru -WindowStyle Hidden `
        -ArgumentList @('start', '-D', "`"$data`"", '-o', "`"-p $Port`"", '-w', '-t', '90', '-l', "`"$log`"")
    $null = $pgCtl.Handle
    if (-not $pgCtl.WaitForExit(120000)) { throw "pg_ctl start did not return in 120 seconds." }
    $started = $true
    if ($pgCtl.ExitCode -ne 0) { throw "pg_ctl start failed ($($pgCtl.ExitCode)): $((Get-Content -LiteralPath $log -Tail 10 -ErrorAction SilentlyContinue) -join ' ')" }
    if (-not (Select-String -LiteralPath $log -Pattern 'ready to accept connections' -Quiet)) { throw "The log at '$log' never said 'ready to accept connections'." }

    $postmaster = Get-RigPidFilePid $data
    if (-not (Test-RigPostmaster -ProcessId $postmaster -DataDir $data)) { throw "postmaster.pid PID $postmaster is not this cluster's postmaster." }

    Add-RigRegistryLine -DataDir $data -ProcessId $postmaster -Port $Port -Owner $Owner
    Write-Host "Rig started: data $data, PID $postmaster, port $Port, owner $Owner. Registered in $script:RigRegistryPath."
    Write-Host "Stop it with: pwsh -NoProfile -File $(Join-Path $PSScriptRoot 'rig-stop.ps1') -DataDir $data"
}
catch {
    # Leave nothing behind that no registry line points at.
    $failure = $_
    if ($started -or (Test-Path -LiteralPath $rig)) {
        $cleanup = Stop-RigCluster -DataDir $data
        if ($cleanup.Refused -or $cleanup.Note) { Write-Warning "Cleanup after the failed start: $($cleanup.Refused) $($cleanup.Note)" }
    }
    throw $failure
}
