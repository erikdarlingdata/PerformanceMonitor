<#
.SYNOPSIS
  Starts, stops, or describes the throwaway PostgreSQL clusters the Darling.Tests live classes need (#6656).

.DESCRIPTION
  Lifted from build.yml's darling-pg job (the "Initialize and start throwaway PostgreSQL" and "Initialize and start the
  log-format PostgreSQL clusters" steps, the "Warn if a PostgreSQL worker crashed" and "Stop PostgreSQL" steps, and the
  DARLING_TEST_PG* env block of "Run Darling PG tests"), so the new PR workflow's test legs set up the same eight
  clusters the old shards did. Run from the repository root, after Darling/artifacts/pg-runtime.zip was extracted to
  Darling/artifacts/pg-runtime.

  -Mode Start   initdb + start the 5541 store and the seven log-format clusters (5542-5548), create the `darling`
                database on each, and run the 5545 workload (one real error, connection and lock-wait event in its log).
  -Mode Env     print the DARLING_TEST_PG* variables the live classes read, one NAME=VALUE per line, and nothing else.
                run-ci-leg.ps1 reads them from here so the ports live in one place.
  -Mode Stop    warn on a worker that died with an exception, then stop every cluster that has a data directory.
                Best-effort and always exits 0, so it can sit in an always() step.

  The superuser is `darling` with trust auth: acceptable only on an ephemeral, loopback-only CI runner with throwaway
  data. The worker settings on 5541 are the product's own derivation (timescaledb.max_background_workers =
  HypertableCount + 2 = 74, max_worker_processes = 3 + 74 + 8 = 85; #1888); CiClusterWorkerSizingTests keys on the
  literal `"$dataDir\postgresql.conf"` line below, so keep that variable name for the store cluster.
#>
param(
    [ValidateSet('Start', 'Env', 'Stop')][string] $Mode = 'Start',
    # The repository root (the workspace). Default: the current directory.
    [string] $Root = (Get-Location).Path,
    # Where the data directories and logs go. Default: the runner temp folder, else the system temp folder.
    [string] $TempDir = $(if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [System.IO.Path]::GetTempPath().TrimEnd('\', '/') })
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$bin = Join-Path $Root 'Darling\artifacts\pg-runtime\pgsql\bin'

if ($Mode -eq 'Env') {
    $c = { param($port) "Host=127.0.0.1;Port=$port;Username=darling;Database=darling" }
    $vars = [ordered]@{
        DARLING_TEST_PG                     = & $c 5541
        DARLING_TEST_PGRUNTIME              = (Join-Path $Root 'Darling\artifacts\pg-runtime')
        DARLING_TEST_PG_CSVLOG              = & $c 5542
        DARLING_TEST_PG_CSVLOG_AUTOEXPLAIN  = & $c 5542
        # csvlog-only from its first start, with log_timezone = 'UTC': PgLogTailStderrSiblingLiveTests' #4019 case.
        DARLING_TEST_PG_CSVLOG_ONLY         = & $c 5542
        DARLING_TEST_PG_JSONLOG             = & $c 5543
        DARLING_TEST_PG_AUTOEXPLAIN         = & $c 5544
        DARLING_TEST_PG_CSVLOG_SIBLING      = & $c 5544
        DARLING_TEST_PG_LOG_TARGET          = & $c 5545
        DARLING_TEST_PG_UTC_LOG_TARGET      = & $c 5545
        # #4704: dedicated rotation clusters (5546 stderr, 5547 csvlog, 5548 jsonlog).
        DARLING_TEST_PG_LOGROTATE           = & $c 5546
        DARLING_TEST_PG_LOGROTATE_CSV       = & $c 5547
        DARLING_TEST_PG_LOGROTATE_JSON      = & $c 5548
        # #6866: log_timezone is a named local zone (5549 Europe/Berlin on stderr, 5550 America/Los_Angeles on csvlog).
        DARLING_TEST_PG_LOGZONE_BERLIN      = & $c 5549
        DARLING_TEST_PG_LOGZONE_LA          = & $c 5550
    }
    foreach ($k in $vars.Keys) { Write-Output "$k=$($vars[$k])" }
    exit 0
}

if ($Mode -eq 'Stop') {
    # PR #5480: a TimescaleDB job worker died with 0xC0000005 in two CI runs and both read "success", because the
    # postmaster restarts and the tests retry. The crash shows only in the PostgreSQL log, so this turns it into a warning.
    $log = Join-Path $TempDir 'darling-pg.log'
    if (Test-Path $log) {
        $hits = @(Select-String -Path $log -Pattern 'terminated by exception' -SimpleMatch)
        foreach ($hit in ($hits | Select-Object -First 5)) {
            $line = $hit.Line
            if ($line.Length -gt 300) { $line = $line.Substring(0, 300) }
            Write-Host "::warning title=PostgreSQL worker terminated by exception::$line"
        }
        if ($hits.Count -gt 0) { Write-Host "$($hits.Count) 'terminated by exception' line(s) in darling-pg.log" }
    }
    # #4160: each stop is independent and best-effort: a cluster that never started has no data dir to stop, and one
    # stop failing must not skip the others' cleanup.
    if (Test-Path (Join-Path $bin 'pg_ctl.exe')) {
        foreach ($name in 'darling-pgdata', 'darling-pgdata-csvlog', 'darling-pgdata-jsonlog', 'darling-pgdata-stderrcsvlog',
                          'darling-pgdata-logtarget', 'darling-pgdata-rotate-stderr', 'darling-pgdata-rotate-csvlog',
                          'darling-pgdata-rotate-jsonlog', 'darling-pgdata-logzone-berlin', 'darling-pgdata-logzone-la') {
            $dataDir = Join-Path $TempDir $name
            if (Test-Path $dataDir) { & (Join-Path $bin 'pg_ctl.exe') -D $dataDir -m fast -w stop }
        }
    }
    exit 0
}

# ---- Start ----
if (-not (Test-Path (Join-Path $bin 'pg_ctl.exe'))) { throw "pg-runtime is not extracted: no $bin\pg_ctl.exe" }

# The store cluster (port 5541). #1888: the worker settings are not optional: PostgreSQL's default
# max_worker_processes = 8 cannot launch TimescaleDB's per-hypertable policy jobs, and the tests would run a
# configuration no customer runs.
$dataDir = Join-Path $TempDir 'darling-pgdata'
$logFile = Join-Path $TempDir 'darling-pg.log'
& "$bin\initdb.exe" -D $dataDir -U darling -A trust --encoding=UTF8
if ($LASTEXITCODE -ne 0) { throw "initdb failed ($LASTEXITCODE)" }
Add-Content -Path "$dataDir\postgresql.conf" -Value "shared_preload_libraries = 'timescaledb,pg_stat_statements'"
Add-Content -Path "$dataDir\postgresql.conf" -Value "port = 5541"
Add-Content -Path "$dataDir\postgresql.conf" -Value "listen_addresses = '127.0.0.1'"
Add-Content -Path "$dataDir\postgresql.conf" -Value "timescaledb.max_background_workers = 74"
Add-Content -Path "$dataDir\postgresql.conf" -Value "max_worker_processes = 85"
# #5549: the session id (%c) on every line, so a pooled connection reused by the next test can be told from two sessions.
Add-Content -Path "$dataDir\postgresql.conf" -Value "log_line_prefix = '%m [%p] %c '"
& "$bin\pg_ctl.exe" -D $dataDir -l $logFile -w start
if ($LASTEXITCODE -ne 0) { if (Test-Path $logFile) { Get-Content $logFile -Tail 50 }; throw "pg_ctl start failed ($LASTEXITCODE)" }
& "$bin\createdb.exe" -h 127.0.0.1 -p 5541 -U darling darling
if ($LASTEXITCODE -ne 0) { throw "createdb failed ($LASTEXITCODE)" }

# #4160: seven more small clusters, one per log-format shape, so the csvlog/jsonlog/auto_explain/rotation live tests stop
# skipping in CI. No TimescaleDB jobs run on these, so 8/16 workers is plenty.
#   5542 csvlog: PgLogEventsCsvlogLiveTests, PgDeadlocksCsvlogLiveTests, PgRaiseShapedRecordsLiveTests and
#     PgPlanCaptureCsvLiveTests (auto_explain). deadlock_timeout is short so a two-session deadlock fires inside a test's
#     patience; log_min_messages is notice because LOG ranks above ERROR in the server-log severity order and a
#     stricter filter would drop the deadlock/duration LOG rows these tests read.
#   5543 jsonlog: PgLogEventsJsonlogLiveTests (it asserts csvlog is OFF, so no csvlog sibling here).
#   5544 stderr+csvlog with the %Q prefix: PgPlanCaptureLiveTests, PgLogTailStderrSiblingLiveTests.
#   5545 stderr, %u@%d prefix, UTC: the two self-hosted log end-to-end tests; the workload below leaves one real event
#     of each family (error, connection, lock_wait) in its log before the tests read it.
#   5546 stderr, 5547 csvlog, 5548 jsonlog: the #4699 log-rotation pins. They call pg_rotate_logfile(), so each gets its
#     own cluster: a rotation on 5542/5543/5545 would move the file under the other log classes reading them (#4702).
#   5549 stderr in Europe/Berlin, 5550 csvlog in America/Los_Angeles: PgLogNamedZoneLiveTests (#6866), a log written in a
#     named local zone, whose lines Darling has to store at the true UTC instant.
# clusterDataDir, not dataDir: CiClusterWorkerSizingTests' ConfAppend regex is keyed on the literal path
# "$dataDir\postgresql.conf" the 5541 cluster's block writes, and these clusters' own worker counts (8/16, not the
# hypertable-derived 74/85) would fail that guard as drift if this function reused the same variable name.
function Start-LogFormatCluster($clusterDataDir, $logFile, $port, $extraConfLines) {
    & "$bin\initdb.exe" -D $clusterDataDir -U darling -A trust --encoding=UTF8
    if ($LASTEXITCODE -ne 0) { throw "initdb failed for port $port ($LASTEXITCODE)" }
    Add-Content -Path "$clusterDataDir\postgresql.conf" -Value "port = $port"
    Add-Content -Path "$clusterDataDir\postgresql.conf" -Value "listen_addresses = '127.0.0.1'"
    Add-Content -Path "$clusterDataDir\postgresql.conf" -Value "timescaledb.max_background_workers = 8"
    Add-Content -Path "$clusterDataDir\postgresql.conf" -Value "max_worker_processes = 16"
    Add-Content -Path "$clusterDataDir\postgresql.conf" -Value "deadlock_timeout = '100ms'"
    Add-Content -Path "$clusterDataDir\postgresql.conf" -Value "compute_query_id = on"
    Add-Content -Path "$clusterDataDir\postgresql.conf" -Value "log_min_messages = notice"
    foreach ($line in $extraConfLines) { Add-Content -Path "$clusterDataDir\postgresql.conf" -Value $line }
    & "$bin\pg_ctl.exe" -D $clusterDataDir -l $logFile -w start
    if ($LASTEXITCODE -ne 0) { if (Test-Path $logFile) { Get-Content $logFile -Tail 50 }; throw "pg_ctl start failed for port $port ($LASTEXITCODE)" }
    & "$bin\createdb.exe" -h 127.0.0.1 -p $port -U darling darling
    if ($LASTEXITCODE -ne 0) { throw "createdb failed for port $port ($LASTEXITCODE)" }
}

function Get-TempPath([string] $name) { return (Join-Path $TempDir $name) }

Start-LogFormatCluster (Get-TempPath 'darling-pgdata-csvlog') (Get-TempPath 'darling-pg-csvlog.log') 5542 @(
    "logging_collector = on",
    "log_destination = 'csvlog'",
    "log_timezone = 'UTC'"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-jsonlog') (Get-TempPath 'darling-pg-jsonlog.log') 5543 @(
    "logging_collector = on",
    "log_destination = 'jsonlog'"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-stderrcsvlog') (Get-TempPath 'darling-pg-stderrcsvlog.log') 5544 @(
    "logging_collector = on",
    "log_destination = 'stderr,csvlog'",
    "log_line_prefix = '%m [%p] %Q '"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-logtarget') (Get-TempPath 'darling-pg-logtarget.log') 5545 @(
    "logging_collector = on",
    "log_destination = 'stderr'",
    "log_timezone = 'UTC'",
    "log_line_prefix = '%m %u@%d [%p] '",
    "log_connections = on",
    "log_disconnections = on",
    "log_lock_waits = on"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-rotate-stderr') (Get-TempPath 'darling-pg-rotate-stderr.log') 5546 @(
    "logging_collector = on",
    "log_destination = 'stderr'",
    "log_timezone = 'UTC'",
    "log_line_prefix = '%m [%p] %Q '",
    "log_lock_waits = on"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-rotate-csvlog') (Get-TempPath 'darling-pg-rotate-csvlog.log') 5547 @(
    "logging_collector = on",
    "log_destination = 'csvlog'",
    "log_timezone = 'UTC'",
    "log_lock_waits = on"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-rotate-jsonlog') (Get-TempPath 'darling-pg-rotate-jsonlog.log') 5548 @(
    "logging_collector = on",
    "log_destination = 'jsonlog'",
    "log_timezone = 'UTC'",
    "log_lock_waits = on"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-logzone-berlin') (Get-TempPath 'darling-pg-logzone-berlin.log') 5549 @(
    "logging_collector = on",
    "log_destination = 'stderr'",
    "log_timezone = 'Europe/Berlin'"
)
Start-LogFormatCluster (Get-TempPath 'darling-pgdata-logzone-la') (Get-TempPath 'darling-pg-logzone-la.log') 5550 @(
    "logging_collector = on",
    "log_destination = 'csvlog'",
    "log_timezone = 'America/Los_Angeles'"
)

# The workload the first 5545 test's doc comment promises: one real event of each family (error, connection, lock_wait)
# written by the server itself before the tests read the log. Bounded and loud: a missing precondition throws rather
# than leaving the log quiet and the later assertion to fail with no explanation. psql takes libpq flags, not an
# Npgsql-style connection string.
$logTargetArgs = @("-h", "127.0.0.1", "-p", "5545", "-U", "darling", "-d", "darling")

# Connection + disconnection: an ordinary short-lived connection.
& "$bin\psql.exe" @logTargetArgs -v ON_ERROR_STOP=1 -c "SELECT 1"
if ($LASTEXITCODE -ne 0) { throw "5545 connection-event workload failed ($LASTEXITCODE)" }

# Error: a division by zero the server itself reports at ERROR.
& "$bin\psql.exe" @logTargetArgs -c "SELECT 1/0"
if ($LASTEXITCODE -eq 0) { throw "5545 error-event workload: SELECT 1/0 unexpectedly succeeded" }

# Lock wait: one background session holds LOCK TABLE inside a transaction and sleeps past deadlock_timeout (100ms) while
# a second session requests the same table and blocks, so log_lock_waits fires a "still waiting for"/"acquired" pair.
# Both are bounded jobs so a stuck lock can never hang the workflow.
& "$bin\psql.exe" @logTargetArgs -v ON_ERROR_STOP=1 -c "CREATE TABLE IF NOT EXISTS lock_target_5545 (id int)"
if ($LASTEXITCODE -ne 0) { throw "5545 lock-wait workload: table setup failed ($LASTEXITCODE)" }
$holder = Start-Job -ScriptBlock {
    param($bin, $connArgs)
    & "$bin\psql.exe" @connArgs -v ON_ERROR_STOP=1 -c "BEGIN; LOCK TABLE lock_target_5545; SELECT pg_sleep(3); COMMIT;"
} -ArgumentList $bin, $logTargetArgs
Start-Sleep -Milliseconds 500
& "$bin\psql.exe" @logTargetArgs -v ON_ERROR_STOP=1 -c "BEGIN; LOCK TABLE lock_target_5545; COMMIT;"
if ($LASTEXITCODE -ne 0) { throw "5545 lock-wait workload: waiter session failed ($LASTEXITCODE)" }
Wait-Job $holder -Timeout 15 | Out-Null
if ($holder.State -ne 'Completed') { throw "5545 lock-wait workload: holder job did not complete ($($holder.State))" }
Receive-Job $holder | Out-Null
Remove-Job $holder

# Failed login: a FATAL the target's own authentication path writes, distinct from the ERROR above. Targets a
# deliberately nonexistent database.
& "$bin\psql.exe" "-h" "127.0.0.1" "-p" "5545" "-U" "darling" "-d" "does_not_exist_5545" -c "SELECT 1" 2>$null
if ($LASTEXITCODE -eq 0) { throw "5545 failed-login workload: connecting to a nonexistent database unexpectedly succeeded" }
# the failed login is expected; don't let its exit code end the step
$global:LASTEXITCODE = 0
exit 0
