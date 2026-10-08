<#
.SYNOPSIS
    Collector-write load for the retention-drain A/B (#5592): stands in for the collectors while a restored
    copy of a large store drains its retention backlog, and measures the drain's effect on those writes.

.DESCRIPTION
    Needs PowerShell 7, psql and pgbench (PostgreSQL 14 or newer client tools). Builds nothing.

    Run mode (default):
      1. Reads, from the store itself, how many rows per hour the collectors wrote into each collect.* table over
         the last 24 hours of data (rates.csv), and how many servers wrote.
      2. Picks the tables that carry -CoveragePercent of those rows and writes one pgbench script per table
         (scripts/), each issuing the collector's write shape with synthetic values.
      3. Runs pgbench in short segments at the measured rate until the drain has finished (no retention DELETE
         seen for -IdleMinutes after one was seen), -FixedDurationSeconds, or the hard cap.
      4. Samples pg_stat_activity once a second (which table the retention backend is deleting from, wait
         events, pg_stat_io and WAL counters) and the tables' n_tup_del every 2 seconds.
      5. Writes phases.csv, latency_by_script.csv and summary.md in the output folder.

    -ReportOnly <folder> rebuilds the report from an existing output folder.
    -Compare <folderA> <folderB> writes a side-by-side compare.md / compare.csv (into -OutputFolder).

    Writes only INSERT / upsert statements shaped like the collectors' (see README.md). Never drops, truncates
    or alters anything. The connection comes from the parameters only. -Password is handed to the child
    processes through PGPASSWORD for the life of the run and removed afterwards.

.EXAMPLE
    pwsh ./Invoke-DrainAbReplay.ps1 -DbHost localhost -Port 5432 -User darling -Database darling `
        -OutputFolder D:\ab\runA -IKnowThisIsACopy
#>
[CmdletBinding(DefaultParameterSetName = 'Run')]
param(
    [Parameter(ParameterSetName = 'Run', Mandatory)] [string] $DbHost,
    [Parameter(ParameterSetName = 'Run')] [int] $Port = 5432,
    [Parameter(ParameterSetName = 'Run', Mandatory)] [string] $User,
    [Parameter(ParameterSetName = 'Run', Mandatory)] [string] $Database,
    [Parameter(ParameterSetName = 'Run')] [string] $Password = '',
    [Parameter(ParameterSetName = 'Run', Mandatory)]
    [Parameter(ParameterSetName = 'Report', Mandatory)]
    [Parameter(ParameterSetName = 'Compare')] [string] $OutputFolder,

    [Parameter(ParameterSetName = 'Report', Mandatory)] [switch] $ReportOnly,
    [Parameter(ParameterSetName = 'Compare', Mandatory)] [string[]] $Compare,
    # -Compare A B: B lands here as the first free positional argument (-Compare A,B works too).
    [Parameter(ParameterSetName = 'Compare', Position = 0)] [string] $CompareB = '',

    # Safety: without this the run refuses unless every row of collect.servers has is_enabled = false.
    [Parameter(ParameterSetName = 'Run')] [switch] $IKnowThisIsACopy,

    # Rate: measured rows/hour x RateScale. 1.0 reproduces the collectors' measured write rate.
    [Parameter(ParameterSetName = 'Run')] [double] $RateScale = 1.0,
    [Parameter(ParameterSetName = 'Run')] [int] $RateWindowHours = 24,
    [Parameter(ParameterSetName = 'Run')] [int] $RateQueryTimeoutSeconds = 600,
    [Parameter(ParameterSetName = 'Run')] [double] $CoveragePercent = 90,
    [Parameter(ParameterSetName = 'Run')] [int] $MaxTables = 40,
    [Parameter(ParameterSetName = 'Run')] [switch] $AllowLowCoverage,

    # Clients: one per server that wrote in the window, capped.
    [Parameter(ParameterSetName = 'Run')] [int] $MaxClients = 32,
    [Parameter(ParameterSetName = 'Run')] [int] $Threads = 1,

    # Stop conditions.
    [Parameter(ParameterSetName = 'Run')] [double] $HardCapHours = 4,
    [Parameter(ParameterSetName = 'Run')] [double] $IdleMinutes = 5,
    [Parameter(ParameterSetName = 'Run')] [double] $NoDrainTimeoutMinutes = 60,
    [Parameter(ParameterSetName = 'Run')] [int] $FixedDurationSeconds = 0,
    [Parameter(ParameterSetName = 'Run')] [int] $SegmentSeconds = 60,

    # Statement shapes.
    [Parameter(ParameterSetName = 'Run')] [int] $MinBatchRows = 1,
    [Parameter(ParameterSetName = 'Run')] [int] $MaxBatchRows = 2000,
    [Parameter(ParameterSetName = 'Run')] [int] $QsDatabases = 3,
    [Parameter(ParameterSetName = 'Run')] [int] $DigestPool = 20000,
    [Parameter(ParameterSetName = 'Run')] [int] $NewDigestPercent = 5,
    [Parameter(ParameterSetName = 'Run')] [int] $QueryTextBytes = 800,
    [Parameter(ParameterSetName = 'Run')] [int] $PlanBytes = 16384,

    # Retention detection and phase building.
    [Parameter(ParameterSetName = 'Run')]
    [Parameter(ParameterSetName = 'Report')] [int] $PhaseGapSeconds = 120,
    # Optional: only count a DELETE as retention when its backend has this application_name ('' = any).
    [Parameter(ParameterSetName = 'Run')] [string] $RetentionAppName = '',
    [Parameter(ParameterSetName = 'Run')]
    [Parameter(ParameterSetName = 'Report')] [string] $ServiceLog = '',

    [Parameter(ParameterSetName = 'Run')] [string] $PsqlPath = 'psql',
    [Parameter(ParameterSetName = 'Run')] [string] $PgbenchPath = 'pgbench',
    [Parameter(ParameterSetName = 'Run')] [double] $LogSamplingRate = 1.0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Inv = [System.Globalization.CultureInfo]::InvariantCulture

# ======================================================================================================
# Helpers
# ======================================================================================================

function Write-Info([string]$Message) { Write-Host ("[{0:HH:mm:ss}] {1}" -f [DateTime]::UtcNow, $Message) }

function Format-Num([double]$Value, [string]$Format = 'F2') { $Value.ToString($Format, $Inv) }

function Get-Pct([double[]]$Sorted, [double]$P) {
    if ($Sorted.Length -eq 0) { return [double]::NaN }
    $i = [int][math]::Ceiling($P * $Sorted.Length) - 1
    if ($i -lt 0) { $i = 0 }
    return $Sorted[$i]
}

function Quote-Arg([string]$Value) { '"' + $Value.Replace('"', '\"') + '"' }

function Escape-SqlLiteral([string]$Value) { $Value.Replace("'", "''") }

function Convert-ToMarkdownTable($Rows, [string[]]$Columns) {
    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.AppendLine('| ' + ($Columns -join ' | ') + ' |')
    [void]$sb.AppendLine('|' + (($Columns | ForEach-Object { '---' }) -join '|') + '|')
    foreach ($row in $Rows) {
        $cells = foreach ($c in $Columns) { "$($row.$c)" }
        [void]$sb.AppendLine('| ' + ($cells -join ' | ') + ' |')
    }
    $sb.ToString()
}

# ======================================================================================================
# Report (shared by the run and -ReportOnly)
# ======================================================================================================

function Read-PgbenchLogs([string]$Folder, [double]$Offset, [double]$T0, [int[]]$Labels, [string[]]$LabelNames, [int]$ScriptCount) {
    # Returns per label: latency (us) lists overall and per script, and schedule lag (us) overall.
    $lat = @{}
    $lag = @{}
    $files = Get-ChildItem -LiteralPath $Folder -File | Where-Object { $_.Name -like 'pgbench_seg*' -and $_.Name -notlike '*.out' -and $_.Name -notlike '*.err' }
    $span = $Labels.Length
    foreach ($file in $files) {
        $reader = [System.IO.StreamReader]::new($file.FullName)
        try {
            while ($null -ne ($line = $reader.ReadLine())) {
                $f = $line.Split(' ')
                if ($f.Length -lt 6) { continue }
                $sec = [long][math]::Floor([double]::Parse($f[4], $Inv) + $Offset - $T0)
                if ($sec -lt 0 -or $sec -ge $span) { continue }
                $label = $Labels[$sec]
                $us = [double]::Parse($f[2], $Inv)
                $sn = [int]$f[3]
                $key = "$label|all"
                if (-not $lat.ContainsKey($key)) { $lat[$key] = [System.Collections.Generic.List[double]]::new() }
                $lat[$key].Add($us)
                $key2 = "$label|$sn"
                if (-not $lat.ContainsKey($key2)) { $lat[$key2] = [System.Collections.Generic.List[double]]::new() }
                $lat[$key2].Add($us)
                if ($f.Length -ge 7) {
                    if (-not $lag.ContainsKey($label)) { $lag[$label] = [System.Collections.Generic.List[double]]::new() }
                    $lag[$label].Add([double]::Parse($f[6], $Inv))
                }
            }
        }
        finally { $reader.Dispose() }
    }
    return @{ Lat = $lat; Lag = $lag }
}

function Write-Report([string]$Folder) {
    $runJson = Join-Path $Folder 'run.json'
    $samplerCsv = Join-Path $Folder 'sampler.csv'
    if (-not (Test-Path -LiteralPath $runJson) -or -not (Test-Path -LiteralPath $samplerCsv)) {
        throw "Folder '$Folder' has no run.json / sampler.csv - not a run folder."
    }
    $run = Get-Content -LiteralPath $runJson -Raw | ConvertFrom-Json
    $offset = [double]$run.clock_offset_seconds

    # ---- sampler rows -----------------------------------------------------------------------------
    $samples = [System.Collections.Generic.List[object]]::new()
    $hasIo = [bool]$run.has_pg_stat_io
    foreach ($line in [System.IO.File]::ReadLines($samplerCsv)) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $c = $line.Split(',')
        if ($c.Length -lt 17 -or $c[0] -notmatch '^\d') { continue }
        $samples.Add([pscustomobject]@{
            ts = [double]::Parse($c[0], $Inv); ret_table = $c[1]; ret_app = $c[2]; ret_wait = $c[3]
            b_active = [int]$c[4]; b_io = [int]$c[5]; b_lock = [int]$c[6]; b_lwlock = [int]$c[7]
            o_active = [int]$c[8]; o_io = [int]$c[9]; o_lock = [int]$c[10]; o_lwlock = [int]$c[11]
            wal = [double]::Parse($c[12], $Inv)
            io_reads = [double]::Parse($c[13], $Inv); io_writes = [double]::Parse($c[14], $Inv)
            io_read_ms = [double]::Parse($c[15], $Inv); io_write_ms = [double]::Parse($c[16], $Inv)
        })
    }
    if ($samples.Count -lt 2) { throw 'The sampler recorded fewer than 2 rows; nothing to report.' }
    $t0 = [math]::Floor($samples[0].ts)
    $t1 = [math]::Floor($samples[$samples.Count - 1].ts)
    $span = [int]($t1 - $t0) + 1

    # ---- phases: events, then a label per second ----------------------------------------------------
    $events = [System.Collections.Generic.List[object]]::new()
    foreach ($s in $samples) { if ($s.ret_table -ne '') { $events.Add([pscustomobject]@{ sec = [long]([math]::Floor($s.ts) - $t0); table = $s.ret_table }) } }
    $nameOf = [System.Collections.Generic.List[string]]::new()
    $nameOf.Add('no drain')
    $labels = [int[]]::new($span)   # 0 = no drain
    $phaseDefs = [System.Collections.Generic.List[object]]::new()   # label -> table, first, last
    $current = -1
    $instances = @{}
    for ($i = 0; $i -lt $events.Count; $i++) {
        $e = $events[$i]
        $prev = if ($i -gt 0) { $events[$i - 1] } else { $null }
        $joins = ($null -ne $prev) -and ($prev.table -eq $e.table) -and (($e.sec - $prev.sec) -le $PhaseGapSeconds) -and ($current -ge 0)
        if (-not $joins) {
            # a new phase instance; it also takes over a short gap after the previous phase
            $n = 1 + $(if ($instances.ContainsKey($e.table)) { $instances[$e.table] } else { 0 })
            $instances[$e.table] = $n
            $nameOf.Add($(if ($n -eq 1) { $e.table } else { "$($e.table) (#$n)" }))
            $phaseDefs.Add([pscustomobject]@{ label = $nameOf.Count - 1; table = $e.table; first = $e.sec; last = $e.sec })
            $current = $nameOf.Count - 1
        }
        else {
            $phaseDefs[$phaseDefs.Count - 1].last = $e.sec
        }
    }
    # label each second: phase instance [first, last] plus the pause after it, when the next DELETE comes
    # within the gap (a short pause between batches belongs to the table being drained).
    for ($p = 0; $p -lt $phaseDefs.Count; $p++) {
        $d = $phaseDefs[$p]
        $endSec = $d.last
        if ($p + 1 -lt $phaseDefs.Count) {
            $nextFirst = $phaseDefs[$p + 1].first
            if (($nextFirst - $d.last) -le $PhaseGapSeconds) { $endSec = $nextFirst - 1 }
        }
        $d | Add-Member -NotePropertyName end -NotePropertyValue $endSec -Force
        for ($s = [int]$d.first; $s -le [int]$endSec -and $s -lt $span; $s++) { $labels[$s] = $d.label }
    }

    # ---- n_tup_del deltas ------------------------------------------------------------------------
    $delCsv = Join-Path $Folder 'deletes.csv'
    $delSeries = @{}   # table -> list of (sec, value)
    if (Test-Path -LiteralPath $delCsv) {
        foreach ($line in [System.IO.File]::ReadLines($delCsv)) {
            $c = $line.Split(',')
            if ($c.Length -lt 3 -or $c[0] -notmatch '^\d') { continue }
            $tb = $c[1]
            if (-not $delSeries.ContainsKey($tb)) { $delSeries[$tb] = [System.Collections.Generic.List[object]]::new() }
            $delSeries[$tb].Add([pscustomobject]@{ sec = [double]::Parse($c[0], $Inv) - $t0; v = [double]::Parse($c[2], $Inv) })
        }
    }
    function Get-DelAt([string]$Table, [double]$Sec) {
        if (-not $delSeries.ContainsKey($Table)) { return $null }
        $best = $null
        foreach ($p in $delSeries[$Table]) { if ($p.sec -le $Sec) { $best = $p.v } else { break } }
        return $best
    }

    # ---- pgbench logs ---------------------------------------------------------------------------
    $scriptNames = @($run.scripts | ForEach-Object { $_.name })
    $logs = Read-PgbenchLogs -Folder $Folder -Offset $offset -T0 $t0 -Labels $labels -LabelNames $nameOf.ToArray() -ScriptCount $scriptNames.Count

    # ---- per label rollup -----------------------------------------------------------------------
    $perLabelSecs = @{}
    $phaseRows = [System.Collections.Generic.List[object]]::new()
    $scriptRows = [System.Collections.Generic.List[object]]::new()
    for ($label = 0; $label -lt $nameOf.Count; $label++) {
        $secIdx = [System.Collections.Generic.List[int]]::new()
        for ($s = 0; $s -lt $span; $s++) { if ($labels[$s] -eq $label) { $secIdx.Add($s) } }
        if ($secIdx.Count -eq 0) { continue }
        $seconds = $secIdx.Count
        $sumB = 0; $sumBio = 0; $sumBlock = 0; $sumBlw = 0
        $firstS = $null; $lastS = $null
        $perSample = @{}
        foreach ($s in $samples) {
            $sec = [int]([math]::Floor($s.ts) - $t0)
            if ($sec -ge 0 -and $sec -lt $span -and $labels[$sec] -eq $label) {
                $sumB += $s.b_active; $sumBio += $s.b_io; $sumBlock += $s.b_lock; $sumBlw += $s.b_lwlock
                if ($null -eq $firstS) { $firstS = $s }
                $lastS = $s
            }
        }
        $walMb = if ($null -ne $firstS -and $lastS.ts -gt $firstS.ts) { ($lastS.wal - $firstS.wal) / 1MB } else { 0 }
        $walRate = if ($seconds -gt 0) { $walMb / $seconds } else { 0 }
        $ioReads = if ($null -ne $firstS) { ($lastS.io_reads - $firstS.io_reads) / [math]::Max(1, $seconds) } else { 0 }
        $ioWrites = if ($null -ne $firstS) { ($lastS.io_writes - $firstS.io_writes) / [math]::Max(1, $seconds) } else { 0 }

        $rowsDeleted = $null; $rowsPerSec = $null
        $startSec = $secIdx[0]; $endSec = $secIdx[$secIdx.Count - 1]
        $tableName = ''
        if ($label -gt 0) {
            $def = $phaseDefs | Where-Object { $_.label -eq $label } | Select-Object -First 1
            $tableName = $def.table
            $before = Get-DelAt $tableName ($startSec - 1)
            $after = Get-DelAt $tableName ($endSec + 1)
            if ($null -ne $after) {
                if ($null -eq $before) { $before = 0 }
                # the first sample already counts rows deleted before this phase; with no earlier sample the
                # delta from the series' first point is the best honest number
                $firstPt = $delSeries[$tableName][0]
                if ($before -eq 0 -and $firstPt.sec -gt ($startSec - 1)) { $before = $firstPt.v }
                $rowsDeleted = [math]::Max(0, $after - $before)
                $rowsPerSec = $rowsDeleted / [math]::Max(1, $seconds)
            }
        }

        $key = "$label|all"
        $arr = [double[]]::new(0)
        if ($logs.Lat.ContainsKey($key)) { $arr = $logs.Lat[$key].ToArray() }
        [Array]::Sort($arr)
        $lagArr = [double[]]::new(0)
        if ($logs.Lag.ContainsKey($label)) { $lagArr = $logs.Lag[$label].ToArray() }
        [Array]::Sort($lagArr)
        $phaseRows.Add([pscustomobject][ordered]@{
            phase = $nameOf[$label]
            table = $tableName
            start_utc = ([DateTimeOffset]::FromUnixTimeMilliseconds([long](($t0 + $startSec) * 1000))).UtcDateTime.ToString('yyyy-MM-dd HH:mm:ss', $Inv)
            end_utc = ([DateTimeOffset]::FromUnixTimeMilliseconds([long](($t0 + $endSec + 1) * 1000))).UtcDateTime.ToString('yyyy-MM-dd HH:mm:ss', $Inv)
            seconds = $seconds
            rows_deleted = $(if ($null -ne $rowsDeleted) { [long]$rowsDeleted } else { '' })
            rows_per_sec = $(if ($null -ne $rowsPerSec) { Format-Num $rowsPerSec 'F1' } else { '' })
            tx = $arr.Length
            tx_per_sec = Format-Num ($arr.Length / [math]::Max(1, $seconds)) 'F1'
            lat_p50_ms = $(if ($arr.Length) { Format-Num ((Get-Pct $arr 0.5) / 1000) 'F1' } else { '' })
            lat_p95_ms = $(if ($arr.Length) { Format-Num ((Get-Pct $arr 0.95) / 1000) 'F1' } else { '' })
            lat_max_ms = $(if ($arr.Length) { Format-Num ($arr[$arr.Length - 1] / 1000) 'F1' } else { '' })
            lag_p95_ms = $(if ($lagArr.Length) { Format-Num ((Get-Pct $lagArr 0.95) / 1000) 'F1' } else { '' })
            lag_max_ms = $(if ($lagArr.Length) { Format-Num ($lagArr[$lagArr.Length - 1] / 1000) 'F1' } else { '' })
            active_samples = $sumB
            io_wait_share_pct = $(if ($sumB -gt 0) { Format-Num (100.0 * $sumBio / $sumB) 'F1' } else { '' })
            lock_wait_share_pct = $(if ($sumB -gt 0) { Format-Num (100.0 * $sumBlock / $sumB) 'F1' } else { '' })
            lwlock_wait_share_pct = $(if ($sumB -gt 0) { Format-Num (100.0 * $sumBlw / $sumB) 'F1' } else { '' })
            wal_mb_per_sec = Format-Num $walRate 'F2'
            store_reads_per_sec = $(if ($hasIo) { Format-Num $ioReads 'F0' } else { 'n/a' })
            store_writes_per_sec = $(if ($hasIo) { Format-Num $ioWrites 'F0' } else { 'n/a' })
        })
        for ($sn = 0; $sn -lt $scriptNames.Count; $sn++) {
            $k = "$label|$sn"
            if (-not $logs.Lat.ContainsKey($k)) { continue }
            $a = $logs.Lat[$k].ToArray(); [Array]::Sort($a)
            $scriptRows.Add([pscustomobject][ordered]@{
                phase = $nameOf[$label]; script = $scriptNames[$sn]; tx = $a.Length
                p50_ms = Format-Num ((Get-Pct $a 0.5) / 1000) 'F1'
                p95_ms = Format-Num ((Get-Pct $a 0.95) / 1000) 'F1'
                max_ms = Format-Num ($a[$a.Length - 1] / 1000) 'F1'
            })
        }
    }
    $phaseRows | Export-Csv -LiteralPath (Join-Path $Folder 'phases.csv') -NoTypeInformation -Encoding utf8
    $scriptRows | Export-Csv -LiteralPath (Join-Path $Folder 'latency_by_script.csv') -NoTypeInformation -Encoding utf8

    # ---- service retention log (optional) ---------------------------------------------------------
    $logLines = @()
    if ($ServiceLog -ne '' -and (Test-Path -LiteralPath $ServiceLog)) {
        $logLines = @(Select-String -LiteralPath $ServiceLog -Pattern 'Retention purge drained|Retention purge: \d+ table' | ForEach-Object { $_.Line.Trim() })
    }

    # ---- summary.md -------------------------------------------------------------------------------
    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.AppendLine("# Drain replay summary: $(Split-Path -Leaf $Folder)")
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine("- Run started: $($run.started_utc) UTC, ended: $($run.ended_utc) UTC, stop reason: $($run.stop_reason)")
    [void]$sb.AppendLine("- Server version: $($run.server_version); pg_stat_io: $(if ($hasIo) { 'sampled' } else { 'not available (needs PostgreSQL 16 or newer)' })")
    [void]$sb.AppendLine("- Clients: $($run.clients), rate: $(Format-Num ([double]$run.tx_per_sec) 'F2') tx/s (rate scale $($run.rate_scale)), tables in the mix: $($run.scripts.Count), measured coverage: $(Format-Num ([double]$run.coverage_percent) 'F1')% of the store's hourly rows")
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('## Per phase')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('A phase is the table the retention backend was deleting from (a pause of up to ' + $PhaseGapSeconds + ' s between batches stays in the phase). "no drain" is every second with no retention DELETE seen. Latency is the replayed write transaction (BEGIN to COMMIT), from pgbench --log.')
    [void]$sb.AppendLine('')
    $cols = 'phase', 'start_utc', 'end_utc', 'seconds', 'rows_deleted', 'rows_per_sec', 'tx_per_sec', 'lat_p50_ms', 'lat_p95_ms', 'lat_max_ms', 'lag_p95_ms', 'lag_max_ms', 'active_samples', 'io_wait_share_pct', 'lock_wait_share_pct', 'lwlock_wait_share_pct', 'wal_mb_per_sec'
    [void]$sb.Append((Convert-ToMarkdownTable $phaseRows $cols))
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('lag_* is pgbench schedule lag: how late a transaction started against its slot in the rate schedule (the closest thing to a skipped collection slot).')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('## Latency by replayed table, per phase')
    [void]$sb.AppendLine('')
    [void]$sb.Append((Convert-ToMarkdownTable $scriptRows @('phase', 'script', 'tx', 'p50_ms', 'p95_ms', 'max_ms')))
    if ($logLines.Count -gt 0) {
        [void]$sb.AppendLine('')
        [void]$sb.AppendLine('## Service retention log lines')
        [void]$sb.AppendLine('')
        foreach ($l in $logLines) { [void]$sb.AppendLine("- $l") }
    }
    Set-Content -LiteralPath (Join-Path $Folder 'summary.md') -Value $sb.ToString() -Encoding utf8
    Write-Info "Report written: $(Join-Path $Folder 'summary.md')"
    return $phaseRows
}

function Write-Compare([string]$A, [string]$B, [string]$OutDir) {
    $pa = Import-Csv -LiteralPath (Join-Path $A 'phases.csv')
    $pb = Import-Csv -LiteralPath (Join-Path $B 'phases.csv')
    $names = [System.Collections.Generic.List[string]]::new()
    foreach ($r in $pa) { if (-not $names.Contains($r.phase)) { $names.Add($r.phase) } }
    foreach ($r in $pb) { if (-not $names.Contains($r.phase)) { $names.Add($r.phase) } }
    $rows = foreach ($n in $names) {
        $ra = $pa | Where-Object { $_.phase -eq $n } | Select-Object -First 1
        $rb = $pb | Where-Object { $_.phase -eq $n } | Select-Object -First 1
        $g = { param($r, $f) if ($null -eq $r) { '' } else { $r.$f } }
        [pscustomobject][ordered]@{
            phase = $n
            A_seconds = & $g $ra 'seconds'; B_seconds = & $g $rb 'seconds'
            A_rows_per_sec = & $g $ra 'rows_per_sec'; B_rows_per_sec = & $g $rb 'rows_per_sec'
            A_p95_ms = & $g $ra 'lat_p95_ms'; B_p95_ms = & $g $rb 'lat_p95_ms'
            A_max_ms = & $g $ra 'lat_max_ms'; B_max_ms = & $g $rb 'lat_max_ms'
            A_lag_max_ms = & $g $ra 'lag_max_ms'; B_lag_max_ms = & $g $rb 'lag_max_ms'
            A_io_wait_pct = & $g $ra 'io_wait_share_pct'; B_io_wait_pct = & $g $rb 'io_wait_share_pct'
        }
    }
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
    $rows | Export-Csv -LiteralPath (Join-Path $OutDir 'compare.csv') -NoTypeInformation -Encoding utf8
    $drainA = ($pa | Where-Object { $_.phase -ne 'no drain' } | Measure-Object -Property seconds -Sum).Sum
    $drainB = ($pb | Where-Object { $_.phase -ne 'no drain' } | Measure-Object -Property seconds -Sum).Sum
    $worstA = ($pa | Where-Object { $_.lat_p95_ms -ne '' } | ForEach-Object { [double]::Parse($_.lat_p95_ms, $Inv) } | Measure-Object -Maximum).Maximum
    $worstB = ($pb | Where-Object { $_.lat_p95_ms -ne '' } | ForEach-Object { [double]::Parse($_.lat_p95_ms, $Inv) } | Measure-Object -Maximum).Maximum
    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.AppendLine('# Drain A/B compare')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine("- A = $A")
    [void]$sb.AppendLine("- B = $B")
    [void]$sb.AppendLine("- Total time in drain phases: A $drainA s, B $drainB s")
    [void]$sb.AppendLine("- Worst per-phase write latency p95: A $worstA ms, B $worstB ms")
    [void]$sb.AppendLine('')
    [void]$sb.Append((Convert-ToMarkdownTable $rows @('phase', 'A_seconds', 'B_seconds', 'A_rows_per_sec', 'B_rows_per_sec', 'A_p95_ms', 'B_p95_ms', 'A_max_ms', 'B_max_ms', 'A_lag_max_ms', 'B_lag_max_ms', 'A_io_wait_pct', 'B_io_wait_pct')))
    Set-Content -LiteralPath (Join-Path $OutDir 'compare.md') -Value $sb.ToString() -Encoding utf8
    Write-Info "Compare written: $(Join-Path $OutDir 'compare.md')"
}

if ($PSCmdlet.ParameterSetName -eq 'Report') {
    [void](Write-Report $OutputFolder)
    return
}
if ($PSCmdlet.ParameterSetName -eq 'Compare') {
    $folders = @($Compare) + $(if ($CompareB -ne '') { @($CompareB) } else { @() })
    if ($folders.Count -ne 2) { throw '-Compare takes exactly two folders: A then B.' }
    $outDir = if ($OutputFolder) { $OutputFolder } else { (Get-Location).Path }
    Write-Compare $folders[0] $folders[1] $outDir
    return
}

# ======================================================================================================
# Run mode
# ======================================================================================================

$script:Psql = $PsqlPath
$script:Pgbench = $PgbenchPath
foreach ($tool in @($script:Psql, $script:Pgbench)) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "Cannot find '$tool'. Pass -PsqlPath / -PgbenchPath." }
}
New-Item -ItemType Directory -Force -Path $OutputFolder | Out-Null
$OutputFolder = (Resolve-Path -LiteralPath $OutputFolder).Path
$scriptsDir = Join-Path $OutputFolder 'scripts'
New-Item -ItemType Directory -Force -Path $scriptsDir | Out-Null
$script:Tmp = Join-Path $OutputFolder 'tmp'
New-Item -ItemType Directory -Force -Path $script:Tmp | Out-Null

$script:ConnUri = 'postgresql://{0}@{1}:{2}/{3}?connect_timeout=10&application_name=drain-ab-replay' -f `
    [uri]::EscapeDataString($User), $DbHost, $Port, [uri]::EscapeDataString($Database)
$samplerUri = $script:ConnUri.Replace('application_name=drain-ab-replay', 'application_name=drain-ab-sampler')

function Invoke-Psql([string]$Sql, [string]$Uri = $script:ConnUri) {
    $f = Join-Path $script:Tmp ('q' + [guid]::NewGuid().ToString('N') + '.sql')
    Set-Content -LiteralPath $f -Value $Sql -Encoding utf8
    try {
        $out = & $script:Psql -X -q -A -t -F '|' -v ON_ERROR_STOP=1 -d $Uri -f $f 2>&1
        $code = $LASTEXITCODE
    }
    finally { Remove-Item -LiteralPath $f -ErrorAction SilentlyContinue }
    $lines = @($out | ForEach-Object { "$_" })
    if ($code -ne 0) { throw ("psql failed (exit $code): " + ($lines -join ' ')) }
    return ,$lines
}

$startedPids = [System.Collections.Generic.List[int]]::new()
$running = [System.Collections.Generic.List[object]]::new()
$pgbenchProc = $null
$samplerReader = $null
try {
    if ($Password -ne '') { $env:PGPASSWORD = $Password }

    # ---- connection and safety --------------------------------------------------------------------
    $ver = Invoke-Psql "SELECT current_setting('server_version_num') || '|' || version();"
    $verNum = [int]($ver[0].Split('|')[0])
    $verText = $ver[0].Split('|', 2)[1]
    $hasIo = $verNum -ge 160000
    Write-Info "Connected: $verText"
    $canSee = (Invoke-Psql "SELECT (rolsuper OR pg_has_role(current_user, 'pg_read_all_stats', 'member'))::int FROM pg_roles WHERE rolname = current_user;")[0]
    if ($canSee -ne '1') { Write-Warning 'This login cannot read other sessions'' query text (needs superuser or pg_read_all_stats / pg_monitor). The retention backend will not be seen and every phase will read "no drain".' }
    $servers = foreach ($ln in (Invoke-Psql 'SELECT server_id, server_name, is_enabled FROM collect.servers ORDER BY server_id;')) {
        $p = $ln.Split('|'); [pscustomobject]@{ id = [int]$p[0]; name = $p[1]; enabled = ($p[2] -eq 't') }
    }
    $servers = @($servers)
    if ($servers.Count -eq 0) { throw 'collect.servers is empty: there are no server ids to write rows for.' }
    $enabled = @($servers | Where-Object { $_.enabled })
    Write-Info ("collect.servers: {0} server(s), {1} enabled" -f $servers.Count, $enabled.Count)
    if ($enabled.Count -gt 0 -and -not $IKnowThisIsACopy) {
        throw "Refusing to run: $($enabled.Count) server(s) in collect.servers are enabled, so real collectors may be writing here. Point this at a restored copy with every server disabled, or pass -IKnowThisIsACopy."
    }
    if ($enabled.Count -gt 0) { Write-Warning "$($enabled.Count) enabled server(s); -IKnowThisIsACopy was passed." }

    # ---- clock offset (server minus this host) -----------------------------------------------------
    $c0 = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
    $srvNow = [double]::Parse((Invoke-Psql 'SELECT extract(epoch FROM clock_timestamp());')[0], $Inv)
    $c1 = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
    $offset = $srvNow - ($c0 + $c1) / 2
    if ([math]::Abs($offset) -lt 1.0) { $offset = 0.0 }

    # ---- step 1: the rate, from the store -------------------------------------------------------------
    $tables = Invoke-Psql @"
SELECT c.relname
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'collect'
AND   c.relkind IN ('r', 'p')
AND   NOT c.relispartition
AND   c.relname !~ '^query_store_interval_(latest|wide)'
AND   EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'collection_time' AND a.attnum > 0 AND NOT a.attisdropped AND a.atttypid = 'timestamp'::regtype)
AND   EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'server_id' AND a.attnum > 0 AND NOT a.attisdropped)
ORDER BY c.relname;
"@
    $anchorText = (Invoke-Psql "SELECT to_char(max(collection_time), 'YYYY-MM-DD HH24:MI:SS.US') FROM collect.collection_log;")[0]
    if ([string]::IsNullOrWhiteSpace($anchorText)) { throw 'collect.collection_log has no rows, so there is no last day to take a rate from.' }
    $hi = $anchorText
    $loText = (Invoke-Psql "SELECT to_char(timestamp '$hi' - interval '$RateWindowHours hours', 'YYYY-MM-DD HH24:MI:SS.US');")[0]
    $activeServers = [int](Invoke-Psql "SELECT count(DISTINCT server_id) FROM collect.collection_log WHERE collection_time > '$loText' AND collection_time <= '$hi';")[0]
    if ($activeServers -lt 1) { $activeServers = $servers.Count }
    Write-Info "Rate window: $loText to $hi (store clock), $activeServers server(s) wrote in it, $($tables.Count) candidate tables"

    $rates = [System.Collections.Generic.List[object]]::new()
    foreach ($t in $tables) {
        $rows = 0.0; $batch = 0.0; $note = ''
        try {
            $r = Invoke-Psql @"
SET statement_timeout = '${RateQueryTimeoutSeconds}s';
SELECT count(*) FROM collect.$t WHERE collection_time > '$loText' AND collection_time <= '$hi';
SELECT coalesce(count(*)::float8 / nullif(count(DISTINCT (server_id, collection_time)), 0), 0)
FROM collect.$t WHERE collection_time > timestamp '$hi' - interval '1 hour' AND collection_time <= '$hi';
"@
            $rows = [double]::Parse($r[0], $Inv); $batch = [double]::Parse($r[1], $Inv)
        }
        catch { $note = 'unmeasured: ' + $_.Exception.Message.Substring(0, [math]::Min(80, $_.Exception.Message.Length)) }
        $perHour = $rows / $RateWindowHours
        $rates.Add([pscustomobject][ordered]@{
            table = $t; rows_in_window = [long]$rows; rows_per_hour = [math]::Round($perHour, 1)
            batch_rows = [math]::Round($batch, 1); share_percent = 0.0; cumulative_percent = 0.0; in_mix = $false; note = $note })
    }
    $totalPerHour = ($rates | Measure-Object -Property rows_per_hour -Sum).Sum
    if ($totalPerHour -le 0) { throw 'No collect.* table has rows in the rate window.' }
    $cum = 0.0
    foreach ($r in ($rates | Sort-Object rows_per_hour -Descending)) {
        $r.share_percent = [math]::Round(100.0 * $r.rows_per_hour / $totalPerHour, 2)
        if ($r.rows_per_hour -gt 0) { $cum += $r.share_percent }
        $r.cumulative_percent = [math]::Round($cum, 2)
    }
    Write-Info ("Store write rate: {0:N0} rows/hour ({1:N1} rows/s) across {2} tables with rows" -f $totalPerHour, ($totalPerHour / 3600), ($rates | Where-Object { $_.rows_per_hour -gt 0 }).Count)

    # ---- step 2: pick the mix and write the scripts -----------------------------------------------------
    $selected = [System.Collections.Generic.List[object]]::new()
    $covered = 0.0
    foreach ($r in ($rates | Where-Object { $_.rows_per_hour -gt 0 } | Sort-Object rows_per_hour -Descending)) {
        if ($covered -ge $CoveragePercent -or $selected.Count -ge $MaxTables) { break }
        $selected.Add($r); $covered += $r.share_percent
    }

    $serverIdsSql = '(ARRAY[' + (($servers | ForEach-Object { $_.id }) -join ',') + ']::integer[])[:sidx + 1]'
    $serverNamesSql = '(ARRAY[' + (($servers | ForEach-Object { "'" + (Escape-SqlLiteral $_.name) + "'" }) -join ',') + ']::text[])[:sidx + 1]'
    $nowSql = "(now() AT TIME ZONE 'UTC')"

    # Catalog: columns of the selected tables (plus the dimension tables the digest shapes need).
    $want = @($selected | ForEach-Object { $_.table }) + @('query_text_dim', 'query_plan_dim')
    $inList = ($want | ForEach-Object { "'" + $_ + "'" }) -join ','
    $colLines = Invoke-Psql @"
SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull::int,
       (a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> '')::int,
       EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid AND i.indisunique AND a.attnum = ANY (i.indkey::int2[]))::int
FROM pg_attribute AS a
JOIN pg_class AS c ON c.oid = a.attrelid
WHERE c.relnamespace = 'collect'::regnamespace
AND   c.relname IN ($inList)
AND   a.attnum > 0 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum;
"@
    $colsByTable = @{}
    foreach ($l in $colLines) {
        $p = $l.Split('|')
        if (-not $colsByTable.ContainsKey($p[0])) { $colsByTable[$p[0]] = [System.Collections.Generic.List[object]]::new() }
        $colsByTable[$p[0]].Add([pscustomobject]@{ name = $p[1]; type = $p[2]; notnull = ($p[3] -eq '1'); skip = ($p[4] -eq '1'); unique = ($p[5] -eq '1') })
    }

    function Get-ValueExpr($col, [string]$table, [hashtable]$ov) {
        if ($ov.ContainsKey($col.name)) { return $ov[$col.name] }
        $type = $col.type
        switch -Regex ($col.name) {
            '^collection_time$' { return $nowSql }
            '^server_id$' { return $serverIdsSql }
            '^server_name$' { return $serverNamesSql }
            '^(collection_id|log_id)$' { return '((floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint * 1048576) + (:rid * 2048 + g))' }
        }
        switch -Regex ($type) {
            '^timestamp without time zone' { return "($nowSql - random() * interval '1 hour')" }
            '^timestamp with time zone' { return "(now() - random() * interval '1 hour')" }
            '^date' { return 'current_date' }
            '^boolean' { return '(random() < 0.5)' }
            '^smallint' { return '(random() * 100)::smallint' }
            '^integer' { if ($col.unique) { return '(random() * 2000000000)::integer' } else { return '(random() * 1000)::integer' } }
            '^bigint\[\]' { return 'ARRAY[1, 2, 3]::bigint[]' }
            '^bigint' { if ($col.unique) { return '(random() * 9000000000000000)::bigint' } else { return '(random() * 100000)::bigint' } }
            '^(double precision|real)' { return '(random() * 1000)' }
            '^numeric\((\d+),(\d+)\)' {
                $pr = [int]$Matches[1]; $sc = [int]$Matches[2]
                $mx = [math]::Min(1000.0, [math]::Pow(10, $pr - $sc) - 1)
                return "(random() * $($mx.ToString($Inv)))::numeric($pr,$sc)"
            }
            '^numeric' { return '(random() * 1000)::numeric' }
            '^(text|character varying|character)' {
                $len = if ($type -match '\((\d+)\)') { [int]$Matches[1] } else { 0 }
                $e = if ($col.unique) { 'md5(random()::text)' } else { "('v' || (random() * 100000)::int)" }
                if ($len -gt 0) { return "left($e, $len)" } else { return $e }
            }
            '^bytea' { return "sha256(convert_to(random()::text, 'UTF8'))" }
            '^jsonb' { return "'{}'::jsonb" }
            '^json' { return "'{}'::json" }
        }
        if ($col.notnull) { throw "no synthetic value for $table.$($col.name) ($type)" }
        return 'NULL'
    }

    function New-InsertSql([string]$table, [int]$batch, [hashtable]$ov) {
        $names = [System.Collections.Generic.List[string]]::new()
        $exprs = [System.Collections.Generic.List[string]]::new()
        foreach ($col in $colsByTable[$table]) {
            if ($col.skip -and -not $ov.ContainsKey($col.name)) { continue }
            $names.Add($col.name)
            $exprs.Add((Get-ValueExpr $col $table $ov))
        }
        return "INSERT INTO collect.$table`n(`n    " + ($names -join ",`n    ") + "`n)`nSELECT`n    " + ($exprs -join ",`n    ") + "`nFROM generate_series(1, $batch) AS g;"
    }

    $header = "\set sidx (:client_id + :nclients * random(0, :spc - 1)) % :nservers`n\set rid random(0, 511)`n"

    # Source of the shapes, cited in each script:
    #   Darling/PerformanceMonitor.Darling.Storage/PgCollectorRowWriter.cs:192 CopyCommandFor - every collector
    #   writes ONE binary COPY per server per collection into its raw table; pgbench cannot COPY, so the same
    #   column list is written with a plain INSERT ... SELECT generate_series (same table, same columns, same rows
    #   per batch, same transaction).
    $dimTextUpsert = @'
INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
SELECT u.digest, u.payload, {NOW}
FROM (SELECT sha256(convert_to('qt' || (:dimbase + g), 'UTF8')) AS digest, repeat('SELECT replay ', {TEXTREPEAT}) || (:dimbase + g) AS payload FROM generate_series(1, {BATCH}) AS g) AS u
WHERE NOT EXISTS (
    SELECT 1 FROM collect.query_text_dim d
    WHERE d.digest = u.digest
    AND   d.last_seen >= {NOW} - INTERVAL '1 hours')
ORDER BY u.digest
ON CONFLICT (digest) DO UPDATE SET last_seen = EXCLUDED.last_seen
WHERE collect.query_text_dim.last_seen < EXCLUDED.last_seen - INTERVAL '1 hours';
'@
    $dimPlanUpsert = @'
INSERT INTO collect.query_plan_dim (digest, query_plan_gz, last_seen)
SELECT u.digest, u.payload, {NOW}
FROM (SELECT sha256(convert_to('qp' || (:dimbase + g), 'UTF8')) AS digest, (SELECT decode(string_agg(md5((:dimbase + g)::text || i::text), ''), 'hex') FROM generate_series(1, {PLANMD5}) AS i) AS payload FROM generate_series(1, {BATCH}) AS g) AS u
WHERE NOT EXISTS (
    SELECT 1 FROM collect.query_plan_dim d
    WHERE d.digest = u.digest
    AND   d.last_seen >= {NOW} - INTERVAL '1 hours')
ORDER BY u.digest
ON CONFLICT (digest) DO UPDATE SET last_seen = EXCLUDED.last_seen
WHERE collect.query_plan_dim.last_seen < EXCLUDED.last_seen - INTERVAL '1 hours';
'@

    $qsDbArray = "ARRAY[" + ((1..$QsDatabases | ForEach-Object { "'replay_db_$_'" }) -join ',') + "]"
    $latestUpsert = @'
WITH
    batch_rows AS
(
    SELECT
        COUNT(*) AS raw_rows,
        COUNT(*) FILTER (WHERE s.execution_type_desc = 'Regular' AND s.first_execution_time IS NULL) AS null_first_execution_rows
    FROM collect.query_store_stats AS s
    WHERE s.server_id = {SID}
    AND   s.collection_time = {NOW}
    AND   s.database_name = ANY ({DBS}::text[])
),
    upserted AS
(
    INSERT INTO collect.query_store_interval_latest AS t
    (
        server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
        collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
        last_execution_time, is_forced_plan, force_failure_count, query_text
    )
    SELECT DISTINCT ON (database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time)
        s.server_id, s.database_name, s.query_id, s.plan_id, s.replica_role, s.runtime_stats_interval_id, s.first_execution_time,
        s.collection_time, s.query_plan_hash, s.query_hash, s.execution_count, s.avg_cpu_time_us, s.avg_duration_us,
        s.last_execution_time, s.is_forced_plan, s.force_failure_count, s.query_text
    FROM collect.query_store_stats AS s
    WHERE s.server_id = {SID}
    AND   s.collection_time = {NOW}
    AND   s.database_name = ANY ({DBS}::text[])
    AND   s.execution_type_desc = 'Regular'
    AND   s.first_execution_time IS NOT NULL
    ORDER BY
        database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time,
        s.execution_count DESC
    ON CONFLICT (server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time)
    DO UPDATE SET
        collection_time = EXCLUDED.collection_time,
        query_plan_hash = EXCLUDED.query_plan_hash,
        query_hash = EXCLUDED.query_hash,
        execution_count = EXCLUDED.execution_count,
        avg_cpu_time_us = EXCLUDED.avg_cpu_time_us,
        avg_duration_us = EXCLUDED.avg_duration_us,
        last_execution_time = EXCLUDED.last_execution_time,
        is_forced_plan = EXCLUDED.is_forced_plan,
        force_failure_count = EXCLUDED.force_failure_count,
        query_text = EXCLUDED.query_text
    WHERE (EXCLUDED.collection_time, EXCLUDED.execution_count) > (t.collection_time, t.execution_count)
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM upserted) AS applied_rows,
    b.raw_rows,
    b.null_first_execution_rows
FROM batch_rows AS b;
'@
    $wideCols = @('collection_time', 'server_id', 'database_name', 'query_id', 'plan_id', 'execution_type_desc', 'first_execution_time', 'last_execution_time', 'module_name', 'query_text', 'query_hash', 'execution_count', 'avg_duration_us', 'min_duration_us', 'max_duration_us', 'avg_cpu_time_us', 'min_cpu_time_us', 'max_cpu_time_us', 'avg_logical_io_reads', 'min_logical_io_reads', 'max_logical_io_reads', 'avg_logical_io_writes', 'min_logical_io_writes', 'max_logical_io_writes', 'avg_physical_io_reads', 'min_physical_io_reads', 'max_physical_io_reads', 'avg_clr_time_us', 'min_clr_time_us', 'max_clr_time_us', 'min_dop', 'max_dop', 'avg_query_max_used_memory', 'min_query_max_used_memory', 'max_query_max_used_memory', 'avg_rowcount', 'min_rowcount', 'max_rowcount', 'avg_num_physical_io_reads', 'min_num_physical_io_reads', 'max_num_physical_io_reads', 'avg_log_bytes_used', 'min_log_bytes_used', 'max_log_bytes_used', 'avg_tempdb_space_used', 'min_tempdb_space_used', 'max_tempdb_space_used', 'plan_type', 'plan_forcing_type', 'is_forced_plan', 'force_failure_count', 'last_force_failure_reason', 'compatibility_level', 'query_plan_hash', 'replica_role', 'runtime_stats_interval_id', 'interval_start_time_utc', 'interval_end_time_utc')
    $wideIdentity = @('server_id', 'database_name', 'query_id', 'plan_id', 'execution_type_desc', 'first_execution_time', 'replica_role', 'runtime_stats_interval_id')
    $wideUpsert = @"
WITH
    batch_rows AS
(
    SELECT
        COUNT(*) AS raw_rows,
        COUNT(*) FILTER (WHERE s.first_execution_time IS NULL) AS null_first_execution_rows
    FROM collect.query_store_stats AS s
    WHERE s.server_id = {SID}
    AND   s.collection_time = {NOW}
    AND   s.database_name = ANY ({DBS}::text[])
),
    upserted AS
(
    INSERT INTO collect.query_store_interval_wide AS t
    (
        $($wideCols -join ",`n        ")
    )
    SELECT DISTINCT ON (database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc)
        $(($wideCols | ForEach-Object { 's.' + $_ }) -join ",`n        ")
    FROM collect.query_store_stats AS s
    WHERE s.server_id = {SID}
    AND   s.collection_time = {NOW}
    AND   s.database_name = ANY ({DBS}::text[])
    AND   s.first_execution_time IS NOT NULL
    ORDER BY
        database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc,
        s.execution_count DESC
    ON CONFLICT (server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc)
    DO UPDATE SET
        $((($wideCols | Where-Object { $wideIdentity -notcontains $_ }) | ForEach-Object { "$_ = EXCLUDED.$_" }) -join ",`n        ")
    WHERE (EXCLUDED.collection_time, EXCLUDED.execution_count) > (t.collection_time, t.execution_count)
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM upserted) AS applied_rows,
    b.raw_rows,
    b.null_first_execution_rows
FROM batch_rows AS b;
"@

    $jobs = [System.Collections.Generic.List[object]]::new()   # name, table, txps, sql, batch
    foreach ($r in $selected) {
        $t = $r.table
        if (-not $colsByTable.ContainsKey($t)) { continue }
        $batch = [int][math]::Min($MaxBatchRows, [math]::Max($MinBatchRows, [math]::Round([double]$r.batch_rows)))
        if ($batch -lt 1) { $batch = 1 }
        $txps = $r.rows_per_hour / 3600.0 / $batch
        $ov = @{}
        $pre = ''
        $post = ''
        $comment = "-- Shape: Darling/PerformanceMonitor.Darling.Storage/PgCollectorRowWriter.cs line 192 (CopyCommandFor), one batch of $batch row(s) per server per collection, as an INSERT because pgbench cannot COPY.`n"
        switch ($t) {
            'query_store_stats' {
                # Raw rows keep the interval identity stable inside an hour so the apply statements conflict the way
                # the real open interval does. Apply statements: QueryStoreIntervalLatest.cs:239 (UpsertSql) and
                # QueryStoreIntervalWide.cs:201 (UpsertSql), run in the same transaction as the COPY
                # (DarlingCollectorRunner.cs:4684, :4692).
                $ov['database_name'] = "'replay_db_' || ((g - 1) % $QsDatabases + 1)"
                $ov['query_id'] = "((g - 1) / $QsDatabases + 1 + :sidx * 100000)::bigint"
                $ov['plan_id'] = "(((g - 1) / $QsDatabases + 1 + :sidx * 100000) * 10)::bigint"
                $ov['execution_type_desc'] = "'Regular'"
                $ov['runtime_stats_interval_id'] = '(floor(extract(epoch FROM now()) / 3600))::bigint'
                $ov['first_execution_time'] = "date_trunc('hour', $nowSql)"
                $ov['interval_start_time_utc'] = "date_trunc('hour', $nowSql)"
                $ov['interval_end_time_utc'] = "date_trunc('hour', $nowSql) + interval '1 hour'"
                $ov['last_execution_time'] = $nowSql
                $ov['execution_count'] = "(extract(epoch FROM $nowSql - date_trunc('hour', $nowSql)) * (1 + g % 7))::bigint + 1"
                $ov['replica_role'] = 'NULL'
                $ov['query_hash'] = "'0x' || md5((g % $QsDatabases)::text)"
                $ov['query_plan_hash'] = "'0x' || md5((g % 97)::text)"
                $ov['query_text'] = "left(repeat('SELECT replay ', $([math]::Max(1, [int]($QueryTextBytes / 14)))) || g, $QueryTextBytes)"
                $ov['query_plan_text'] = 'NULL'
                $ov['module_name'] = 'NULL'
                $sidExpr = $serverIdsSql
                $ins = New-InsertSql $t $batch $ov
                $apply = ($latestUpsert + "`n" + $wideUpsert).Replace('{SID}', $sidExpr).Replace('{NOW}', $nowSql).Replace('{DBS}', $qsDbArray)
                $post = "`n$apply"
                $comment += "-- Apply statements: QueryStoreIntervalLatest.cs line 239 and QueryStoreIntervalWide.cs line 201 (UpsertSql), same transaction (DarlingCollectorRunner.cs lines 4684 and 4692).`n"
                $sql = $ins
            }
            { $_ -in 'query_stats', 'procedure_stats' } {
                # Query text / plan content go to the digest dimensions once (PayloadDimensions.cs:395 UpsertSql); the raw
                # row carries only the digest (PayloadDimensions.cs:122-124). Most digests repeat, a few are new.
                $cn = @($colsByTable[$t] | ForEach-Object { $_.name })
                $ov['query_text'] = 'NULL'; $ov['query_plan_xml'] = 'NULL'
                $ov['query_text_digest'] = "sha256(convert_to('qt' || (:dimbase + g), 'UTF8'))"
                $ov['query_plan_digest'] = "sha256(convert_to('qp' || (:dimbase + g), 'UTF8'))"
                $ov['sql_handle'] = "left(md5((:dimbase + g)::text), 20)"
                $ov['plan_handle'] = "left(md5((:dimbase + g)::text), 20)"
                $ov['query_hash'] = "'0x' || left(md5((:dimbase + g)::text), 16)"
                $ov['query_plan_hash'] = "'0x' || left(md5(('p' || (:dimbase + g))::text), 16)"
                $sql = New-InsertSql $t $batch $ov
                $pre = ''
                if ($cn -contains 'query_text_digest') {
                    $pre += $dimTextUpsert.Replace('{NOW}', $nowSql).Replace('{BATCH}', "$batch").Replace('{TEXTREPEAT}', "$([math]::Max(1, [int]($QueryTextBytes / 14)))") + "`n"
                }
                if ($cn -contains 'query_plan_digest') {
                    $pre += $dimPlanUpsert.Replace('{NOW}', $nowSql).Replace('{BATCH}', "$batch").Replace('{PLANMD5}', "$([math]::Max(1, [int]($PlanBytes / 16)))") + "`n"
                }
                $comment += "-- Dimension upserts first: PayloadDimensions.cs line 395 (UpsertSql), digest columns per PayloadDimensions.cs lines 122-124.`n"
            }
            default { $sql = New-InsertSql $t $batch $ov }
        }
        $isDigest = $t -in 'query_stats', 'procedure_stats'
        $dimSet = if ($isDigest) { "\set isnew random(1, 100)`n\if :isnew <= $NewDigestPercent`n    \set dimbase random($DigestPool, 900000000)`n\else`n    \set dimbase random(1, $DigestPool)`n\endif`n" } else { '' }
        $body = $comment + $header + $dimSet + "BEGIN;`n" + $pre + $sql + $post + "`nEND;`n"
        $jobs.Add([pscustomobject]@{ name = $t; table = $t; txps = $txps; batch = $batch; body = $body; share = $r.share_percent })
    }

    # ---- validation: every script once, rolled back ----------------------------------------------------
    $nclients = [math]::Max(1, [math]::Min($MaxClients, $activeServers))
    $spc = [math]::Max(1, [int][math]::Ceiling($servers.Count / $nclients))
    # pgbench wants the client count to be a multiple of the thread count: take the largest divisor <= -Threads.
    $jobsThreads = 1
    for ($d = [math]::Min($Threads, $nclients); $d -ge 1; $d--) { if ($nclients % $d -eq 0) { $jobsThreads = $d; break } }
    $dvars = @('-D', "nclients=$nclients", '-D', "spc=$spc", '-D', "nservers=$($servers.Count)")
    $good = [System.Collections.Generic.List[object]]::new()
    foreach ($j in $jobs) {
        $valFile = Join-Path $scriptsDir ("validate_{0}.sql" -f $j.name)
        Set-Content -LiteralPath $valFile -Value ($j.body -replace '(?m)^END;\s*$', 'ROLLBACK;') -Encoding utf8
        $out = & $script:Pgbench -n -c 1 -j 1 -t 1 -f $valFile @dvars $script:ConnUri 2>&1
        $text = ($out | ForEach-Object { "$_" }) -join ' '
        if ($LASTEXITCODE -ne 0 -or $text -match 'ERROR:|pgbench: error|FATAL') {
            Write-Warning ("Dropping {0} from the mix: its write does not run against this store ({1})" -f $j.name, $text.Substring(0, [math]::Min(220, $text.Length)))
            continue
        }
        Remove-Item -LiteralPath $valFile
        Set-Content -LiteralPath (Join-Path $scriptsDir ("{0}.sql" -f $j.name)) -Value $j.body -Encoding utf8
        $good.Add($j)
    }
    if ($good.Count -eq 0) { throw 'No table in the mix validated.' }
    $coverage = ($good | Measure-Object -Property share -Sum).Sum
    Write-Info ("Mix: {0} table(s), {1:F1}% of the store's hourly rows" -f $good.Count, $coverage)
    if ($coverage -lt $CoveragePercent -and -not $AllowLowCoverage) {
        throw ("The validated mix covers only {0:F1}% of the hourly rows (wanted {1}%). See the warnings above, or pass -AllowLowCoverage." -f $coverage, $CoveragePercent)
    }
    foreach ($r in $rates) { $r.in_mix = [bool]($good | Where-Object { $_.table -eq $r.table }) }
    $rates | Sort-Object rows_per_hour -Descending | Export-Csv -LiteralPath (Join-Path $OutputFolder 'rates.csv') -NoTypeInformation -Encoding utf8

    $totalTxps = ($good | Measure-Object -Property txps -Sum).Sum * $RateScale
    $minTx = ($good | Measure-Object -Property txps -Minimum).Minimum
    $scriptArgs = @()
    $scriptMeta = @()
    $no = 0
    foreach ($j in $good) {
        $w = [int][math]::Max(1, [math]::Round(1000.0 * $j.txps / (($good | Measure-Object -Property txps -Sum).Sum)))
        $scriptArgs += @('-f', ((Quote-Arg (Join-Path $scriptsDir "$($j.name).sql")) + '@' + $w))
        $scriptMeta += [pscustomobject]@{ no = $no; name = $j.name; weight = $w; batch = $j.batch; tx_per_sec_at_scale_1 = [math]::Round($j.txps, 4) }
        $no++
    }
    Write-Host ''
    Write-Host ("Plan: {0} clients, {1:F2} tx/s (rate scale {2}), servers {3} ({4} enabled), tables:" -f $nclients, $totalTxps, $RateScale, $servers.Count, $enabled.Count)
    $scriptMeta | ForEach-Object { Write-Host ("  {0,-34} weight {1,5}  batch {2,5} rows" -f $_.name, $_.weight, $_.batch) }
    Write-Host ''

    # ---- step 4: samplers ---------------------------------------------------------------------------------
    $samplerCsv = Join-Path $OutputFolder 'sampler.csv'
    $deletesCsv = Join-Path $OutputFolder 'deletes.csv'
    foreach ($f in @($samplerCsv, $deletesCsv)) { if (Test-Path -LiteralPath $f) { Remove-Item -LiteralPath $f } }
    $appFilter = if ($RetentionAppName -ne '') { "AND a.application_name = '" + (Escape-SqlLiteral $RetentionAppName) + "'" } else { '' }
    $ioCols = if ($hasIo) { "(SELECT coalesce(sum(reads), 0) FROM pg_stat_io), (SELECT coalesce(sum(writes), 0) FROM pg_stat_io), (SELECT coalesce(sum(read_time), 0)::bigint FROM pg_stat_io), (SELECT coalesce(sum(write_time), 0)::bigint FROM pg_stat_io)" } else { '0, 0, 0, 0' }
    $isDelete = "(a.query ~* '^\s*(DELETE\s+FROM|SELECT\s+drop_chunks)' $appFilter)"
    $samplerSql = @"
\set QUIET on
\pset format unaligned
\pset tuples_only on
\pset fieldsep ','
\o '$($samplerCsv.Replace('\', '/'))'
WITH act AS
(
    SELECT a.application_name, a.wait_event_type, a.wait_event, a.query,
           $isDelete AS is_ret
    FROM pg_stat_activity AS a
    WHERE a.state = 'active' AND a.backend_type = 'client backend' AND a.pid <> pg_backend_pid()
    AND   a.application_name NOT LIKE 'drain-ab-%'
)
SELECT
    round(extract(epoch FROM clock_timestamp())::numeric, 3),
    coalesce(max(substring(query FROM '(?i)^\s*DELETE\s+FROM\s+(?:ONLY\s+)?(?:"?collect"?\.)?"?([A-Za-z0-9_]+)') ) FILTER (WHERE is_ret), max(substring(query FROM '(?i)drop_chunks\(\s*''?(?:collect\.)?([A-Za-z0-9_]+)')) FILTER (WHERE is_ret), ''),
    coalesce(max(application_name) FILTER (WHERE is_ret), ''),
    coalesce(max(coalesce(wait_event_type, 'CPU') || ':' || coalesce(wait_event, '')) FILTER (WHERE is_ret), ''),
    count(*) FILTER (WHERE application_name = 'pgbench'),
    count(*) FILTER (WHERE application_name = 'pgbench' AND wait_event_type = 'IO'),
    count(*) FILTER (WHERE application_name = 'pgbench' AND wait_event_type = 'Lock'),
    count(*) FILTER (WHERE application_name = 'pgbench' AND wait_event_type = 'LWLock'),
    count(*) FILTER (WHERE application_name <> 'pgbench' AND NOT is_ret),
    count(*) FILTER (WHERE application_name <> 'pgbench' AND NOT is_ret AND wait_event_type = 'IO'),
    count(*) FILTER (WHERE application_name <> 'pgbench' AND NOT is_ret AND wait_event_type = 'Lock'),
    count(*) FILTER (WHERE application_name <> 'pgbench' AND NOT is_ret AND wait_event_type = 'LWLock'),
    CASE WHEN pg_is_in_recovery() THEN 0 ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint END,
    $ioCols
FROM act \watch 1
"@
    $delSql = @"
\set QUIET on
\pset format unaligned
\pset tuples_only on
\pset fieldsep ','
\o '$($deletesCsv.Replace('\', '/'))'
SELECT round(extract(epoch FROM clock_timestamp())::numeric, 3), coalesce(p.relname, c.relname), sum(s.n_tup_del)
FROM pg_stat_all_tables AS s
JOIN pg_class AS c ON c.oid = s.relid
LEFT JOIN pg_inherits AS i ON i.inhrelid = c.oid
LEFT JOIN pg_class AS p ON p.oid = i.inhparent
WHERE s.n_tup_del > 0
AND   (c.relnamespace = 'collect'::regnamespace OR p.relnamespace = 'collect'::regnamespace)
GROUP BY 2 \watch 2
"@
    $samplerFile = Join-Path $OutputFolder 'sampler.sql'
    $delFile = Join-Path $OutputFolder 'deletes.sql'
    Set-Content -LiteralPath $samplerFile -Value $samplerSql -Encoding utf8
    Set-Content -LiteralPath $delFile -Value $delSql -Encoding utf8
    foreach ($pair in @(@($samplerFile, 'sampler'), @($delFile, 'deletes'))) {
        $p = Start-Process -FilePath (Get-Command $script:Psql).Source -ArgumentList @('-X', '-q', '-d', (Quote-Arg $samplerUri), '-f', (Quote-Arg $pair[0])) `
            -RedirectStandardOutput (Join-Path $OutputFolder "$($pair[1]).psql.out") -RedirectStandardError (Join-Path $OutputFolder "$($pair[1]).psql.err") -NoNewWindow -PassThru
        $running.Add($p); $startedPids.Add($p.Id)
    }
    Start-Sleep -Seconds 3
    if (-not (Test-Path -LiteralPath $samplerCsv) -or (Get-Item -LiteralPath $samplerCsv).Length -eq 0) {
        $err = Get-Content -LiteralPath (Join-Path $OutputFolder 'sampler.psql.err') -ErrorAction SilentlyContinue | Select-Object -First 3
        throw ("The sampler wrote nothing after 3 s. " + ($err -join ' '))
    }

    # ---- step 3: run pgbench in segments until a stop condition -------------------------------------------
    $started = [DateTime]::UtcNow
    $runMeta = [ordered]@{
        started_utc = $started.ToString('yyyy-MM-dd HH:mm:ss', $Inv); ended_utc = ''; stop_reason = ''
        server_version = $verText; has_pg_stat_io = $hasIo; clock_offset_seconds = $offset
        clients = $nclients; tx_per_sec = $totalTxps; rate_scale = $RateScale; coverage_percent = $coverage
        rate_window = "$loText .. $hi"; scripts = $scriptMeta
    }
    $segNo = 0
    $lastDelete = $null
    $sawDelete = $false
    $stopReason = $null
    $samplerReader = $null
    $samplerStream = $null
    $lastBeat = [DateTime]::UtcNow
    $currentTable = ''
    $totalRows = [math]::Max(1, $totalPerHour)
    while ($true) {
        $now = [DateTime]::UtcNow
        $elapsed = ($now - $started).TotalSeconds
        if ($null -eq $stopReason) {
            if ($FixedDurationSeconds -gt 0 -and $elapsed -ge $FixedDurationSeconds) { $stopReason = "fixed duration $FixedDurationSeconds s" }
            elseif ($elapsed -ge $HardCapHours * 3600) { $stopReason = "hard cap $HardCapHours h" }
            elseif ($sawDelete -and ($now - $lastDelete).TotalMinutes -ge $IdleMinutes) { $stopReason = "drain finished (no retention DELETE for $IdleMinutes min)" }
            elseif (-not $sawDelete -and $elapsed -ge $NoDrainTimeoutMinutes * 60) { $stopReason = "no retention DELETE seen within $NoDrainTimeoutMinutes min" }
            if ($null -ne $stopReason) { Write-Info "Stopping: $stopReason (waiting for the current pgbench segment to end)" }
        }
        $benchRunning = ($null -ne $pgbenchProc) -and (-not $pgbenchProc.HasExited)
        if ($null -ne $pgbenchProc -and $pgbenchProc.HasExited -and $pgbenchProc.ExitCode -ne 0) {
            $errText = (Get-Content -LiteralPath (Join-Path $OutputFolder ("pgbench_seg{0:D4}.err" -f $segNo)) -ErrorAction SilentlyContinue | Select-Object -First 2) -join ' '
            throw "pgbench exited with code $($pgbenchProc.ExitCode): $errText"
        }
        if (-not $benchRunning) {
            if ($null -ne $stopReason) { break }
            $segNo++
            $segSeconds = $SegmentSeconds
            if ($FixedDurationSeconds -gt 0) { $segSeconds = [int][math]::Max(1, [math]::Min($SegmentSeconds, $FixedDurationSeconds - $elapsed)) }
            $pbArgs = @('-n', '-c', $nclients, '-j', $jobsThreads, '-R', ($totalTxps.ToString('F4', $Inv)), '-T', $segSeconds,
                '--log', '--log-prefix', (Quote-Arg (Join-Path $OutputFolder ("pgbench_seg{0:D4}" -f $segNo))), '--sampling-rate', ($LogSamplingRate.ToString($Inv)),
                '-D', "nclients=$nclients", '-D', "spc=$spc", '-D', "nservers=$($servers.Count)") + $scriptArgs + @((Quote-Arg $script:ConnUri))
            $pgbenchProc = Start-Process -FilePath (Get-Command $script:Pgbench).Source -ArgumentList $pbArgs `
                -RedirectStandardOutput (Join-Path $OutputFolder ("pgbench_seg{0:D4}.out" -f $segNo)) -RedirectStandardError (Join-Path $OutputFolder ("pgbench_seg{0:D4}.err" -f $segNo)) -NoNewWindow -PassThru
            $running.Add($pgbenchProc); $startedPids.Add($pgbenchProc.Id)
        }
        # read new sampler lines
        if ($null -eq $samplerReader) {
            $samplerStream = [System.IO.File]::Open($samplerCsv, 'Open', 'Read', 'ReadWrite')
            $samplerReader = [System.IO.StreamReader]::new($samplerStream)
        }
        while ($null -ne ($ln = $samplerReader.ReadLine())) {
            $c = $ln.Split(',')
            if ($c.Length -ge 2 -and $c[1] -ne '') { $sawDelete = $true; $lastDelete = [DateTime]::UtcNow; $currentTable = $c[1] }
        }
        if (($now - $lastBeat).TotalSeconds -ge 30) {
            Write-Info ("elapsed {0:F0} s, segment {1}, retention: {2}" -f $elapsed, $segNo, $(if ($sawDelete -and ($now - $lastDelete).TotalSeconds -lt 20) { "deleting from $currentTable" } elseif ($sawDelete) { 'paused/finished' } else { 'not seen yet' }))
            $lastBeat = $now
        }
        Start-Sleep -Seconds 1
    }
    Start-Sleep -Seconds 3   # one more sampler tick after the last transaction
    $runMeta.ended_utc = [DateTime]::UtcNow.ToString('yyyy-MM-dd HH:mm:ss', $Inv)
    $runMeta.stop_reason = $stopReason
}
finally {
    if ($null -ne $samplerReader) { $samplerReader.Dispose() }
    # Stop only the processes this script started, by PID.
    foreach ($p in $running) {
        try { if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force } } catch { }
    }
    if ($Password -ne '') { Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue }
    if (Test-Path -LiteralPath $script:Tmp) { Remove-Item -LiteralPath $script:Tmp -Recurse -Force -ErrorAction SilentlyContinue }
}

($runMeta | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath (Join-Path $OutputFolder 'run.json') -Encoding utf8
$phases = Write-Report $OutputFolder
Write-Host ''
Write-Host ($phases | Format-Table phase, seconds, rows_per_sec, tx_per_sec, lat_p50_ms, lat_p95_ms, lat_max_ms, io_wait_share_pct -AutoSize | Out-String)
