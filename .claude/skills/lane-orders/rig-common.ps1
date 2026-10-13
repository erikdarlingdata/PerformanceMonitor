# Shared by rig-start.ps1, rig-stop.ps1 and rig-sweep.ps1 (#5787). Dot-source only.
#
# A rig is one throwaway PostgreSQL cluster an agent starts for live test classes:
#   <RigDir>\pgsql\   the runtime zip, extracted
#   <RigDir>\data\    the cluster (initdb)
#   <RigDir>\pg.log   the server log
# Every rig is registered in one file outside every repo, so a sweep can find the ones whose agent is gone.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# One documented location, outside every repo and never committed. Tab-separated, no header, one line per cluster:
#   data_dir <TAB> pid <TAB> port <TAB> started_utc <TAB> owner
$script:RigRegistryPath = 'C:\GitHub\worktrees\.rig-registry.tsv'

# The only places a rig data directory may live. Everything else is refused, including a registry line that points
# elsewhere: the main checkout, Program Files, ProgramData (the Darling service's own managed cluster) and the rest.
$script:RigAllowedRoots = @(
    'C:\GitHub\worktrees\',
    'C:\GitHub\PerformanceMonitor\.claude\worktrees\',
    'C:\Users\edarl\AppData\Local\Temp\claude\'
)

# Top-level entries a rig folder holds. A folder with anything else in it keeps those files: only data and pgsql go.
$script:RigFolderEntries = @('pgsql', 'data', 'pg.log')

function ConvertTo-RigPath {
    param([Parameter(Mandatory)][string]$Path)
    $full = [System.IO.Path]::GetFullPath($Path.Replace('/', '\'))
    return $full.TrimEnd('\')
}

# Returns $null when the path is an acceptable rig data directory, or the reason it is refused.
function Get-RigPathRefusal {
    param([Parameter(Mandatory)][string]$DataDir)
    $full = ConvertTo-RigPath $DataDir
    $rigFolder = Split-Path -Parent $full
    $root = $script:RigAllowedRoots | Where-Object { ($rigFolder + '\').StartsWith($_, [System.StringComparison]::OrdinalIgnoreCase) -and ($rigFolder + '\').Length -gt $_.Length } | Select-Object -First 1
    if (-not $root) {
        return "'$full' is not inside a rig folder under an allowed root ($($script:RigAllowedRoots -join ', '))."
    }
    if (Test-Path -LiteralPath (Join-Path $rigFolder '.git')) {
        return "'$rigFolder' holds a .git entry: it is a repo or worktree root, not a rig folder."
    }
    foreach ($p in @($rigFolder, $full)) {
        $item = Get-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue
        if ($item -and ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
            return "'$p' is a junction or symbolic link."
        }
    }
    return $null
}

# Reads under the registry lock too: Remove-RigRegistryLine rewrites the whole file, and a reader outside the lock
# can meet a sharing violation or a half-written file (#5787 review).
function Read-RigRegistry {
    $lines = @(Invoke-RigRegistryLocked {
        if (Test-Path -LiteralPath $script:RigRegistryPath) { Get-Content -LiteralPath $script:RigRegistryPath }
    })
    $rows = foreach ($line in $lines) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $f = $line.Split("`t")
        [pscustomobject]@{
            DataDir    = $f[0]
            Pid        = if ($f.Count -gt 1) { $f[1] } else { '' }
            Port       = if ($f.Count -gt 2) { $f[2] } else { '' }
            StartedUtc = if ($f.Count -gt 3) { $f[3] } else { '' }
            Owner      = if ($f.Count -gt 4) { $f[4] } else { '' }
            Line       = $line
        }
    }
    return @($rows)
}

# Every registry read and change runs under one machine-wide mutex: lanes start and stop rigs at the same time.
# A named mutex is owned by its thread, so a holder may nest calls (rig-sweep.ps1 holds it across one rig's
# decide-and-stop, and Stop-RigCluster takes it again to drop the line). The wait covers one rig's stop (up to
# about 90 seconds) by another caller.
function Invoke-RigRegistryLocked {
    param([Parameter(Mandatory)][scriptblock]$Body)
    $mutex = [System.Threading.Mutex]::new($false, 'Global\PerformanceMonitorRigRegistry')
    $held = $false
    try {
        try { $held = $mutex.WaitOne([TimeSpan]::FromSeconds(180)) } catch [System.Threading.AbandonedMutexException] { $held = $true }
        if (-not $held) { throw "Timed out waiting for the rig registry lock." }
        & $Body
    }
    finally {
        if ($held) { $mutex.ReleaseMutex() }
        $mutex.Dispose()
    }
}

function Add-RigRegistryLine {
    param([string]$DataDir, [int]$ProcessId, [int]$Port, [string]$Owner)
    $started = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    $line = ($DataDir, $ProcessId, $Port, $started, ($Owner -replace "[`t`r`n]", ' ')) -join "`t"
    Invoke-RigRegistryLocked {
        $dir = Split-Path -Parent $script:RigRegistryPath
        if (-not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
        Add-Content -LiteralPath $script:RigRegistryPath -Value $line -Encoding utf8NoBOM
    }
}

function Remove-RigRegistryLine {
    param([Parameter(Mandatory)][string]$DataDir)
    $target = ConvertTo-RigPath $DataDir
    Invoke-RigRegistryLocked {
        if (-not (Test-Path -LiteralPath $script:RigRegistryPath)) { return }
        $keep = @(Get-Content -LiteralPath $script:RigRegistryPath | Where-Object {
            -not [string]::IsNullOrWhiteSpace($_) -and
            -not [string]::Equals((ConvertTo-RigPath ($_.Split("`t")[0])), $target, [System.StringComparison]::OrdinalIgnoreCase)
        })
        Set-Content -LiteralPath $script:RigRegistryPath -Value $keep -Encoding utf8NoBOM
    }
}

# Every running postgres.exe postmaster (no --forkchild), with the data directory its command line names.
function Get-PostmasterProcesses {
    $all = @(Get-CimInstance Win32_Process -Filter "Name = 'postgres.exe'")
    foreach ($p in $all) {
        $cmd = [string]$p.CommandLine
        if ($cmd -match '--forkchild') { continue }
        $data = $null
        if ($cmd -match '(?:^|\s)-D\s*"([^"]+)"') { $data = $Matches[1] }
        elseif ($cmd -match '(?:^|\s)-D\s*(\S+)') { $data = $Matches[1] }
        [pscustomobject]@{
            ProcessId   = [int]$p.ProcessId
            DataDir     = if ($data) { ConvertTo-RigPath $data } else { '' }
            CommandLine = $cmd
            Children    = @($all | Where-Object { $_.ParentProcessId -eq $p.ProcessId }).Count
        }
    }
}

# True only when the PID is a live postgres.exe postmaster whose command line names this data directory.
function Test-RigPostmaster {
    param([int]$ProcessId, [string]$DataDir)
    $target = ConvertTo-RigPath $DataDir
    $proc = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if (-not $proc -or $proc.ProcessName -ne 'postgres') { return $false }
    $cim = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId"
    if (-not $cim) { return $false }
    $cmd = ([string]$cim.CommandLine).Replace('/', '\')
    return $cmd.IndexOf($target, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
}

# The postmaster PID from <data>\postmaster.pid, or 0.
function Get-RigPidFilePid {
    param([string]$DataDir)
    $pidFile = Join-Path $DataDir 'postmaster.pid'
    if (-not (Test-Path -LiteralPath $pidFile)) { return 0 }
    $first = Get-Content -LiteralPath $pidFile -TotalCount 1
    $n = 0
    if ([int]::TryParse(([string]$first).Trim(), [ref]$n)) { return $n }
    return 0
}

# Stops the cluster in DataDir (pg_ctl first, then the verified postmaster PID), deletes the rig, drops its
# registry line. Returns an object saying what it did. Refuses any path Get-RigPathRefusal rejects, touching nothing.
function Stop-RigCluster {
    param([Parameter(Mandatory)][string]$DataDir, [int]$RegisteredPid = 0)
    $data = ConvertTo-RigPath $DataDir
    $refusal = Get-RigPathRefusal $data
    if ($refusal) {
        return [pscustomobject]@{ DataDir = $data; Refused = $refusal; Stopped = $false; Deleted = $false; Note = '' }
    }
    $rigFolder = Split-Path -Parent $data
    $result = [pscustomobject]@{ DataDir = $data; Refused = $null; Stopped = $false; Deleted = $false; Note = '' }

    if ((Test-Path -LiteralPath $data) -and -not (Test-Path -LiteralPath (Join-Path $data 'PG_VERSION'))) {
        $result.Refused = "'$data' exists but holds no PG_VERSION: it is not a PostgreSQL data directory."
        return $result
    }

    # The postmaster to stop: postmaster.pid first, the registered PID second; either must prove itself.
    $postmaster = 0
    foreach ($candidate in @((Get-RigPidFilePid $data), $RegisteredPid)) {
        if ($candidate -gt 0 -and (Test-RigPostmaster -ProcessId $candidate -DataDir $data)) { $postmaster = $candidate; break }
    }

    if ($postmaster -eq 0) {
        # A live postgres at the postmaster.pid PID that does not name this data directory: deleting under a running
        # server would tear files out from under it, so keep everything and say so.
        $filePid = Get-RigPidFilePid $data
        $proc = if ($filePid -gt 0) { Get-Process -Id $filePid -ErrorAction SilentlyContinue } else { $null }
        if ($proc -and $proc.ProcessName -eq 'postgres') {
            $result.Refused = "postmaster.pid names live postgres PID $filePid, whose command line does not name '$data'; nothing touched."
            return $result
        }
    }

    if ($postmaster -gt 0) {
        $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId = $postmaster AND Name = 'postgres.exe'" | ForEach-Object { [int]$_.ProcessId })
        $pgCtl = Join-Path $rigFolder 'pgsql\bin\pg_ctl.exe'
        if (Test-Path -LiteralPath $pgCtl) {
            $out = & $pgCtl stop -D $data -m fast -w -t 60 2>&1
            if ($LASTEXITCODE -ne 0) { $result.Note += "pg_ctl stop exited $LASTEXITCODE ($(($out | Out-String).Trim())); "  }
        }
        else {
            $result.Note += 'pg_ctl.exe missing; stopped the verified postmaster PID; '
        }
        if (Get-Process -Id $postmaster -ErrorAction SilentlyContinue) {
            # Re-verify right before the kill: never stop a PID that is not this cluster's postmaster.
            if (Test-RigPostmaster -ProcessId $postmaster -DataDir $data) {
                Stop-Process -Id $postmaster -Force
            }
        }
        # Backends exit on their own once the postmaster is gone; give them time, then stop the stragglers by PID.
        $deadline = [DateTime]::UtcNow.AddSeconds(20)
        while ([DateTime]::UtcNow -lt $deadline) {
            $alive = @($children | Where-Object { Get-Process -Id $_ -ErrorAction SilentlyContinue })
            if ($alive.Count -eq 0 -and -not (Get-Process -Id $postmaster -ErrorAction SilentlyContinue)) { break }
            Start-Sleep -Milliseconds 500
        }
        foreach ($c in $children) {
            $proc = Get-Process -Id $c -ErrorAction SilentlyContinue
            $cim = Get-CimInstance Win32_Process -Filter "ProcessId = $c"
            if ($proc -and $proc.ProcessName -eq 'postgres' -and $cim -and $cim.ParentProcessId -eq $postmaster) {
                Stop-Process -Id $c -Force
            }
        }
        if (Get-Process -Id $postmaster -ErrorAction SilentlyContinue) {
            $result.Note += "postmaster $postmaster still running; folder kept; "
            return $result
        }
        $result.Stopped = $true
    }

    # Delete: the whole rig folder when it holds only rig entries; otherwise data and pgsql only.
    if (Test-Path -LiteralPath $rigFolder) {
        $extra = @(Get-ChildItem -LiteralPath $rigFolder -Force | Where-Object { $script:RigFolderEntries -notcontains $_.Name })
        $targets = if ($extra.Count -eq 0) { @($rigFolder) } else { @($data, (Join-Path $rigFolder 'pgsql')) | Where-Object { Test-Path -LiteralPath $_ } }
        if ($extra.Count -gt 0) { $result.Note += "rig folder holds other files ($(($extra.Name | Select-Object -First 3) -join ', ')); removed data and pgsql only; " }
        foreach ($t in $targets) {
            for ($attempt = 1; $attempt -le 5; $attempt++) {
                try { Remove-Item -LiteralPath $t -Recurse -Force; break }
                catch { if ($attempt -eq 5) { $result.Note += "delete of '$t' failed: $($_.Exception.Message); "; return $result } Start-Sleep -Seconds 2 }
            }
        }
        $result.Deleted = $true
    }

    Remove-RigRegistryLine $data
    return $result
}
