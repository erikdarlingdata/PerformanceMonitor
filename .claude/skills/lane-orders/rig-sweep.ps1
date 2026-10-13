<#
.SYNOPSIS
  Stops and deletes every registered rig whose owner is gone or whose postmaster is gone, and lists running
  postgres.exe postmasters nobody registered (#5787).
.DESCRIPTION
  For each line of C:\GitHub\worktrees\.rig-registry.tsv:
    - owner is a path that no longer exists      -> stop, delete the rig, drop the line;
    - the registered postmaster PID is gone      -> stale: stop whatever is left, delete the rig, drop the line;
    - otherwise                                  -> keep.
  A line whose data directory is outside the allowed roots is refused and kept. Then every running postgres.exe
  postmaster whose data directory is not registered is listed with its PID and data directory, and left running.
  Run it at every wave boundary.
.EXAMPLE
  pwsh -NoProfile -File C:\GitHub\PerformanceMonitor\.claude\skills\lane-orders\rig-sweep.ps1
#>
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'rig-common.ps1')

$stopped = 0; $deleted = 0; $kept = 0; $refused = 0

foreach ($row in Read-RigRegistry) {
    $data = ConvertTo-RigPath $row.DataDir
    $pmPid = 0
    [void][int]::TryParse($row.Pid, [ref]$pmPid)

    # An owner is a worktree path or an agent id; only a path can be checked for existence.
    $ownerIsPath = [System.IO.Path]::IsPathRooted($row.Owner)
    $ownerGone = $ownerIsPath -and -not (Test-Path -LiteralPath $row.Owner)
    $pidGone = -not ($pmPid -gt 0 -and (Test-RigPostmaster -ProcessId $pmPid -DataDir $data))

    if (-not $ownerGone -and -not $pidGone) {
        Write-Host "keep     $data (PID $pmPid, port $($row.Port), owner $($row.Owner), started $($row.StartedUtc))"
        $kept++
        continue
    }

    $why = if ($ownerGone) { "owner '$($row.Owner)' no longer exists" } else { "postmaster PID $pmPid is gone" }
    $r = Stop-RigCluster -DataDir $data -RegisteredPid $pmPid
    if ($r.Refused) {
        Write-Host "REFUSED  $data ($why): $($r.Refused) Line kept."
        $refused++
        continue
    }
    if ($r.Stopped) { $stopped++ }
    if ($r.Deleted) { $deleted++ }
    Write-Host ("sweep    {0} ({1}): stopped {2}, deleted {3}. {4}" -f $data, $why, $r.Stopped, $r.Deleted, $r.Note).Trim()
}

# Registered data directories after the sweep, to tell which running postmasters nobody registered.
$registered = @(Read-RigRegistry | ForEach-Object { ConvertTo-RigPath $_.DataDir })
$unregistered = @(Get-PostmasterProcesses | Where-Object { -not $_.DataDir -or $registered -notcontains $_.DataDir })
foreach ($u in $unregistered) {
    $dataText = if ($u.DataDir) { $u.DataDir } else { "(no -D on the command line: $($u.CommandLine))" }
    Write-Host "UNREGISTERED  PID $($u.ProcessId)  data $dataText  ($($u.Children) child processes). Not stopped."
}

Write-Host "Summary: stopped $stopped, deleted $deleted, kept $kept, unregistered $($unregistered.Count), refused $refused."
