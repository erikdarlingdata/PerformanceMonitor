<#
.SYNOPSIS
  Stops a rig by its data directory, deletes the rig folder and drops its registry line (#5787).
.DESCRIPTION
  Stops with <rig>\pgsql\bin\pg_ctl.exe stop -m fast. When pg_ctl is missing or the server outlives it, stops the
  postmaster PID from <data>\postmaster.pid, but only after confirming that PID is postgres.exe and its command line
  names this data directory. Never stops anything by image name. Refuses, touching nothing, a data directory that is
  not inside a rig folder under an allowed root (see rig-common.ps1). Works on unregistered clusters too.
.EXAMPLE
  pwsh -NoProfile -File C:\GitHub\PerformanceMonitor\.claude\skills\lane-orders\rig-stop.ps1 -DataDir <RigDir>\data
#>
param(
    [Parameter(Mandatory)][string]$DataDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'rig-common.ps1')

$data = ConvertTo-RigPath $DataDir
$registered = @(Read-RigRegistry | Where-Object { [string]::Equals((ConvertTo-RigPath $_.DataDir), $data, [System.StringComparison]::OrdinalIgnoreCase) })
$registeredPid = 0
if ($registered.Count -gt 0) { [void][int]::TryParse($registered[0].Pid, [ref]$registeredPid) }

$r = Stop-RigCluster -DataDir $data -RegisteredPid $registeredPid
if ($r.Refused) {
    Write-Host "Refused: $($r.Refused)"
    exit 2
}
Write-Host ("Rig {0}: stopped {1}, deleted {2}, registry line {3}. {4}" -f $data, $r.Stopped, $r.Deleted,
    $(if ($registered.Count -gt 0) { 'dropped' } else { 'none (unregistered)' }), $r.Note).Trim()
if ($r.Note -match 'still running|failed') { exit 1 }
