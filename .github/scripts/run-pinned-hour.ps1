<#
.SYNOPSIS
  Runs the live Darling.Tests classes a pull request added or changed with the test clock pinned to one UTC time (#8753).

.DESCRIPTION
  Run from the repository root, after the build artifact was unpacked and start-test-pg.ps1 started the clusters, as the
  pinned-hours job of pr.yml does. It sets DARLING_TEST_CLOCK_UTC (read only by Darling/Darling.Tests/LiveClock.cs) and runs
  run-ci-leg.ps1 as one leg over the named classes: the same class listing, the same store environment and the same
  per-class checks every pull request test leg uses. The job calls it twice, once for the late-day edge (23:30 on the
  previous UTC day) and once for the midnight edge (00:30 on the current UTC day).

  A failure prints an error annotation that names the pinned time. A class that fails here and passes in the test legs
  failed because of the hour, not because of the change.

  -Classes   comma list of simple class names (ci-select.py pinned-classes writes it).
  -ClockUtc  the pinned time, ISO-8601 UTC, for example 2026-11-11T23:30:00Z.
  -Label     a short name for the pass (late, early). It keeps the two passes' result files apart.
  -DryRun    print the slice and what would run, run no test.

  Exit code: run-ci-leg.ps1's, 0 when every class passed.
#>
param(
    [Parameter(Mandatory)][string] $Classes,
    [Parameter(Mandatory)][string] $ClockUtc,
    [Parameter(Mandatory)][string] $Label,
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

[Environment]::SetEnvironmentVariable('DARLING_TEST_CLOCK_UTC', $ClockUtc)
$count = @($Classes -split ',' | Where-Object { $_ }).Count
Write-Host "Pinned-hour pass '$Label': DARLING_TEST_CLOCK_UTC=$ClockUtc, $count class(es): $Classes"
Write-Host "Every live test that seeds against LiveClock sees $ClockUtc as now. A failure below that the ordinary test legs did not show is the hour, not the change."

$leg = @{ Leg = 1; LegCount = 1; Areas = 'classes'; OnlyDarlingClasses = $Classes; NoTreeGuards = $true; ResultPrefix = "darling-pinned-$Label"; RunAttempt = $Label }
if ($DryRun) { $leg.DryRun = $true }

$global:LASTEXITCODE = 0
& (Join-Path $PSScriptRoot 'run-ci-leg.ps1') @leg
$exit = $LASTEXITCODE
if ($exit -ne 0) {
    Write-Host "::error title=Failed with the clock pinned to $ClockUtc::The classes $Classes failed with DARLING_TEST_CLOCK_UTC=$ClockUtc ('$Label' pass). If the same classes pass in the test legs at the real clock, the hour of the day is what failed, not the change. LiveClock.cs says how a live test should seed and why a pin behind the real clock can also disagree with the product's own clock."
    exit $exit
}
Write-Host "Pinned-hour pass '$Label' passed at $ClockUtc."
