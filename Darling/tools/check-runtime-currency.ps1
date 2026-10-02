<#
.SYNOPSIS
  Fails when the PostgreSQL + TimescaleDB runtime that fetch-pg-runtime.ps1 pins has fallen behind upstream
  security servicing (#3906).

.DESCRIPTION
  Run weekly by .github/workflows/runtime-currency.yml. Run it by hand any time to answer "is the bundled
  store runtime current?". Two checks:

    1. PostgreSQL. The pinned $pgVersion must be the newest minor of its major, and that major must still be
       supported, per https://www.postgresql.org/versions.json. PostgreSQL ships a minor release about once a
       quarter and nearly every one fixes CVEs, so falling behind is itself the finding. 3.8.0 shipped 18.4 a
       month after 18.6 fixed 23 CVEs in it.
    2. TimescaleDB. No published security advisory on timescale/timescaledb may cover the pinned $tsVersion.
       A newer release on its own is NOT a failure. TimescaleDB ships every few weeks, and a new runtime is a
       store upgrade on every field host (DarlingStoreUpgrade), so it moves when there is a reason, and an
       advisory is that reason. 3.8.0 shipped 2.28.1 after GHSA-hcfx-29v5-2rcw was fixed in 2.29.1.
       Only the pinned version is checked. The older builds the runtime carries (fetch-pg-runtime.ps1's
       $tsCarried) are loaded only by a store whose update to the pin failed, which raises its own alert.
       An advisory already tracked by an issue is listed in $acknowledged. It is reported as a warning
       while it still applies, and the run fails once it no longer does, so the entry gets deleted.

  The .NET runtime packed into the self-contained builds is not checked here; Dependabot's dotnet-sdk updates
  cover it (.github/dependabot.yml).

  Exits 0 when current, and 1 with one line per finding otherwise. When the network or a response's shape
  fails, the script throws, because a check that cannot answer must not read as a pass.

.PARAMETER FetchScript
  The script whose pins are checked. Defaults to fetch-pg-runtime.ps1 beside this one.

.EXAMPLE
  pwsh -File Darling\tools\check-runtime-currency.ps1
#>

#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$FetchScript = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if ([string]::IsNullOrWhiteSpace($FetchScript)) {
    $FetchScript = Join-Path $PSScriptRoot 'fetch-pg-runtime.ps1'
}

$pins = Get-Content -Raw -Path $FetchScript

# The same single-quoted-literal shape DarlingPgRuntimeVersionPinTests reads, so the two cannot disagree
# about what "the pinned version" is.
function Get-Pin([string]$Name) {
    $pattern = '(?m)^\$' + [regex]::Escape($Name) + '\s*=\s*''([^'']+)'''
    $match = [regex]::Match($pins, $pattern)
    if (-not $match.Success) {
        throw "$FetchScript no longer defines `$$Name as a single-quoted literal."
    }
    return $match.Groups[1].Value
}

# Every version in a free-text field. TimescaleDB's advisories spell their ranges in prose
# ("2.7.0 through 2.29.0", "1.x,2.x through 2.5.1") and their fixes as lists ("2.5.2, 2.6.0"). A wildcard
# component reads as zero, so "1.x" is 1.0: the start of that line.
function Get-Versions([string]$Text) {
    if ([string]::IsNullOrWhiteSpace($Text)) { return @() }
    return @([regex]::Matches($Text, '\d+(?:\.(?:\d+|x)){1,3}') | ForEach-Object { [version]($_.Value -replace 'x', '0') })
}

# Whether $Pinned already carries the fix. A list names one fix per maintained line, so the fix that
# counts is the one on the pin's own major.minor. A pin on a line with no listed fix is fixed only at or
# above the newest fix: 2.28.1 is NOT fixed by a "2.27.3, 2.28.2" pair just because it is above 2.27.3.
function Test-Fixed([version]$Pinned, [version[]]$Patched) {
    if ($Patched.Count -eq 0) { return $false }
    $sameLine = @($Patched | Where-Object { $_.Major -eq $Pinned.Major -and $_.Minor -eq $Pinned.Minor })
    if ($sameLine.Count -gt 0) {
        return $Pinned -ge ($sameLine | Sort-Object | Select-Object -First 1)
    }
    return $Pinned -ge ($Patched | Sort-Object | Select-Object -Last 1)
}

# Advisories already known to cover the pinned version, each tied to the issue that moves the pin past
# it. They are reported as warnings instead of failing the run. Otherwise the run is red for a tracked
# reason every week, and a NEW finding (the next PostgreSQL minor, a second advisory) changes nothing
# anyone can see. An entry that stops matching fails the run instead, so the list cannot outlive its
# reason: delete the entry in the change that moves the pin.
$acknowledged = @{}

$findings = [System.Collections.Generic.List[string]]::new()
$notices = [System.Collections.Generic.List[string]]::new()

# ---- PostgreSQL ------------------------------------------------------------------------------------------
$pgPinned = [version](Get-Pin 'pgVersion')
$releases = Invoke-RestMethod -Uri 'https://www.postgresql.org/versions.json'
$line = @($releases | Where-Object { [string]$_.major -eq [string]$pgPinned.Major })
if ($line.Count -ne 1) {
    throw "versions.json lists $($line.Count) entries for PostgreSQL $($pgPinned.Major); expected exactly one."
}

$pgLatest = [version]("$($line[0].major).$($line[0].latestMinor)")
Write-Host "PostgreSQL: pinned $pgPinned, newest $pgLatest, supported=$($line[0].supported)"

if ($pgPinned -lt $pgLatest) {
    $findings.Add("PostgreSQL $pgLatest is out and fetch-pg-runtime.ps1 pins $pgPinned. Move the EDB binaries zip to $pgLatest (release notes, security section first: https://www.postgresql.org/docs/release/$pgLatest/).")
}
if (-not $line[0].supported) {
    $findings.Add("PostgreSQL $($pgPinned.Major) is out of community support (EOL $($line[0].eolDate)). The bundle needs a new major, which is a pg_upgrade for every field store.")
}

# ---- TimescaleDB -----------------------------------------------------------------------------------------
$tsPinned = [version](Get-Pin 'tsVersion')
$headers = @{ Accept = 'application/vnd.github+json'; 'X-GitHub-Api-Version' = '2022-11-28' }
if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) {
    $headers.Authorization = "Bearer $($env:GITHUB_TOKEN)"
}

# Invoke-RestMethod hands a JSON array down the pipeline as ONE object, so it is enumerated explicitly;
# @() alone would wrap the whole list as a single advisory.
$advisories = @(Invoke-RestMethod -Headers $headers `
    -Uri 'https://api.github.com/repos/timescale/timescaledb/security-advisories?state=published&per_page=100' |
    ForEach-Object { $_ })
Write-Host "TimescaleDB: pinned $tsPinned, $($advisories.Count) published advisories"

$covering = [System.Collections.Generic.HashSet[string]]::new()

foreach ($advisory in $advisories) {
    $vulnerabilities = @()
    if ($advisory.PSObject.Properties['vulnerabilities']) {
        $vulnerabilities = @($advisory.vulnerabilities | Where-Object { $null -ne $_ })
    }
    if ($vulnerabilities.Count -eq 0) {
        $findings.Add("$($advisory.ghsa_id) lists no vulnerable versions, so whether it covers TimescaleDB $tsPinned cannot be judged here - read it: $($advisory.html_url)")
        continue
    }

    foreach ($vulnerability in $vulnerabilities) {
        $patched = @(Get-Versions $vulnerability.patched_versions)
        if (Test-Fixed $tsPinned $patched) { continue }

        # The range bounds count only when the text gives both ends ("A through B"). A single version is an
        # upper bound in some spelling ("< 2.30.1", "through 2.29.0"), which means affected from the start,
        # so it sets no floor - using it as one would clear every pin below the very version it names.
        $bounds = @(Get-Versions $vulnerability.vulnerable_version_range | Sort-Object)
        if ($bounds.Count -ge 2 -and ($tsPinned -lt $bounds[0] -or $tsPinned -gt $bounds[-1])) { continue }

        $fix = if ($patched.Count -gt 0) { "fixed in $($vulnerability.patched_versions)" } else { 'no fixed version published yet' }
        $message = "TimescaleDB $tsPinned is covered by $($advisory.ghsa_id) ($($advisory.severity)): $($advisory.summary) - affects $($vulnerability.vulnerable_version_range), $fix. $($advisory.html_url)"
        [void]$covering.Add([string]$advisory.ghsa_id)

        if ($acknowledged.ContainsKey([string]$advisory.ghsa_id)) {
            $notices.Add("$message Acknowledged; tracked in $($acknowledged[[string]$advisory.ghsa_id]).")
        }
        else {
            $findings.Add($message)
        }
    }
}

foreach ($id in $acknowledged.Keys) {
    if (-not $covering.Contains($id)) {
        $findings.Add("$id is acknowledged in check-runtime-currency.ps1 but no longer covers TimescaleDB $tsPinned. Delete the entry ($($acknowledged[$id])).")
    }
}

# ---- Verdict ---------------------------------------------------------------------------------------------
foreach ($notice in $notices) {
    if ($env:GITHUB_ACTIONS -eq 'true') {
        Write-Host "::warning title=Known and tracked::$notice"
    }
    else {
        Write-Host "KNOWN: $notice"
    }
}

if ($findings.Count -eq 0) {
    Write-Host "The bundled store runtime is current, apart from $($notices.Count) acknowledged finding(s)."
    exit 0
}

foreach ($finding in $findings) {
    if ($env:GITHUB_ACTIONS -eq 'true') {
        Write-Host "::error title=Bundled store runtime is behind::$finding"
    }
    else {
        Write-Host "BEHIND: $finding"
    }
}
exit 1
