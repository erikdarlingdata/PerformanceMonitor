<#
.SYNOPSIS
  Runs one shard of the Darling.Tests suite: cuts the class list by hash, chunks it under the command-line
  budget, and runs the test host once per chunk (#5616).

.DESCRIPTION
  The ONE copy of the Darling shard logic. build.yml's darling-pg job (every pull request and push) and
  nightly.yml's darling-pg job both call it, so the two cut the suite the same way. What differs between the
  callers is passed in; the env blocks, the cluster set-up, the build fetching and the path gates stay in each
  workflow. Run from the repository root, after Darling.Tests was built in Release.

  -Shard / -ShardCount   this leg's index and the matrix's leg count (strategy.job-total).
  -EventName             the workflow event. Only 'pull_request' leaves out Cost=Slow classes (#5459 change 5);
                         the SLOW_PR, SLOW_REPO and SLOW_BASE_REF environment variables carry the rest of that lookup
                         (a pull request into main, SLOW_BASE_REF=main, skips nothing).
  -GuardBuildUsed        'true' only when this leg used the Guard job's build, which proves that job ran the
                         Stage=Guard classes; the listing then leaves them out so each class still runs once.
  -ResultPrefix          the .trx file name prefix: TestResults/<prefix>-<shard>-<chunk>.trx.
  -SelectionFile         the test map selection the pull request's test-map-select job wrote (#5459); empty or
                         missing: no selection. A usable one replaces the Cost=Slow skip and keeps only the
                         classes it names; FULL, unreadable or empty means every class this leg was cut runs.
  -ScopeFilter           extra runner arguments for the class listing, for a caller that runs part of the suite
                         (the build job's Lite-reads pass: '-trait Stage=Guard -trait Reads=Lite'). Empty: all.
  -RunAttempt            the workflow's run attempt, in the timing file name.
#>
param(
    [Parameter(Mandatory)][int] $Shard,
    [Parameter(Mandatory)][int] $ShardCount,
    [string] $EventName = '',
    [string] $GuardBuildUsed = '',
    [string] $ResultPrefix = 'darling-pr',
    [string] $RunAttempt = '1',
    [string] $SelectionFile = '',
    [string] $ScopeFilter = ''
)

$ErrorActionPreference = 'Stop'
# The cut and the run must agree (#5459): the listing every shard cuts from leaves the Guard classes out exactly
# when the Guard job ran them, so each remaining class still lands in exactly one shard. No artifact: all classes.
$guardFilter = @(if ($GuardBuildUsed -eq 'true') { '-trait-'; 'Stage=Guard' })
$scopeFilter = @($ScopeFilter -split ' ' | Where-Object { $_ })
$lines = @(dotnet run --project Darling/Darling.Tests/Darling.Tests.csproj -c Release --no-build -- -list classes/json @guardFilter @scopeFilter)
if ($LASTEXITCODE -ne 0) { throw "class listing failed with exit code $LASTEXITCODE" }
# Only the runner's one-line JSON array is parsed: the dotnet CLI can print its own notice on stdout,
# and a line starting "W" after the array failed a shard's ConvertFrom-Json on 2026-09-23.
$json = @($lines | Where-Object { $_ -match '^\s*\[' })
if ($json.Count -ne 1) { throw "expected one JSON array line from the class listing, got $($json.Count): $($lines -join ' | ')" }
$classes = @($json[0] | ConvertFrom-Json)
if ($classes.Count -eq 0) { throw 'the class listing returned no classes - the shard would silently run nothing' }

$sha = [System.Security.Cryptography.SHA256]::Create()
$mine = @($classes | Where-Object {
    ([System.BitConverter]::ToUInt32($sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($_)), 0) % $ShardCount) -eq $Shard
})
Write-Host "shard ${Shard}: $($mine.Count) of $($classes.Count) classes"
if ($mine.Count -eq 0) { throw "shard ${Shard} selected zero of $($classes.Count) classes - refusing to run, because a runner with no -class arguments runs the whole suite" }

# #5459 live selection: a pull request run whose gate pinned a test map keeps only the classes the map picked for its
# diff (the test-map-select job's selection.json, downloaded by the workflow into -SelectionFile). It is applied AFTER
# the cut and the zero check above, so every shard still agrees on which shard owns which class. `map-keep` answers
# FULL (any reason: the selection is FULL, missing, unreadable or empty) or SELECTED plus the names; anything but a
# clean SELECTED answer, an unreadable answer included, leaves $selected null and this shard runs everything it was
# cut. A pull request with a usable selection does not also get the Cost=Slow skip below: the selection replaces it.
$selected = $null
if ($EventName -eq 'pull_request' -and $SelectionFile -and (Test-Path -LiteralPath $SelectionFile)) {
    $keep = @(python .github/scripts/ci-select.py map-keep --selection $SelectionFile --suite darling | Where-Object { $_ })
    if ($LASTEXITCODE -eq 0 -and $keep.Count -gt 1 -and $keep[0] -match '^SELECTED \d+$') {
        $selected = [System.Collections.Generic.HashSet[string]]::new([string[]]@($keep | Select-Object -Skip 1), [System.StringComparer]::Ordinal)
        Write-Host "shard ${Shard}: the test map selected $($selected.Count) darling classes for this pull request"
    }
    else {
        Write-Host "shard ${Shard}: no usable test map selection ($($keep -join ' ')), so this shard runs every class it was cut"
    }
}
if ($null -ne $selected) {
    $kept = @($mine | Where-Object { $selected.Contains(($_ -split '\.')[-1]) })
    Write-Host "shard ${Shard}: $($mine.Count - $kept.Count) classes outside the test map selection left out, $($kept.Count) to run"
    $mine = $kept
    if ($mine.Count -eq 0) { Write-Host "shard ${Shard}: no class of this shard is in the test map selection, so nothing runs"; exit 0 }
}
elseif ($EventName -eq 'pull_request') {
    # #5459 change 5: a pull request without a usable selection leaves out the Cost=Slow classes its change does
    # not reach (the rule is ci-select.py's slow_skip; a push, a merge-queue run, the nightly and a release run
    # every class). A shard that cannot list the pull request's files skips nothing. Like the selection above it
    # happens AFTER the cut and the zero check, and an emptied shard ends here rather than reaching a runner with
    # no -class arguments.
    $skip = @(python .github/scripts/ci-select.py slow-skip --suite darling --event $EventName "--base-ref=$env:SLOW_BASE_REF" --repo $env:SLOW_REPO --pr $env:SLOW_PR | Where-Object { $_ })
    $kept = @($mine | Where-Object { $skip -notcontains ($_ -split '\.')[-1] })
    Write-Host "shard ${Shard}: $($mine.Count - $kept.Count) Cost=Slow classes left out on this pull request, $($kept.Count) to run"
    $mine = $kept
    if ($mine.Count -eq 0) { Write-Host "shard ${Shard}: every class of this shard is a skipped Cost=Slow class, so nothing runs"; exit 0 }
}

# One runner invocation per chunk, never one for the whole shard: Windows CreateProcess caps the
# complete command line at 32,767 characters, and a Darling PG shard's -class list alone reached ~32,660
# (#5100). The budget counts only the -class arguments; the other ~170 characters (the dotnet
# path, 'run --project ... --no-build --', and the -trx argument) must fit in what is left, and
# 'dotnet run' hands the same arguments on to the test host. Every chunk runs even after one
# fails, so a failure in an early chunk never hides the rest of the shard's results.
$argBudget = 30000
$chunks = [System.Collections.Generic.List[string[]]]::new()
$current = [System.Collections.Generic.List[string]]::new()
$currentLength = 0
foreach ($class in $mine) {
    $cost = '-class'.Length + 1 + $class.Length + 1
    if ($current.Count -gt 0 -and ($currentLength + $cost) -gt $argBudget) {
        $chunks.Add($current.ToArray())
        $current = [System.Collections.Generic.List[string]]::new()
        $currentLength = 0
    }
    $current.Add($class)
    $currentLength += $cost
}
if ($current.Count -gt 0) { $chunks.Add($current.ToArray()) }

# A failing chunk must set $LASTEXITCODE, not throw, or the later chunks would be skipped.
$PSNativeCommandUseErrorActionPreference = $false
# The runner output of every chunk is kept beside the reports (the darling-runner-log-N artifact, #5587).
New-Item -ItemType Directory -Force -Path TestResults | Out-Null
$failedChunks = 0
$chunkNumber = 0
foreach ($chunk in $chunks) {
    $chunkNumber++
    $chunkArgs = foreach ($class in $chunk) { '-class'; $class }
    $chunkLength = ($chunk | ForEach-Object { '-class'.Length + 1 + $_.Length + 1 } | Measure-Object -Sum).Sum
    Write-Host "shard ${Shard} chunk $chunkNumber of $($chunks.Count): $($chunk.Count) classes, $chunkLength characters"
    # -xml: per-test durations for the darling-tests-timing artifact (#5459 change 4: the test map's
    # selection estimates need a per-class Darling time, and none was recorded before).
    dotnet run --project Darling/Darling.Tests/Darling.Tests.csproj -c Release --no-build -- @chunkArgs -trx TestResults/$ResultPrefix-${Shard}-$chunkNumber.trx -xml TestResults/darling-timing-${Shard}-$chunkNumber-a${RunAttempt}.xml -longRunning 300 | Tee-Object -FilePath TestResults/darling-runner-${Shard}.log -Append
    if ($LASTEXITCODE -ne 0) {
        $failedChunks++
        Write-Host "shard ${Shard} chunk $chunkNumber of $($chunks.Count) failed with exit code $LASTEXITCODE"
    }
}
if ($failedChunks -gt 0) { Write-Host "shard ${Shard}: $failedChunks of $($chunks.Count) chunks failed"; exit 1 }
exit 0
