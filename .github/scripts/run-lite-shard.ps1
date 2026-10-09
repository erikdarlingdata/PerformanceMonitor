<#
.SYNOPSIS
  Runs one shard of the Lite.Tests suite: cuts the class list (by recorded timings when a source is pinned,
  else by class-name hash), chunks it under the command-line budget, and runs the test host once per chunk
  (#5616).

.DESCRIPTION
  The ONE copy of the Lite shard logic. build.yml's lite-tests job (every pull request and push) and
  nightly.yml's lite-tests job both call it. What differs between the callers is passed in; the env blocks, the
  restore and build steps and the path gates stay in each workflow. Run from the repository root, after
  Lite.Tests was built in Release.

  -Shard / -ShardCount   this leg's index and the matrix's leg count (strategy.job-total).
  -JobIndex              strategy.job-index; equal to -Shard only while `shard` is the matrix's one axis.
  -EventName             the workflow event. Only 'pull_request' leaves out Cost=Slow classes (#5459 change 5);
                         the SLOW_PR and SLOW_REPO environment variables carry the rest of that lookup.
  -GuardBuildUsed        'true' only when this leg used the Guard job's build (see the Darling script).
  -ScopeMode             'reads' runs only the classes that read a Darling tree; anything else cuts the whole suite.
  -TimingRun / -TimingArtifacts
                         the timing source the gate pinned for the whole run (empty: none, so the hash cut)
                         and the exact artifact names it pinned. The nightly passes neither.
  -RunAttempt            the workflow's run attempt, in the timing file name.
  -SelectionFile         the test map selection the pull request's test-map-select job wrote (#5459); empty or
                         missing: no selection. A usable one replaces the Cost=Slow skip and keeps only the
                         classes it names; FULL, unreadable or empty means every class this leg was cut runs.
#>
param(
    [Parameter(Mandatory)][int] $Shard,
    [Parameter(Mandatory)][int] $ShardCount,
    [int] $JobIndex = -1,
    [string] $EventName = '',
    [string] $GuardBuildUsed = '',
    [string] $ScopeMode = '',
    [string] $TimingRun = '',
    [string] $TimingArtifacts = '',
    [string] $RunAttempt = '1',
    [string] $SelectionFile = ''
)

$ErrorActionPreference = 'Stop'
# The cut and the run must agree (#5459): every listing below (the full one the shards cut from and the
# Darling-reads one) leaves the Guard classes out exactly when the Guard job ran them. No artifact: all classes.
$guardFilter = @(if ($GuardBuildUsed -eq 'true') { '-trait-'; 'Stage=Guard' })
function Get-LiteClasses([string[]] $filter) {
    $lines = @(dotnet run --project Lite.Tests/Lite.Tests.csproj -c Release --no-build -- -list classes/json @guardFilter @filter)
    if ($LASTEXITCODE -ne 0) { throw "class listing failed with exit code $LASTEXITCODE" }
    # Only the runner's one-line JSON array is parsed; see run-darling-pg-shard.ps1 (a CLI notice on stdout).
    $json = @($lines | Where-Object { $_ -match '^\s*\[' })
    if ($json.Count -ne 1) { throw "expected one JSON array line from the class listing, got $($json.Count): $($lines -join ' | ')" }
    return @($json[0] | ConvertFrom-Json)
}

$classes = @(Get-LiteClasses @())
if ($classes.Count -eq 0) { throw 'the class listing returned no classes - the shard would silently run nothing' }

if ($ScopeMode -eq 'reads') {
    # A diff that reaches only the Darling trees Lite.Tests reads: run the classes that carry the
    # trait, all of them, in this one leg. The trait set is read off the built assembly, so it is
    # whatever exists on THIS commit; DarlingReadsTraitGuardTests fails a class that reads a
    # Darling path without the trait. An empty set is not "nothing to run": it would mean the trait
    # went missing, so this leg runs the WHOLE suite rather than none of it.
    $mine = @(Get-LiteClasses @('-trait', 'Reads=Darling'))
    if ($mine.Count -eq 0) {
        Write-Host '::warning title=Lite Darling-reads selection is empty::No Lite.Tests class carries [Trait("Reads", "Darling")], so this leg runs the whole Lite suite instead.'
        $mine = $classes
    }
    Write-Host "Darling-reads run: $($mine.Count) of $($classes.Count) classes"
}
else {
    # #5208: all four legs must cut the suite from the SAME inputs (see the lite-tests job header in build.yml). The gate
    # pinned one timing source for the whole run. With none pinned the cut is the class-name hash
    # (SHA-256, first byte, modulo the shard count), exactly as before the packer. With one, this
    # leg downloads that run's timings and packs by duration. A pinned source this leg cannot
    # fetch FAILS the leg: falling back to the hash here alone, while the other legs cut by
    # duration, would leave the four cuts not partitioning the suite.
    $shards = $ShardCount
    # job-total is the shard count only while `shard` is the matrix's one axis. A second axis would cut into
    # buckets no leg reads; on a one-axis matrix job-index equals the shard value.
    if ($JobIndex -ne $Shard) { throw "the lite-tests matrix is no longer the single shard axis (job-index ${JobIndex}, shard ${Shard}), so strategy.job-total is not the shard count" }
    if ($TimingRun -match '^\d+$') {
        $planDir = Join-Path $env:RUNNER_TEMP 'lite-shard-plan'
        $timingDir = Join-Path $planDir 'timings'
        New-Item -ItemType Directory -Force -Path $timingDir | Out-Null
        $classesFile = Join-Path $planDir 'classes.txt'
        Set-Content -Path $classesFile -Value $classes -Encoding utf8NoBOM

        $PSNativeCommandUseErrorActionPreference = $false
        $downloaded = $false
        foreach ($attempt in 1..3) {
            # Start every try from an empty folder: gh will not overwrite a file that a partial try left.
            Remove-Item -Recurse -Force -Path $timingDir -ErrorAction SilentlyContinue
            New-Item -ItemType Directory -Force -Path $timingDir | Out-Null
            gh run download $TimingRun --pattern 'lite-tests-timing-*' --dir $timingDir
            if ($LASTEXITCODE -eq 0) { $downloaded = $true; break }
            Write-Host "timing download attempt $attempt of 3 failed with exit code $LASTEXITCODE"
            if ($attempt -lt 3) { Start-Sleep -Seconds (5 * $attempt) }
        }
        if (-not $downloaded) { throw "could not download the Lite timings of run $TimingRun that the gate pinned - failing this shard rather than cutting alone, because the other shards may have cut by duration" }
        # The gate pinned the exact artifact set. A leg that got fewer (one expired or was deleted since,
        # which gh skips without failing) would cut from different inputs than the other legs.
        $expected = @(($TimingArtifacts -split ',') | Where-Object { $_ } | Sort-Object)
        $got = @(Get-ChildItem -Path $timingDir -Directory | ForEach-Object { $_.Name } | Sort-Object)
        if ($expected.Count -eq 0 -or ($expected -join ',') -ne ($got -join ',')) { throw "downloaded timing artifacts [$($got -join ', ')] are not the [$($expected -join ', ')] the gate pinned from run $TimingRun - failing this shard rather than cutting from different inputs than the other shards" }

        $packOutput = @(python .github/scripts/lite-shard-pack.py pack --classes $classesFile --shards $shards --out $planDir --timings $timingDir)
        $packExit = $LASTEXITCODE
        $packOutput | ForEach-Object { Write-Host $_ }
        if ($packExit -ne 0) { throw 'the shard packer failed or could not prove its cut total - failing this shard rather than guessing' }
        # The packer itself falls back to the hash cut when the timings are unusable (missing, corrupt,
        # empty, all zero, or under 80% of this commit's classes); it says which it used, and so does
        # the notice below.
        $method = if (($packOutput -join ' ') -match 'shards by (\w+);') { $Matches[1] } else { 'an unreported method' }
        # The guard again, on the very files this shard reads.
        python .github/scripts/lite-shard-pack.py reconcile --classes $classesFile --shards $shards --out $planDir
        if ($LASTEXITCODE -ne 0) { throw 'the shard files are not exactly the discovered classes - failing this shard' }

        $mine = @(Get-Content -Path (Join-Path $planDir "shard-$Shard.txt") | Where-Object { $_.Trim() })
        # A fingerprint of the whole plan, identical on every leg when they cut from the same inputs. Each
        # shard file is labelled, so the same classes split differently across shards give a different id
        # (joined bare, [A, B] + [C] and [A] + [B, C] hashed the same). Nothing compares the legs' ids: it
        # is a diagnostic to read side by side, and the pinned artifact set above is what keeps them equal.
        $planText = (0..($shards - 1) | ForEach-Object { "shard-$_`n" + (Get-Content -Raw -Path (Join-Path $planDir "shard-$_.txt")) }) -join ''
        $planId = [System.Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($planText))).Substring(0, 12).ToLower()
        Write-Host "::notice title=Lite shard plan::cut by $method (timings of run $TimingRun); plan $planId. Every shard of this run must print this same id."
    }
    else {
        $sha = [System.Security.Cryptography.SHA256]::Create()
        $mine = @($classes | Where-Object {
            $hash = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($_))
            ($hash[0] % $shards) -eq $Shard
        })
        Write-Host "::notice title=Lite shard plan::no timing source pinned, so this run cuts by class-name hash"
    }
    Write-Host "shard ${Shard}: $($mine.Count) of $($classes.Count) classes"
}
if ($mine.Count -eq 0) { throw "shard ${Shard} selected zero of $($classes.Count) classes - refusing to run, because a runner with no -class arguments runs the whole suite" }

# #5459 live selection: a pull request run whose gate pinned a test map keeps only the classes the map picked for its
# diff (the test-map-select job's selection.json, downloaded by the workflow into -SelectionFile). It is applied AFTER
# the cut and the zero check above, so every shard still agrees on which shard owns which class. `map-keep` answers
# FULL (any reason: the selection is FULL, missing, unreadable or empty) or SELECTED plus the names; anything but a
# clean SELECTED answer, an unreadable answer included, leaves $selected null and this shard runs everything it was
# cut. A pull request with a usable selection does not also get the Cost=Slow skip below: the selection replaces it.
$selected = $null
if ($EventName -eq 'pull_request' -and $SelectionFile -and (Test-Path -LiteralPath $SelectionFile)) {
    $keep = @(python .github/scripts/ci-select.py map-keep --selection $SelectionFile --suite lite | Where-Object { $_ })
    if ($LASTEXITCODE -eq 0 -and $keep.Count -gt 1 -and $keep[0] -match '^SELECTED \d+$') {
        $selected = [System.Collections.Generic.HashSet[string]]::new([string[]]@($keep | Select-Object -Skip 1), [System.StringComparer]::Ordinal)
        Write-Host "shard ${Shard}: the test map selected $($selected.Count) lite classes for this pull request"
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
    $skip = @(python .github/scripts/ci-select.py slow-skip --suite lite --event $EventName --repo $env:SLOW_REPO --pr $env:SLOW_PR | Where-Object { $_ })
    $kept = @($mine | Where-Object { $skip -notcontains ($_ -split '\.')[-1] })
    Write-Host "shard ${Shard}: $($mine.Count - $kept.Count) Cost=Slow classes left out on this pull request, $($kept.Count) to run"
    $mine = $kept
    if ($mine.Count -eq 0) { Write-Host "shard ${Shard}: every class of this shard is a skipped Cost=Slow class, so nothing runs"; exit 0 }
}

# One runner invocation per chunk, never one for the whole shard: Windows CreateProcess caps the
# complete command line at 32,767 characters, and a Darling PG shard's -class list alone reached ~32,660
# (#5100). The budget counts only the -class arguments; the other ~170 characters (the dotnet
# path and 'run --project ... --no-build --') must fit in what is left, and
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
New-Item -ItemType Directory -Force -Path TestResults | Out-Null
$failedChunks = 0
$chunkNumber = 0
foreach ($chunk in $chunks) {
    $chunkNumber++
    $chunkArgs = foreach ($class in $chunk) { '-class'; $class }
    $chunkLength = ($chunk | ForEach-Object { '-class'.Length + 1 + $_.Length + 1 } | Measure-Object -Sum).Sum
    Write-Host "shard ${Shard} chunk $chunkNumber of $($chunks.Count): $($chunk.Count) classes, $chunkLength characters"
    # -xml: per-test durations for the lite-tests-timing artifact (#5208).
    dotnet run --project Lite.Tests/Lite.Tests.csproj -c Release --no-build -- @chunkArgs -xml TestResults/lite-timing-${Shard}-$chunkNumber-a${RunAttempt}.xml -longRunning 300 | Tee-Object -FilePath TestResults/lite-runner-${Shard}.log -Append
    if ($LASTEXITCODE -ne 0) {
        $failedChunks++
        Write-Host "shard ${Shard} chunk $chunkNumber of $($chunks.Count) failed with exit code $LASTEXITCODE"
    }
}
if ($failedChunks -gt 0) { Write-Host "shard ${Shard}: $failedChunks of $($chunks.Count) chunks failed"; exit 1 }
exit 0
