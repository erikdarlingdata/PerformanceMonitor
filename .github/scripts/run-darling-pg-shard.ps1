<#
.SYNOPSIS
  Runs one shard of the Darling.Tests suite: cuts the class list by recorded duration, chunks it under the command-line
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
  -OnlyClasses           comma list of simple class names (#6734): keep only those classes of the listing, before the
                         cut. Empty: no narrowing. With names, a shard the cut gave none of them passes without running a
                         test (the plan has fewer classes than legs, or the hash cut left it empty, and the other legs run
                         every named class). Without names, a shard that selects zero classes is an error.
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
    [string] $ScopeFilter = '',
    [string] $OnlyClasses = ''
)

$ErrorActionPreference = 'Stop'
# The cut and the run must agree (#5459): the listing every shard cuts from leaves the Guard classes out exactly
# when the Guard job ran them, so each remaining class still lands in exactly one shard. No artifact: all classes.
$guardFilter = @(if ($GuardBuildUsed -eq 'true') { '-trait-'; 'Stage=Guard' })
# Not named $scopeFilter: PowerShell variable names are case-insensitive, so that name is the typed [string] $ScopeFilter
# parameter, which turns the array back into one string, and splatting a string hands the runner its characters as
# separate arguments (exit 3 on every pull request that changes only Lite files).
$scopeArgs = @($ScopeFilter -split ' ' | Where-Object { $_ })
function Get-DarlingClasses([string[]] $scopeArgs) {
    # The parameter shadows the caller's $scopeArgs on purpose: the listing takes the Guard exclusion first, then a scope.
    $lines = @(dotnet run --project Darling/Darling.Tests/Darling.Tests.csproj -c Release --no-build -- -list classes/json @guardFilter @scopeArgs)
    if ($LASTEXITCODE -ne 0) { throw "class listing failed with exit code $LASTEXITCODE" }
    # Only the runner's one-line JSON array is parsed: the dotnet CLI can print its own notice on stdout,
    # and a line starting "W" after the array failed a shard's ConvertFrom-Json on 2026-09-23.
    $json = @($lines | Where-Object { $_ -match '^\s*\[' })
    if ($json.Count -ne 1) { throw "expected one JSON array line from the class listing, got $($json.Count): $($lines -join ' | ')" }
    return @($json[0] | ConvertFrom-Json)
}
# #5961: the Lite-reads part of a scope is the classes ci-select.py's source scan says read a Lite path, not a trait on
# the assembly. xunit ANDs filters of different kinds, so the part cannot ride in the one listing call: the listing runs
# once for the rest of the scope (the Guard stage) and once, unfiltered, to match the scan's names against. An empty scan
# adds no class, which is what an empty `-trait Reads=Lite` listing did; a scope that ends up with no class at all still
# stops at the zero check below, never at a runner given no -class arguments (that runs the whole suite).
$readsLite = $false
$restArgs = [System.Collections.Generic.List[string]]::new()
for ($i = 0; $i -lt $scopeArgs.Count; $i++) {
    if ($scopeArgs[$i] -eq '-trait' -and ($i + 1) -lt $scopeArgs.Count -and $scopeArgs[$i + 1] -eq 'Reads=Lite') { $readsLite = $true; $i++ }
    else { $restArgs.Add($scopeArgs[$i]) }
}
if ($readsLite) {
    $readsNames = @(python .github/scripts/ci-select.py reads-classes --suite darling | Where-Object { $_ })
    if ($LASTEXITCODE -ne 0) { throw "ci-select.py reads-classes failed with exit code $LASTEXITCODE" }
    Write-Host "Lite-reads scan: $($readsNames.Count) Darling.Tests classes read a Lite path"
    $everyClass = @(Get-DarlingClasses @())
    $restClasses = if ($restArgs.Count -gt 0) { @(Get-DarlingClasses $restArgs.ToArray()) } else { @() }
    $classes = @($everyClass | Where-Object { $readsNames -contains $_ -or $restClasses -contains $_ })
}
else {
    $classes = @(Get-DarlingClasses $scopeArgs)
}
if ($OnlyClasses) {
    $only = @($OnlyClasses -split ',' | Where-Object { $_ })
    $classes = @($classes | Where-Object { $only -contains ($_ -split '[.+/]')[-1] })
}
if ($classes.Count -eq 0) { throw 'the class listing returned no classes - the shard would silently run nothing' }

# #5669: the cut balances the shards by recorded class duration (.github/scripts/darling-shard-pack.py, from the committed
# table darling-shard-weights.json) instead of a class-name hash, which left one shard 4 to 6 minutes over the mean. Every
# shard runs the same deterministic assignment over the same listing, and the packer proves each class lands in exactly one
# shard before it prints. A packer failure stops this shard: falling back to the hash here would cut this shard differently
# from the others and could run a class twice or never. A single-shard caller (the build job's Lite-reads pass) needs no cut.
if ($ShardCount -le 1) {
    $mine = $classes
} else {
    $classesFile = Join-Path ([System.IO.Path]::GetTempPath()) "darling-shard-classes-${Shard}.txt"
    [System.IO.File]::WriteAllLines($classesFile, [string[]]$classes)
    $packed = @(python .github/scripts/darling-shard-pack.py pack --classes $classesFile --shards $ShardCount --shard $Shard)
    if ($LASTEXITCODE -ne 0) { throw "darling-shard-pack.py failed with exit code $LASTEXITCODE" }
    Write-Host "shard ${Shard}: $($packed | Where-Object { $_ -like '#*' })"
    $mine = @($packed | Where-Object { $_ -and $_ -notlike '#*' })
}
Write-Host "shard ${Shard}: $($mine.Count) of $($classes.Count) classes"
if ($mine.Count -eq 0 -and $OnlyClasses) {
    # #6734: the classes plan names a few classes, and the cut may give some shard none of them (the hash cut does, when the
    # weight table knows under 80 percent of them). pack() proves every class lands in exactly one shard, so the other
    # shards run all of them. This is not the runner-with-no-class case below: nothing is started here.
    Write-Host "shard ${Shard}: none of the $($classes.Count) named classes is in this shard, so it runs no test. The other legs run them."
    exit 0
}
if ($mine.Count -eq 0) { throw "shard ${Shard} selected zero of $($classes.Count) classes - refusing to run, because a runner with no -class arguments runs the whole suite" }

# The key a class is selected by: its own simple name, split on the same [.+/] as test-map.py's simple_name and
# ci-select.py's scan_classes. The runner lists a nested class as Namespace.Outer+Inner, and the selection holds
# `Inner`; splitting on '.' alone would key it `Outer+Inner`, so a nested class (a live PostgreSQL one included) would
# be left out of every narrowed pull request (#5459).
function Get-ClassKey([string] $FullName) { return ($FullName -split '[.+/]')[-1] }

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
    $kept = @($mine | Where-Object { $selected.Contains((Get-ClassKey $_)) })
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
    $kept = @($mine | Where-Object { $skip -notcontains (Get-ClassKey $_) })
    Write-Host "shard ${Shard}: $($mine.Count - $kept.Count) Cost=Slow classes left out on this pull request, $($kept.Count) to run"
    $mine = $kept
    if ($mine.Count -eq 0) { Write-Host "shard ${Shard}: every class of this shard is a skipped Cost=Slow class, so nothing runs"; exit 0 }
}

# #5942 the CI quarantine: a test that dev's own latest push broke is left out of an unrelated pull request. The
# test-map-select job decided it once and put quarantine.json beside the selection (ci-select.py quarantine); the
# honored tests of this suite become `-method- <test>` arguments on every chunk below. A missing or unreadable decision
# leaves nothing out, so every doubt runs the test.
$quarantineArgs = @()
if ($EventName -eq 'pull_request' -and $SelectionFile) {
    $quarantineFile = Join-Path (Split-Path -Parent $SelectionFile) 'quarantine.json'
    if (Test-Path -LiteralPath $quarantineFile) {
        $quarantineArgs = @(python .github/scripts/ci-select.py quarantine-args --quarantine $quarantineFile --suite darling | Where-Object { $_ })
        if ($LASTEXITCODE -ne 0) { $quarantineArgs = @() }
        if ($quarantineArgs.Count -gt 0) { Write-Host "shard ${Shard}: the CI quarantine leaves out $($quarantineArgs.Count / 2) test(s): $(($quarantineArgs | Where-Object { $_ -ne '-method-' }) -join ', ')" }
    }
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
    dotnet run --project Darling/Darling.Tests/Darling.Tests.csproj -c Release --no-build -- @chunkArgs @quarantineArgs -trx TestResults/$ResultPrefix-${Shard}-$chunkNumber.trx -xml TestResults/darling-timing-${Shard}-$chunkNumber-a${RunAttempt}.xml -longRunning 300 | Tee-Object -FilePath TestResults/darling-runner-${Shard}.log -Append
    if ($LASTEXITCODE -ne 0) {
        $failedChunks++
        Write-Host "shard ${Shard} chunk $chunkNumber of $($chunks.Count) failed with exit code $LASTEXITCODE"
    }
}
if ($failedChunks -gt 0) { Write-Host "shard ${Shard}: $failedChunks of $($chunks.Count) chunks failed"; exit 1 }
exit 0
