<#
.SYNOPSIS
  Runs leg <Leg> of <LegCount> of the pull request test matrix: a weighted slice of the Darling.Tests classes and of the
  Lite.Tests classes, from test assemblies the build job already built (#6656).

.DESCRIPTION
  Run from the repository root, after the build artifact was unpacked (Darling/Darling.Tests and Lite.Tests bin/Release and
  obj/project.assets.json in place; nothing is restored or built here) and, for the Darling live classes, after
  start-test-pg.ps1 started the clusters. The workflow decides N from the test-minutes and passes the same
  -LegCount and -Areas to every leg, so the N slices partition the classes.

  Every class runs, Stage=Guard ones included: nothing is left out for a Guard job, a test map, a Cost=Slow skip or a
  quarantine, because none of them exist on this path. Weights are the committed tables (darling-shard-weights.json and
  lite-shard-weights.json), packed by darling-shard-pack.py, which proves every class lands in exactly one slice. The
  assignment is deterministic, and this script prints the slice before running it.

  -Areas  all     the whole Darling suite and the whole Lite suite.
          darling the whole Darling suite, and of Lite.Tests the classes that read a Darling path (ci-select.py reads-classes
                  --suite lite), which is the scope build.yml gives a Darling-only change.
          classes only the test classes the pull request changed (#6734): -OnlyDarlingClasses and -OnlyLiteClasses, comma lists of
                  simple names. A side with no name runs nothing. A leg the cut gave none of the named classes runs no test and
                  passes, because the other legs run them (the hash cut can leave a leg empty). Every named class must still be
                  in the test host's listing, or the leg fails.
          lite    the whole Lite suite, and of Darling.Tests the Stage=Guard classes plus the classes that read a Lite
                  path (ci-select.py reads-classes), which is the scope build.yml gives a Lite-only change.
  The Darling slice is cut by run-darling-pg-shard.ps1 (shard Leg-1 of LegCount); the Lite slice is cut here, because
  run-lite-shard.ps1 cuts by a pinned timing run or a hash and has no weights-file path. Both write the per-test timing
  xml and the runner log under TestResults/, named by the 0-based shard (Leg-1) like the old shard scripts.

  Leg 1 also runs the whole-tree guards (areas all and darling): the whole Darling suite once more with no
  DARLING_TEST_PG* set, the run build.yml's darling-tree-guards job made, about half a minute. Under -Areas lite the legs
  already run the Guard and Lite-reads classes it would have run, so the step is skipped.

  -DryRun       print the slice and what would run, run no test and start no PostgreSQL. With no built assembly the
                class list comes from the weights file instead (a class the sources no longer declare is dropped by the
                packer; the Stage=Guard part of the lite scope needs the built assembly and is left out of that listing).
  -AllLegs      with -DryRun: print every leg 1..LegCount, for checking the partition.
  -NoTreeGuards leave out the whole-tree guards step on leg 1.

  Exit code: 0 when every piece passed, 1 when any piece failed or could not run. All pieces run even after one fails.
#>
param(
    [int] $Leg = 1,
    [Parameter(Mandatory)][int] $LegCount,
    [ValidateSet('all', 'darling', 'lite', 'classes')][string] $Areas = 'all',
    # Not -DarlingClasses / -LiteClasses: PowerShell variable names ignore case, so those would be the [string] typed
    # $darlingClasses and $liteClasses lists below, and every list assigned to them would collapse into one string (#6734).
    [string] $OnlyDarlingClasses = '',
    [string] $OnlyLiteClasses = '',
    [string] $RunAttempt = '1',
    [string] $ResultPrefix = 'darling-ci',
    [switch] $DryRun,
    [switch] $AllLegs,
    [switch] $NoTreeGuards
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
if ($LegCount -lt 1) { throw "-LegCount must be at least 1, got $LegCount" }
if ($AllLegs -and -not $DryRun) { throw '-AllLegs is only for -DryRun' }
if (-not $AllLegs -and ($Leg -lt 1 -or $Leg -gt $LegCount)) { throw "-Leg must be 1..$LegCount, got $Leg" }

$scripts = $PSScriptRoot
$darlingProject = 'Darling/Darling.Tests/Darling.Tests.csproj'
$liteProject = 'Lite.Tests/Lite.Tests.csproj'
$darlingDll = 'Darling/Darling.Tests/bin/Release/net10.0-windows/Darling.Tests.dll'
$liteWeights = Join-Path $scripts 'lite-shard-weights.json'
$darlingWeights = Join-Path $scripts 'darling-shard-weights.json'
# Every area runs some Darling.Tests classes: all of them, or under 'lite' the Guard and Lite-reads scope.
$runDarling = $Areas -ne 'classes' -or [bool]$OnlyDarlingClasses
$runLite = $Areas -ne 'classes' -or [bool]$OnlyLiteClasses
if ($Areas -eq 'classes' -and -not $OnlyDarlingClasses -and -not $OnlyLiteClasses) { throw '-Areas classes needs -OnlyDarlingClasses or -OnlyLiteClasses' }
function Select-ByKey([string[]] $names, [string] $csv) {
    $only = @($csv -split ',' | Where-Object { $_ })
    $picked = @($names | Where-Object { $only -contains ($_ -split '[.+/]')[-1] })
    # #6734: every class the plan named must be in the listing. A name that matches nothing would otherwise run no test
    # and pass the leg green.
    $found = @($picked | ForEach-Object { ($_ -split '[.+/]')[-1] })
    $missing = @($only | Where-Object { $found -notcontains $_ })
    if ($missing.Count -gt 0) {
        $message = "#6734: the plan named test classes the listing does not have: $($missing -join ', ')"
        if ($DryRun) { Write-Warning $message } else { throw $message }
    }
    return $picked
}
# The Darling scope under -Areas lite, in run-darling-pg-shard.ps1's own -ScopeFilter vocabulary.
$darlingScopeFilter = if ($Areas -eq 'lite') { '-trait Stage=Guard -trait Reads=Lite' } else { '' }

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) 'ci-leg'
New-Item -ItemType Directory -Force -Path $tmp | Out-Null

function Get-WeightedNames([string] $weightsFile, [string] $prefix) {
    $table = Get-Content -Raw -LiteralPath $weightsFile | ConvertFrom-Json
    return @($table.classes.PSObject.Properties.Name | Where-Object { $_.StartsWith($prefix, [System.StringComparison]::Ordinal) })
}

# One test host listing: the same `-list classes/json` call the shard scripts make (see run-darling-pg-shard.ps1).
function Get-ListedClasses([string] $project, [string[]] $extra) {
    $lines = @(dotnet run --project $project -c Release --no-build -- -list classes/json @extra)
    if ($LASTEXITCODE -ne 0) { throw "class listing of $project failed with exit code $LASTEXITCODE" }
    $json = @($lines | Where-Object { $_ -match '^\s*\[' })
    if ($json.Count -ne 1) { throw "expected one JSON array line from the class listing of $project, got $($json.Count)" }
    return @($json[0] | ConvertFrom-Json)
}

function Get-DarlingClasses {
    if ($Areas -eq 'classes') {
        if (Test-Path -LiteralPath $darlingDll) { return @(Select-ByKey @(Get-ListedClasses $darlingProject @()) $OnlyDarlingClasses) }
        if (-not $DryRun) { throw "no built Darling.Tests at $darlingDll - the build artifact was not unpacked" }
        return @(Select-ByKey @(Get-WeightedNames $darlingWeights 'Darling.Tests.') $OnlyDarlingClasses)
    }
    if (-not (Test-Path -LiteralPath $darlingDll)) {
        if (-not $DryRun) { throw "no built Darling.Tests at $darlingDll - the build artifact was not unpacked" }
        Write-Host "(dry run, no built Darling.Tests: Darling classes read from darling-shard-weights.json)"
        $names = Get-WeightedNames $darlingWeights 'Darling.Tests.'
        if ($Areas -eq 'lite') {
            $reads = @(python -I (Join-Path $scripts 'ci-select.py') reads-classes --suite darling | Where-Object { $_ })
            if ($LASTEXITCODE -ne 0) { throw 'ci-select.py reads-classes failed' }
            Write-Host "(dry run, lite scope: only the $($reads.Count) Lite-reads names; the Stage=Guard part needs the built assembly)"
            return @($names | Where-Object { $reads -contains $_ })
        }
        return $names
    }
    if ($Areas -ne 'lite') { return @(Get-ListedClasses $darlingProject @()) }
    $every = @(Get-ListedClasses $darlingProject @())
    $guard = @(Get-ListedClasses $darlingProject @('-trait', 'Stage=Guard'))
    $reads = @(python -I (Join-Path $scripts 'ci-select.py') reads-classes --suite darling | Where-Object { $_ })
    if ($LASTEXITCODE -ne 0) { throw 'ci-select.py reads-classes failed' }
    return @($every | Where-Object { $reads -contains $_ -or $guard -contains $_ })
}

function Get-LiteClasses {
    # Under -Areas darling only the Lite classes that read a Darling file run (ci-plan.py counts them as lite_scope reads).
    $reads = @()
    if ($Areas -eq 'darling') {
        $reads = @(python -I (Join-Path $scripts 'ci-select.py') reads-classes --suite lite | Where-Object { $_ })
        if ($LASTEXITCODE -ne 0) { throw 'ci-select.py reads-classes failed' }
    }
    if (-not (Test-Path -LiteralPath 'Lite.Tests/bin/Release')) {
        if (-not $DryRun) { throw 'no built Lite.Tests under Lite.Tests/bin/Release - the build artifact was not unpacked' }
        Write-Host '(dry run, no built Lite.Tests: Lite classes read from lite-shard-weights.json)'
        $names = @(Get-WeightedNames $liteWeights '')
    }
    else { $names = @(Get-ListedClasses $liteProject @()) }
    if ($Areas -eq 'classes') { return @(Select-ByKey $names $OnlyLiteClasses) }
    if ($Areas -ne 'darling') { return $names }
    Write-Host "(darling scope: only the Lite classes that read a Darling file, $($reads.Count) named by the source scan)"
    return @($names | Where-Object { $reads -contains $_ })
}

# darling-shard-pack.py pack: the same packer, weights and proof run-darling-pg-shard.ps1 uses. Returns the header line
# and this shard's classes. A packer failure stops the leg: guessing a cut here could run a class twice or never.
function Get-Slice([string[]] $classes, [string] $weightsFile, [int] $shard, [string] $label) {
    if ($classes.Count -eq 0) { throw "$label has no classes to cut" }
    $file = Join-Path $tmp "$label-classes.txt"
    [System.IO.File]::WriteAllLines($file, [string[]]$classes)
    $packed = @(python -I (Join-Path $scripts 'darling-shard-pack.py') pack --classes $file --shards $LegCount --shard $shard --weights $weightsFile)
    if ($LASTEXITCODE -ne 0) { throw "darling-shard-pack.py failed for $label with exit code $LASTEXITCODE" }
    return [pscustomobject]@{
        Header  = ($packed | Where-Object { $_ -like '#*' }) -join ' '
        Classes = @($packed | Where-Object { $_ -and $_ -notlike '#*' })
    }
}

function Write-Slice([string] $title, $slice, [int] $total) {
    Write-Host "::group::$title - $($slice.Classes.Count) of $total classes ($($slice.Header))"
    foreach ($c in $slice.Classes) { Write-Host "  $c" }
    if ($slice.Classes.Count -eq 0 -and $Areas -eq 'classes') { Write-Host "  (none of the named classes is in this slice: it runs no test, and the other legs run them)" }
    Write-Host '::endgroup::'
}

# ---- dry run: show the slices, run nothing --------------------------------------------------------------------------
if ($DryRun) {
    $darlingClasses = if ($runDarling) { Get-DarlingClasses } else { @() }
    $liteClasses = if ($runLite) { Get-LiteClasses } else { @() }
    $legs = if ($AllLegs) { 1..$LegCount } else { @($Leg) }
    $seen = @{}
    foreach ($i in $legs) {
        Write-Host "=== leg $i of $LegCount (areas $Areas) ==="
        if ($runDarling) {
            $s = Get-Slice $darlingClasses $darlingWeights ($i - 1) "darling-$i"
            Write-Slice "Darling.Tests slice, leg $i" $s $darlingClasses.Count
            foreach ($c in $s.Classes) { $seen["D:$c"] = 1 + [int]$seen["D:$c"] }
        }
        if ($runLite) {
            $s = Get-Slice $liteClasses $liteWeights ($i - 1) "lite-$i"
            Write-Slice "Lite.Tests slice, leg $i" $s $liteClasses.Count
            foreach ($c in $s.Classes) { $seen["L:$c"] = 1 + [int]$seen["L:$c"] }
        }
        if ($i -eq 1 -and $runDarling -and $Areas -notin 'lite', 'classes' -and -not $NoTreeGuards) { Write-Host 'leg 1 would also run the whole-tree guards (no DARLING_TEST_PG*)' }
    }
    if ($AllLegs) {
        $expected = $darlingClasses.Count * [int]$runDarling + $liteClasses.Count * [int]$runLite
        $dupes = @($seen.GetEnumerator() | Where-Object { $_.Value -ne 1 })
        Write-Host "partition check: $($seen.Count) distinct classes in the slices, $expected expected, $($dupes.Count) in more than one slice"
        if ($dupes.Count -gt 0 -or $seen.Count -ne $expected) { exit 1 }
    }
    Write-Host 'dry run: no test ran'
    exit 0
}

# ---- a real leg -----------------------------------------------------------------------------------------------------
$failed = [System.Collections.Generic.List[string]]::new()
$shard = $Leg - 1
New-Item -ItemType Directory -Force -Path TestResults | Out-Null
Write-Host "leg $Leg of ${LegCount}: areas $Areas, shard $shard"

# The slices first, so a log that dies early still names what the leg was to run. Both are recomputed by the callees below
# from the same inputs with the same deterministic packer; this print is for the reader.
$darlingClasses = @()
$liteClasses = @()
if ($runDarling) {
    $darlingClasses = @(Get-DarlingClasses)
    Write-Slice "Darling.Tests slice, leg $Leg" (Get-Slice $darlingClasses $darlingWeights $shard "darling-$Leg") $darlingClasses.Count
}
$liteSlice = $null
if ($runLite) {
    $liteClasses = @(Get-LiteClasses)
    $liteSlice = Get-Slice $liteClasses $liteWeights $shard "lite-$Leg"
    Write-Slice "Lite.Tests slice, leg $Leg" $liteSlice $liteClasses.Count
}

# Runs one piece without letting its failure (a throw or a non-zero exit) skip the pieces after it.
function Invoke-Piece([string] $name, [scriptblock] $body) {
    Write-Host "::group::$name"
    try {
        $global:LASTEXITCODE = 0
        & $body
        if ($LASTEXITCODE -ne 0) { $failed.Add("$name (exit $LASTEXITCODE)"); Write-Host "$name failed with exit code $LASTEXITCODE" }
    }
    catch {
        $failed.Add("$name ($($_.Exception.Message))")
        Write-Host "::error title=$name::$($_.Exception.Message)"
    }
    Write-Host '::endgroup::'
}

# The environment the Darling live classes read, from start-test-pg.ps1 (one place for the ports).
$testEnv = @(& (Join-Path $scripts 'start-test-pg.ps1') -Mode Env -Root (Get-Location).Path)
if ($testEnv.Count -eq 0) { throw 'start-test-pg.ps1 -Mode Env printed nothing' }

# Whole-tree guards (leg 1): the whole Darling suite, no store, as build.yml's darling-tree-guards job ran it.
if ($Leg -eq 1 -and $runDarling -and $Areas -notin 'lite', 'classes' -and -not $NoTreeGuards) {
    Invoke-Piece 'Whole-tree guards' {
        $saved = @{}
        foreach ($n in (Get-ChildItem Env: | Where-Object { $_.Name -like 'DARLING_TEST_*' }).Name) { $saved[$n] = [Environment]::GetEnvironmentVariable($n); Remove-Item "Env:$n" }
        try {
            dotnet run --project $darlingProject -c Release --no-build -- -trx "TestResults/$ResultPrefix-treeguards.trx" -xml "TestResults/darling-timing-treeguards-a$RunAttempt.xml" -longRunning 300 | Tee-Object -FilePath 'TestResults/darling-runner-treeguards.log' -Append
        }
        finally { foreach ($n in $saved.Keys) { [Environment]::SetEnvironmentVariable($n, $saved[$n]) } }
    }
}

if ($runDarling) {
    foreach ($line in $testEnv) { $kv = $line -split '=', 2; [Environment]::SetEnvironmentVariable($kv[0], $kv[1]) }
    Invoke-Piece "Darling.Tests slice $Leg of $LegCount" {
        $shardParams = @{ Shard = $shard; ShardCount = $LegCount; ResultPrefix = $ResultPrefix; RunAttempt = $RunAttempt }
        if ($darlingScopeFilter) { $shardParams.ScopeFilter = $darlingScopeFilter }
        if ($Areas -eq 'classes') { $shardParams.OnlyClasses = $OnlyDarlingClasses }
        & (Join-Path $scripts 'run-darling-pg-shard.ps1') @shardParams
    }
}

if ($runLite) {
    Invoke-Piece "Lite.Tests slice $Leg of $LegCount" {
        # One runner invocation per chunk, under the 32,767-character command-line cap (#5100): see run-lite-shard.ps1.
        $argBudget = 30000
        $chunks = [System.Collections.Generic.List[string[]]]::new()
        $current = [System.Collections.Generic.List[string]]::new()
        $length = 0
        foreach ($class in $liteSlice.Classes) {
            $cost = '-class'.Length + 1 + $class.Length + 1
            if ($current.Count -gt 0 -and ($length + $cost) -gt $argBudget) { $chunks.Add($current.ToArray()); $current = [System.Collections.Generic.List[string]]::new(); $length = 0 }
            $current.Add($class)
            $length += $cost
        }
        if ($current.Count -gt 0) { $chunks.Add($current.ToArray()) }
        if ($chunks.Count -eq 0) {
            # Only the classes plan may hand a leg none of its few classes: the other legs run them all (the packer proves it).
            if ($Areas -ne 'classes') { throw "lite shard $shard has no class to run" }
            Write-Host "lite shard ${shard}: no named class in this slice, so it runs no test. The other legs run them."
        }
        $failedChunks = 0
        $n = 0
        foreach ($chunk in $chunks) {
            $n++
            $chunkArgs = foreach ($class in $chunk) { '-class'; $class }
            Write-Host "lite shard $shard chunk $n of $($chunks.Count): $($chunk.Count) classes"
            dotnet run --project $liteProject -c Release --no-build -- @chunkArgs -xml "TestResults/lite-timing-$shard-$n-a$RunAttempt.xml" -longRunning 300 | Tee-Object -FilePath "TestResults/lite-runner-$shard.log" -Append
            if ($LASTEXITCODE -ne 0) { $failedChunks++; Write-Host "lite shard $shard chunk $n failed with exit code $LASTEXITCODE" }
        }
        # Piece-level exit status: the Invoke-Piece check reads $LASTEXITCODE.
        $global:LASTEXITCODE = if ($failedChunks -gt 0) { 1 } else { 0 }
    }
}

if ($failed.Count -gt 0) {
    Write-Host "leg $Leg of ${LegCount} FAILED: $($failed -join '; ')"
    exit 1
}
Write-Host "leg $Leg of ${LegCount} passed"
exit 0
