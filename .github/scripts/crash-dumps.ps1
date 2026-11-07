<#
.SYNOPSIS
  Crash dumps for the test hosts the CI scripts start (#8190). Dot-source this file, call Enable-CrashDumps once before the
  first host starts, and call Write-CrashMarker with each host's exit code after it exits.

.DESCRIPTION
  A test host that dies with a native fault reports no failing test and leaves no stack to start from: an access violation
  exits with -1073741819 (0xC0000005) and a "Fatal error" line, and the leg is red with nothing to read. Enable-CrashDumps sets
  the runtime's crash-dump variables so a host that dies that way writes a minidump of itself under TestResults/crash-dumps.
  The type is the stack-and-module dump (1): it names the faulting frame and every thread's stack and stays tens of MB, so the
  upload fits a step; a heap dump of a host holding several 20 MB plans would be GBs. The name carries the host's executable
  name and process id, so a leg whose chunks each died keeps every dump.

  Write-CrashMarker records the exit code of a host that died beside the dumps. A negative code is a Windows status (an
  access violation, a stack overflow, an unhandled exception's 0xE0434352), never a count of failed tests, so that is the test.
  The marker keeps the folder non-empty when the runtime could not write the dump, and the workflow uploads the folder as its
  own artifact whenever it has a file.

  Both functions change nothing when the caller already set DOTNET_DbgEnableMiniDump, so a workflow can point the dumps elsewhere.
#>
$script:crashDumpDir = Join-Path (Get-Location).Path 'TestResults/crash-dumps'

function Enable-CrashDumps {
    New-Item -ItemType Directory -Force -Path $script:crashDumpDir | Out-Null
    if ($env:DOTNET_DbgEnableMiniDump) { return }
    $env:DOTNET_DbgEnableMiniDump = '1'
    $env:DOTNET_DbgMiniDumpType = '1'
    $env:DOTNET_DbgMiniDumpName = Join-Path $script:crashDumpDir '%e-%p.dmp'
    $env:DOTNET_CreateDumpDiagnostics = '1'
    Write-Host "crash dumps: a test host that dies with a native fault writes a minidump under $script:crashDumpDir"
}

function Write-CrashMarker([string] $piece, [int] $exitCode) {
    if ($exitCode -ge 0) { return }
    New-Item -ItemType Directory -Force -Path $script:crashDumpDir | Out-Null
    $status = '0x{0:X8}' -f $exitCode
    Add-Content -LiteralPath (Join-Path $script:crashDumpDir 'exit-codes.txt') -Value "$piece exited with $exitCode ($status)"
    Write-Host "${piece}: the test host died with exit code $exitCode ($status); the dump it wrote, if any, is under $script:crashDumpDir"
}
