<#
.SYNOPSIS
    One-time cleanup of orphaned Monitor_LongQueries_*.trc trace files left on
    disk by PerformanceMonitor (issue #972).

.DESCRIPTION
    PerformanceMonitor versions through 2.11.0 accumulated stale SQL Trace files
    in the SQL Server error log directory:

      * the trace collector recreated the trace on every collection cycle, and
      * it created the trace without a rollover file-count cap, so SQL Server
        never deleted old files, and
      * the data-retention job's xp_delete_file call could not delete .trc
        files at all (xp_delete_file only handles backups / maintenance-plan
        reports), and in fact failed the job outright.

    Version 3.0.0 fixes the source: collect.trace_management_collector now
    keeps a single long-running trace bounded with a rollover file-count cap
    (@filecount), so SQL Server deletes its own old files going forward. This
    script is the one-time sweep for the files already on disk.

    Run it ON the SQL Server host, as a Windows account that can delete the
    trace files - a local Administrator, or the SQL Server service account.
    It will NOT touch files belonging to a trace that is currently running, and
    it skips any file that is locked.

    Preview first with -WhatIf.

.PARAMETER SqlInstance
    SQL Server instance to query for the error log directory and running
    traces. Defaults to the default local instance (".").

.PARAMETER SqlUser
    SQL login for SQL Server authentication. Omit to use Windows authentication.

.PARAMETER SqlPassword
    Password for -SqlUser.

.PARAMETER ErrorLogPath
    Explicit error log directory, used only if the instance cannot be queried
    for it. Normally leave this unset.

.EXAMPLE
    .\Remove-OrphanedTraceFiles.ps1 -WhatIf
    Show what would be deleted on the default local instance.

.EXAMPLE
    .\Remove-OrphanedTraceFiles.ps1
    Delete the orphaned trace files on the default local instance.

.EXAMPLE
    .\Remove-OrphanedTraceFiles.ps1 -SqlInstance SQLPROD\INST1 -SqlUser sa -SqlPassword (Read-Host -AsSecureString | ConvertFrom-SecureString -AsPlainText)
    Use a named instance and SQL authentication.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param
(
    [string]$SqlInstance = ".",
    [string]$SqlUser,
    [string]$SqlPassword,
    [string]$ErrorLogPath
)

$ErrorActionPreference = "Stop"
$filePattern = "Monitor_LongQueries_*.trc"

function Get-ConnectionString
{
    $base = "Server=$SqlInstance;Database=master;Application Name=PerformanceMonitor Trace Cleanup;Connect Timeout=15;"
    if ($SqlUser)
    {
        return $base + "User ID=$SqlUser;Password=$SqlPassword;"
    }
    return $base + "Integrated Security=SSPI;"
}

# --- Query the instance for the error log directory and any running traces ---
$activePrefixes = New-Object System.Collections.Generic.List[string]
$connection = $null

try
{
    $connection = New-Object System.Data.SqlClient.SqlConnection
    $connection.ConnectionString = Get-ConnectionString
    $connection.Open()

    if (-not $ErrorLogPath)
    {
        $cmd = $connection.CreateCommand()
        $cmd.CommandText = "SELECT CONVERT(nvarchar(4000), SERVERPROPERTY('ErrorLogFileName'));"
        $errorLogFile = [string]$cmd.ExecuteScalar()
        if ($errorLogFile)
        {
            $ErrorLogPath = Split-Path -Path $errorLogFile -Parent
        }
    }

    # Running traces - best effort; needs ALTER TRACE permission.
    try
    {
        $cmd = $connection.CreateCommand()
        $cmd.CommandText = "SELECT path FROM sys.traces WHERE is_default = 0 AND status = 1 AND path IS NOT NULL;"
        $reader = $cmd.ExecuteReader()
        while ($reader.Read())
        {
            # Reduce a running trace's current file (base.trc or base_7.trc)
            # to its base prefix so every rollover file of that trace is spared.
            $name   = [System.IO.Path]::GetFileName([string]$reader.GetValue(0))
            $prefix = $name -replace '\.trc$', '' -replace '_?\d+$', ''
            if ($prefix) { $activePrefixes.Add($prefix) }
        }
        $reader.Close()
    }
    catch
    {
        Write-Warning ("Could not read sys.traces (" + $_.Exception.Message + "). A running trace's files will be protected only by the in-use check.")
    }
}
catch
{
    Write-Warning ("Could not query SQL Server (" + $_.Exception.Message + ").")
}
finally
{
    if ($connection) { $connection.Dispose() }
}

if (-not $ErrorLogPath)
{
    throw "Could not determine the error log directory. Re-run with -ErrorLogPath pointing at the SQL Server error log folder."
}

if (-not (Test-Path -LiteralPath $ErrorLogPath))
{
    throw "Error log directory not found: $ErrorLogPath"
}

Write-Output "Error log directory : $ErrorLogPath"
if ($activePrefixes.Count -gt 0)
{
    Write-Output ("Running trace(s)    : " + ($activePrefixes -join ", ") + " (will be skipped)")
}

# --- Delete the orphaned trace files ---
$candidates = @(Get-ChildItem -LiteralPath $ErrorLogPath -Filter $filePattern -File -ErrorAction SilentlyContinue)
$deleted = 0
$skippedActive = 0
$skippedLocked = 0
$failed = 0

foreach ($file in $candidates)
{
    $belongsToActiveTrace = $false
    foreach ($prefix in $activePrefixes)
    {
        if ($file.Name.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase))
        {
            $belongsToActiveTrace = $true
            break
        }
    }
    if ($belongsToActiveTrace)
    {
        $skippedActive++
        continue
    }

    # Skip files held open (the active trace's current file, a reader, AV, etc.).
    try
    {
        $handle = [System.IO.File]::Open($file.FullName, "Open", "ReadWrite", "None")
        $handle.Close()
    }
    catch
    {
        $skippedLocked++
        continue
    }

    if ($PSCmdlet.ShouldProcess($file.FullName, "Delete orphaned trace file"))
    {
        try
        {
            Remove-Item -LiteralPath $file.FullName -Force -ErrorAction Stop
            $deleted++
        }
        catch
        {
            Write-Warning ("Could not delete " + $file.FullName + " (" + $_.Exception.Message + ").")
            $failed++
        }
    }
}

Write-Output ""
Write-Output "Trace files found       : $($candidates.Count)"
Write-Output "Deleted                 : $deleted"
Write-Output "Skipped (running trace) : $skippedActive"
Write-Output "Skipped (in use)        : $skippedLocked"
Write-Output "Failed                  : $failed"

if ($failed -gt 0)
{
    Write-Output ""
    Write-Output "Some files could not be deleted. Re-run this script as a local Administrator"
    Write-Output "or as the SQL Server service account, which owns the trace files."
}
