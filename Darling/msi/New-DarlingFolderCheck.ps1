<#
.SYNOPSIS
    #6480: builds the install folder check the package runs before DarlingPreStart (see darling-folder-check.ps1).
.DESCRIPTION
    Copies Get-UntrustedWriteGrantees, Get-DarlingPreLockTrustedSidsForRerun and Get-DarlingExtraServiceWriteDirectories,
    with every function of the script they call, out of the install-darling.ps1 being packaged, drops their comments,
    puts darling-folder-check.ps1 after them, and writes the result twice: as a .ps1 to read, and as a WiX include that
    defines DarlingFolderCheckCommand, the -EncodedCommand argument of a loader that decompresses that text and runs
    it. The build fails when a function is missing, the
    result does not parse, or the command line would be too long for Windows.
#>
param(
    [Parameter(Mandatory)][string]$InstallScript,
    [Parameter(Mandatory)][string]$Driver,
    [Parameter(Mandatory)][string]$OutDir
)
$ErrorActionPreference = 'Stop'
$language = [System.Management.Automation.Language.Parser]

function Read-Ast([string]$path, [ref]$tokens) {
    $errors = $null
    $ast = $language::ParseFile($path, $tokens, [ref]$errors)
    if ($errors) { throw "$path does not parse: $($errors[0].Message)" }
    return $ast
}

$tokens = $null
$ast = Read-Ast $InstallScript ([ref]$tokens)
$defined = @{}
foreach ($statement in $ast.EndBlock.Statements) {
    if ($statement -is [System.Management.Automation.Language.FunctionDefinitionAst]) { $defined[$statement.Name] = $statement }
}

# The roots are what step 1a calls (and darling-folder-check.ps1 calls the same three); the closure is every function of the
# script any of them calls, found from the code, never listed by hand, so a helper step 1a gains is carried too.
$roots = 'Get-UntrustedWriteGrantees', 'Get-DarlingPreLockTrustedSidsForRerun', 'Get-DarlingExtraServiceWriteDirectories', 'Fail'
$queue = New-Object System.Collections.Generic.Queue[string]
foreach ($name in $roots) {
    if (-not $defined.ContainsKey($name)) { throw "$InstallScript has no function $name, which the install folder check is built from." }
    $queue.Enqueue($name)
}
$taken = New-Object System.Collections.Generic.List[string]
while ($queue.Count -gt 0) {
    $name = $queue.Dequeue()
    if ($taken.Contains($name)) { continue }
    $taken.Add($name)
    foreach ($call in $defined[$name].FindAll({ param($node) $node -is [System.Management.Automation.Language.CommandAst] }, $true)) {
        $called = $call.GetCommandName()
        if ($called -and $defined.ContainsKey($called) -and -not $taken.Contains($called)) { $queue.Enqueue($called) }
    }
}

# Each function's text without its comments, cut out by the parser's own comment tokens (a # inside a string or a
# here-string is not one), in the order the script defines them.
function Remove-Comments([string]$text, [int]$offset) {
    $builder = New-Object System.Text.StringBuilder $text
    $comments = @($tokens | Where-Object { $_.Kind -eq 'Comment' -and $_.Extent.StartOffset -ge $offset -and $_.Extent.EndOffset -le ($offset + $text.Length) } | Sort-Object { $_.Extent.StartOffset } -Descending)
    foreach ($comment in $comments) { [void]$builder.Remove($comment.Extent.StartOffset - $offset, $comment.Extent.EndOffset - $comment.Extent.StartOffset) }
    return (($builder.ToString() -split "`r?`n" | Where-Object { $_.Trim() -ne '' } | ForEach-Object { $_.TrimEnd() }) -join "`n")
}
$parts = foreach ($definition in ($defined.Values | Where-Object { $taken.Contains($_.Name) } | Sort-Object { $_.Extent.StartOffset })) {
    Remove-Comments $definition.Extent.Text $definition.Extent.StartOffset
}

$driverTokens = $null
$driverAst = Read-Ast $Driver ([ref]$driverTokens)
$tokens = $driverTokens
$driverText = Remove-Comments $driverAst.Extent.Text $driverAst.Extent.StartOffset
$script = ((@($parts) + $driverText) -join "`n") + "`n"

$checkErrors = $null
[void]$language::ParseInput($script, [ref]$null, [ref]$checkErrors)
if ($checkErrors) { throw "The generated install folder check does not parse: $($checkErrors[0].Message)" }

# -EncodedCommand of the script itself would be about 52,000 characters (UTF-16, then base64), over the 32,767 a
# command line can hold. So the script is gzipped and base64-encoded into a one-line loader that decompresses and runs
# it, and the loader is what -EncodedCommand carries. The command line then holds only base64 characters, which
# neither Windows Installer's formatting ([ ] { }) nor WiX's preprocessor ($( )) reads as anything of theirs.
$memory = New-Object System.IO.MemoryStream
$gzip = New-Object System.IO.Compression.GZipStream($memory, [System.IO.Compression.CompressionMode]::Compress)
$bytes = (New-Object System.Text.UTF8Encoding $false).GetBytes($script)
$gzip.Write($bytes, 0, $bytes.Length)
$gzip.Dispose()
$compressed = [Convert]::ToBase64String($memory.ToArray())
# Under -EncodedCommand with its output redirected, Windows PowerShell also writes progress and Write-Host's records to
# stderr as CLIXML, and WixQuietExec would put that in the MSI log. Progress is turned off first, and 6>&1 sends
# Write-Host's lines to the output as plain text. The exit code is the script's own.
$loader = "`$ProgressPreference = 'SilentlyContinue'; `$s = New-Object System.IO.StreamReader(New-Object System.IO.Compression.GZipStream((New-Object System.IO.MemoryStream(,[Convert]::FromBase64String('$compressed'))), [System.IO.Compression.CompressionMode]::Decompress)); & ([scriptblock]::Create(`$s.ReadToEnd())) 6>&1"
$encoded = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($loader))
# The rest of the command line (the quoted powershell.exe path and the switches) is under 200 characters.
if ($encoded.Length -gt 32500) { throw "The generated install folder check is $($encoded.Length) characters encoded, over the 32500 a command line can carry with the rest of it. Shorten it." }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
[System.IO.File]::WriteAllText((Join-Path $OutDir 'darling-folder-check.generated.ps1'), $script, (New-Object System.Text.UTF8Encoding $false))
$include = "<?xml version=`"1.0`" encoding=`"utf-8`"?>`n<!-- Generated by New-DarlingFolderCheck.ps1 from install-darling.ps1 and darling-folder-check.ps1. Do not edit. -->`n<Include>`n  <?define DarlingFolderCheckCommand =`"$encoded`" ?>`n</Include>`n"
[System.IO.File]::WriteAllText((Join-Path $OutDir 'DarlingFolderCheck.wxi'), $include, (New-Object System.Text.UTF8Encoding $false))
Write-Host "Install folder check: $($taken.Count) functions from $InstallScript ($($taken -join ', ')), $($script.Length) characters, $($encoded.Length) encoded."
