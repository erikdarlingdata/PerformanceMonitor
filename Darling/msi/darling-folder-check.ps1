# #6480: the install folder check the package runs as SYSTEM right before DarlingPreStart (Package.wxs).
#
# DarlingPreStart runs [INSTALLFOLDER]install-darling.ps1 as SYSTEM, and that script's own step 1a (no account outside
# SYSTEM, Administrators and TrustedInstaller can write the install tree) is read from the same folder it checks, so it
# cannot vouch for the file it is in. This check applies the same rule before that, from code the package carries:
# New-DarlingFolderCheck.ps1 copies step 1a's functions out of the install-darling.ps1 being packaged (the same file
# installed in the folder), puts this file after them, and the package carries the result in its own command line
# (-EncodedCommand of a loader that holds it gzipped). So the rule is step 1a's own code, never a second copy of it, and nothing it runs is
# read from INSTALLFOLDER.
#
# Not runnable on its own: Get-UntrustedWriteGrantees and the functions it calls come first in the generated script.
#
# That command line takes no arguments, so the folder is read from the marker value this install wrote a moment earlier
# (WriteRegistryValues runs before InstallServices, and this check after it): HKLM\SOFTWARE\PerformanceMonitor Darling,
# InstallFolder, which is [INSTALLFOLDER]. Only administrators can write that key.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$serviceName = 'PerformanceMonitor Darling'

$folder = try { (Get-ItemProperty -LiteralPath 'Registry::HKEY_LOCAL_MACHINE\SOFTWARE\PerformanceMonitor Darling' -Name InstallFolder -ErrorAction Stop).InstallFolder } catch { $null }
if (-not $folder) { Fail 'Setup could not read the install folder from HKLM\SOFTWARE\PerformanceMonitor Darling (InstallFolder), so it did not run install-darling.ps1.' }

# The same inputs step 1a gives the same functions: the folder in $PSScriptRoot's spelling (no trailing backslash),
# darling.json in it, and the service record ServiceInstall has just written.
$root = $folder.TrimEnd('\')
$configPath = Join-Path $root 'darling.json'
$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
$preLockTrusted = Get-DarlingPreLockTrustedSidsForRerun $serviceName $existing
$records = @(Get-UntrustedWriteGrantees $root $preLockTrusted.Trusted -AsRecords -Recurse -ServiceSids $preLockTrusted.ServiceSids -ServiceDirectories @(Get-DarlingExtraServiceWriteDirectories $root $configPath))
if ($records.Count -eq 0) {
    Write-Host "Install folder check passed: no account outside SYSTEM, Administrators and TrustedInstaller can change files in $root."
    exit 0
}

# The accounts, by name, from the findings that name one (a write grant or an owner).
$accounts = @(@(foreach ($record in $records) {
    if ($record.Kind -eq 'Grant') { $record.Text.Substring(0, $record.Text.Length - " on $($record.Path)".Length) }
    elseif ($record.Kind -eq 'Owner' -and $record.Text -match '\(owned by (.+)\)$') { $Matches[1] }
}) | Sort-Object -Unique)
$who = if ($accounts.Count -gt 0) { $accounts -join ', ' } else { 'none by name; see the list below' }
$lines = ($records | Select-Object -First 20 | ForEach-Object { "  $($_.Text)" }) -join "`n"
$more = if ($records.Count -gt 20) { "`n  ...and $($records.Count - 20) more." } else { '' }
Fail @"
Setup stopped before running install-darling.ps1: accounts other than SYSTEM, Administrators and TrustedInstaller can change files in the install folder $root.
Accounts that can write there: $who
$lines$more
A folder made directly under C:\ gets this access from the drive itself, which gives Authenticated Users Modify.
To fix it, install into a folder under Program Files (the default), or remove those permissions from $root and run setup again. Setup rolled back this install.
"@
