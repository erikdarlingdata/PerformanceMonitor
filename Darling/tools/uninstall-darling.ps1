<#
.SYNOPSIS
Uninstalls the PerformanceMonitor Darling service and removes the viewer shortcuts.

.DESCRIPTION
Run from an ELEVATED PowerShell. Stops and deletes the Windows service, removes the scoped Windows
Firewall rules it installed, removes the Desktop / Start Menu 'Darling Viewer' shortcuts, and removes any account's direct
grant that install-darling.ps1 gave when it was asked to add someone to the operator group (#6478; see below).
With -PurgeData it also removes the operator group itself when the install made it (#6953).
DELIBERATELY LEFT IN PLACE: the extracted program folder,
darling.json, and everything under %ProgramData%\PerformanceMonitorDarling (the PostgreSQL store
with all collected history, the DPAPI credential files, and the service logs) - so an uninstall
is reversible by simply re-running install-darling.ps1. To destroy the collected data too, pass
-PurgeData and answer the confirmation.

On a machine the MSI package installed (the HKLM InstalledBy = msi marker), a plain run refuses and changes nothing: remove it from Settings > Apps > Installed apps
or with msiexec /x, so Windows Installer stays the one owner (#6480). Only -MsiUninstall, which the package passes itself, goes on.

.PARAMETER PurgeData
ALSO delete %ProgramData%\PerformanceMonitorDarling - the entire PostgreSQL store (all collected
history), the generated store credentials, and the service logs. Irreversible, and it prompts. Once the store is deleted it also removes the local
operator group, but only when the install made it. A group an administrator made is left (#6953).

.PARAMETER MsiUninstall
For the MSI package's uninstall custom action only (#6480), which runs this before Windows Installer stops and removes the service.
Stops the service (waiting up to 90 seconds for a clean stop), removes the scoped firewall rules and any account's direct grant on
darling.json and the store, and leaves the rest to Windows Installer: the service is not deleted (the package's ServiceInstall row
does that) and the Darling Viewer shortcuts are not touched (the package owns them). Never asks a question: without -PurgeData
-ConfirmPurge the store is kept, as is darling.json. The package's folder and everything else this script leaves in place stay as
they are.

.PARAMETER ConfirmPurge
With -PurgeData: delete the store without the typed DELETE confirmation. For a caller that cannot answer (the MSI's PURGEDATA=1
passes -MsiUninstall -PurgeData -ConfirmPurge). Without it, -PurgeData asks as before, and under -MsiUninstall it keeps the store.
Does nothing without -PurgeData.
#>
[CmdletBinding()]
param(
    [switch]$PurgeData,
    [switch]$MsiUninstall,
    [switch]$ConfirmPurge
)

$ErrorActionPreference = 'Stop'
$serviceName = 'PerformanceMonitor Darling'

# The MSI package (#6480) writes HKLM\SOFTWARE\PerformanceMonitor Darling, value InstalledBy = msi, and removes it at uninstall. A
# machine that holds it belongs to Windows Installer: a script install over it would leave two owners for one service. Returns the
# refusal text, or $null when the marker is absent. The registry read is a parameter so a test can stand in for the machine; the
# default reads the 64-bit view, so a 32-bit PowerShell is not sent to WOW6432Node and does not miss the marker. Kept byte-identical
# in install-darling.ps1, upgrade-darling.ps1 and uninstall-darling.ps1.
function Get-DarlingMsiInstallRefusal([string]$ScriptName, [scriptblock]$ReadMarker = $null) {
    if (-not $ReadMarker) {
        $ReadMarker = {
            $base = [Microsoft.Win32.RegistryKey]::OpenBaseKey([Microsoft.Win32.RegistryHive]::LocalMachine, [Microsoft.Win32.RegistryView]::Registry64)
            try {
                $key = $base.OpenSubKey('SOFTWARE\PerformanceMonitor Darling')
                if ($key) { try { $key.GetValue('InstalledBy') } finally { $key.Dispose() } }
            }
            finally { $base.Dispose() }
        }
    }
    if ("$(& $ReadMarker)" -ne 'msi') { return $null }
    return @"
PerformanceMonitor Darling on this machine was installed by the MSI package, so $ScriptName will not change it: two installers
would then own one service.
  To update it: run the newer MSI. Windows Installer replaces the files in place and keeps darling.json and the store.
  To remove it: Settings > Apps > Installed apps > PerformanceMonitor Darling > Uninstall. That keeps the store unless you tick
  the box to remove it.
  To go back to script installs: uninstall the MSI as above, extract the zip into a fresh folder, and run install-darling.ps1 there.
Nothing was installed or changed.
"@
}

$identity = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $identity.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host 'ERROR: Run this script from an ELEVATED PowerShell.' -ForegroundColor Red
    exit 1
}

# A plain run on an MSI install would remove the service and the shortcuts but leave the marker, and the next install-darling.ps1 would
# then refuse; Windows Installer owns the removal, so refuse here as install and upgrade do (#6480). The package's own uninstall action
# passes -MsiUninstall and goes on.
if (-not $MsiUninstall) {
    $msiRefusal = Get-DarlingMsiInstallRefusal 'uninstall-darling.ps1'
    if ($msiRefusal) { Write-Host $msiRefusal -ForegroundColor Red; exit 1 }
}

$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if ($existing) {
    if ($existing.Status -ne 'Stopped') {
        Write-Host 'Stopping the service (a clean stop checkpoints the bundled store)...'
        Stop-Service -Name $serviceName -Force
        (Get-Service -Name $serviceName).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(90))
    }

    if ($MsiUninstall) {
        # Windows Installer deletes the service itself (its ServiceInstall row), after this script has stopped it (#6480).
        Write-Host "Service '$serviceName' is stopped; Windows Installer removes it."
    }
    else {
        & sc.exe delete $serviceName | Out-Null
        if ($LASTEXITCODE -ne 0) { Write-Host "sc delete failed ($LASTEXITCODE)." -ForegroundColor Red; exit 1 }
        Write-Host "Service '$serviceName' removed."
    }
}
else {
    Write-Host "Service '$serviceName' is not installed - nothing to remove."
}

# Scoped firewall rules (#1771). Created by install-darling.ps1 because the service account cannot create
# them; removed here for the same reason - nothing else ever will, and leaving an inbound allow rule behind
# for a service that no longer exists is exactly the litter an uninstall should clear.
#
# Matched by the shared DisplayName PREFIX rather than by rebuilding each port-specific name, so a store/MCP/
# web rule left over from a PREVIOUS port is removed too - reconstructing names from the current darling.json
# would strand those, and darling.json may not even be readable at this point. The C# side pins this literal
# against all three real rule-name builders (DarlingFirewallCheck.RuleNamePrefix), so a rename cannot silently
# orphan rules here. A wildcard that matches nothing is not an error (verified), but the try/catch keeps a
# firewall hiccup from aborting the rest of an uninstall under ErrorActionPreference = 'Stop'.
try {
    $rules = @(Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling *' -ErrorAction SilentlyContinue)
    if ($rules.Count -gt 0) {
        Remove-NetFirewallRule -DisplayName 'PerformanceMonitor Darling *' -ErrorAction Stop
        Write-Host "Removed $($rules.Count) scoped firewall rule(s)."
    }
    else {
        Write-Host 'No Darling firewall rules to remove.'
    }
}
catch {
    Write-Host "Could not remove the firewall rules ($($_.Exception.Message)). Remove them by hand:" -ForegroundColor Yellow
    Write-Host "  Remove-NetFirewallRule -DisplayName 'PerformanceMonitor Darling *'" -ForegroundColor Yellow
}

# The 'Darling Viewer' shortcut paths under the named special folders (#6475). A folder that does not resolve (GetFolderPath
# returns '' when there is no such folder for the account) is left out. The folder lookup is a parameter so a test can stand in
# for the machine. Kept byte-identical in install-darling.ps1 and uninstall-darling.ps1.
function Get-DarlingShortcutPaths([string[]]$Folders, [scriptblock]$GetFolder = $null) {
    if (-not $GetFolder) { $GetFolder = { param($name) [Environment]::GetFolderPath($name) } }
    $paths = @()
    foreach ($folder in $Folders) {
        $base = & $GetFolder $folder
        if ($base) { $paths += (Join-Path $base 'Darling Viewer.lnk') }
    }
    return $paths
}

# The all-users shortcuts the installer makes now (#6475), and the running account's own Desktop and Start Menu, where an
# older install put them.
# Under -MsiUninstall the package made the all-users shortcuts and removes them itself, so none is touched here (#6480).
foreach ($lnkPath in $(if ($MsiUninstall) { @() } else { Get-DarlingShortcutPaths @('CommonDesktopDirectory', 'CommonPrograms', 'Desktop', 'Programs') })) {
    if (Test-Path $lnkPath) {
        Remove-Item $lnkPath -Force
        Write-Host "Removed shortcut $lnkPath"
    }
}

$dataRoot = Join-Path ([Environment]::GetFolderPath('CommonApplicationData')) 'PerformanceMonitorDarling'

# Direct operator grants (#6478). When install-darling.ps1 is asked to add a person to the operator group it also gives that
# person a direct entry of the group's access, so the Viewer works in the session that is already open: read on darling.json
# and on the store's admin and viewer credential files, traverse on the store folder. The service drops such an entry when it
# next rewrites those permissions at a start, but these files are left in place by this uninstall and no service is left to
# rewrite them, so the entry would stay. Each of those four paths is protected and holds only SYSTEM, Administrators, the
# service account and the operator group, so any other entry written directly on it (not inherited) is removed, and nothing else
# is changed. The group itself and its members are left alone here. -PurgeData removes the group later, when the install made it (#6953).
function Remove-DarlingDirectOperatorGrants([string]$configPath, [string]$storeFolder, [string]$groupName) {
    $ErrorActionPreference = 'Continue'
    $keep = @('S-1-5-18', 'S-1-5-32-544')
    # The group's own SID is what keeps its entries. If it cannot be resolved, the loop below could not tell the group's
    # entries from a direct grant and would remove them, so nothing is removed at all.
    try { $keep += (New-Object System.Security.Principal.NTAccount($env:COMPUTERNAME, $groupName)).Translate([System.Security.Principal.SecurityIdentifier]).Value }
    catch {
        Write-Host "Could not resolve the operator group '$groupName' ($($_.Exception.Message)), so the direct grants were left in place on darling.json and the store folder." -ForegroundColor Yellow
        return
    }
    $paths = @($configPath)
    if ($storeFolder) {
        $paths += $storeFolder
        $paths += (Join-Path $storeFolder 'pg-admin-credential.dpapi')
        $paths += (Join-Path $storeFolder 'pg-viewer-credential.dpapi')
    }
    foreach ($path in $paths) {
        $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        # A link is never followed: icacls would change what it points at.
        if ($null -eq $item -or ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) { continue }
        try { $rules = @((Get-Acl -LiteralPath $path).Access) }
        catch { Write-Host "Could not read the permissions of $path ($($_.Exception.Message))." -ForegroundColor Yellow; continue }
        foreach ($rule in $rules) {
            if ($rule.IsInherited -or $rule.AccessControlType -ne 'Allow') { continue }
            try { $sid = $rule.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value } catch { continue }
            if ($keep -contains $sid -or $sid -like 'S-1-5-80-*') { continue }
            $null = & icacls.exe $path /remove:g "*$sid" 2>&1
            if ($LASTEXITCODE -eq 0) { Write-Host "Removed the direct grant for $($rule.IdentityReference) on $path." }
            else { Write-Host "Could not remove the direct grant for $($rule.IdentityReference) on $path (icacls exit $LASTEXITCODE). Run: icacls `"$path`" /remove:g *$sid" -ForegroundColor Yellow }
        }
    }
}

# The operator group itself (#6953). The installer and the upgrade script make the group when it is missing and give it this
# exact comment (install-darling.ps1, Initialize-DarlingOperatorGroup), so a group that still carries the comment was made by
# the install, and a group with any other comment is the administrator's own and stays. Only -PurgeData removes it: a plain
# uninstall keeps it so a reinstall gets back the same members. It is removed with its members still in it, because it only
# exists for this install. The next three functions are kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingNativeText($output) {
    (@($output) | ForEach-Object { if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" } } | Where-Object { $_ -and $_.Trim() }) -join ' '
}

# The Windows product type: 1 workstation, 2 domain controller, 3 server (Win32_OperatingSystem.ProductType), or 0 when it
# cannot be read. Its own function so a test can stand in for the machine. Kept byte-identical in install-darling.ps1 and
# upgrade-darling.ps1.
function Get-DarlingProductType {
    try { return [int](Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction Stop).ProductType }
    catch { return 0 }
}

# True on a domain controller. A domain controller has no local account database: a group made there with net localgroup
# is a domain group, which the machine-qualified lookup cannot find and which would exist for the whole domain. Nothing
# here creates or grants a local group on one. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Test-DarlingDomainController {
    return ((Get-DarlingProductType) -eq 2)
}

# The comment (description) of a local group, as the machine stores it, or $null when it has none. Read from Win32_Group, not
# from the text of net localgroup, which is translated. Its own function so a test can stand in for the machine.
function Get-DarlingLocalGroupComment([string]$groupName) {
    $group = Get-CimInstance -ClassName Win32_Group -Filter "LocalAccount=True AND Domain='$env:COMPUTERNAME' AND Name='$groupName'" -ErrorAction Stop
    if ($null -eq $group) { return $null }
    return [string]@($group)[0].Description
}

# Removes the local operator group when the install made it. Says what it did in one line and never throws, so a failure here
# does not stop the rest of the purge. net.exe, as the installer makes the group, judged by its exit code with the error
# preference local to this function (see Initialize-DarlingOperatorGroup in install-darling.ps1 for why).
function Remove-DarlingOperatorGroup([string]$groupName) {
    $ErrorActionPreference = 'Continue'
    # Must equal the comment Initialize-DarlingOperatorGroup in install-darling.ps1 gives the group it makes.
    $installComment = 'Members can run the PerformanceMonitor Darling Viewer and CLI without elevation (read access to darling.json and the store role credentials).'
    try {
        if (Test-DarlingDomainController) {
            Write-Host "This machine is a domain controller, which has no local groups, so the group '$groupName' was not looked up or removed."
            return
        }
        # Machine-qualified, so only this machine's own account database answers: an unqualified name is also looked up in the
        # domain, and a group of that name made there must never be the one removed.
        $account = New-Object System.Security.Principal.NTAccount($env:COMPUTERNAME, $groupName)
        try { $null = $account.Translate([System.Security.Principal.SecurityIdentifier]) }
        catch [System.Security.Principal.IdentityNotMappedException] { return }
        $comment = Get-DarlingLocalGroupComment $groupName
        if ($comment -cne $installComment) {
            Write-Host "Left the local group '$groupName' in place because the install did not create it. To remove it, run: net localgroup `"$groupName`" /delete"
            return
        }
        $output = & net.exe localgroup $groupName /delete 2>&1
        if ($LASTEXITCODE -eq 0) { Write-Host "Removed the local group '$groupName', which the install created." }
        else { Write-Host "Could not remove the local group '$groupName' (net exit $LASTEXITCODE`: $(Get-DarlingNativeText $output)). Run: net localgroup `"$groupName`" /delete" -ForegroundColor Yellow }
    }
    catch { Write-Host "Could not remove the local group '$groupName' ($($_.Exception.Message)). Run: net localgroup `"$groupName`" /delete" -ForegroundColor Yellow }
}

# The store folder: the parent of postgres.dataDirectory in darling.json (the default is under %ProgramData%), or none when the
# store is not managed. Read with a regex, not ConvertFrom-Json, which Windows PowerShell 5.1 fails on darling.json's comments.
$operatorConfigPath = Join-Path $PSScriptRoot 'darling.json'
$operatorStoreFolder = $dataRoot
if (Test-Path -LiteralPath $operatorConfigPath -PathType Leaf) {
    try {
        $configText = (Get-Content -LiteralPath $operatorConfigPath -Raw -ErrorAction Stop) -split "`r?`n" | ForEach-Object { $_ -replace '(?<!:)//.*$', '' }
        $configText = $configText -join "`n"
        $managed = [regex]::Matches($configText, '"managed"\s*:\s*(true|false)')
        $dataDirectory = [regex]::Matches($configText, '"dataDirectory"\s*:\s*"((?:[^"\\]|\\.)*)"')
        if ($managed.Count -gt 0 -and $managed[$managed.Count - 1].Groups[1].Value -eq 'false') { $operatorStoreFolder = $null }
        elseif ($dataDirectory.Count -gt 0 -and $dataDirectory[$dataDirectory.Count - 1].Groups[1].Value.Trim()) {
            $operatorStoreFolder = [System.IO.Path]::GetDirectoryName([System.IO.Path]::GetFullPath([regex]::Unescape($dataDirectory[$dataDirectory.Count - 1].Groups[1].Value)).TrimEnd('\'))
        }
    }
    catch { }
}
$operatorGroupName = 'PerformanceMonitor Darling Operators'
try { Remove-DarlingDirectOperatorGrants $operatorConfigPath $operatorStoreFolder $operatorGroupName }
catch { Write-Host "Could not check for direct operator grants ($($_.Exception.Message))." -ForegroundColor Yellow }

# Whether the store may be deleted. -ConfirmPurge is the answer already given; a caller that cannot be asked (-MsiUninstall) and gave
# none keeps the store; otherwise the person types DELETE, as before. The question is a parameter so a test can answer it.
function Test-DarlingPurgeConfirmed([bool]$ConfirmPurge, [bool]$CannotAsk, [scriptblock]$ReadAnswer = $null) {
    if ($ConfirmPurge) { return $true }
    if ($CannotAsk) { return $false }
    if (-not $ReadAnswer) { $ReadAnswer = { Read-Host "Type the word DELETE to confirm" } }
    return ((& $ReadAnswer) -ceq 'DELETE')
}

if ($PurgeData) {
    $purgeDone = $true
    if (Test-Path $dataRoot) {
        Write-Host ''
        Write-Host "-PurgeData will DELETE $dataRoot - the ENTIRE PostgreSQL store (all collected history)," -ForegroundColor Yellow
        Write-Host 'the generated store credentials, and the service logs. This cannot be undone.' -ForegroundColor Yellow
        if (Test-DarlingPurgeConfirmed ([bool]$ConfirmPurge) ([bool]$MsiUninstall)) {
            Remove-Item $dataRoot -Recurse -Force
            Write-Host 'Store, credentials, and logs deleted.'
        }
        else {
            $purgeDone = $false
            Write-Host 'Purge NOT confirmed - data left in place.'
        }
    }
    # The operator group goes with a purge that went ahead (#6953), after the grants above are removed. A purge that was not
    # confirmed leaves the store, so it leaves the group too.
    if ($purgeDone) { Remove-DarlingOperatorGroup $operatorGroupName }
}
elseif ($MsiUninstall) {
    Write-Host ''
    Write-Host "Left in place (installing the MSI again restores the install):"
    Write-Host "  - darling.json beside the program files"
    Write-Host "  - $dataRoot (the store with all collected history, credentials, logs)"
}
else {
    Write-Host ''
    Write-Host "Left in place (re-running install-darling.ps1 fully restores the install):"
    Write-Host "  - this program folder and its darling.json"
    Write-Host "  - $dataRoot (the store with all collected history, credentials, logs)"
}
