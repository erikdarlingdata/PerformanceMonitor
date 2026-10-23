<#
.SYNOPSIS
Uninstalls the PerformanceMonitor Darling service and removes the viewer shortcuts.

.DESCRIPTION
Run from an ELEVATED PowerShell. Stops and deletes the Windows service, removes the scoped Windows
Firewall rules it installed, and removes the Desktop / Start Menu 'Darling Viewer' shortcuts.
DELIBERATELY LEFT IN PLACE: the extracted program folder,
darling.json, and everything under %ProgramData%\PerformanceMonitorDarling (the PostgreSQL store
with all collected history, the DPAPI credential files, and the service logs) - so an uninstall
is reversible by simply re-running install-darling.ps1. To destroy the collected data too, pass
-PurgeData and answer the confirmation.

.PARAMETER PurgeData
ALSO delete %ProgramData%\PerformanceMonitorDarling - the entire PostgreSQL store (all collected
history), the generated store credentials, and the service logs. Irreversible; prompts.
#>
[CmdletBinding()]
param(
    [switch]$PurgeData
)

$ErrorActionPreference = 'Stop'
$serviceName = 'PerformanceMonitor Darling'

$identity = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $identity.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host 'ERROR: Run this script from an ELEVATED PowerShell.' -ForegroundColor Red
    exit 1
}

$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if ($existing) {
    if ($existing.Status -ne 'Stopped') {
        Write-Host 'Stopping the service (a clean stop checkpoints the bundled store)...'
        Stop-Service -Name $serviceName -Force
        (Get-Service -Name $serviceName).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(90))
    }

    & sc.exe delete $serviceName | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Host "sc delete failed ($LASTEXITCODE)." -ForegroundColor Red; exit 1 }
    Write-Host "Service '$serviceName' removed."
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
foreach ($lnkPath in (Get-DarlingShortcutPaths @('CommonDesktopDirectory', 'CommonPrograms', 'Desktop', 'Programs'))) {
    if (Test-Path $lnkPath) {
        Remove-Item $lnkPath -Force
        Write-Host "Removed shortcut $lnkPath"
    }
}

$dataRoot = Join-Path ([Environment]::GetFolderPath('CommonApplicationData')) 'PerformanceMonitorDarling'
if ($PurgeData) {
    if (Test-Path $dataRoot) {
        Write-Host ''
        Write-Host "-PurgeData will DELETE $dataRoot - the ENTIRE PostgreSQL store (all collected history)," -ForegroundColor Yellow
        Write-Host 'the generated store credentials, and the service logs. This cannot be undone.' -ForegroundColor Yellow
        $answer = Read-Host "Type the word DELETE to confirm"
        if ($answer -ceq 'DELETE') {
            Remove-Item $dataRoot -Recurse -Force
            Write-Host 'Store, credentials, and logs deleted.'
        }
        else {
            Write-Host 'Purge NOT confirmed - data left in place.'
        }
    }
}
else {
    Write-Host ''
    Write-Host "Left in place (re-running install-darling.ps1 fully restores the install):"
    Write-Host "  - this program folder and its darling.json"
    Write-Host "  - $dataRoot (the store with all collected history, credentials, logs)"
}
