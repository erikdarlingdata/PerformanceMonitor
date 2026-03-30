namespace Installer.Core.Models;

/// <summary>
/// Result codes for installation operations.
/// Maps to CLI exit codes for backward compatibility.
/// </summary>
public enum InstallationResultCode
{
    Success = 0,
    InvalidArguments = 1,
    ConnectionFailed = 2,
    CriticalScriptFailed = 3,
    PartialInstallation = 4,
    VersionCheckFailed = 5,
    SqlFilesNotFound = 6,
    UninstallFailed = 7,
    UpgradesFailed = 8
}
