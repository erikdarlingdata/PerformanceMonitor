namespace Installer.Core.Models;

/// <summary>
/// Information about an applicable upgrade.
/// </summary>
public class UpgradeInfo
{
    public string Path { get; set; } = string.Empty;
    public string FolderName { get; set; } = string.Empty;
    public Version? FromVersion { get; set; }
    public Version? ToVersion { get; set; }
}
