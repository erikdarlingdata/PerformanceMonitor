namespace Installer.Core.Models;

/// <summary>
/// Progress information for installation steps.
/// </summary>
public class InstallationProgress
{
    public string Message { get; set; } = string.Empty;
    public string Status { get; set; } = "Info"; // Info, Success, Error, Warning
    public int? CurrentStep { get; set; }
    public int? TotalSteps { get; set; }
    public int? ProgressPercent { get; set; }
}
