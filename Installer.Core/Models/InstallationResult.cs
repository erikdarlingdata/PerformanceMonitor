namespace Installer.Core.Models;

/// <summary>
/// Installation result summary.
/// </summary>
public class InstallationResult
{
    public bool Success { get; set; }
    public int FilesSucceeded { get; set; }
    public int FilesFailed { get; set; }
    public List<(string FileName, string ErrorMessage)> Errors { get; } = new();
    public List<(string Message, string Status)> LogMessages { get; } = new();
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public string? ReportPath { get; set; }
}
