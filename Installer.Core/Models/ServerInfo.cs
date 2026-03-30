namespace Installer.Core.Models;

/// <summary>
/// Server information returned from connection test.
/// </summary>
public class ServerInfo
{
    public string ServerName { get; set; } = string.Empty;
    public string SqlServerVersion { get; set; } = string.Empty;
    public string SqlServerEdition { get; set; } = string.Empty;
    public bool IsConnected { get; set; }
    public string? ErrorMessage { get; set; }
    public int EngineEdition { get; set; }
    public int ProductMajorVersion { get; set; }

    /// <summary>
    /// Returns true if the SQL Server version is supported (2016+).
    /// Only checked for on-prem Standard (2) and Enterprise (3).
    /// Azure MI (8) is always current and skips the check.
    /// </summary>
    public bool IsSupportedVersion =>
        EngineEdition is 8 || ProductMajorVersion >= 13;

    /// <summary>
    /// Human-readable version name for error messages.
    /// </summary>
    public string ProductMajorVersionName => ProductMajorVersion switch
    {
        11 => "SQL Server 2012",
        12 => "SQL Server 2014",
        13 => "SQL Server 2016",
        14 => "SQL Server 2017",
        15 => "SQL Server 2019",
        16 => "SQL Server 2022",
        17 => "SQL Server 2025",
        _ => $"SQL Server (version {ProductMajorVersion})"
    };
}
