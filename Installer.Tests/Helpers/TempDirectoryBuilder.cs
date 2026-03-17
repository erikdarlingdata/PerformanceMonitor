namespace Installer.Tests.Helpers;

/// <summary>
/// Creates temporary directory structures mimicking the installer's
/// install/ and upgrades/ layout for unit testing file discovery and upgrade ordering.
/// </summary>
public sealed class TempDirectoryBuilder : IDisposable
{
    public string RootPath { get; }
    public string InstallPath => Path.Combine(RootPath, "install");
    public string UpgradesPath => Path.Combine(RootPath, "upgrades");

    public TempDirectoryBuilder()
    {
        RootPath = Path.Combine(Path.GetTempPath(), $"pm_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(RootPath);
    }

    /// <summary>
    /// Creates an install/ directory with the given SQL file names (content is dummy SQL).
    /// </summary>
    public TempDirectoryBuilder WithInstallFiles(params string[] fileNames)
    {
        Directory.CreateDirectory(InstallPath);
        foreach (var name in fileNames)
        {
            File.WriteAllText(Path.Combine(InstallPath, name), $"-- {name}");
        }
        return this;
    }

    /// <summary>
    /// Creates an upgrades/{from}-to-{to}/ directory with an upgrade.txt listing the given scripts.
    /// </summary>
    public TempDirectoryBuilder WithUpgrade(string fromVersion, string toVersion, params string[] scriptNames)
    {
        var folderName = $"{fromVersion}-to-{toVersion}";
        var upgradePath = Path.Combine(UpgradesPath, folderName);
        Directory.CreateDirectory(upgradePath);

        File.WriteAllLines(
            Path.Combine(upgradePath, "upgrade.txt"),
            scriptNames);

        foreach (var name in scriptNames)
        {
            File.WriteAllText(Path.Combine(upgradePath, name), $"-- {name}");
        }

        return this;
    }

    /// <summary>
    /// Creates an upgrades folder with an invalid (non-version) name.
    /// </summary>
    public TempDirectoryBuilder WithMalformedUpgradeFolder(string folderName)
    {
        Directory.CreateDirectory(Path.Combine(UpgradesPath, folderName));
        return this;
    }

    /// <summary>
    /// Creates an upgrade folder WITHOUT an upgrade.txt file.
    /// </summary>
    public TempDirectoryBuilder WithUpgradeNoManifest(string fromVersion, string toVersion)
    {
        var folderName = $"{fromVersion}-to-{toVersion}";
        Directory.CreateDirectory(Path.Combine(UpgradesPath, folderName));
        return this;
    }

    /// <summary>
    /// Adds a non-SQL file to the install directory.
    /// </summary>
    public TempDirectoryBuilder WithNonSqlFile(string fileName)
    {
        Directory.CreateDirectory(InstallPath);
        File.WriteAllText(Path.Combine(InstallPath, fileName), "not sql");
        return this;
    }

    /// <summary>
    /// Returns install files filtered the same way the real installer does:
    /// matching pattern, excluding 00_/97_/99_, sorted alphabetically.
    /// Returns full paths suitable for passing to ExecuteInstallationAsync.
    /// </summary>
    public List<string> GetFilteredInstallFiles()
    {
        if (!Directory.Exists(InstallPath))
            return [];

        var pattern = new System.Text.RegularExpressions.Regex(@"^\d{2}[a-z]?_.*\.sql$");
        return Directory.GetFiles(InstallPath, "*.sql")
            .Where(f =>
            {
                var name = Path.GetFileName(f);
                if (!pattern.IsMatch(name)) return false;
                if (name.StartsWith("00_", StringComparison.Ordinal) ||
                    name.StartsWith("97_", StringComparison.Ordinal) ||
                    name.StartsWith("99_", StringComparison.Ordinal))
                    return false;
                return true;
            })
            .OrderBy(f => Path.GetFileName(f))
            .ToList();
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(RootPath))
                Directory.Delete(RootPath, recursive: true);
        }
        catch
        {
            // Best effort cleanup
        }
    }
}
