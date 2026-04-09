using System.Reflection;
using System.Text;
using Installer.Core.Models;

namespace Installer.Core;

/// <summary>
/// Identifies an SQL installation script.
/// </summary>
public record ScriptFile(string Name, string Identifier);

/// <summary>
/// Abstracts the source of SQL installation and upgrade scripts.
/// FileSystem mode reads from install/ and upgrades/ directories (CLI, GUI).
/// Embedded mode reads from assembly resources (Dashboard).
/// </summary>
public abstract class ScriptProvider
{
    /// <summary>
    /// Create a provider that reads scripts from the filesystem.
    /// </summary>
    public static ScriptProvider FromDirectory(string monitorRootDirectory)
        => new FileSystemScriptProvider(monitorRootDirectory);

    /// <summary>
    /// Create a provider that reads scripts from embedded assembly resources.
    /// </summary>
    public static ScriptProvider FromEmbeddedResources(Assembly? assembly = null)
        => new EmbeddedResourceScriptProvider(assembly ?? typeof(ScriptProvider).Assembly);

    /// <summary>
    /// Auto-discover: search filesystem starting from CWD and executable directory,
    /// walking up to 5 parent directories. Falls back to embedded resources.
    /// </summary>
    /// <param name="log">Optional logging callback for diagnostics.</param>
    public static ScriptProvider AutoDiscover(Action<string>? log = null)
    {
        var startDirs = new[] { Directory.GetCurrentDirectory(), AppDomain.CurrentDomain.BaseDirectory }
            .Distinct()
            .ToList();

        log?.Invoke($"AutoDiscover: searching from [{string.Join(", ", startDirs)}]");

        foreach (string startDir in startDirs)
        {
            DirectoryInfo? searchDir = new DirectoryInfo(startDir);
            for (int i = 0; i < 6 && searchDir != null; i++)
            {
                string installFolder = Path.Combine(searchDir.FullName, "install");
                if (Directory.Exists(installFolder))
                {
                    var sqlFiles = Directory.GetFiles(installFolder, "*.sql")
                        .Where(f => Patterns.SqlFilePattern().IsMatch(Path.GetFileName(f)))
                        .ToList();
                    if (sqlFiles.Count > 0)
                    {
                        log?.Invoke($"AutoDiscover: found {sqlFiles.Count} scripts in {installFolder}");
                        return new FileSystemScriptProvider(searchDir.FullName);
                    }
                }

                var rootFiles = Directory.GetFiles(searchDir.FullName, "*.sql")
                    .Where(f => Patterns.SqlFilePattern().IsMatch(Path.GetFileName(f)))
                    .ToList();
                if (rootFiles.Count > 0)
                {
                    log?.Invoke($"AutoDiscover: found {rootFiles.Count} scripts in {searchDir.FullName}");
                    return new FileSystemScriptProvider(searchDir.FullName);
                }

                log?.Invoke($"AutoDiscover: no scripts in {searchDir.FullName}, trying parent");
                searchDir = searchDir.Parent;
            }
        }

        log?.Invoke("AutoDiscover: no filesystem scripts found, falling back to embedded resources");
        return FromEmbeddedResources();
    }

    /// <summary>
    /// Returns the filtered, sorted list of install scripts (excludes 00_/97_/99_).
    /// </summary>
    public abstract List<ScriptFile> GetInstallFiles();

    /// <summary>
    /// Reads the content of an install script.
    /// </summary>
    public abstract string ReadScript(ScriptFile file);

    /// <summary>
    /// Reads the content of an install script asynchronously.
    /// </summary>
    public abstract Task<string> ReadScriptAsync(ScriptFile file, CancellationToken cancellationToken = default);

    /// <summary>
    /// Finds applicable upgrades from currentVersion to targetVersion.
    /// Returns empty list if currentVersion is null (clean install) or no upgrades apply.
    /// </summary>
    /// <param name="currentVersion">Currently installed version, or null for clean install.</param>
    /// <param name="targetVersion">Target version to upgrade to.</param>
    /// <param name="onWarning">Optional callback for warnings (e.g., missing upgrade.txt).</param>
    public abstract List<UpgradeInfo> GetApplicableUpgrades(
        string? currentVersion,
        string targetVersion,
        Action<string>? onWarning = null);

    /// <summary>
    /// Reads the upgrade manifest (upgrade.txt) for a given upgrade.
    /// Returns script names in execution order, skipping comments and blank lines.
    /// </summary>
    public abstract List<string> GetUpgradeManifest(UpgradeInfo upgrade);

    /// <summary>
    /// Reads an upgrade script's content.
    /// </summary>
    public abstract string ReadUpgradeScript(UpgradeInfo upgrade, string scriptName);

    /// <summary>
    /// Reads an upgrade script's content asynchronously.
    /// </summary>
    public abstract Task<string> ReadUpgradeScriptAsync(
        UpgradeInfo upgrade, string scriptName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns true if the given upgrade script file exists.
    /// </summary>
    public abstract bool UpgradeScriptExists(UpgradeInfo upgrade, string scriptName);

    /// <summary>
    /// Returns the content of the troubleshooting script (99_installer_troubleshooting.sql), or null if not found.
    /// </summary>
    public abstract string? ReadTroubleshootingScript();

    /// <summary>
    /// Core upgrade-discovery logic shared by both providers.
    /// </summary>
    protected static List<UpgradeInfo> FilterUpgrades(
        IEnumerable<UpgradeInfo> candidates,
        string? currentVersion,
        string targetVersion)
    {
        if (currentVersion == null)
            return [];

        if (!Version.TryParse(currentVersion, out var currentRaw))
            return [];
        var current = new Version(currentRaw.Major, currentRaw.Minor, currentRaw.Build);

        if (!Version.TryParse(targetVersion, out var targetRaw))
            return [];
        var target = new Version(targetRaw.Major, targetRaw.Minor, targetRaw.Build);

        return candidates
            .Where(x => x.FromVersion != null && x.ToVersion != null)
            .Where(x => x.ToVersion > current)
            .Where(x => x.ToVersion <= target)
            .OrderBy(x => x.FromVersion)
            .ToList();
    }

    /// <summary>
    /// Parses an upgrade folder name like "1.2.0-to-1.3.0" into an UpgradeInfo.
    /// Returns null if the name doesn't match the expected pattern.
    /// </summary>
    protected static UpgradeInfo? ParseUpgradeFolderName(string folderName, string path)
    {
        if (!folderName.Contains("-to-", StringComparison.Ordinal))
            return null;

        var parts = folderName.Split("-to-");
        var from = Version.TryParse(parts[0], out var f) ? f : null;
        var to = parts.Length > 1 && Version.TryParse(parts[1], out var t) ? t : null;

        if (from == null || to == null)
            return null;

        return new UpgradeInfo
        {
            Path = path,
            FolderName = folderName,
            FromVersion = from,
            ToVersion = to
        };
    }

    /// <summary>
    /// Parses upgrade.txt content into a list of script names.
    /// </summary>
    protected static List<string> ParseUpgradeManifest(IEnumerable<string> lines)
    {
        return lines
            .Where(line => !string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith('#'))
            .Select(line => line.Trim())
            .ToList();
    }
}

/// <summary>
/// Reads scripts from the filesystem (install/ and upgrades/ directories).
/// </summary>
internal sealed class FileSystemScriptProvider : ScriptProvider
{
    private readonly string _rootDirectory;
    private readonly string _sqlDirectory;

    public FileSystemScriptProvider(string monitorRootDirectory)
    {
        _rootDirectory = monitorRootDirectory;

        string installFolder = Path.Combine(monitorRootDirectory, "install");
        _sqlDirectory = Directory.Exists(installFolder) ? installFolder : monitorRootDirectory;
    }

    public override List<ScriptFile> GetInstallFiles()
    {
        if (!Directory.Exists(_sqlDirectory))
            return [];

        return Directory.GetFiles(_sqlDirectory, "*.sql")
            .Select(f => new ScriptFile(Path.GetFileName(f), f))
            .Where(f => Patterns.SqlFilePattern().IsMatch(f.Name))
            .Where(f => !Patterns.ExcludedPrefixes.Any(p => f.Name.StartsWith(p, StringComparison.Ordinal)))
            .OrderBy(f => f.Name, StringComparer.Ordinal)
            .ToList();
    }

    public override string ReadScript(ScriptFile file) =>
        File.ReadAllText(file.Identifier);

    public override Task<string> ReadScriptAsync(ScriptFile file, CancellationToken cancellationToken = default) =>
        File.ReadAllTextAsync(file.Identifier, cancellationToken);

    public override List<UpgradeInfo> GetApplicableUpgrades(
        string? currentVersion,
        string targetVersion,
        Action<string>? onWarning = null)
    {
        string upgradesDir = Path.Combine(_rootDirectory, "upgrades");
        if (!Directory.Exists(upgradesDir))
            return [];

        var allFolders = Directory.GetDirectories(upgradesDir)
            .Select(d => ParseUpgradeFolderName(Path.GetFileName(d), d))
            .Where(x => x != null)
            .Cast<UpgradeInfo>()
            .ToList();

        var filtered = FilterUpgrades(allFolders, currentVersion, targetVersion);

        var result = new List<UpgradeInfo>();
        foreach (var upgrade in filtered)
        {
            string manifestPath = Path.Combine(upgrade.Path, "upgrade.txt");
            if (File.Exists(manifestPath))
            {
                result.Add(upgrade);
            }
            else
            {
                onWarning?.Invoke($"Upgrade folder '{upgrade.FolderName}' has no upgrade.txt — skipped");
            }
        }
        return result;
    }

    public override List<string> GetUpgradeManifest(UpgradeInfo upgrade)
    {
        string manifestPath = Path.Combine(upgrade.Path, "upgrade.txt");
        return ParseUpgradeManifest(File.ReadAllLines(manifestPath));
    }

    public override string ReadUpgradeScript(UpgradeInfo upgrade, string scriptName) =>
        File.ReadAllText(Path.Combine(upgrade.Path, scriptName));

    public override Task<string> ReadUpgradeScriptAsync(
        UpgradeInfo upgrade, string scriptName, CancellationToken cancellationToken = default) =>
        File.ReadAllTextAsync(Path.Combine(upgrade.Path, scriptName), cancellationToken);

    public override bool UpgradeScriptExists(UpgradeInfo upgrade, string scriptName) =>
        File.Exists(Path.Combine(upgrade.Path, scriptName));

    public override string? ReadTroubleshootingScript()
    {
        string path = Path.Combine(_sqlDirectory, "99_installer_troubleshooting.sql");
        return File.Exists(path) ? File.ReadAllText(path) : null;
    }
}

/// <summary>
/// Reads scripts from embedded assembly resources.
/// Resource names follow: {AssemblyName}.Resources.install.{filename}
/// and {AssemblyName}.Resources.upgrades.{from}-to-{to}.{filename}
/// </summary>
internal sealed class EmbeddedResourceScriptProvider : ScriptProvider
{
    private readonly Assembly _assembly;
    private readonly string _resourcePrefix;

    public EmbeddedResourceScriptProvider(Assembly assembly)
    {
        _assembly = assembly;
        _resourcePrefix = assembly.GetName().Name ?? "Installer.Core";
    }

    public override List<ScriptFile> GetInstallFiles()
    {
        string installPrefix = $"{_resourcePrefix}.Resources.install.";

        return _assembly.GetManifestResourceNames()
            .Where(r => r.StartsWith(installPrefix, StringComparison.Ordinal))
            .Select(r => new ScriptFile(
                Name: r[installPrefix.Length..],
                Identifier: r))
            .Where(f => Patterns.SqlFilePattern().IsMatch(f.Name))
            .Where(f => !Patterns.ExcludedPrefixes.Any(p => f.Name.StartsWith(p, StringComparison.Ordinal)))
            .OrderBy(f => f.Name, StringComparer.Ordinal)
            .ToList();
    }

    public override string ReadScript(ScriptFile file) =>
        ReadResource(file.Identifier);

    public override Task<string> ReadScriptAsync(ScriptFile file, CancellationToken cancellationToken = default) =>
        Task.FromResult(ReadResource(file.Identifier));

    public override List<UpgradeInfo> GetApplicableUpgrades(
        string? currentVersion,
        string targetVersion,
        Action<string>? onWarning = null)
    {
        string upgradesPrefix = $"{_resourcePrefix}.Resources.upgrades.";

        /*
        MSBuild mangles embedded resource names: folder "2.2.0-to-2.3.0" becomes
        "_2._2._0_to_2._3._0" (dots → namespace separators, hyphens → underscores,
        digit-leading segments → underscore prefix). Extract the mangled name and
        recover the original for version parsing. Store mangled name in Path for
        resource lookups; original in FolderName for display/version parsing.
        */
        var mangledNames = _assembly.GetManifestResourceNames()
            .Where(r => r.StartsWith(upgradesPrefix, StringComparison.Ordinal))
            .Select(r => r[upgradesPrefix.Length..])
            .Select(r => Patterns.EmbeddedUpgradeFolderPattern().Match(r))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value)
            .Distinct()
            .ToList();

        var allUpgrades = mangledNames
            .Select(mangled =>
            {
                string original = UnmangleUpgradeFolderName(mangled);
                return ParseUpgradeFolderName(original, mangled);
            })
            .Where(x => x != null)
            .Cast<UpgradeInfo>()
            .ToList();

        var filtered = FilterUpgrades(allUpgrades, currentVersion, targetVersion);

        var result = new List<UpgradeInfo>();
        foreach (var upgrade in filtered)
        {
            string manifestResource = $"{upgradesPrefix}{upgrade.Path}.upgrade.txt";
            if (_assembly.GetManifestResourceNames().Contains(manifestResource))
            {
                result.Add(upgrade);
            }
            else
            {
                onWarning?.Invoke($"Upgrade folder '{upgrade.FolderName}' has no upgrade.txt — skipped");
            }
        }
        return result;
    }

    public override List<string> GetUpgradeManifest(UpgradeInfo upgrade)
    {
        string upgradesPrefix = $"{_resourcePrefix}.Resources.upgrades.";
        string manifestResource = $"{upgradesPrefix}{upgrade.Path}.upgrade.txt";
        string content = ReadResource(manifestResource);
        return ParseUpgradeManifest(content.Split('\n'));
    }

    public override string ReadUpgradeScript(UpgradeInfo upgrade, string scriptName)
    {
        string upgradesPrefix = $"{_resourcePrefix}.Resources.upgrades.";
        string resource = $"{upgradesPrefix}{upgrade.Path}.{scriptName}";
        return ReadResource(resource);
    }

    public override Task<string> ReadUpgradeScriptAsync(
        UpgradeInfo upgrade, string scriptName, CancellationToken cancellationToken = default) =>
        Task.FromResult(ReadUpgradeScript(upgrade, scriptName));

    public override bool UpgradeScriptExists(UpgradeInfo upgrade, string scriptName)
    {
        string upgradesPrefix = $"{_resourcePrefix}.Resources.upgrades.";
        string resource = $"{upgradesPrefix}{upgrade.Path}.{scriptName}";
        return _assembly.GetManifestResourceNames().Contains(resource);
    }

    /// <summary>
    /// Reverses MSBuild's resource name mangling for upgrade folder names.
    /// "_2._2._0_to_2._3._0" → "2.2.0-to-2.3.0"
    /// </summary>
    private static string UnmangleUpgradeFolderName(string mangled)
    {
        /*
        MSBuild mangling:
        - dots in folder names become namespace separator dots
        - hyphens become underscores
        - segments starting with a digit get an underscore prefix
        Reverse: remove leading underscores from digit segments,
        rejoin with dots, then restore the hyphen in "-to-".
        */
        var segments = mangled.Split('.');
        for (int i = 0; i < segments.Length; i++)
        {
            if (segments[i].Length > 1 && segments[i][0] == '_' && char.IsDigit(segments[i][1]))
                segments[i] = segments[i][1..];
        }
        string result = string.Join(".", segments);
        return result.Replace("_to_", "-to-");
    }

    public override string? ReadTroubleshootingScript()
    {
        string resource = $"{_resourcePrefix}.Resources.install.99_installer_troubleshooting.sql";
        return _assembly.GetManifestResourceNames().Contains(resource)
            ? ReadResource(resource)
            : null;
    }

    private string ReadResource(string resourceName)
    {
        using var stream = _assembly.GetManifestResourceStream(resourceName)
            ?? throw new FileNotFoundException($"Embedded resource not found: {resourceName}");
        using var reader = new StreamReader(stream, Encoding.UTF8);
        return reader.ReadToEnd();
    }
}
