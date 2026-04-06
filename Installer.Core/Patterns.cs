using System.Text.RegularExpressions;

namespace Installer.Core;

/// <summary>
/// Shared compiled regex patterns for SQL file processing.
/// </summary>
public static partial class Patterns
{
    /// <summary>
    /// Matches numbered SQL installation files (e.g., "01_install_database.sql", "41a_extra.sql").
    /// </summary>
    [GeneratedRegex(@"^\d{2}[a-z]?_.*\.sql$")]
    public static partial Regex SqlFilePattern();

    /// <summary>
    /// Matches SQLCMD :r include directives for removal before execution.
    /// </summary>
    public static readonly Regex SqlCmdDirectivePattern = new(
        @"^:r\s+.*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>
    /// Splits SQL content on GO batch separators (case-insensitive, with optional trailing comments).
    /// </summary>
    public static readonly Regex GoBatchSplitter = new(
        @"^\s*GO\s*(?:--[^\r\n]*)?\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

    /// <summary>
    /// Matches MSBuild-mangled upgrade folder names from embedded resource names.
    /// MSBuild converts "2.2.0-to-2.3.0" to "_2._2._0_to_2._3._0" (dots become namespace
    /// separators, hyphens become underscores, digit-leading segments get underscore prefix).
    /// </summary>
    [GeneratedRegex(@"^(_\d+\._\d+\._\d+_to_\d+\._\d+\._\d+)\.")]
    public static partial Regex EmbeddedUpgradeFolderPattern();

    /// <summary>
    /// Prefixes that indicate excluded scripts (uninstall, test, troubleshooting).
    /// </summary>
    public static readonly string[] ExcludedPrefixes = ["00_", "97_", "99_"];

    /// <summary>
    /// Prefixes that indicate critical installation scripts (abort on failure).
    /// </summary>
    public static readonly string[] CriticalPrefixes = ["01_", "02_", "03_"];

    /// <summary>
    /// Filters and sorts SQL installation files using the standard rules:
    /// include files matching SqlFilePattern, exclude 00_/97_/99_ prefixes, sort alphabetically.
    /// </summary>
    public static List<string> FilterInstallFiles(IEnumerable<string> fileNames)
    {
        return fileNames
            .Where(f => SqlFilePattern().IsMatch(f))
            .Where(f => !ExcludedPrefixes.Any(p => f.StartsWith(p, StringComparison.Ordinal)))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// Returns true if the given file name represents a critical installation script.
    /// </summary>
    public static bool IsCriticalFile(string fileName)
    {
        return CriticalPrefixes.Any(p => fileName.StartsWith(p, StringComparison.Ordinal));
    }
}
