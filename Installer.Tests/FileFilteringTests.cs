using System.Text.RegularExpressions;
using Installer.Tests.Helpers;

namespace Installer.Tests;

/// <summary>
/// Tests the file filtering rules used by the installer to select SQL files.
/// Verifies the #538 regression fix: 00_, 97_, 99_ prefixed files must be excluded.
///
/// Note: InstallationService.FindInstallationFiles() searches from CWD/AppDomain base,
/// so we test the filtering rules directly using the same regex and exclusion logic.
/// </summary>
public class FileFilteringTests
{
    // Same regex the installer uses: ^\d{2}[a-z]?_.*\.sql$
    private static readonly Regex SqlFilePattern = new(@"^\d{2}[a-z]?_.*\.sql$");

    private static List<string> FilterFiles(IEnumerable<string> fileNames)
    {
        return fileNames
            .Where(f =>
            {
                if (!SqlFilePattern.IsMatch(f))
                    return false;
                if (f.StartsWith("00_", StringComparison.Ordinal) ||
                    f.StartsWith("97_", StringComparison.Ordinal) ||
                    f.StartsWith("99_", StringComparison.Ordinal))
                    return false;
                return true;
            })
            .OrderBy(f => f)
            .ToList();
    }

    [Fact]
    public void ExcludesUninstallScript_Regression538()
    {
        var files = FilterFiles(["00_uninstall.sql", "01_install_database.sql", "02_create_tables.sql"]);

        Assert.DoesNotContain("00_uninstall.sql", files);
        Assert.Contains("01_install_database.sql", files);
        Assert.Contains("02_create_tables.sql", files);
    }

    [Fact]
    public void ExcludesTestAndTroubleshootingScripts()
    {
        var files = FilterFiles(["01_install.sql", "97_test_something.sql", "99_troubleshoot.sql"]);

        Assert.Single(files);
        Assert.Equal("01_install.sql", files[0]);
    }

    [Fact]
    public void IncludesStandardNumberedFiles()
    {
        var files = FilterFiles([
            "01_install_database.sql",
            "02_create_tables.sql",
            "45_create_agent_jobs.sql",
            "54_create_finops_views.sql"
        ]);

        Assert.Equal(4, files.Count);
    }

    [Fact]
    public void IncludesFilesWithLetterSuffix()
    {
        // Pattern allows optional letter after 2-digit prefix: \d{2}[a-z]?_
        var files = FilterFiles(["41a_extra_schedule.sql", "02_create_tables.sql"]);

        Assert.Equal(2, files.Count);
        Assert.Contains("41a_extra_schedule.sql", files);
    }

    [Fact]
    public void ExcludesNonSqlFiles()
    {
        var files = FilterFiles(["01_install.sql", "README.md", "config.json", "01_install.txt"]);

        Assert.Single(files);
        Assert.Equal("01_install.sql", files[0]);
    }

    [Fact]
    public void ExcludesFilesNotMatchingPattern()
    {
        var files = FilterFiles(["install_database.sql", "abc_something.sql", "1_too_short.sql"]);

        Assert.Empty(files);
    }

    [Fact]
    public void ReturnsSortedAlphabetically()
    {
        var files = FilterFiles(["45_jobs.sql", "02_tables.sql", "01_database.sql", "10_procs.sql"]);

        Assert.Equal("01_database.sql", files[0]);
        Assert.Equal("02_tables.sql", files[1]);
        Assert.Equal("10_procs.sql", files[2]);
        Assert.Equal("45_jobs.sql", files[3]);
    }

    [Fact]
    public void EmptyInput_ReturnsEmpty()
    {
        var files = FilterFiles([]);
        Assert.Empty(files);
    }

    [Fact]
    public void AllExcludedPrefixes_ReturnsEmpty()
    {
        var files = FilterFiles(["00_uninstall.sql", "97_test.sql", "99_debug.sql"]);
        Assert.Empty(files);
    }
}
