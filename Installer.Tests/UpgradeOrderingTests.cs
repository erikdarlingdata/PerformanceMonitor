using System.Text.RegularExpressions;
using Installer.Core;
using Installer.Core.Models;
using Installer.Tests.Helpers;

namespace Installer.Tests;

/// <summary>
/// Tests the upgrade folder discovery and ordering logic.
/// Uses temp directories to simulate various upgrade folder configurations.
/// </summary>
public class UpgradeOrderingTests
{
    [Fact]
    public void ReturnsCorrectUpgradesForVersionRange()
    {
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("1.3.0", "2.0.0", "01_schema.sql")
            .WithUpgrade("2.0.0", "2.1.0", "01_columns.sql")
            .WithUpgrade("2.1.0", "2.2.0", "01_compress.sql");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("1.3.0", "2.2.0");

        Assert.Equal(3, upgrades.Count);
        Assert.Equal("1.3.0-to-2.0.0", upgrades[0].FolderName);
        Assert.Equal("2.0.0-to-2.1.0", upgrades[1].FolderName);
        Assert.Equal("2.1.0-to-2.2.0", upgrades[2].FolderName);
    }

    [Fact]
    public void SkipsAlreadyAppliedUpgrades()
    {
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("1.3.0", "2.0.0", "01_schema.sql")
            .WithUpgrade("2.0.0", "2.1.0", "01_columns.sql")
            .WithUpgrade("2.1.0", "2.2.0", "01_compress.sql");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("2.0.0", "2.2.0");

        Assert.Equal(2, upgrades.Count);
        Assert.Equal("2.0.0-to-2.1.0", upgrades[0].FolderName);
        Assert.Equal("2.1.0-to-2.2.0", upgrades[1].FolderName);
    }

    [Fact]
    public void AlreadyAtTargetVersion_ReturnsEmpty()
    {
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.0.0", "2.1.0", "01_columns.sql")
            .WithUpgrade("2.1.0", "2.2.0", "01_compress.sql");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("2.2.0", "2.2.0");

        Assert.Empty(upgrades);
    }

    [Fact]
    public void FourPartVersion_NormalizedToThreePart()
    {
        // The installer normalizes 4-part "2.2.0.0" (from DB) to 3-part "2.2.0" (folder names)
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.1.0", "2.2.0", "01_compress.sql");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("2.1.0.0", "2.2.0");

        Assert.Single(upgrades);
        Assert.Equal("2.1.0-to-2.2.0", upgrades[0].FolderName);
    }

    [Fact]
    public void MalformedFolderNames_Skipped()
    {
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.0.0", "2.1.0", "01_columns.sql")
            .WithMalformedUpgradeFolder("not-a-version")
            .WithMalformedUpgradeFolder("foo-to-bar");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("2.0.0", "2.2.0");

        Assert.Single(upgrades);
        Assert.Equal("2.0.0-to-2.1.0", upgrades[0].FolderName);
    }

    [Fact]
    public void MissingUpgradeTxt_FolderSkipped()
    {
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.0.0", "2.1.0", "01_columns.sql")
            .WithUpgradeNoManifest("2.1.0", "2.2.0");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("2.0.0", "2.2.0");

        Assert.Single(upgrades);
        Assert.Equal("2.0.0-to-2.1.0", upgrades[0].FolderName);
    }

    [Fact]
    public void NoUpgradesFolder_ReturnsEmpty()
    {
        using var dir = new TempDirectoryBuilder();
        // Don't create any upgrade folders

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("2.0.0", "2.2.0");

        Assert.Empty(upgrades);
    }

    [Fact]
    public void NullCurrentVersion_ReturnsEmpty()
    {
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.0.0", "2.1.0", "01_columns.sql");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades(null, "2.2.0");

        Assert.Empty(upgrades);
    }

    [Fact]
    public void OrderedByFromVersion()
    {
        // Create folders in reverse order to verify sorting
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.1.0", "2.2.0", "01_c.sql")
            .WithUpgrade("1.3.0", "2.0.0", "01_a.sql")
            .WithUpgrade("2.0.0", "2.1.0", "01_b.sql");

        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("1.3.0", "2.2.0");

        Assert.Equal(3, upgrades.Count);
        Assert.Equal(new Version(1, 3, 0), upgrades[0].FromVersion);
        Assert.Equal(new Version(2, 0, 0), upgrades[1].FromVersion);
        Assert.Equal(new Version(2, 1, 0), upgrades[2].FromVersion);
    }

    [Fact]
    public void DoesNotIncludeFutureUpgrades()
    {
        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.0.0", "2.1.0", "01_a.sql")
            .WithUpgrade("2.1.0", "2.2.0", "01_b.sql")
            .WithUpgrade("2.2.0", "2.3.0", "01_c.sql");

        // Target is 2.2.0, so 2.2.0-to-2.3.0 should NOT be included
        var upgrades = ScriptProvider.FromDirectory(dir.RootPath).GetApplicableUpgrades("2.0.0", "2.2.0");

        Assert.Equal(2, upgrades.Count);
        Assert.DoesNotContain(upgrades, u => u.FolderName == "2.2.0-to-2.3.0");
    }

    [Fact]
    public void EmbeddedResources_FindsUpgradeFolders()
    {
        // Regression test for #772: MSBuild mangles embedded resource names
        // (e.g., "2.2.0-to-2.3.0" → "_2._2._0_to_2._3._0"), which broke
        // upgrade discovery when using Split('.')[0].
        var provider = ScriptProvider.FromEmbeddedResources();
        var upgrades = provider.GetApplicableUpgrades("2.2.0", "2.5.0");

        Assert.NotEmpty(upgrades);
        Assert.Contains(upgrades, u => u.FolderName == "2.2.0-to-2.3.0");
    }

    [Theory]
    [InlineData("_2._2._0_to_2._3._0.upgrade.txt", "_2._2._0_to_2._3._0")]
    [InlineData("_2._2._0_to_2._3._0.03_add_growth_vlf_columns.sql", "_2._2._0_to_2._3._0")]
    [InlineData("_10._1._0_to_10._2._0.01_schema.sql", "_10._1._0_to_10._2._0")]
    public void EmbeddedUpgradeFolderPattern_ExtractsMangledName(string resourceSuffix, string expectedMangled)
    {
        var match = Patterns.EmbeddedUpgradeFolderPattern().Match(resourceSuffix);

        Assert.True(match.Success);
        Assert.Equal(expectedMangled, match.Groups[1].Value);
    }

    [Theory]
    [InlineData("not-a-version")]
    [InlineData("readme.txt")]
    [InlineData("README.md")]
    public void EmbeddedUpgradeFolderPattern_RejectsNonVersionStrings(string input)
    {
        Assert.False(Patterns.EmbeddedUpgradeFolderPattern().Match(input).Success);
    }
}
