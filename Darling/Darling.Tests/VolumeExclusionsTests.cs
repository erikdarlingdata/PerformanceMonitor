/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6098: the fleet-wide excluded-volumes list of the Volume Free Space alert. The match rule (<see cref="VolumeExclusions"/>)
/// is checked as a pure function, and the behaviour through the shared engine: an excluded always-full volume no longer sets
/// the tier, another breached volume still alerts with its own tier, and a server whose only low volumes are excluded
/// takes the resolved path. Both products read the same engine, so these cover Lite's evaluation too.
/// </summary>
public sealed class VolumeExclusionsTests
{
    /* ---------------- the match rule ---------------- */

    [Theory]
    [InlineData("S:\\", "S:\\")]
    [InlineData("S:\\", "S:")]
    [InlineData("S:\\", "s:\\")]
    [InlineData("S:", "S:\\")]
    [InlineData("s:/", "S:\\")]
    [InlineData("/var/opt/mssql/", "/var/opt/mssql")]
    [InlineData("/var/opt/mssql", "/var/opt/mssql/")]
    [InlineData("/VAR/opt/MSSQL", "/var/opt/mssql/")]
    [InlineData("  S:\\  ", "S:\\")]
    [InlineData("/", "/")]
    [InlineData("C:\\Mounts\\Data\\", "c:\\mounts\\data")]
    public void IsExcluded_MatchesTheWholeMountPoint_CaseInsensitive_TrailingSeparatorOnEitherSideIgnored(string excluded, string mountPoint)
    {
        Assert.True(VolumeExclusions.IsExcluded(mountPoint, new[] { excluded }));
    }

    [Theory]
    [InlineData("D:\\", "D:\\Data")]
    [InlineData("D:\\Data", "D:\\")]
    [InlineData("/var/opt", "/var/opt/mssql")]
    [InlineData("/var/opt/mssql", "/var/opt")]
    [InlineData("/", "/var")]
    [InlineData("S:\\", "T:\\")]
    [InlineData("", "S:\\")]
    [InlineData("   ", "S:\\")]
    public void IsExcluded_IsNeverAPrefixMatch_AndABlankEntryMatchesNothing(string excluded, string mountPoint)
    {
        Assert.False(VolumeExclusions.IsExcluded(mountPoint, new[] { excluded }));
    }

    [Fact]
    public void IsExcluded_EmptyOrNullList_ExcludesNothing()
    {
        Assert.False(VolumeExclusions.IsExcluded("S:\\", null));
        Assert.False(VolumeExclusions.IsExcluded("S:\\", new List<string>()));
        Assert.False(VolumeExclusions.IsExcluded(null, new[] { "S:\\" }));
    }

    [Fact]
    public void Normalize_TrimsDropsBlanksAndDeduplicatesByTheComparisonForm_KeepingTheFirstTyped()
    {
        var normalized = VolumeExclusions.Normalize(new[] { " S:\\ ", "s:", "", "  ", "/mnt/scratch/", "/MNT/scratch", "T:\\" });

        Assert.Equal(new[] { "S:\\", "/mnt/scratch/", "T:\\" }, normalized);
        Assert.Empty(VolumeExclusions.Normalize(null));
    }

    /// <summary>The Settings windows join the list with ", " and split on "," when saving, so an entry holding a comma
    /// would turn into two entries (and could silence a different volume) on an unrelated save. It is dropped.</summary>
    [Fact]
    public void Normalize_DropsAnEntryHoldingAComma()
    {
        Assert.Equal(new[] { "S:\\" }, VolumeExclusions.Normalize(new[] { "/mnt/a,b", "S:\\", "x, y" }));
    }

    /// <summary>Only an entry made entirely of "/" is the root; a stray backslash must not exclude the Linux root volume.</summary>
    [Theory]
    [InlineData("\\")]
    [InlineData("\\\\")]
    [InlineData("\\/")]
    [InlineData("/\\")]
    [InlineData(" \\ ")]
    public void SeparatorOnlyEntry_ThatIsNotAllSlashes_IsDropped_AndMatchesNoMountPoint(string entry)
    {
        Assert.Empty(VolumeExclusions.Normalize(new[] { entry }));
        Assert.Null(VolumeExclusions.Key(entry));
        Assert.False(VolumeExclusions.IsExcluded("/", new[] { entry }));
    }

    [Theory]
    [InlineData("/")]
    [InlineData("//")]
    [InlineData(" / ")]
    public void AnEntryOfSlashesAlone_StaysTheRootVolume(string entry)
    {
        Assert.Equal(new[] { entry.Trim() }, VolumeExclusions.Normalize(new[] { entry }));
        Assert.True(VolumeExclusions.IsExcluded("/", new[] { entry }));
        Assert.False(VolumeExclusions.IsExcluded("/var", new[] { entry }));
    }

    [Fact]
    public void Normalize_DropsControlCharacterAndOverLongEntries_AndKeepsTheLimitExactly()
    {
        var atLimit = "/" + new string('a', VolumeExclusions.MaxEntryLength - 1);
        var overLimit = atLimit + "a";
        var normalized = VolumeExclusions.Normalize(new[] { "/bad\0nul", "/bad\ttab", "/bad\nline", overLimit, atLimit, "/ok" });

        Assert.Equal(new[] { atLimit, "/ok" }, normalized);
    }

    [Fact]
    public void Normalize_CapsTheListAtTheEntryLimit()
    {
        var many = Enumerable.Range(0, VolumeExclusions.MaxEntries + 40).Select(i => "/mnt/v" + i).ToList();

        var normalized = VolumeExclusions.Normalize(many);

        Assert.Equal(VolumeExclusions.MaxEntries, normalized.Count);
        Assert.Equal("/mnt/v0", normalized[0]);
    }

    [Fact]
    public void Refusal_NamesTheFirstProblem_AndPassesAnAcceptableList()
    {
        Assert.Null(VolumeExclusions.Refusal(new[] { "S:\\", "/mnt/scratch/", "", "\\" }, "excluded_volumes"));
        Assert.Null(VolumeExclusions.Refusal(null, "excluded_volumes"));
        Assert.Contains("excluded_volumes", VolumeExclusions.Refusal(new[] { "/mnt/a,b" }, "excluded_volumes"));
        Assert.NotNull(VolumeExclusions.Refusal(new[] { "/a\0b" }, "excluded_volumes"));
        Assert.NotNull(VolumeExclusions.Refusal(new[] { "/" + new string('a', VolumeExclusions.MaxEntryLength) }, "excluded_volumes"));
        Assert.NotNull(VolumeExclusions.Refusal(Enumerable.Repeat("/x", VolumeExclusions.MaxEntries + 1), "excluded_volumes"));
        Assert.Null(VolumeExclusions.Refusal(Enumerable.Repeat("/x", VolumeExclusions.MaxEntries), "excluded_volumes"));
    }

    /// <summary>The alert-side excluded-databases bounds: 128 characters, no control character, 256 names, commas allowed.</summary>
    [Fact]
    public void ExcludedDatabaseNames_BoundsLengthCountAndControlCharacters_ButAllowsCommas()
    {
        var atLimit = new string('d', ExcludedDatabaseNames.MaxNameLength);
        var normalized = ExcludedDatabaseNames.Normalize(new[] { "Sales,2024", atLimit, atLimit + "d", "bad\0name", "bad\nname", "tempdb" });

        Assert.Equal(new[] { "Sales,2024", atLimit, "tempdb" }, normalized);
        Assert.Equal(ExcludedDatabaseNames.MaxEntries,
            ExcludedDatabaseNames.Normalize(Enumerable.Range(0, ExcludedDatabaseNames.MaxEntries + 5).Select(i => "db" + i)).Count);

        Assert.Null(ExcludedDatabaseNames.Refusal(new[] { "Sales,2024", atLimit }, "excluded_databases"));
        Assert.NotNull(ExcludedDatabaseNames.Refusal(new[] { atLimit + "d" }, "excluded_databases"));
        Assert.NotNull(ExcludedDatabaseNames.Refusal(new[] { "a\0b" }, "excluded_databases"));
        Assert.NotNull(ExcludedDatabaseNames.Refusal(Enumerable.Repeat("db", ExcludedDatabaseNames.MaxEntries + 1), "excluded_databases"));
    }

    [Fact]
    public void Filter_ReturnsTheInputWhenNothingIsExcluded_AndDropsOnlyTheNamedVolumes()
    {
        var volumes = new List<VolumeFreeSpaceInfo>
        {
            new() { MountPoint = "S:\\", TotalMb = 1000, FreeMb = 10 },
            new() { MountPoint = "D:\\", TotalMb = 1000, FreeMb = 500 },
        };

        Assert.Same(volumes, VolumeExclusions.Filter(volumes, new List<string>()));
        Assert.Equal(new[] { "D:\\" }, VolumeExclusions.Filter(volumes, new[] { "s:" }).Select(v => v.MountPoint));
    }

    /* ---------------- the settings seam (Darling) ---------------- */

    [Fact]
    public void DarlingAlertSettings_ExposesTheConfiguredList_AndIsEmptyByDefault()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        Assert.Empty(settings.ExcludedVolumes);

        config.Alerts.ExcludedVolumes.Add("S:\\");
        Assert.Equal(new[] { "S:\\" }, settings.ExcludedVolumes);
    }

    /* ---------------- through the engine ---------------- */

    private static VolumeFreeSpaceInfo Volume(string mount, double freeMb) => new() { MountPoint = mount, TotalMb = 102400, FreeMb = freeMb };

    /// <summary>1 percent free (1 GB): critically low. 8 percent free (8 GB): breached, not critical.</summary>
    private const double CriticalFreeMb = 1024;
    private const double WarningFreeMb = 8192;

    [Fact]
    public async Task AnExcludedAlwaysFullVolume_NoLongerSetsTheTier_AndAnotherBreachedVolumeAlertsWithItsOwn()
    {
        /* Without the exclusion the 1 percent S: is the worst volume and the fire is Critical. */
        var control = new AlertEngineTests.Harness();
        control.Settings.LowDiskEnabled = true;
        control.Adapter.Volumes.Add(Volume("S:\\", CriticalFreeMb));
        control.Adapter.Volumes.Add(Volume("D:\\", WarningFreeMb));
        await control.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var before = Assert.Single(control.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, before.Severity);
        Assert.StartsWith("S:\\", before.CurrentValue);

        /* With it excluded, D: is the worst remaining volume: its headline, its (Warning) tier, and a card that does not list S:. */
        var h = new AlertEngineTests.Harness();
        h.Settings.LowDiskEnabled = true;
        h.Settings.ExcludedVolumesList.Add("s:");
        h.Adapter.Volumes.Add(Volume("S:\\", CriticalFreeMb));
        h.Adapter.Volumes.Add(Volume("D:\\", WarningFreeMb));
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Volume Free Space", fired.MetricName);
        Assert.StartsWith("D:\\", fired.CurrentValue);
        Assert.Null(fired.Severity);
        Assert.DoesNotContain("S:\\", fired.DetailText ?? "");
    }

    [Fact]
    public async Task WhenOnlyExcludedVolumesAreBreached_NothingFires()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LowDiskEnabled = true;
        h.Settings.ExcludedVolumesList.Add("S:\\");
        h.Adapter.Volumes.Add(Volume("S:\\", CriticalFreeMb));
        h.Adapter.Volumes.Add(Volume("D:\\", 60_000));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task ExcludingTheOnlyBreachedVolume_OnAServerThatWasAlerting_TakesTheResolvedPath()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LowDiskEnabled = true;
        h.Adapter.Volumes.Add(Volume("S:\\", CriticalFreeMb));
        var engine = h.Build();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        h.Settings.ExcludedVolumesList.Add("S:");
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Volume Free Space Resolved", Assert.Single(h.Resolutions).Title);
    }

    [Theory]
    [InlineData("S:\\")]
    [InlineData("S:")]
    [InlineData("s:\\")]
    [InlineData("s:/")]
    public async Task EveryMatchingSpellingExcludesTheVolume(string excluded)
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LowDiskEnabled = true;
        h.Settings.ExcludedVolumesList.Add(excluded);
        h.Adapter.Volumes.Add(Volume("S:\\", CriticalFreeMb));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Theory]
    [InlineData("/var/opt/mssql")]
    [InlineData("/var/opt/mssql/")]
    [InlineData("/VAR/OPT/MSSQL")]
    public async Task APosixMountPointMatchesWithOrWithoutItsTrailingSlash(string excluded)
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LowDiskEnabled = true;
        h.Settings.ExcludedVolumesList.Add(excluded);
        h.Adapter.Volumes.Add(Volume("/var/opt/mssql/", CriticalFreeMb));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AnEmptyList_ChangesNothing_AndAParentMountIsNotExcludedByAChildEntry()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LowDiskEnabled = true;
        h.Adapter.Volumes.Add(Volume("S:\\", CriticalFreeMb));
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        var prefix = new AlertEngineTests.Harness();
        prefix.Settings.LowDiskEnabled = true;
        prefix.Settings.ExcludedVolumesList.Add("S:\\Data");
        prefix.Adapter.Volumes.Add(Volume("S:\\", CriticalFreeMb));
        await prefix.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(prefix.Deliverer.Outcomes);
    }
}
