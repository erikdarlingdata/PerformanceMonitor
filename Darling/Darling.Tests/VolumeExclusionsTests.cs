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
