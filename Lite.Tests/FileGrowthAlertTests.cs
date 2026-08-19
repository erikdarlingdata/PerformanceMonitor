/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2349: the database file-growth alert — the gap between the two alerts that already look at disk.
///
/// <para><c>tempdb Space</c> fires on reserved ÷ (reserved + unallocated). Autogrowth adds unallocated extents,
/// so the denominator grows with the file and the percentage FALLS as tempdb balloons — it answers "is tempdb
/// internally full right now", which is a real question and structurally not this one. <c>Volume Free Space</c>
/// fires on the consequence, by which point a restart is overdue, and cannot attribute the space to one file.
/// Between them sits a file that has grown large but has not yet filled its disk.</para>
/// </summary>
public class FileGrowthAlertTests
{
    private const string Server = "SQLPROD01";

    private static DatabaseFileGrowthInfo File(
        string db = "tempdb", string name = "tempdev", double sizeMb = 100_000, double growthMb = 0,
        double windowMinutes = 60, double volumeTotalMb = 500_000, double volumeFreeMb = 200_000) =>
        new()
        {
            DatabaseName = db,
            FileName = name,
            PhysicalName = $@"D:\data\{name}.mdf",
            FileTypeDesc = "ROWS",
            TotalSizeMb = sizeMb,
            GrowthMb = growthMb,
            GrowthWindowMinutes = windowMinutes,
            VolumeMountPoint = @"D:\",
            VolumeTotalMb = volumeTotalMb,
            VolumeFreeMb = volumeFreeMb,
        };

    /// <summary>
    /// The RISE gate is the point of the alert: an event, not a level. #2157's reasoning applies exactly — a
    /// level alone re-pages every cooldown about a size that has been true since Tuesday, which trains people
    /// to mute it, while "80 GB in the last hour" is the thing worth waking up for.
    /// </summary>
    [Fact]
    public void TheRiseGate_FiresOnGrowth_EvenWhenTheVolumeIsRoomy()
    {
        var files = new List<DatabaseFileGrowthInfo> { File(sizeMb: 90_000, growthMb: 40_000, volumeTotalMb: 4_000_000) };

        /* 2% of a 4 TB volume — the level gate cannot see this, and it is exactly the case the issue is about. */
        Assert.True(files[0].VolumePercent < 5);

        var breached = AlertContextBuilders.GetBreachedFiles(files, riseMb: 10_240, volumePercent: 60);

        Assert.Single(breached);
    }

    /// <summary>
    /// The LEVEL gate catches the file that is already large and stopped moving — the state the rise gate goes
    /// quiet about by design. Self-scaling, which is what makes ONE global setting usable across a fleet: the
    /// same 60% catches a 128 GB file on a small volume and a 1.6 TB file on a large one.
    /// </summary>
    [Fact]
    public void TheLevelGate_FiresOnAFileThatIsLargeButNoLongerGrowing()
    {
        var files = new List<DatabaseFileGrowthInfo> { File(sizeMb: 400_000, growthMb: 0, volumeTotalMb: 500_000) };

        var breached = AlertContextBuilders.GetBreachedFiles(files, riseMb: 10_240, volumePercent: 60);

        Assert.Single(breached);
        Assert.Equal(80, breached[0].VolumePercent);
    }

    /// <summary>A file breaching neither gate is silent, which is most files most of the time.</summary>
    [Fact]
    public void AQuietFile_DoesNotFire()
    {
        var files = new List<DatabaseFileGrowthInfo> { File(sizeMb: 50_000, growthMb: 100, volumeTotalMb: 500_000) };

        Assert.Empty(AlertContextBuilders.GetBreachedFiles(files, riseMb: 10_240, volumePercent: 60));
    }

    /// <summary>
    /// Zero disables ONE gate rather than being nonsense, so an operator can run rise-only or level-only
    /// without a second switch — and disabling one must not silently disable the other.
    /// </summary>
    [Fact]
    public void ZeroDisablesOneGate_NotBoth()
    {
        var grew = new List<DatabaseFileGrowthInfo> { File(sizeMb: 90_000, growthMb: 40_000, volumeTotalMb: 4_000_000) };
        var large = new List<DatabaseFileGrowthInfo> { File(sizeMb: 400_000, growthMb: 0, volumeTotalMb: 500_000) };

        /* level off: the rise still fires, the large-but-static file does not */
        Assert.Single(AlertContextBuilders.GetBreachedFiles(grew, riseMb: 10_240, volumePercent: 0));
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(large, riseMb: 10_240, volumePercent: 0));

        /* rise off: the level still fires, the growing-but-small-share file does not */
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(grew, riseMb: 0, volumePercent: 60));
        Assert.Single(AlertContextBuilders.GetBreachedFiles(large, riseMb: 0, volumePercent: 60));
    }

    /// <summary>
    /// A file on a volume with no size reported (Azure SQL DB has no volume stats) must not divide by zero and
    /// must not fire the level gate on a fabricated 0%.
    /// </summary>
    [Fact]
    public void AFileWithNoVolumeStats_IsNotLevelGated()
    {
        var files = new List<DatabaseFileGrowthInfo> { File(sizeMb: 400_000, growthMb: 0, volumeTotalMb: 0) };

        Assert.Equal(0, files[0].VolumePercent);
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(files, riseMb: 10_240, volumePercent: 60));
    }

    /// <summary>
    /// Ordered by share of volume, because that is how close this is to becoming a <c>Volume Free Space</c>
    /// page. A 40 GB rise on a 4 TB volume is less urgent than a 10 GB file that is now 80% of a small one.
    /// </summary>
    [Fact]
    public void BreachedFiles_AreOrderedByHowCloseTheyAreToFillingTheirVolume()
    {
        var files = new List<DatabaseFileGrowthInfo>
        {
            File(db: "big", name: "f1", sizeMb: 90_000, growthMb: 80_000, volumeTotalMb: 4_000_000),
            File(db: "tight", name: "f2", sizeMb: 400_000, growthMb: 20_000, volumeTotalMb: 500_000),
        };

        var breached = AlertContextBuilders.GetBreachedFiles(files, riseMb: 10_240, volumePercent: 60);

        Assert.Equal(2, breached.Count);
        Assert.Equal("tight", breached[0].DatabaseName);
    }

    /// <summary>
    /// Fingerprinted per FILE, not per database. Eight tempdb data files growing together are eight files and
    /// one problem, but a log file running away while its data files sit still is a different incident — and
    /// collapsing on database name would merge the two and pool their totals.
    /// </summary>
    [Fact]
    public void IncidentsAreFingerprintedPerFile_AndCarryTheDatabase()
    {
        var incidents = AlertContextBuilders.FileGrowthIncidents(Server, new List<DatabaseFileGrowthInfo>
        {
            File(db: "tempdb", name: "tempdev"),
            File(db: "tempdb", name: "templog"),
        });

        Assert.Equal(2, incidents.Count);
        Assert.Equal(2, incidents.Select(i => i.DedupKey).Distinct(StringComparer.Ordinal).Count());
        Assert.All(incidents, i => Assert.Equal("tempdb", i.Database));
    }

    /// <summary>
    /// #2362's rule: the observation list is UNCAPPED while the card renders a subset. Observing only what is
    /// displayed would reset the total of any file that fell out of the top N.
    /// </summary>
    [Fact]
    public void TheIncidentListIsUncapped()
    {
        var files = Enumerable.Range(0, 12).Select(i => File(db: "db", name: $"f{i}")).ToList();

        Assert.Equal(12, AlertContextBuilders.FileGrowthIncidents(Server, files).Count);
    }

    /// <summary>
    /// The card names what an operator needs to act without opening the Viewer, including the percent-autogrowth
    /// misconfiguration — each growth bigger than the last is exactly how a file gets away from someone, and the
    /// WS3 advisory knows the pattern but does not alert on it.
    /// </summary>
    [Fact]
    public void TheCardNamesTheFileTheVolumeAndAPercentAutogrowth()
    {
        var f = File(sizeMb: 400_000, growthMb: 40_000, volumeTotalMb: 500_000);
        f.IsPercentGrowth = true;
        f.GrowthPct = 10;

        var context = AlertContextBuilders.BuildFileGrowthContext(Server, new List<DatabaseFileGrowthInfo> { f });

        Assert.NotNull(context);
        var fields = context!.Details.SelectMany(d => d.Fields).ToList();

        Assert.Contains(fields, x => x.Item1 == "Database" && x.Item2 == "tempdb");
        Assert.Contains(fields, x => x.Item1 == "Physical Name");
        Assert.Contains(fields, x => x.Item1 == "Volume Free");
        Assert.Contains(fields, x => x.Item1 == "Autogrowth" && x.Item2.Contains("percent growth", StringComparison.Ordinal));
    }

    /// <summary>
    /// A window holding one sample reports zero growth, not a rise of the whole file — the difference between
    /// "no rise observed" and "this file appeared from nothing", which is what a freshly-collecting server
    /// would otherwise look like.
    /// </summary>
    [Fact]
    public void ASingleSampleWindow_ReportsNoRise()
    {
        var f = File(sizeMb: 400_000, growthMb: 0, windowMinutes: 0);

        Assert.Equal(0, f.GrowthMb);
        Assert.Equal(0, f.GrowthMbPerHour);
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(new List<DatabaseFileGrowthInfo> { f }, riseMb: 10_240, volumePercent: 0));
    }

    /// <summary>The rate is derived from the MEASURED window, so a collection gap cannot make a slow rise
    /// look fast.</summary>
    [Theory]
    [InlineData(6000, 60, 6000)]
    [InlineData(6000, 30, 12000)]
    [InlineData(6000, 120, 3000)]
    public void TheRateUsesTheMeasuredWindow(double growthMb, double windowMinutes, double expectedPerHour)
    {
        var f = File(growthMb: growthMb, windowMinutes: windowMinutes);

        Assert.Equal(expectedPerHour, f.GrowthMbPerHour, precision: 3);
    }
}
