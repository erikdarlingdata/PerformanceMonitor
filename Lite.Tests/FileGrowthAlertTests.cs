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

        var breached = AlertContextBuilders.GetBreachedFiles(files, riseMbPerHour: 10_240, volumePercent: 60, lookbackMinutes: 60);

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

        var breached = AlertContextBuilders.GetBreachedFiles(files, riseMbPerHour: 10_240, volumePercent: 60, lookbackMinutes: 60);

        Assert.Single(breached);
        Assert.Equal(80, breached[0].VolumePercent);
    }

    /// <summary>A file breaching neither gate is silent, which is most files most of the time.</summary>
    [Fact]
    public void AQuietFile_DoesNotFire()
    {
        var files = new List<DatabaseFileGrowthInfo> { File(sizeMb: 50_000, growthMb: 100, volumeTotalMb: 500_000) };

        Assert.Empty(AlertContextBuilders.GetBreachedFiles(files, riseMbPerHour: 10_240, volumePercent: 60, lookbackMinutes: 60));
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
        Assert.Single(AlertContextBuilders.GetBreachedFiles(grew, riseMbPerHour: 10_240, volumePercent: 0, lookbackMinutes: 60));
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(large, riseMbPerHour: 10_240, volumePercent: 0, lookbackMinutes: 60));

        /* rise off: the level still fires, the growing-but-small-share file does not */
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(grew, riseMbPerHour: 0, volumePercent: 60, lookbackMinutes: 60));
        Assert.Single(AlertContextBuilders.GetBreachedFiles(large, riseMbPerHour: 0, volumePercent: 60, lookbackMinutes: 60));
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
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(files, riseMbPerHour: 10_240, volumePercent: 60, lookbackMinutes: 60));
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

        var breached = AlertContextBuilders.GetBreachedFiles(files, riseMbPerHour: 10_240, volumePercent: 60, lookbackMinutes: 60);

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
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(new List<DatabaseFileGrowthInfo> { f }, riseMbPerHour: 10_240, volumePercent: 0, lookbackMinutes: 60));
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

    /* ---------------- #3539 A8c: the rise threshold is a RATE ---------------- */

    /// <summary>
    /// The threshold means megabytes per HOUR on every lookback. Before #3539 A8c the stored number was compared
    /// against the raw growth inside the window, so a 10 GB bar meant 10 GB per five minutes on a store with a
    /// short lookback and 10 GB per day on one with a long one — the same file growing at the same rate paged
    /// on one and not the other. Now a file growing at exactly the threshold rate for the whole window breaches
    /// on every lookback the clamp allows, and one growing a tenth under it breaches on none.
    /// </summary>
    [Theory]
    [InlineData(5)]
    [InlineData(30)]
    [InlineData(60)]
    [InlineData(240)]
    [InlineData(1440)]
    public void TheSameGrowthRate_GivesTheSameVerdict_OnEveryLookback(int lookbackMinutes)
    {
        const int riseMbPerHour = 10_240;
        var atTheRate = riseMbPerHour * lookbackMinutes / 60.0;

        var onTheBar = new List<DatabaseFileGrowthInfo> { File(growthMb: atTheRate, windowMinutes: lookbackMinutes, volumeTotalMb: 4_000_000) };
        var under = new List<DatabaseFileGrowthInfo> { File(growthMb: atTheRate * 0.9, windowMinutes: lookbackMinutes, volumeTotalMb: 4_000_000) };

        Assert.Single(AlertContextBuilders.GetBreachedFiles(onTheBar, riseMbPerHour, volumePercent: 0, lookbackMinutes));
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(under, riseMbPerHour, volumePercent: 0, lookbackMinutes));
    }

    /// <summary>
    /// The lie, as arithmetic: 2 GB inside a five-minute window is 24 GB/hr. The per-window reading held it to
    /// the full 10,240 and stayed silent; 24 GB/hr against a 10 GB/hr bar pages. And the reverse on a long
    /// window: 12 GB over a day is 512 MB/hr, which the per-window reading paged on and the rate does not.
    /// </summary>
    [Fact]
    public void ThePerWindowReading_GaveTheOppositeVerdict_AtBothEndsOfTheClamp()
    {
        var burstOnAShortWindow = new List<DatabaseFileGrowthInfo> { File(growthMb: 2_048, windowMinutes: 5, volumeTotalMb: 4_000_000) };
        var crawlOnALongWindow = new List<DatabaseFileGrowthInfo> { File(growthMb: 12_288, windowMinutes: 1440, volumeTotalMb: 4_000_000) };

        /* per-window: 2048 < 10240 silent, 12288 >= 10240 pages */
        Assert.True(burstOnAShortWindow[0].GrowthMb < 10_240);
        Assert.True(crawlOnALongWindow[0].GrowthMb >= 10_240);

        /* per hour: the burst pages, the crawl does not */
        Assert.Single(AlertContextBuilders.GetBreachedFiles(burstOnAShortWindow, riseMbPerHour: 10_240, volumePercent: 0, lookbackMinutes: 5));
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(crawlOnALongWindow, riseMbPerHour: 10_240, volumePercent: 0, lookbackMinutes: 1440));
    }

    /// <summary>
    /// The bar the growth is held to, in megabytes inside the window: the rate times the window in hours. The
    /// shipped 10,240 on the shipped 60-minute lookback is 10,240 — the number every store on the defaults was
    /// always compared against, which is what makes this change silent for them. 5 minutes asks for 853 MB,
    /// a day for 240 GB.
    /// </summary>
    [Theory]
    [InlineData(10_240, 60, 10_240.0)]
    [InlineData(10_240, 5, 853.333)]
    [InlineData(10_240, 1440, 245_760.0)]
    [InlineData(1_024, 30, 512.0)]
    [InlineData(0, 60, 0.0)]
    public void TheBarIsTheRateTimesTheWindowInHours(int riseMbPerHour, int lookbackMinutes, double expectedBarMb)
    {
        Assert.Equal(expectedBarMb, AlertContextBuilders.FileGrowthRiseBarMb(riseMbPerHour, lookbackMinutes), precision: 3);
    }

    /// <summary>
    /// Growth observed over LESS than the window is held to the WHOLE window's bar — unobserved time counts
    /// as no growth. The alternative, reading the rate off the measured span, extrapolates: a server that
    /// started collecting five minutes ago with one 1 GB autogrowth in that span would read 12 GB/hr, page
    /// the default bar, and resolve at the next sample when the span widened. Same conservative stance the
    /// single-sample window already takes ("no rise observed", not "the whole file appeared").
    /// </summary>
    [Fact]
    public void GrowthObservedOverLessThanTheWindow_IsHeldToTheWholeWindowsBar()
    {
        var freshServer = File(growthMb: 1_024, windowMinutes: 5, volumeTotalMb: 4_000_000);

        /* The card's rate WOULD read as over the bar; the gate does not use it. */
        Assert.Equal(12_288, freshServer.GrowthMbPerHour, precision: 3);
        Assert.Empty(AlertContextBuilders.GetBreachedFiles(new List<DatabaseFileGrowthInfo> { freshServer }, riseMbPerHour: 10_240, volumePercent: 0, lookbackMinutes: 60));

        /* The same growth on a lookback as short as the span is a real 12 GB/hr and pages. */
        Assert.Single(AlertContextBuilders.GetBreachedFiles(new List<DatabaseFileGrowthInfo> { freshServer }, riseMbPerHour: 10_240, volumePercent: 0, lookbackMinutes: 5));
    }

    /// <summary>The card's rate carries the same unit phrase the threshold line and both Settings windows use,
    /// so the two numbers read as comparable.</summary>
    [Fact]
    public void TheCardsRate_UsesTheSharedUnitPhrase()
    {
        var f = File(sizeMb: 400_000, growthMb: 40_000, windowMinutes: 60, volumeTotalMb: 500_000);

        var context = AlertContextBuilders.BuildFileGrowthContext(Server, new List<DatabaseFileGrowthInfo> { f });

        var growth = Assert.Single(context!.Details.SelectMany(d => d.Fields), x => x.Item1 == "Growth");
        Assert.Equal($"39.1 GB in 60 min (40000 {AlertContextBuilders.FileGrowthRiseUnit})", growth.Item2);
        Assert.Equal("MB/hr", AlertContextBuilders.FileGrowthRiseUnit);
    }
    /* ---------------- #3636: the rise gate's split-out helpers, and the observation stamp both reads carry ---------------- */

    /// <summary>
    /// The two gates as the engine's #3636 guard asks them: <see cref="AlertContextBuilders.BreachesRiseGate"/>
    /// and <see cref="AlertContextBuilders.BreachesLevelGate"/> are what <see cref="AlertContextBuilders.GetBreachedFiles"/>
    /// applies, so the guard (rise-only files are held to the observation stamp; level files are always news)
    /// cannot classify a file differently from the breach list that put it on the card. Held on the three shapes
    /// the alert distinguishes: rise-only, level-only, both.
    /// </summary>
    [Fact]
    public void TheGateHelpers_AgreeWithTheBreachList_OnRiseOnlyLevelOnlyAndBoth()
    {
        var riseOnly = File(sizeMb: 90_000, growthMb: 40_000, volumeTotalMb: 4_000_000);
        var levelOnly = File(sizeMb: 400_000, growthMb: 0, volumeTotalMb: 500_000);
        var both = File(sizeMb: 400_000, growthMb: 40_000, volumeTotalMb: 500_000);
        var neither = File(sizeMb: 50_000, growthMb: 100, volumeTotalMb: 500_000);

        Assert.True(AlertContextBuilders.BreachesRiseGate(riseOnly, riseMbPerHour: 10_240, lookbackMinutes: 60));
        Assert.False(AlertContextBuilders.BreachesLevelGate(riseOnly, volumePercent: 60));

        Assert.False(AlertContextBuilders.BreachesRiseGate(levelOnly, riseMbPerHour: 10_240, lookbackMinutes: 60));
        Assert.True(AlertContextBuilders.BreachesLevelGate(levelOnly, volumePercent: 60));

        Assert.True(AlertContextBuilders.BreachesRiseGate(both, riseMbPerHour: 10_240, lookbackMinutes: 60));
        Assert.True(AlertContextBuilders.BreachesLevelGate(both, volumePercent: 60));

        /* Zero disables each helper exactly as it disables the gate in the breach list. */
        Assert.False(AlertContextBuilders.BreachesRiseGate(riseOnly, riseMbPerHour: 0, lookbackMinutes: 60));
        Assert.False(AlertContextBuilders.BreachesLevelGate(levelOnly, volumePercent: 0));

        /* And the rise helper is the A8c bar, not the raw knob: 40 GB in a 5-minute window is held to 853 MB. */
        Assert.True(AlertContextBuilders.BreachesRiseGate(File(growthMb: 1_024, windowMinutes: 5, volumeTotalMb: 4_000_000), riseMbPerHour: 10_240, lookbackMinutes: 5));

        foreach (var f in new[] { riseOnly, levelOnly, both, neither })
        {
            var inList = AlertContextBuilders.GetBreachedFiles(new List<DatabaseFileGrowthInfo> { f }, riseMbPerHour: 10_240, volumePercent: 60, lookbackMinutes: 60).Count == 1;
            var byHelpers = AlertContextBuilders.BreachesRiseGate(f, 10_240, 60) || AlertContextBuilders.BreachesLevelGate(f, 60);
            Assert.Equal(byHelpers, inList);
        }
    }

    /// <summary>
    /// #3636: both SKUs' file-growth reads project the newest sample's <c>collection_time</c> as
    /// <c>observed_at</c>, LAST, so the fourteen ordinals both readers already bind do not move. The two texts
    /// are not equal (DuckDB has no <c>DISTINCT ON</c>, so Lite's is a <c>ROW_NUMBER()</c> rewrite — the
    /// FileGrowthAlertStoreTests pin holds that shape), so this pins the one clause #3636 added to each, read
    /// from source on the Darling side through <see cref="Lite.Tests.ParitySource"/>.
    /// </summary>
    [Fact]
    public void BothSkusFileGrowthReads_CarryTheObservationStamp_Last()
    {
        var lite = PerformanceMonitorLite.Services.LocalDataService.DatabaseFileGrowthSql.ReplaceLineEndings("\n");
        Assert.EndsWith(
            "c.max_size_mb,\n    c.collection_time AS observed_at\nFROM windowed c",
            lite[..(lite.IndexOf("FROM windowed c", StringComparison.Ordinal) + "FROM windowed c".Length)],
            StringComparison.Ordinal);

        var darling = Lite.Tests.ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/DarlingAlertReadAdapter.cs");
        var darlingSql = darling[(darling.IndexOf("public const string DatabaseFileGrowthSql = @\"", StringComparison.Ordinal) + "public const string DatabaseFileGrowthSql = @\"".Length)..];
        darlingSql = darlingSql[..darlingSql.IndexOf("\";", StringComparison.Ordinal)].ReplaceLineEndings("\n");
        Assert.EndsWith(
            "c.max_size_mb,\n    c.collection_time AS observed_at\nFROM current_files c",
            darlingSql[..(darlingSql.IndexOf("FROM current_files c", StringComparison.Ordinal) + "FROM current_files c".Length)],
            StringComparison.Ordinal);

        /* The same window bound on both — the stamp is the newest row INSIDE the lookback, not the newest ever. */
        Assert.Contains("collection_time >= $2", lite, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", darlingSql, StringComparison.Ordinal);
    }
}
