/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// How the Query Store Clutter panel and the Memory Pressure Events chart of the desktop viewer are wired to say where their data
/// starts (#4966): the probe beside the read, the one window start both take, the banner declared above the surface, and the named
/// source the Memory Pressure Events probe needs. These read the source files, as the other data-start wiring pins do, because
/// the tab itself needs a window to run; the banner's behavior is in <c>ViewerClutterAndMemoryPressureDataStartTests</c> and the
/// store-backed answers are in <c>ViewerClutterAndMemoryPressureDataStartLiveTests</c>.
/// </summary>
public sealed class ViewerClutterAndMemoryPressureDataStartPinTests
{
    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    private static string AllTabFiles() =>
        string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

    // ── Query Store Clutter ──

    /* The panel draws from two ranged sources, the plan churn from query_store_stats and the read cost from the collection log, so the
       coverage rule applies to each: both are probed through the shared floor from the window's own start, one call each (the shared
       probe's several-source form answers the EARLIEST start, the opposite of this panel's rule), and the later start is the answer. */
    [Fact]
    public void TheClutterProbe_GoesThroughTheSharedFloor_OverBothRangedSources_FromTheWindowsStart_AndKeepsTheLaterStart()
    {
        var source = ViewerFile("ViewerDataService.QueryStoreClutter.cs");

        Assert.Equal(1, Matches(source, @"public Task<DateTime\?> GetQueryStoreClutterDataStartAsync\(int serverId, DateTime startUtc, DateTime endUtc,"));
        var probe = Regex.Match(source, @"public Task<DateTime\?> GetQueryStoreClutterDataStartAsync\(.*?cancellationToken\)\);\r?\n", RegexOptions.Singleline).Value;
        Assert.NotEmpty(probe);
        Assert.Contains("LaterStartAsync(", probe, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"query_store_stats\")", probe, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.Source.ForCollectionLog()", probe, StringComparison.Ordinal);
        Assert.Equal(2, Matches(probe, @"DataWindowFloor\.GetForServerAsync\(_dataSource,[^;]*?serverId,\s*startUtc,\s*endUtc,"));
        Assert.DoesNotContain("DataWindowFloor.GetAsync(", source, StringComparison.Ordinal);

        /* The choice keeps the later of two starts, and the one that came back when only one did. */
        var later = MethodBody(source, @"internal static async Task<DateTime\?> LaterStartAsync\(");
        Assert.Matches(@"churn >= cost \? churn : cost", later);
        Assert.Contains("?? readCost", later, StringComparison.Ordinal);
    }

    /* The probe starts beside the read and both take the window's start the caller worked out once; the banner is raised after the
       grid is bound, through the one step the tests run too. */
    [Fact]
    public void TheClutterTab_StartsItsProbeBesideTheRead_WithTheSameStart_AndRaisesTheBannerThroughOneStep()
    {
        var load = MethodBody(ViewerFile("ViewerServerTab.QueryStoreClutter.cs"), @"private async Task LoadQueryStoreClutterAsync\(");

        Assert.Equal(1, Matches(load, @"var dataStartTask = _dataService\.GetQueryStoreClutterDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load, @"await _dataService\.GetQueryStoreClutterAsync\(\s*_server\.ServerId,\s*startUtc,\s*endUtc,"));
        Assert.True(
            load.IndexOf("GetQueryStoreClutterDataStartAsync", StringComparison.Ordinal) < load.IndexOf("await _dataService.GetQueryStoreClutterAsync", StringComparison.Ordinal),
            "the probe starts before the read is awaited, so the two run together");
        Assert.Equal(1, Matches(load, @"await ShowQueryStoreClutterDataStartAsync\(QueryStoreClutterTruncationBanner,\s*dataStartTask,\s*startUtc\);"));
        /* One computation of the window: the method takes it, and never works out a second one. */
        Assert.DoesNotContain("GetWindowUtc()", load, StringComparison.Ordinal);
        Assert.DoesNotContain("UpdateTruncationBanner(", load, StringComparison.Ordinal);
    }

    [Fact]
    public void TheClutterStep_RaisesTheBanner_ThroughTheSharedStep_AndTheFailureGuard()
    {
        var queries = ViewerFile("ViewerServerTab.QueryStoreClutter.cs");
        var step = Regex.Match(queries, @"internal static async Task ShowQueryStoreClutterDataStartAsync\(.*?;\r?\n", RegexOptions.Singleline).Value;

        Assert.NotEmpty(step);
        Assert.Matches(@"UpdateTruncationBanner\(\s*banner,\s*await DataStartOrNullAsync\(probe,\s*""Query Store Clutter""\),\s*startUtc\);", step);
    }

    [Fact]
    public void EveryTabRead_OfTheClutterPanel_HasItsProbe()
    {
        var tabs = AllTabFiles();

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryStoreClutterAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryStoreClutterDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"ShowQueryStoreClutterDataStartAsync\(QueryStoreClutterTruncationBanner,"));
    }

    [Fact]
    public void TheClutterBanner_SitsAboveTheGrid_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""2"" x:Name=""QueryStoreClutterTruncationBanner""[^>]*/>");
        Assert.True(banner.Success, "QueryStoreClutterTruncationBanner is not declared in ViewerServerTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Matches(@"<DataGrid Grid\.Row=""3"" x:Name=""QueryStoreClutterGrid""", xaml);
        Assert.Matches(@"<TextBlock Grid\.Row=""4"" x:Name=""QueryStoreClutterRecommendation""", xaml);
        Assert.Matches(@"<Expander Grid\.Row=""5"" Header=""Query Store overhead \(per server\)""", xaml);
    }

    // ── Memory Pressure Events ──

    /* The table is indexed on (server_id, sample_time), not on its prefix time column, so the generic factory refuses it, and it must
       keep refusing: a table the probe cannot read through its index would otherwise be probed on the wrong column. */
    [Fact]
    public void TheGenericFactory_StillRefusesMemoryPressureEvents()
    {
        Assert.False(DataWindowFloor.Source.TryForCollectorTable("memory_pressure_events", out _));
        Assert.Throws<ArgumentException>(() => DataWindowFloor.Source.ForCollectorTable("memory_pressure_events"));

        /* The reason, so a changed index is noticed here: the collector stamps its partition column collection_time, and the index
           the chart's read rides keys on the payload's own sample_time. */
        var schema = CollectorCatalog.All.Single(c => c.TargetTable == "memory_pressure_events");
        Assert.Equal("collection_time", schema.PrefixTimeColumnName);
        Assert.EndsWith("(server_id, sample_time);", PgSchemaGenerator.CreateIndex(schema), StringComparison.Ordinal);
    }

    /* The table is sparse: a server goes days with no pressure event. Its edge is the purge's cutoff, which only the schedule gives. If
       the table ever joins one of the two groups that get no schedule edge, the named source would walk to its oldest row, and a quiet
       start would read as a false note: this fails first, so the factory is revisited. */
    [Fact]
    public void MemoryPressureEvents_GetsItsEdgeFromTheSchedule_NotFromItsOldestRow()
    {
        Assert.DoesNotContain("memory_pressure_events", TimescaleSupport.RawRelations);
        Assert.DoesNotContain("memory_pressure_events", DarlingRetentionHorizons.BaselineServingRawCollectors);
        Assert.True(CollectorScheduleDefaults.All.TryGetValue("memory_pressure_events", out var schedule));
        Assert.True(schedule!.RetentionDays >= 1);
    }

    [Fact]
    public void TheMemoryPressureProbe_GoesThroughTheNamedSource_FromTheWindowsStart()
    {
        var source = ViewerFile("ViewerDataService.Memory.cs");

        Assert.Equal(1, Matches(source, @"public Task<DateTime\?> GetMemoryPressureEventsDataStartAsync\(int serverId, DateTime startUtc, DateTime endUtc,"));
        Assert.Contains("DataWindowFloor.Source.ForMemoryPressureEvents()", source, StringComparison.Ordinal);
        Assert.Matches(@"DataWindowFloor\.GetForServerAsync\(_dataSource, DataWindowFloor\.Source\.ForMemoryPressureEvents\(\),\s*serverId,\s*startUtc,\s*endUtc,", source);
    }

    /* The probe starts beside the six reads, inside the declared fan-out width and outside the join (a probe that throws must not cost
       the chart its rows), and the window the load worked out is the one the read, the probe, the chart's axis and the banner use. */
    [Fact]
    public void TheMemoryLoad_StartsItsProbeBesideTheReads_WithTheSameStart_AndNeverJoinsIt()
    {
        var load = MethodBody(ViewerFile("ViewerServerTab.Memory.cs"), @"private async Task LoadMemoryAsync\(");

        Assert.Equal(1, Matches(load, @"var \(startUtc, endUtc\) = GetWindowUtc\(\);"));
        Assert.Equal(1, Matches(load, @"using var readFanOut = ViewerReadFanOut\.Of\(7\);"));
        Assert.Equal(1, Matches(load, @"var pressureDataStartTask = _dataService\.GetMemoryPressureEventsDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load, @"var pressureTask = _dataService\.GetMemoryPressureEventsAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.DoesNotMatch(@"Task\.WhenAll\([^)]*pressureDataStartTask", load);
        Assert.Equal(1, Matches(load, @"RenderMemoryPressureEventsChart\(pressureTask\.Result,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load,
            @"await ShowMemoryPressureEventsDataStartAsync\(MemoryPressureEventsTruncationBanner,\s*pressureDataStartTask,\s*startUtc,\s*pressureTask\.Result\);"));
    }

    /* The chart no longer asks for a window of its own: a second GetWindowUtc() moves a relative range's start by the time the reads
       took, and the banner would be compared with a start the read never used. */
    [Fact]
    public void TheMemoryPressureChart_TakesTheLoadsWindow_AndWorksOutNoneOfItsOwn()
    {
        var render = MethodBody(ViewerFile("ViewerServerTab.Memory.cs"), @"private void RenderMemoryPressureEventsChart\(");

        Assert.Matches(@"private void RenderMemoryPressureEventsChart\(List<MemoryPressureEventRow> data, DateTime startUtc, DateTime endUtc\)", render);
        Assert.DoesNotContain("GetWindowUtc()", render, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryTabRead_OfTheMemoryPressureChart_HasItsProbe()
    {
        var tabs = AllTabFiles();

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetMemoryPressureEventsAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetMemoryPressureEventsDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"ShowMemoryPressureEventsDataStartAsync\(MemoryPressureEventsTruncationBanner,"));
    }

    [Fact]
    public void TheMemoryPressureBanner_SitsAboveTheChart_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""0"" x:Name=""MemoryPressureEventsTruncationBanner""[^>]*/>");
        Assert.True(banner.Success, "MemoryPressureEventsTruncationBanner is not declared in ViewerServerTab.xaml, in the row above the chart");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Matches(@"<scottplot:WpfPlot Grid\.Row=""1"" x:Name=""MemoryPressureEventsChart""", xaml);
        Assert.Matches(@"<TextBlock Grid\.Row=""1"" x:Name=""MemoryPressureEventsNoDataMessage""", xaml);
    }
}
