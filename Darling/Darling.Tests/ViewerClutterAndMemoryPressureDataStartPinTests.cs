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
[Trait("Stage", "Guard")]
public sealed class ViewerClutterAndMemoryPressureDataStartPinTests
{
    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string AllTabFiles() =>
        string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

    // ── Query Store Clutter ──

    /* The panel draws from two ranged sources, the plan churn from query_store_stats and the read cost from the collection log, so the
       coverage rule applies to each: both are probed through the shared floor from the window's own start, one call each (the shared
       probe's several-source form answers the EARLIEST start, the opposite of this panel's rule), and the later start is the answer. */
    /* The probe starts beside the read and both take the window's start the caller worked out once; the banner is raised after the
       grid is bound, through the one step the tests run too. */
    /* #5022: the panel's reads run beside its probe, and the load declares it. Its own read walks five arms one after another and the
       probe walks its two calls one after another, so two reads are in flight together. ViewerCommandDeadlines prices every command off
       the width the context declares, so a load that declares none prices both against a lone read. The shell's LoadInnerTabAsync
       declares none (and a width it declared would multiply with this one), so the load does: before either read starts, and released
       when its own read is done, so the banner step's wait on the probe is not priced against a read that has finished. */
    [Fact]
    public void EveryTabRead_OfTheClutterPanel_HasItsProbe()
    {
        var tabs = AllTabFiles();

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryStoreClutterAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryStoreClutterDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"ShowQueryStoreClutterDataStartAsync\(QueryStoreClutterTruncationBanner,"));
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

    /* The probe starts beside the six reads, inside the declared fan-out width and outside the join (a probe that throws must not cost
       the chart its rows), and the window the load worked out is the one the read, the probe, the chart's axis and the banner use. */
    /* The chart no longer asks for a window of its own: a second GetWindowUtc() moves a relative range's start by the time the reads
       took, and the banner would be compared with a start the read never used. */
    [Fact]
    public void EveryTabRead_OfTheMemoryPressureChart_HasItsProbe()
    {
        var tabs = AllTabFiles();

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetMemoryPressureEventsAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetMemoryPressureEventsDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"ShowMemoryPressureEventsDataStartAsync\(MemoryPressureEventsTruncationBanner,"));
    }

}
