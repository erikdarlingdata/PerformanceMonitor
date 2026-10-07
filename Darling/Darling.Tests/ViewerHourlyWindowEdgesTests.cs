/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5329 lane C2: the viewer's hourly Top Queries and Top Procedures reads bound each read at the materialization
/// ceiling of the relation that answers the window's END (the service's rule, now one method on
/// <see cref="RollupCoverage"/>) and show the window's real edges (<see cref="HourlyWindowEdges.Note"/>, the MCP
/// tools' own text) beside the tier disclosure on the grid's banner. These are the no-database pins;
/// <c>ViewerHourlyWindowEdgesLiveTests</c> pins the behavior against real rollups.
/// </summary>
public sealed class ViewerHourlyWindowEdgesTests
{
    private static readonly DateTime Aligned = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Unaligned = new(2026, 1, 5, 10, 30, 0, DateTimeKind.Utc);

    private static RollupCoverage Coverage(
        IReadOnlyDictionary<string, DateTime>? floors = null, IReadOnlyDictionary<string, DateTime>? ceilings = null) =>
        new(floors ?? new Dictionary<string, DateTime>(StringComparer.Ordinal),
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All,
            ceilings ?? new Dictionary<string, DateTime>(StringComparer.Ordinal));

    private static string Utc(DateTime instant) =>
        DateTime.SpecifyKind(instant, DateTimeKind.Utc).ToString("o", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public void HourlyCeilingSql_IsEmptyWithoutACeiling_AndBoundsTheBucketWithOne()
    {
        Assert.Equal("", ViewerDataService.HourlyCeilingSql(null));
        Assert.Contains("bucket < $6", ViewerDataService.HourlyCeilingSql(Aligned), StringComparison.Ordinal);
    }

    [Fact]
    public void HourlySql_BindsTheCeilingOnlyWhenOneIsKnown_OnBothGrids()
    {
        const string From = "collect.query_stats_io_hourly AS f";
        foreach (var build in new Func<DateTime?, string>[]
        {
            ceiling => ViewerDataService.BuildTopQueriesHourlySql(From, withIo: true, ceiling: ceiling),
            ceiling => ViewerDataService.BuildTopQueriesHourlySql(From, withIo: false, ceiling: ceiling),
            ceiling => ViewerDataService.BuildTopProceduresHourlySql(From, withIo: true, ceiling: ceiling),
            ceiling => ViewerDataService.BuildTopProceduresHourlySql(From, withIo: false, ceiling: ceiling),
        })
        {
            var bounded = build(Aligned);
            Assert.Contains("bucket < $6", bounded, StringComparison.Ordinal);
            /* The ceiling rides beside the window end, ahead of the grouping. */
            Assert.True(
                bounded.IndexOf("bucket < $3", StringComparison.Ordinal) < bounded.IndexOf("bucket < $6", StringComparison.Ordinal)
                && bounded.IndexOf("bucket < $6", StringComparison.Ordinal) < bounded.IndexOf("GROUP BY", StringComparison.Ordinal));

            /* A null ceiling means no bound: today's text, with no sixth parameter. */
            var open = build(null);
            Assert.DoesNotContain("$6", open, StringComparison.Ordinal);
            Assert.Contains("bucket < $3", open, StringComparison.Ordinal);
        }
    }

    /// <summary>The ceiling is that of the relation that answers the window's END: the io view's own, and for the interval
    /// rollup the successor's when the successor serves the window and the legacy's when it does not.</summary>
    [Fact]
    public void HourlyEndCeiling_IsTheCeilingOfTheRelationThatAnswersTheEnd()
    {
        var ceilings = new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsIoHourlyView] = Aligned.AddHours(5),
            [TimescaleSupport.QueryStatsHourlyView] = Aligned.AddHours(2),
            [TimescaleSupport.QueryStatsIntervalHourlyView] = Aligned.AddHours(3),
        };

        /* The io view has no successor: its own ceiling. */
        Assert.Equal(Aligned.AddHours(5),
            Coverage(ceilings: ceilings).HourlyEndCeiling(TimescaleSupport.QueryStatsIoHourlyView, Aligned));

        /* The successor reaches the window's start as far back as the legacy does: it answers, and its ceiling bounds. */
        var successorServes = new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsHourlyView] = Aligned.AddDays(-1),
            [TimescaleSupport.QueryStatsIntervalHourlyView] = Aligned.AddDays(-1),
        };
        Assert.Equal(Aligned.AddHours(3),
            Coverage(successorServes, ceilings).HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, Aligned));

        /* The successor holds nothing and the legacy covers the window: the legacy answers, its ceiling bounds. */
        var legacyServes = new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsHourlyView] = Aligned.AddDays(-1),
        };
        Assert.Equal(Aligned.AddHours(2),
            Coverage(legacyServes, ceilings).HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, Aligned));

        /* The successor starts after the window's start while the legacy covers it: a stitched read, whose END is served by the
           successor, so the successor's ceiling bounds. */
        var stitched = new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsHourlyView] = Aligned.AddDays(-1),
            [TimescaleSupport.QueryStatsIntervalHourlyView] = Aligned.AddHours(1),
        };
        Assert.Equal(Aligned.AddHours(3),
            Coverage(stitched, ceilings).HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, Aligned));

        /* No measured ceiling: no bound. */
        Assert.Null(Coverage().HourlyEndCeiling(TimescaleSupport.QueryStatsIoHourlyView, Aligned));
    }

    [Fact]
    public void HourlyEdgesNote_NamesACeilingTheFloorAndAnUnalignedEnd_AndSaysNothingWhenNoEdgeMoved()
    {
        var end = new DateTime(2026, 1, 5, 12, 15, 0, DateTimeKind.Utc);

        /* Ceiling known and at or before the end: nothing after it was read, and the note names it. */
        var ceiling = new DateTime(2026, 1, 5, 11, 0, 0, DateTimeKind.Utc);
        var cut = ViewerDataService.HourlyEdgesNote(Aligned, end, null, ceiling);
        Assert.NotNull(cut);
        Assert.Contains("materialized only to " + Utc(ceiling), cut, StringComparison.Ordinal);
        Assert.Contains("nothing after it was read", cut, StringComparison.Ordinal);
        Assert.DoesNotContain("ceiling unknown", cut, StringComparison.Ordinal);

        /* Ceiling null: no bound, and no ceiling text beyond "unknown". */
        var unknown = ViewerDataService.HourlyEdgesNote(Aligned, end, null, null);
        Assert.Contains("materialization ceiling unknown", unknown, StringComparison.Ordinal);
        Assert.DoesNotContain("materialized only to", unknown, StringComparison.Ordinal);

        /* Ceiling past an unaligned end: the end's bucket is counted whole. */
        var past = ViewerDataService.HourlyEdgesNote(Aligned, end, null, end.AddHours(3));
        Assert.Contains("is included whole", past, StringComparison.Ordinal);

        /* An unaligned start whose rollup starts later: the note names the floor and the data it leaves out. */
        var floor = new DateTime(2026, 1, 5, 12, 0, 0, DateTimeKind.Utc);
        var late = ViewerDataService.HourlyEdgesNote(Unaligned, end, floor, end.AddHours(3));
        Assert.Contains("no bucket before " + Utc(floor), late, StringComparison.Ordinal);
        Assert.Contains($"the data from {Utc(Unaligned)} to {Utc(floor)} is not included", late, StringComparison.Ordinal);

        /* A floor after an ALIGNED start, with the end cut at the ceiling, names the span actually served. */
        var floor11 = new DateTime(2026, 1, 5, 11, 0, 0, DateTimeKind.Utc);
        var servedFrom = ViewerDataService.HourlyEdgesNote(Aligned, end, floor11, floor);
        Assert.Contains("served from " + Utc(floor11) + " to " + Utc(floor), servedFrom, StringComparison.Ordinal);

        /* A floor at or before the start moves no start edge: an aligned window with a known ceiling past an aligned end says nothing. */
        var alignedEnd = new DateTime(2026, 1, 5, 12, 0, 0, DateTimeKind.Utc);
        Assert.Null(ViewerDataService.HourlyEdgesNote(Aligned, alignedEnd, null, alignedEnd.AddHours(3)));
        /* ... and a server whose first bucket IS the aligned start (the per-server probe's answer) moves none either. */
        Assert.Null(ViewerDataService.HourlyEdgesNote(Aligned, alignedEnd, Aligned, alignedEnd.AddHours(3)));
    }

    [Fact]
    public void HourlyBannerSuffix_IsNullForRaw_TheTierSuffixAloneForHourlyWithoutANote_AndCarriesTheNoteOtherwise()
    {
        Assert.Null(ViewerServerTab.HourlyBannerSuffix("raw", false, null));
        Assert.Null(ViewerServerTab.HourlyBannerSuffix("raw", true, "a note a raw read never has"));

        var bare = ViewerServerTab.HourlyBannerSuffix("hourly", false, null);
        Assert.Contains("aggregated hourly", bare, StringComparison.Ordinal);
        Assert.DoesNotContain("Window edges", bare, StringComparison.Ordinal);
        Assert.Equal(bare, ViewerServerTab.HourlyBannerSuffix("hourly", false, ""));

        var withNote = ViewerServerTab.HourlyBannerSuffix("hourly", false, "the hourly rollup is materialized only to X; nothing after it was read");
        Assert.StartsWith(bare, withNote, StringComparison.Ordinal);
        Assert.EndsWith("Window edges: the hourly rollup is materialized only to X; nothing after it was read", withNote, StringComparison.Ordinal);
    }

    /// <summary>#5329: on the io route the grid FILLS reads, physical reads and writes, so the banner must not call them
    /// blank; the stitched route's text still does. Both keep what the rollup lacks (per-caller detail, spills, min/max).</summary>
    [Fact]
    public void HourlyBannerSuffix_OnTheIoRoute_DoesNotCallReadsAndWritesBlank_AndTheStitchedRouteStill_Does()
    {
        var stitched = ViewerServerTab.HourlyBannerSuffix("hourly", false, null)!;
        var io = ViewerServerTab.HourlyBannerSuffix("hourly", true, null)!;

        Assert.Contains("reads, writes, spills", stitched, StringComparison.Ordinal);
        Assert.DoesNotContain("reads, writes", io, StringComparison.Ordinal);
        Assert.Contains("per-caller detail, spills and min/max CPU and duration are not kept", io, StringComparison.Ordinal);

        var withNote = ViewerServerTab.HourlyBannerSuffix("hourly", true, "an edge");
        Assert.StartsWith(io, withNote, StringComparison.Ordinal);
        Assert.EndsWith("Window edges: an edge", withNote, StringComparison.Ordinal);
    }

    /// <summary>Source pins: each hourly arm takes the ceiling from the shared rule (never a second copy), binds it, and
    /// answers the note; the service's reader delegates to the same method; the tab passes the note to the banner.</summary>
    [Theory]
    [InlineData("ViewerDataService.QueryStats.cs", "GetTopQueriesByCpuHourlyAsync", "BuildTopQueriesHourlySql")]
    [InlineData("ViewerDataService.ProcedureStats.cs", "GetTopProceduresByCpuHourlyAsync", "BuildTopProceduresHourlySql")]
    public void HourlyArm_BoundsAtTheSharedCeiling_AndAnswersTheNote(string file, string method, string builder)
    {
        var source = File.ReadAllText(FindSource("PerformanceMonitor.Darling.Viewer", file));
        var start = source.IndexOf("> " + method + "(", StringComparison.Ordinal);
        Assert.True(start >= 0, method + " not found");
        var end = source.IndexOf("internal static string BuildTop", start, StringComparison.Ordinal);
        var body = source.Substring(start, end - start);

        Assert.Contains("coverage.HourlyEndCeiling(answeringView, startUtc)", body, StringComparison.Ordinal);
        Assert.Contains(builder + "(fromClause, withIo: useIo, ceiling: ceiling)", body, StringComparison.Ordinal);
        Assert.Contains("AddHourlyCeilingParameter(command, ceiling)", body, StringComparison.Ordinal);
        /* #5329: the start edge is this SERVER's first bucket, from the one probe the service runs too (in Storage). */
        Assert.Contains("coverage.GetHourlyFirstBucketAsync(", body, StringComparison.Ordinal);
        Assert.Contains("_dataSource, answeringView, serverId, startUtc, endUtc, ceiling", body, StringComparison.Ordinal);
        Assert.Contains("HourlyEdgesNote(startUtc, endUtc, firstBucket, ceiling)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("HourlyServedFloor", body, StringComparison.Ordinal);
        Assert.Contains("coverage.StitchedRelationSql(answeringView", body, StringComparison.Ordinal);
    }

    [Fact]
    public void ServiceReader_DelegatesItsCeilingToTheSharedRule_AndTheTabShowsTheNote()
    {
        var reader = File.ReadAllText(FindSource("PerformanceMonitor.Darling.Service", Path.Combine("Mcp", "DarlingDataReader.cs")));
        var at = reader.IndexOf("private static DateTime? HourlyEndCeiling(", StringComparison.Ordinal);
        Assert.True(at >= 0);
        var declaration = reader.Substring(at, 220);
        Assert.Contains("coverage.HourlyEndCeiling(legacy, startUtc)", declaration, StringComparison.Ordinal);
        Assert.DoesNotContain("StitchFloor(", declaration, StringComparison.Ordinal);

        var tab = File.ReadAllText(FindSource("PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Queries.cs"));
        Assert.Equal(4, System.Text.RegularExpressions.Regex.Matches(
            tab, @"HourlyBannerSuffix\(read\.Tier, read\.IoRoute, read\.HourlyEdgesNote\), HourlyServedOf\(read\.Tier, read\.HourlyFirstBucket\)").Count);
    }

    private static string FindSource(string project, string file)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, project, file);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new FileNotFoundException("Could not locate " + file + " above " + AppContext.BaseDirectory);
    }
}
