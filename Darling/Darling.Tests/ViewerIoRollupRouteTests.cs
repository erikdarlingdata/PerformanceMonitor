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
/// #5329 lane C: the viewer's hourly route for Top Queries and Top Procedures reads the io hourly rollup, and
/// fills reads, physical reads and writes from its three sums, only when the store has the view and its first
/// bucket is at or before the window's start. These are the no-database pins of that decision and of the SQL it
/// produces; <c>ViewerIoRollupRouteLiveTests</c> pins the behavior against real rollups.
/// </summary>
public sealed class ViewerIoRollupRouteTests
{
    private static readonly DateTime Start = new(2026, 1, 5, 1, 0, 0, DateTimeKind.Utc);

    private static RollupCoverage CoverageWithIoFloor(string view, DateTime? floor)
    {
        var floors = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        if (floor is { } f)
        {
            floors[view] = f;
        }

        return new RollupCoverage(floors, new Dictionary<string, DateTime>(StringComparer.Ordinal), RollupAvailability.All);
    }

    [Theory]
    [InlineData(TimescaleSupport.QueryStatsIoHourlyView)]
    [InlineData(TimescaleSupport.ProcedureStatsIoHourlyView)]
    public void IoHourlyCoversWindow_IsTrueOnlyWhenTheViewExistsAndItsFloorIsAtOrBeforeTheStart(string view)
    {
        /* Covering: the floor is the window start, or earlier. */
        Assert.True(ViewerDataService.IoHourlyCoversWindow(RollupAvailability.All, CoverageWithIoFloor(view, Start), view, Start));
        Assert.True(ViewerDataService.IoHourlyCoversWindow(RollupAvailability.All, CoverageWithIoFloor(view, Start.AddDays(-3)), view, Start));

        /* Starting after the window start: the early part of the window is not in the view, so reads would be short. */
        Assert.False(ViewerDataService.IoHourlyCoversWindow(RollupAvailability.All, CoverageWithIoFloor(view, Start.AddHours(1)), view, Start));

        /* Built but empty (no floor), and a store that does not have the view at all: both today's route, no error. */
        Assert.False(ViewerDataService.IoHourlyCoversWindow(RollupAvailability.All, CoverageWithIoFloor(view, null), view, Start));
        Assert.False(ViewerDataService.IoHourlyCoversWindow(
            RollupAvailability.WithoutIoHourlies, CoverageWithIoFloor(view, Start.AddDays(-3)), view, Start));
        Assert.False(ViewerDataService.IoHourlyCoversWindow(RollupAvailability.None, RollupCoverage.Unknown, view, Start));
    }

    [Fact]
    public void HourlySql_CarriesTheThreeIoSumsOnlyWhenTheIoViewIsTheRelation()
    {
        const string From = "collect.query_stats_io_hourly AS f";
        foreach (var (without, with) in new[]
        {
            (ViewerDataService.BuildTopQueriesHourlySql(From), ViewerDataService.BuildTopQueriesHourlySql(From, withIo: true)),
            (ViewerDataService.BuildTopProceduresHourlySql(From), ViewerDataService.BuildTopProceduresHourlySql(From, withIo: true)),
        })
        {
            Assert.DoesNotContain("logical_reads_sum", without, StringComparison.Ordinal);
            Assert.DoesNotContain("physical_reads_sum", without, StringComparison.Ordinal);
            Assert.DoesNotContain("logical_writes_sum", without, StringComparison.Ordinal);

            /* Positions 5, 6 and 7 for queries (3, 4, 5 are executions and the two times); the reader reads them by position. */
            Assert.Contains("CAST(SUM(logical_reads_sum) AS bigint)", with, StringComparison.Ordinal);
            Assert.Contains("CAST(SUM(physical_reads_sum) AS bigint)", with, StringComparison.Ordinal);
            Assert.Contains("CAST(SUM(logical_writes_sum) AS bigint)", with, StringComparison.Ordinal);
            Assert.True(
                with.IndexOf("logical_reads_sum", StringComparison.Ordinal) < with.IndexOf("physical_reads_sum", StringComparison.Ordinal)
                && with.IndexOf("physical_reads_sum", StringComparison.Ordinal) < with.IndexOf("logical_writes_sum", StringComparison.Ordinal));

            /* The ranking, the window bounds and the grouping are the same text either way. */
            Assert.Contains("bucket >= $2", with, StringComparison.Ordinal);
            Assert.Contains("bucket < $3", with, StringComparison.Ordinal);
            Assert.Contains("HAVING", with, StringComparison.Ordinal);
        }

        /* No new ranking: the ORDER BY is untouched (queries rank by elapsed, procedures by worker time). */
        Assert.Contains("ORDER BY SUM(elapsed_time_sum) DESC", ViewerDataService.BuildTopQueriesHourlySql(From, withIo: true), StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(worker_time_sum) DESC", ViewerDataService.BuildTopProceduresHourlySql(From, withIo: true), StringComparison.Ordinal);
    }

    /// <summary>Source pin: both arms decide with <c>IoHourlyCoversWindow</c> and name the io view only through
    /// <c>StitchedRelationSql</c> (the standing no-literal-relation-name rule), never a literal.</summary>
    [Theory]
    [InlineData("ViewerDataService.QueryStats.cs", "QueryStatsIoHourlyView", "GetTopQueriesByCpuHourlyAsync")]
    [InlineData("ViewerDataService.ProcedureStats.cs", "ProcedureStatsIoHourlyView", "GetTopProceduresByCpuHourlyAsync")]
    public void HourlyArm_DecidesWithTheCoverageHelper_AndNamesTheIoViewOnlyThroughTheStitch(string file, string viewConstant, string method)
    {
        var source = File.ReadAllText(FindViewerSource(file));
        var start = source.IndexOf("> " + method + "(", StringComparison.Ordinal);
        Assert.True(start >= 0, method + " not found");
        var end = source.IndexOf("internal static string BuildTop", start, StringComparison.Ordinal);
        var body = source.Substring(start, end - start);

        Assert.Contains("IoHourlyCoversWindow(rollups, coverage, TimescaleSupport." + viewConstant, body, StringComparison.Ordinal);
        Assert.Contains("coverage.StitchedRelationSql(TimescaleSupport." + viewConstant, body, StringComparison.Ordinal);
        Assert.DoesNotContain("_io_hourly", StripComments(body), StringComparison.Ordinal);
    }

    private static string StripComments(string text) =>
        System.Text.RegularExpressions.Regex.Replace(text, @"/\*.*?\*/|//[^\n]*", "", System.Text.RegularExpressions.RegexOptions.Singleline);

    private static string FindViewerSource(string file)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "PerformanceMonitor.Darling.Viewer", file);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new FileNotFoundException("Could not locate " + file + " above " + AppContext.BaseDirectory);
    }
}
