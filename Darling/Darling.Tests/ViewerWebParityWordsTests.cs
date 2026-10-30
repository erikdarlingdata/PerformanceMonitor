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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6651: the Viewer says what the web pages and the MCP tools say after #5704 and #5705 -- the labels of a few figures, no tool or key
/// name in a sentence for a person, no per-row repeat of the job's newest success, and a break (not a line across it) in the Top Queries
/// and Top Procedures strip where the service was off.
/// </summary>
public sealed class ViewerWebParityWordsTests
{
    [Fact]
    public void FinOpsCpuLabels_NameSqlServersOwnCpu_AsTheWebPageDoes()
    {
        Assert.Equal("Avg SQL Server CPU", ViewerLabels.AvgSqlServerCpu);
        Assert.Equal("P95 SQL Server CPU", ViewerLabels.P95SqlServerCpu);
        Assert.Equal("Max SQL Server CPU", ViewerLabels.MaxSqlServerCpu);
        Assert.Equal("Avg SQL Server CPU %", ViewerLabels.AvgSqlServerCpuPercent);
        Assert.Equal("P95 SQL Server CPU %", ViewerLabels.P95SqlServerCpuPercent);
        Assert.Equal("Max SQL Server CPU %", ViewerLabels.MaxSqlServerCpuPercent);
    }

    [Fact]
    public void MemoryPlanCacheLabel_SaysItCountsEveryCacheStore()
        => Assert.Equal("Plan cache (all cache stores)", ViewerLabels.PlanCacheAllStores);

    [Fact]
    public void UncorroboratedRouteNote_IsProseForAPerson_WithNoToolNameOrKey()
    {
        var note = ViewerLabels.UncorroboratedRouteNote;
        Assert.Contains("Alert Settings page", note, StringComparison.Ordinal);
        Assert.DoesNotContain("get_", note, StringComparison.Ordinal);
        Assert.DoesNotContain("_source", note, StringComparison.Ordinal);
        Assert.DoesNotContain("uncorroborated_route", note, StringComparison.Ordinal);
    }

    /// <summary>W21(a): the job's newest success repeated on every row (also beside runs from earlier minutes, and "Never" for a job that
    /// retries) is gone from the row the grid binds; the web page removed the column.</summary>
    [Fact]
    public void JobHistoryRow_HasNoLastSuccessColumnValue()
        => Assert.Null(typeof(ViewerJobHistoryRow).GetProperty("LastSuccessfulRunLocal"));

    private static List<TimeSliceBucket> Hourly(DateTime start, int count)
        => Enumerable.Range(0, count).Select(i => new TimeSliceBucket { BucketTime = start.AddHours(i), Value = 10 + i }).ToList();

    private static List<(int Start, int Count)> RunsAfterLoad(List<TimeSliceBucket> real, DateTime windowStart, DateTime windowEnd)
        => StaTestThread.Run(() =>
        {
            var slicer = new TimeRangeSlicerControl();
            slicer.LoadData(real, "Sessions", windowStart, windowEnd);
            return slicer.DrawnRuns.ToList();
        });

    /// <summary>The service was off overnight: LoadData fills every hour of the window (for the bars and the axis), and the line the strip
    /// draws still breaks across the hours the real buckets did not have. Judged on the filled buckets no gap would ever show.</summary>
    [Fact]
    public void Strip_BreaksWhereTheServiceWasOff_InsteadOfDrawingAcrossIt()
    {
        var evening = Hourly(new DateTime(2026, 10, 22, 18, 0, 0, DateTimeKind.Utc), 4);   // 18:00-21:00
        var morning = Hourly(new DateTime(2026, 10, 23, 7, 0, 0, DateTimeKind.Utc), 4);    // 07:00-10:00
        var window = (Start: new DateTime(2026, 10, 22, 18, 0, 0, DateTimeKind.Utc), End: new DateTime(2026, 10, 23, 10, 30, 0, DateTimeKind.Utc));

        var runs = RunsAfterLoad(evening.Concat(morning).ToList(), window.Start, window.End);

        /* 17 filled hours; indexes 4-12 (22:00-06:00) are the missing stretch and are not drawn. */
        Assert.Equal(new[] { (0, 4), (13, 4) }, runs);
    }

    [Fact]
    public void Strip_WithAShortHole_OrFewBuckets_StaysOneRun()
    {
        var start = new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc);
        var holey = Hourly(start, 5).Concat(Hourly(start.AddHours(7), 5)).ToList();   // two missing hours: a 3h jump is not over 3 x the 1h median
        Assert.Equal(new[] { (0, 12) }, RunsAfterLoad(holey, start, start.AddHours(11)));

        /* Fewer than two spacings have no rhythm to be silent against, so two buckets 9 hours apart never break. */
        var two = new List<TimeSliceBucket> { Hourly(start, 1)[0], Hourly(start.AddHours(9), 1)[0] };
        Assert.Equal(new[] { (0, 10) }, RunsAfterLoad(two, start, start.AddHours(9)));

        var steady = Hourly(start, 20);
        Assert.Equal(new[] { (0, 20) }, RunsAfterLoad(steady, start, start.AddHours(19)));
    }
}
