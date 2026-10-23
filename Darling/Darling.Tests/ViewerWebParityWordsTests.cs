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

    private static List<DateTime> EveryFiveMinutes(DateTime start, int count)
        => Enumerable.Range(0, count).Select(i => start.AddMinutes(5 * i)).ToList();

    [Fact]
    public void Strip_BreaksWhereTheServiceWasOff_InsteadOfDrawingAcrossIt()
    {
        var evening = EveryFiveMinutes(new DateTime(2026, 10, 22, 18, 0, 0, DateTimeKind.Utc), 12);
        var morning = EveryFiveMinutes(new DateTime(2026, 10, 23, 7, 0, 0, DateTimeKind.Utc), 12);

        var runs = TimeRangeSlicerControl.SplitAtGaps(evening.Concat(morning).ToList());

        Assert.Equal(new[] { (0, 12), (12, 12) }, runs);
    }

    [Fact]
    public void Strip_WithNoGap_IsOneRun_AndAFewPointsNeverBreak()
    {
        var steady = EveryFiveMinutes(new DateTime(2026, 10, 23, 7, 0, 0, DateTimeKind.Utc), 20);
        Assert.Equal(new[] { (0, 20) }, TimeRangeSlicerControl.SplitAtGaps(steady));

        /* Fewer than three points have no rhythm to be silent against. */
        var two = new List<DateTime> { steady[0], steady[0].AddHours(9) };
        Assert.Equal(new[] { (0, 2) }, TimeRangeSlicerControl.SplitAtGaps(two));
        Assert.Empty(TimeRangeSlicerControl.SplitAtGaps(new List<DateTime>()));
    }
}
