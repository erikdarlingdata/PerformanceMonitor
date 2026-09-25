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
using PerformanceMonitorDashboard.Controls;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #4313 (follows #4305/#4317's fix for the Overview ghost line): the Query Stats / Proc Stats / Query
/// Store comparison grids built their current window from <c>DateTime.UtcNow</c> under a preset range --
/// ServerTab.xaml.cs's GetComparisonRange and QueryPerformanceContent.Comparison.cs's
/// RefreshComparisonAsync both had the identical <c>?? DateTime.UtcNow</c> fallback -- while the reads they
/// feed (DatabaseService.QueryPerformance.Comparison.cs's GetQueryStatsComparisonAsync,
/// GetProcedureStatsComparisonAsync, GetQueryStoreComparisonAsync) filter on collection_time, which is
/// server-local (collect.query_stats/proc_stats/query_store_stats default it to SYSDATETIME()). A third
/// spot, CorrelatedTimelineLanesControl.xaml.cs's RefreshAsync baseline referenceTime, had the same
/// fallback feeding SqlServerBaselineProvider.GetBaselineAsync's hour-of-day/day-of-week bucket match,
/// also against collection_time. All three now go through GetCurrentWindowServerLocal /
/// GetOverviewComparisonRange (#4305's shared, already-tested server-local window logic) instead of an
/// independent UTC-anchored resample. A custom range was already correct (ServerTab's pickers convert to
/// server time via ServerTimeHelper.DisplayTimeToServerTime before GetComparisonRange/RefreshComparisonAsync
/// ever see fromDate/toDate).
/// </summary>
public class QueryComparisonWindowOffsetTests
{
    private static readonly DateTime FixedUtcNow = new(2026, 3, 10, 16, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// The grids' current window (what GetComparisonRange/RefreshComparisonAsync now compute via
    /// GetCurrentWindowServerLocal) under a preset range, on a server east and west of UTC: end is the
    /// server's local now, not FixedUtcNow itself.
    /// </summary>
    [Theory]
    [InlineData(300)]   // UTC+5
    [InlineData(-420)]  // UTC-7
    public void GridsCurrentWindow_PresetRange_UsesServerLocalNow(int utcOffsetMinutes)
    {
        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 24, fromDate: null, toDate: null, FixedUtcNow, utcOffsetMinutes);

        var expectedEnd = FixedUtcNow.AddMinutes(utcOffsetMinutes);
        Assert.Equal(expectedEnd, end);
        Assert.Equal(expectedEnd.AddHours(-24), start);
        Assert.NotEqual(FixedUtcNow, end);
    }

    /// <summary>The grids' current window under a custom range: supplied bounds pass through verbatim.</summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GridsCurrentWindow_CustomRange_UsesSuppliedBoundsVerbatim(int utcOffsetMinutes)
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);

        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 24, from, to, FixedUtcNow, utcOffsetMinutes);

        Assert.Equal(from, start);
        Assert.Equal(to, end);
    }

    /// <summary>
    /// GetComparisonRange's three dropdown options (Yesterday / Last week / Same day last week), east and
    /// west of UTC, under a preset range: the grids' baseline (From, To) shifts the SERVER-LOCAL current
    /// window back exactly 1 or 7 days, not a UTC-anchored one.
    /// </summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetComparisonRangeEquivalent_PresetRange_ShiftsServerLocalWindow(int utcOffsetMinutes)
    {
        var (currentStart, currentEnd) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 24, null, null, FixedUtcNow, utcOffsetMinutes);

        var yesterday = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            1, hoursBack: 24, null, null, FixedUtcNow, utcOffsetMinutes);
        Assert.NotNull(yesterday);
        Assert.Equal(currentStart.AddDays(-1), yesterday!.Value.From);
        Assert.Equal(currentEnd.AddDays(-1), yesterday.Value.To);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            2, hoursBack: 24, null, null, FixedUtcNow, utcOffsetMinutes);
        Assert.NotNull(lastWeek);
        Assert.Equal(currentStart.AddDays(-7), lastWeek!.Value.From);
        Assert.Equal(currentEnd.AddDays(-7), lastWeek.Value.To);

        var sameDayLastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            3, hoursBack: 24, null, null, FixedUtcNow, utcOffsetMinutes);
        Assert.Equal(lastWeek, sameDayLastWeek);

        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(0, 24, null, null, FixedUtcNow, utcOffsetMinutes));
    }

    /// <summary>Same as above under a custom range (fromDate/toDate supplied, already server-local).</summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetComparisonRangeEquivalent_CustomRange_ShiftsSuppliedWindow(int utcOffsetMinutes)
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            2, hoursBack: 24, from, to, FixedUtcNow, utcOffsetMinutes);
        Assert.NotNull(lastWeek);
        Assert.Equal(from.AddDays(-7), lastWeek!.Value.From);
        Assert.Equal(to.AddDays(-7), lastWeek.Value.To);
    }

    /// <summary>
    /// #4313's source pin. Confirmed by hand-reverting each call site to its pre-fix line, running this
    /// test (it fails on the reverted assertion), then restoring: ServerTab.xaml.cs's GetComparisonRange,
    /// QueryPerformanceContent.Comparison.cs's RefreshComparisonAsync, and CorrelatedTimelineLanesControl's
    /// baseline referenceTime all used a bare <c>?? DateTime.UtcNow</c> preset-range fallback before #4313.
    /// Scoped to the Dashboard's copy only -- Lite's CorrelatedTimelineLanesControl.xaml.cs mirror keeps
    /// the same pattern pending its own follow-up issue (filed from #4313, not fixed here).
    /// </summary>
    [Fact]
    public void GridComparisonCallSites_UseServerLocalHelper_NotBareUtcNowFallback()
    {
        var serverTabSource = Read("deprecated", "Dashboard", "ServerTab.xaml.cs");
        var queryPerfComparisonSource = Read("deprecated", "Dashboard", "Controls", "QueryPerformanceContent.Comparison.cs");
        var lanesSource = Read("deprecated", "Dashboard", "Controls", "CorrelatedTimelineLanesControl.xaml.cs");

        Assert.False(serverTabSource.Contains("var currentEnd = _globalToDate ?? DateTime.UtcNow;"),
            "GetComparisonRange still falls back to a raw UTC now under a preset range (#4313).");
        Assert.Contains("Controls.CorrelatedTimelineLanesControl.GetOverviewComparisonRange(", serverTabSource);

        Assert.False(queryPerfComparisonSource.Contains("var currentEnd = _queryStatsToDate ?? DateTime.UtcNow;"),
            "RefreshComparisonAsync still falls back to a raw UTC now under a preset range (#4313).");
        Assert.Contains("CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(", queryPerfComparisonSource);

        Assert.False(lanesSource.Contains("var referenceTime = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);"),
            "RefreshAsync's baseline referenceTime still falls back to a raw UTC now (#4313).");
        Assert.Contains("var referenceTime = GetCurrentWindowServerLocal(", lanesSource);
    }

    /* ---------------- helpers (mirrors OverviewComparisonWindowOffsetTests) ---------------- */

    private static string Read(params string[] pathParts)
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, $"Could not locate the repo root (a directory containing deprecated/Dashboard/Dashboard.csproj) by walking up from {AppContext.BaseDirectory}.");
        var path = Path.Combine(new[] { root! }.Concat(pathParts).ToArray());
        Assert.True(File.Exists(path), $"Source file not found: {path}");
        return File.ReadAllText(path);
    }

    private static string? FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 10 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir.FullName, "deprecated", "Dashboard", "Dashboard.csproj")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        return null;
    }
}
