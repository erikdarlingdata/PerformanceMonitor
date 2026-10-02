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
/// #4305 (mirrors Lite's #4296, PR #4309): the Server Trends tab's correlated-lanes "Compare to" ghost-line
/// overlay (<see cref="CorrelatedTimelineLanesControl.GetOverviewComparisonRange"/> /
/// <see cref="CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal"/>, called from
/// <c>ResourceMetricsContent.RefreshServerTrendsAsync</c>) built its current window from
/// <c>DateTime.UtcNow</c> under a PRESET range -- a UTC basis -- while the correlated lanes' own reads
/// (<c>DatabaseService.GetCpuUtilizationAsync</c>, <c>GetTotalWaitStatsTrendAsync</c>, etc.) treat a
/// supplied fromDate/toDate as SERVER-LOCAL: their preset-range SQL branch filters collection_time against
/// SYSDATETIME(), not SYSUTCDATETIME() (every collect.* table's collection_time column defaults to
/// SYSDATETIME(), confirmed across install/02_create_tables.sql, 03_create_config_tables.sql and
/// 06_ensure_collection_table.sql). On a server not on UTC that shifted the reference window, and
/// <c>CorrelatedTimelineLanesControl.RefreshAsync</c>'s <c>timeShift</c> and <c>ComparisonLabel</c> had the
/// identical fallback, shifting the ghost line's X-axis alignment and its "N days ago" label the same way.
/// A custom range was already correct (ServerTab's pickers convert to server time via
/// ServerTimeHelper.DisplayTimeToServerTime before RefreshServerTrendsAsync ever sees fromDate/toDate).
/// </summary>
public class OverviewComparisonWindowOffsetTests
{
    private static readonly DateTime FixedUtcNow = new(2026, 3, 10, 16, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Preset range (fromDate/toDate both null): the current window end is the SERVER's local now, not
    /// FixedUtcNow itself -- proving the offset actually applies (the pre-#4305 bug was end == raw UTC now
    /// on any server with a non-zero offset).
    /// </summary>
    [Theory]
    [InlineData(300)]   // UTC+5
    [InlineData(-420)]  // UTC-7
    public void GetCurrentWindowServerLocal_PresetRange_AppliesServerOffset(int utcOffsetMinutes)
    {
        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, fromDate: null, toDate: null, FixedUtcNow, utcOffsetMinutes);

        var expectedEnd = FixedUtcNow.AddMinutes(utcOffsetMinutes);
        Assert.Equal(expectedEnd, end);
        Assert.Equal(expectedEnd.AddHours(-6), start);
        Assert.NotEqual(FixedUtcNow, end);
    }

    /// <summary>Custom range: fromDate/toDate pass through untouched, on either side of UTC.</summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetCurrentWindowServerLocal_CustomRange_UsesSuppliedBoundsVerbatim(int utcOffsetMinutes)
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);

        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, from, to, FixedUtcNow, utcOffsetMinutes);

        Assert.Equal(from, start);
        Assert.Equal(to, end);
    }

    /// <summary>
    /// Under a preset range, on a server east AND west of UTC, the reference window is the current
    /// SERVER-LOCAL window shifted back exactly 1 day (Yesterday) or 7 days (Last week / Same day last
    /// week), and CurrentFrom - From (the <c>timeShift</c> RefreshAsync applies) is EXACTLY that -- not off
    /// by the server's offset, and not off by a live-clock sampling gap, because CurrentFrom rides along
    /// from the SAME current-window computation refFrom was built from.
    /// </summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetOverviewComparisonRange_PresetRange_ShiftsServerLocalWindowExactly(int utcOffsetMinutes)
    {
        var (currentStart, currentEnd) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, null, null, FixedUtcNow, utcOffsetMinutes);

        var yesterday = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            1, hoursBack: 6, null, null, FixedUtcNow, utcOffsetMinutes);
        Assert.NotNull(yesterday);
        Assert.Equal(currentStart.AddDays(-1), yesterday!.Value.From);
        Assert.Equal(currentEnd.AddDays(-1), yesterday.Value.To);
        Assert.Equal(currentStart, yesterday.Value.CurrentFrom);
        Assert.Equal(TimeSpan.FromDays(1), yesterday.Value.CurrentFrom - yesterday.Value.From);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            2, hoursBack: 6, null, null, FixedUtcNow, utcOffsetMinutes);
        Assert.NotNull(lastWeek);
        Assert.Equal(currentStart.AddDays(-7), lastWeek!.Value.From);
        Assert.Equal(TimeSpan.FromDays(7), lastWeek.Value.CurrentFrom - lastWeek.Value.From);

        var sameDayLastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            3, hoursBack: 6, null, null, FixedUtcNow, utcOffsetMinutes);
        Assert.Equal(lastWeek, sameDayLastWeek);

        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(0, 6, null, null, FixedUtcNow, utcOffsetMinutes));
        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(-1, 6, null, null, FixedUtcNow, utcOffsetMinutes));
    }

    /// <summary>Same as above, under a custom range (fromDate/toDate supplied, already server-local).</summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetOverviewComparisonRange_CustomRange_ShiftsSuppliedWindowExactly(int utcOffsetMinutes)
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            2, hoursBack: 6, from, to, FixedUtcNow, utcOffsetMinutes);
        Assert.NotNull(lastWeek);
        Assert.Equal(from.AddDays(-7), lastWeek!.Value.From);
        Assert.Equal(to.AddDays(-7), lastWeek.Value.To);
        Assert.Equal(from, lastWeek.Value.CurrentFrom);
        Assert.Equal(TimeSpan.FromDays(7), lastWeek.Value.CurrentFrom - lastWeek.Value.From);
    }

    /// <summary>
    /// #4305's source pin. Confirmed by running this test against this branch's parent commit (pre-#4305):
    /// it failed on all three call-site assertions -- CompareToCombo_SelectionChanged fed
    /// ResourceMetricsContent the shared, UTC-preset-fallback GetComparisonRange() result, and
    /// RefreshAsync's timeShift/ComparisonLabel both sampled DateTime.UtcNow directly.
    /// </summary>
    [Fact]
    public void OverviewComparisonCallSites_UseServerLocalHelper_NotBareUtcNowFallback()
    {
        var serverTabSource = Read("deprecated", "Dashboard", "ServerTab.xaml.cs");
        var resourceMetricsSource = Read("deprecated", "Dashboard", "Controls", "ResourceMetricsContent.xaml.cs");
        var lanesSource = Read("deprecated", "Dashboard", "Controls", "CorrelatedTimelineLanesControl.xaml.cs");

        Assert.False(serverTabSource.Contains("await ResourceMetricsContent.SetComparisonRangeAsync(comparisonRange);"),
            "CompareToCombo_SelectionChanged still feeds ResourceMetricsContent the shared GetComparisonRange() " +
            "tuple, whose preset-range fallback is a raw UTC now -- wrong basis for the correlated lanes' " +
            "server-local reads (#4305).");
        Assert.Contains("await ResourceMetricsContent.SetComparisonRangeAsync(CompareToCombo.SelectedIndex);", serverTabSource);

        Assert.Contains("CorrelatedTimelineLanesControl.GetOverviewComparisonRange(", resourceMetricsSource);
        Assert.DoesNotContain("(DateTime From, DateTime To)? ComparisonRange { get; set; }", resourceMetricsSource);

        Assert.False(lanesSource.Contains("var timeShift = (fromDate ?? DateTime.UtcNow.AddHours(-hoursBack)) - refFrom;"),
            "RefreshAsync's timeShift still falls back to a raw DateTime.UtcNow (#4305).");
        Assert.Contains("var timeShift = comparisonRange.Value.CurrentFrom - refFrom;", lanesSource);

        Assert.False(lanesSource.Contains("var currentStart = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);"),
            "ComparisonLabel still falls back to a raw DateTime.UtcNow (#4305).");
        Assert.Contains("var daysBack = (range.CurrentFrom - range.From).TotalDays;", lanesSource);
    }

    /* ---------------- helpers (mirrors DashboardMirrorPinTests) ---------------- */

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
