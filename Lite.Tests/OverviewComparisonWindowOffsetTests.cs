/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4296: the Overview tab's correlated-lanes "Compare to" ghost-line overlay
/// (<see cref="CorrelatedTimelineLanesControl.GetOverviewComparisonRange"/> /
/// <see cref="CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal"/>, called from
/// <c>ServerTab.RefreshOverviewAsync</c>) built its current window from <c>DateTime.UtcNow</c> under a
/// PRESET range -- a UTC basis -- while the correlated lanes' own reads
/// (<c>LocalDataService.GetCpuUtilizationAsync</c>, <c>GetTotalWaitTrendAsync</c>, etc.) treat a supplied
/// fromDate/toDate as SERVER-LOCAL. On a server not on UTC that shifted the reference window,
/// and <c>CorrelatedTimelineLanesControl.RefreshAsync</c>'s <c>timeShift</c> and <c>ComparisonLabel</c> had
/// the identical fallback, shifting the ghost line's X-axis alignment and its "N days ago" label the same
/// way. A custom range was already correct (the pickers convert to server time before
/// <c>RefreshOverviewAsync</c> ever sees fromDate/toDate).
/// </summary>
public sealed class OverviewComparisonWindowOffsetTests
{
    private static readonly DateTime FixedUtcNow = new(2026, 3, 10, 16, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Preset range (fromDate/toDate both null): the current window end is the SERVER's local now, not
    /// FixedUtcNow itself -- proving the offset actually applies (the pre-#4296 bug was end == raw UTC now
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
    /// #4296 ruling items 1/2: under a preset range, on a server east AND west of UTC, the reference
    /// window is the current SERVER-LOCAL window shifted back exactly 1 day (Yesterday) or 7 days (Last
    /// week / Same day last week), and CurrentFrom - From (the <c>timeShift</c> RefreshAsync applies) is
    /// EXACTLY that -- not off by the server's offset, and not off by a live-clock sampling gap, because
    /// CurrentFrom rides along from the SAME current-window computation refFrom was built from.
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
    /// #4296 ruling item 4's source pin. Confirmed by checking out ServerTab.Refresh.cs and
    /// CorrelatedTimelineLanesControl.xaml.cs from this branch's parent commit (pre-#4296) and re-running
    /// this test: both call-site assertions failed against that source -- RefreshOverviewAsync called the
    /// shared, UTC-preset-fallback GetComparisonRange(), and RefreshAsync's timeShift/ComparisonLabel both
    /// sampled DateTime.UtcNow directly.
    /// </summary>
    [Fact]
    public void OverviewComparisonCallSites_UseServerLocalHelper_NotBareUtcNowFallback()
    {
        var refreshSource = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs"));
        var lanesSource = File.ReadAllText(ControlsFile("CorrelatedTimelineLanesControl.xaml.cs"));

        Assert.False(refreshSource.Contains("var comparison = GetComparisonRange();"),
            "RefreshOverviewAsync still calls the shared GetComparisonRange() (ServerTab.Comparison.cs), " +
            "whose preset-range fallback is a raw UTC now -- wrong basis for the correlated lanes' " +
            "server-local reads (#4296).");
        Assert.Contains("CorrelatedTimelineLanesControl.GetOverviewComparisonRange(", refreshSource);

        Assert.False(lanesSource.Contains("var timeShift = (fromDate ?? DateTime.UtcNow.AddHours(-hoursBack)) - refFrom;"),
            "RefreshAsync's timeShift still falls back to a raw DateTime.UtcNow (#4296).");
        Assert.Contains("var timeShift = comparisonRange.Value.CurrentFrom - refFrom;", lanesSource);

        Assert.False(lanesSource.Contains("var currentStart = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);"),
            "ComparisonLabel still falls back to a raw DateTime.UtcNow (#4296).");
        Assert.Contains("var daysBack = (range.CurrentFrom - range.From).TotalDays;", lanesSource);
    }

    /// <summary>
    /// #4320: under a CUSTOM range, RefreshAsync's baseline reference time is fromDate converted back to
    /// UTC (fromDate - utcOffsetMinutes). <c>BaselineLocalClock.LocalKey</c> expects UTC and converts it to
    /// server-local itself, so passing a server-local fromDate straight through (the pre-#4320 bug) shifted
    /// the baseline's local-hour lookup a SECOND time, by the server's own offset.
    /// </summary>
    [Theory]
    [InlineData(300)]   // UTC+5
    [InlineData(-420)]  // UTC-7
    public void GetBaselineReferenceTimeUtc_CustomRange_ConvertsFromDateBackToUtc(int utcOffsetMinutes)
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);

        var referenceTime = CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(
            hoursBack: 6, from, FixedUtcNow, utcOffsetMinutes);

        Assert.Equal(from.AddMinutes(-utcOffsetMinutes), referenceTime);
        Assert.NotEqual(from, referenceTime);
    }

    /// <summary>
    /// #4320: under a PRESET range (fromDate null), the baseline reference time is utcNow.AddHours(-hoursBack),
    /// unchanged -- it's already UTC and needs no conversion, regardless of the server's offset.
    /// </summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetBaselineReferenceTimeUtc_PresetRange_UsesUtcNowUnchanged(int utcOffsetMinutes)
    {
        var referenceTime = CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(
            hoursBack: 6, null, FixedUtcNow, utcOffsetMinutes);

        Assert.Equal(FixedUtcNow.AddHours(-6), referenceTime);
    }

    /// <summary>
    /// #4320 ruling's source pin. Confirmed by checking out CorrelatedTimelineLanesControl.xaml.cs from this
    /// branch's parent commit (pre-#4320) and re-running this assertion against that source: it failed --
    /// RefreshAsync's referenceTime fell back to "fromDate ?? DateTime.UtcNow.AddHours(-hoursBack)" directly,
    /// passing a custom range's server-local fromDate to GetBaselineForLaneAsync as if it were UTC.
    /// </summary>
    [Fact]
    public void BaselineReferenceTime_UsesHelper_NotFromDateDirectly()
    {
        var lanesSource = File.ReadAllText(ControlsFile("CorrelatedTimelineLanesControl.xaml.cs"));

        Assert.False(lanesSource.Contains("var referenceTime = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);"),
            "RefreshAsync's referenceTime still falls back to a raw fromDate, which is server-local under a " +
            "custom range but is passed to GetBaselineForLaneAsync as if it were UTC (#4320).");
        Assert.Contains(
            "var referenceTime = GetBaselineReferenceTimeUtc(hoursBack, fromDate, DateTime.UtcNow, utcOffsetMinutes);",
            lanesSource);
    }

    private static string ControlsFile(string name) => Path.Combine(ControlsDir(), name);

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
