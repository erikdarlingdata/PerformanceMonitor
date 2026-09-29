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
using PerformanceMonitor.Analysis.Baselines;
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

    /* #4766: US Eastern, whose 2026 spring change is 8 March at 07:00 UTC (02:00 EST jumps to 03:00 EDT). The
       winter offset rides along as the fallback a server with no zone id would have; the zone wins. */
    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    private static DateTime Local(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

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
            hoursBack: 6, fromDate: null, toDate: null, FixedUtcNow, ServerClock.FixedOffset(utcOffsetMinutes));

        var expectedEnd = FixedUtcNow.AddMinutes(utcOffsetMinutes);
        Assert.Equal(expectedEnd, end);
        Assert.Equal(expectedEnd.AddHours(-6), start);
        Assert.NotEqual(FixedUtcNow, end);
    }

    /// <summary>Custom range: fromDate/toDate are UTC instants (#4766) and are shown on the server's clock, on either side of UTC.</summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetCurrentWindowServerLocal_CustomRange_RendersTheSuppliedUtcBoundsOnTheServersClock(int utcOffsetMinutes)
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);

        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, from, to, FixedUtcNow, ServerClock.FixedOffset(utcOffsetMinutes));

        Assert.Equal(from.AddMinutes(utcOffsetMinutes), start);
        Assert.Equal(to.AddMinutes(utcOffsetMinutes), end);
    }

    /// <summary>
    /// #4766: a preset window that spans a spring-forward change is hoursBack REAL hours long. At 09:00 UTC on
    /// 8 March (05:00 EDT) a 6-hour window starts at 03:00 UTC, which was 22:00 EST the evening before: seven
    /// hours apart on the wall clock, six in real time. Subtracting hoursBack from the local end (one offset
    /// for the whole window) starts it at 23:00, which is only five real hours back.
    /// </summary>
    [Fact]
    public void GetCurrentWindowServerLocal_PresetRange_AcrossSpringForward_IsHoursBackRealHoursLong()
    {
        var clock = Eastern();
        var utcNow = new DateTime(2026, 3, 8, 9, 0, 0, DateTimeKind.Utc);

        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, fromDate: null, toDate: null, utcNow, clock);

        Assert.Equal(Local(2026, 3, 8, 5, 0), end);
        Assert.Equal(Local(2026, 3, 7, 22, 0), start);
        Assert.Equal(TimeSpan.FromHours(6), clock.ToUtc(end) - clock.ToUtc(start));
    }

    /// <summary>
    /// #4766: the same window across the fall-back change (1 November, 06:00 UTC): at 09:00 UTC (04:00 EST) a
    /// 6-hour window starts at 03:00 UTC, which was 23:00 EDT the night before.
    /// </summary>
    [Fact]
    public void GetCurrentWindowServerLocal_PresetRange_AcrossFallBack_IsHoursBackRealHoursLong()
    {
        var clock = Eastern();
        var utcNow = new DateTime(2026, 11, 1, 9, 0, 0, DateTimeKind.Utc);

        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, fromDate: null, toDate: null, utcNow, clock);

        Assert.Equal(Local(2026, 11, 1, 4, 0), end);
        Assert.Equal(Local(2026, 10, 31, 23, 0), start);
        Assert.Equal(TimeSpan.FromHours(6), clock.ToUtc(end) - clock.ToUtc(start));
    }

    /// <summary>
    /// #4766: only toDate given (the window ends at a chosen time and reaches hoursBack back): the start is the
    /// server-local time of (toDate as an instant) minus hoursBack, so it is hoursBack real hours across the
    /// change too. toDate 05:00 EDT on 8 March is 09:00 UTC; six hours back is 03:00 UTC, 22:00 EST.
    /// </summary>
    [Fact]
    public void GetCurrentWindowServerLocal_ToDateOnly_AcrossSpringForward_IsHoursBackRealHoursLong()
    {
        var clock = Eastern();
        var to = Local(2026, 3, 8, 9, 0);   /* 09:00 UTC, the instant of 05:00 EDT */

        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, fromDate: null, to, FixedUtcNow, clock);

        Assert.Equal(Local(2026, 3, 8, 5, 0), end);
        Assert.Equal(Local(2026, 3, 7, 22, 0), start);
        Assert.Equal(TimeSpan.FromHours(6), clock.ToUtc(end) - clock.ToUtc(start));
    }

    /// <summary>
    /// #4766: a fixed-offset clock (a server with no zone id) still gives the old wall-clock arithmetic: the same
    /// window at any date is hoursBack wall hours, because the offset never moves.
    /// </summary>
    [Fact]
    public void GetCurrentWindowServerLocal_PresetRange_FixedOffsetClock_IsWallClockArithmetic()
    {
        var clock = ServerClock.FixedOffset(-300);
        var utcNow = new DateTime(2026, 3, 8, 9, 0, 0, DateTimeKind.Utc);

        var (start, end) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, fromDate: null, toDate: null, utcNow, clock);

        Assert.Equal(Local(2026, 3, 8, 4, 0), end);
        Assert.Equal(Local(2026, 3, 7, 22, 0), start);
    }

    /// <summary>
    /// #4766: the X-axis window the lanes are pinned to. A preset range on US Eastern across the spring change is
    /// hoursBack REAL hours: at 09:00 UTC on 8 March (05:00 EDT) the 6-hour axis starts at 22:00 EST the evening
    /// before, not the 23:00 that "local now minus 6 wall-clock hours" gives, which would cut the first real hour off
    /// the axis while its samples are still plotted.
    /// </summary>
    [Fact]
    public void GetXAxisWindow_PresetRange_AcrossSpringForward_StartsHoursBackRealHoursBeforeNow()
    {
        var clock = Eastern();
        var utcNow = new DateTime(2026, 3, 8, 9, 0, 0, DateTimeKind.Utc);

        var (start, end) = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, utcNow, clock);

        Assert.Equal(Local(2026, 3, 8, 5, 0), end);
        Assert.Equal(Local(2026, 3, 7, 22, 0), start);
    }

    /// <summary>
    /// #4766: a custom range pins the axis to its own bounds, untouched (they are server-local already); with only
    /// one bound supplied the axis falls back to the preset window, as SyncXAxes always did.
    /// </summary>
    [Fact]
    public void GetXAxisWindow_CustomRange_UsesBothBoundsVerbatim_AndOneBoundFallsBackToThePreset()
    {
        var clock = Eastern();
        var from = Local(2026, 3, 1, 9, 0);
        var to = Local(2026, 3, 1, 17, 0);

        Assert.Equal((from, to), CorrelatedTimelineLanesControl.GetXAxisWindow(6, from, to, FixedUtcNow, clock));

        var preset = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, FixedUtcNow, clock);
        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(6, from, null, FixedUtcNow, clock));
        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, to, FixedUtcNow, clock));
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
        var clock = ServerClock.FixedOffset(utcOffsetMinutes);
        var (currentStart, currentEnd) = CorrelatedTimelineLanesControl.GetCurrentWindowServerLocal(
            hoursBack: 6, null, null, FixedUtcNow, clock);

        var yesterday = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            1, hoursBack: 6, null, null, FixedUtcNow, clock);
        Assert.NotNull(yesterday);
        Assert.Equal(currentStart.AddDays(-1), yesterday!.Value.From);
        Assert.Equal(currentEnd.AddDays(-1), yesterday.Value.To);
        Assert.Equal(currentStart, yesterday.Value.CurrentFrom);
        Assert.Equal(TimeSpan.FromDays(1), yesterday.Value.CurrentFrom - yesterday.Value.From);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            2, hoursBack: 6, null, null, FixedUtcNow, clock);
        Assert.NotNull(lastWeek);
        Assert.Equal(currentStart.AddDays(-7), lastWeek!.Value.From);
        Assert.Equal(TimeSpan.FromDays(7), lastWeek.Value.CurrentFrom - lastWeek.Value.From);

        var sameDayLastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            3, hoursBack: 6, null, null, FixedUtcNow, clock);
        Assert.Equal(lastWeek, sameDayLastWeek);

        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(0, 6, null, null, FixedUtcNow, clock));
        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(-1, 6, null, null, FixedUtcNow, clock));
    }

    /// <summary>Same as above, under a custom range (fromDate/toDate supplied as UTC, shown on the server clock).</summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetOverviewComparisonRange_CustomRange_ShiftsSuppliedWindowExactly(int utcOffsetMinutes)
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            2, hoursBack: 6, from, to, FixedUtcNow, ServerClock.FixedOffset(utcOffsetMinutes));
        Assert.NotNull(lastWeek);
        var fromLocal = from.AddMinutes(utcOffsetMinutes);
        var toLocal = to.AddMinutes(utcOffsetMinutes);
        Assert.Equal(fromLocal.AddDays(-7), lastWeek!.Value.From);
        Assert.Equal(toLocal.AddDays(-7), lastWeek.Value.To);
        Assert.Equal(fromLocal, lastWeek.Value.CurrentFrom);
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
    /// UTC (through the server's clock; a fixed-offset clock subtracts its offset). <c>BaselineLocalClock.LocalKey</c> expects UTC and converts it to
    /// server-local itself, so passing a server-local fromDate straight through (the pre-#4320 bug) shifted
    /// the baseline's local-hour lookup a SECOND time, by the server's own offset.
    /// </summary>
    [Fact]
    public void GetBaselineReferenceTimeUtc_CustomRange_IsTheUtcBoundUnchanged()
    {
        var from = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);

        var referenceTime = CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(hoursBack: 6, from, FixedUtcNow);

        Assert.Equal(from, referenceTime);
    }

    /// <summary>
    /// #4766: a custom fromDate converts back to UTC with the offset in force AT fromDate, not today's. On US
    /// Eastern, 10:00 on 1 March is EST (15:00 UTC) and 10:00 on 9 March is EDT (14:00 UTC); one offset for
    /// both would put one of them an hour out and key the baseline's local hour from the wrong hour. Today
    /// (FixedUtcNow) is itself on daylight time, so the winter fromDate is the far-side case.
    /// </summary>
    [Fact]
    public void GetBaselineReferenceTimeUtc_CustomRange_IsTheUtcBoundInEitherSeason()
    {
        var clock = Eastern();

        Assert.Equal(Local(2026, 3, 1, 10, 0), CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(6, Local(2026, 3, 1, 10, 0), FixedUtcNow));
        Assert.Equal(Local(2026, 3, 9, 10, 0), CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(6, Local(2026, 3, 9, 10, 0), FixedUtcNow));
    }

    /// <summary>
    /// #4766: a preset baseline reference time stays the UTC instant hoursBack ago, across a change too.
    /// </summary>
    [Fact]
    public void GetBaselineReferenceTimeUtc_PresetRange_AcrossSpringForward_IsTheUtcInstantHoursBackAgo()
    {
        var utcNow = new DateTime(2026, 3, 8, 9, 0, 0, DateTimeKind.Utc);

        Assert.Equal(
            new DateTime(2026, 3, 8, 3, 0, 0),
            CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(6, null, utcNow));
    }

    /// <summary>
    /// #4320: under a PRESET range (fromDate null), the baseline reference time is utcNow.AddHours(-hoursBack),
    /// unchanged -- it's already UTC and needs no conversion, regardless of the server's offset.
    /// </summary>
    [Fact]
    public void GetBaselineReferenceTimeUtc_PresetRange_UsesUtcNowUnchanged()
    {
        var referenceTime = CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(hoursBack: 6, null, FixedUtcNow);

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
            "var referenceTime = GetBaselineReferenceTimeUtc(hoursBack, fromDate, DateTime.UtcNow);",
            lanesSource);
    }

    private static string ControlsFile(string name) => Path.Combine(ControlsDir(), name);

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
