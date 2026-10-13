/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5562: the Viewer's side of the shared time range picker. <see cref="ViewerTimeRangeWindow"/> is the pure core: the
/// window a held range reads, the whole hours the few integer readers take, the legacy preference index, what Apply to
/// All sends, and the cadence note. (A ViewerServerTab needs the app's resources, so the rules are pinned here.)
/// </summary>
public sealed class ViewerTimeRangeWindowTests
{
    private static readonly DateTime Now = new(2026, 10, 8, 15, 0, 0, DateTimeKind.Unspecified);
    private static readonly TimeZoneInfo Eastern = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");

    [Theory]
    [InlineData(0, "1h")]
    [InlineData(1, "4h")]
    [InlineData(2, "12h")]
    [InlineData(3, "1d")]
    [InlineData(4, "1w")]
    [InlineData(5, "1d")]
    [InlineData(-1, "1d")]
    public void LegacyIndex_MapsToTheRangeItAlwaysMeant(int index, string expectedId)
    {
        Assert.Equal(expectedId, ViewerTimeRangeWindow.FromLegacyIndex(index).Id);
    }

    [Fact]
    public void ACalendarPeriod_FlowsThroughTheWindow_InTheDisplayZone()
    {
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), Now, Eastern);

        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), start);   // local midnight, EDT
        Assert.Equal(Now, end);
        Assert.True(live);
    }

    [Fact]
    public void AFinishedPeriod_KeepsItsInstants_AndIsNotLive()
    {
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday), Now, Eastern);

        Assert.Equal(new DateTime(2026, 10, 7, 4, 0, 0), start);
        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), end);
        Assert.False(live);
    }

    [Theory]
    [InlineData(5, 1)]
    [InlineData(45, 1)]
    [InlineData(60, 1)]
    [InlineData(61, 2)]
    [InlineData(24 * 60, 24)]
    public void ASubHourSpan_FlowsThroughTheWindow_AndRoundsUpToWholeHoursOnlyForTheIntegerReaders(int minutes, int expectedHours)
    {
        var (start, end, _) = ViewerTimeRangeWindow.Window(TimeRangeSpec.Relative(TimeSpan.FromMinutes(minutes)), Now, Eastern);

        Assert.Equal(TimeSpan.FromMinutes(minutes), end - start);
        Assert.Equal(expectedHours, ViewerTimeRangeWindow.HoursBack(start, end));
    }

    [Fact]
    public void ARangeThatCannotResolveYet_FallsBackToTwentyFourHours()
    {
        // A since-range whose start has not happened yet cannot resolve: the Viewer's historical 24 hours stands in.
        var nowUtc = new DateTime(2026, 10, 8, 4, 1, 0, DateTimeKind.Unspecified);
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.SinceInstant(nowUtc.AddHours(1)), nowUtc, Eastern);

        Assert.Equal(TimeSpan.FromHours(24), end - start);
        Assert.True(live);
    }

    [Fact]
    public void TodayInItsFirstMinutes_ReadsMidnightToNow_NeverAnotherRange()
    {
        // 00:01 in New York: exempt from the 5-minute floor (seat ruling), so it reads 00:00 to now.
        var justAfterMidnight = new DateTime(2026, 10, 8, 4, 1, 0, DateTimeKind.Unspecified);
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), justAfterMidnight, Eastern);

        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), start);
        Assert.Equal(justAfterMidnight, end);
        Assert.True(live);
    }

    [Theory]
    [InlineData("Queries", "Query Store by Duration", "query_store")]
    [InlineData("Queries", "Active Queries", "query_snapshots")]
    [InlineData("Queries", null, "query_stats")]
    [InlineData("Blocking", "Current Waits", "waiting_tasks")]
    [InlineData("Blocking", "Blocked Process Reports", "blocked_process_report")]
    [InlineData("Memory", "Memory Clerks", "memory_clerks")]
    [InlineData("CPU", "CPU Scheduler", "cpu_scheduler_stats")]
    [InlineData("Wait Stats", "anything", "wait_stats")]
    public void TheMainCollector_FollowsTheSubTabOnScreen_AsLiteDoes(string tab, string? sub, string expected)
    {
        Assert.Equal(expected, ViewerTimeRangeWindow.MainCollectorFor(tab, sub));
        Assert.True(PerformanceMonitor.Collectors.CollectorScheduleDefaults.All.ContainsKey(expected));
    }

    [Theory]
    [InlineData("400000mo")]
    [InlineData("1000000d")]
    [InlineData("999999w")]
    public void ACorruptPreferenceRange_FallsBackToTheIndexWithoutThrowing(string corrupt)
    {
        /* #5562 review H1: a hand-edited viewer-preferences.json used to throw OverflowException from Load() at startup. */
        var prefs = new ViewerPreferences { DefaultTimeRange = corrupt, DefaultTimeRangeIndex = 1 }.Normalize();
        Assert.Equal("4h", prefs.DefaultTimeRange);
        Assert.Equal(1, prefs.DefaultTimeRangeIndex);
    }

    [Fact]
    public void JobHistory_LongestChoice_IsAYear()
    {
        Assert.Equal(TimeSpan.FromDays(365), ViewerTimeRangeWindow.JobHistoryLongestChoice.Span);
        Assert.False(TimeRangeSpec.TryFromId("1y", out _));
    }

    [Fact]
    public void ApplyToAll_SendsACalendarPeriodAsTheInstantsItNames_AndLeavesARollingRangeAlone()
    {
        var rolling = TimeRangeSpec.Relative(TimeSpan.FromHours(4));
        Assert.Same(rolling, TimeRangePresets.ForBroadcast(rolling, Now, Eastern));

        var today = TimeRangePresets.ForBroadcast(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), Now, Eastern);
        Assert.Equal(TimeRangeKind.Since, today.Kind);
        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), today.StartUtc);

        var yesterday = TimeRangePresets.ForBroadcast(TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday), Now, Eastern);
        Assert.Equal(TimeRangeKind.Fixed, yesterday.Kind);
        Assert.Equal(new DateTime(2026, 10, 7, 4, 0, 0), yesterday.StartUtc);
        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), yesterday.EndUtc);
    }

    [Fact]
    public void Preferences_LegacyIndexOnly_MapsToTheNewStringId()
    {
        var prefs = new ViewerPreferences { DefaultTimeRangeIndex = 4 }.Normalize();

        Assert.Equal("1w", prefs.DefaultTimeRange);
        Assert.Equal(4, prefs.DefaultTimeRangeIndex);
    }

    [Fact]
    public void Preferences_APeriodId_WinsOverTheLegacyIndex_AndAFixedRangeIsNeverKept()
    {
        var period = new ViewerPreferences { DefaultTimeRange = "previous-week", DefaultTimeRangeIndex = 0 }.Normalize();
        Assert.Equal("previous-week", period.DefaultTimeRange);
        Assert.Equal(3, period.DefaultTimeRangeIndex);   // the nearest the legacy combo could hold: its default

        var fixedRange = new ViewerPreferences
        {
            DefaultTimeRange = "fixed:2026-10-01T04:00:00Z/2026-10-02T04:00:00Z",
            DefaultTimeRangeIndex = 1,
        }.Normalize();
        Assert.Equal("4h", fixedRange.DefaultTimeRange);

        var unknown = new ViewerPreferences { DefaultTimeRange = "nonsense" }.Normalize();
        Assert.Equal("1d", unknown.DefaultTimeRange);
    }

    [Theory]
    [InlineData("Overview", "cpu_utilization")]
    [InlineData("Wait Stats", "wait_stats")]
    [InlineData("Queries", "query_stats")]
    [InlineData("Plan Viewer", null)]
    public void TheMainCollector_IsNamedByTheInnerTab(string header, string? expected)
    {
        Assert.Equal(expected, ViewerTimeRangeWindow.MainCollectorFor(header));
    }

    [Fact]
    public void TheSampleInterval_IsTheShippedCadence_WhenNothingOverridesIt()
    {
        Assert.Equal(TimeSpan.FromMinutes(1), ViewerTimeRangeWindow.SampleIntervalFor("wait_stats", 7, Array.Empty<CollectorScheduleRow>()));
        Assert.Null(ViewerTimeRangeWindow.SampleIntervalFor(null, 7, null));
        Assert.Null(ViewerTimeRangeWindow.SampleIntervalFor("not_a_collector", 7, null));
    }

    [Fact]
    public void ADataStart_ShowsTheNoteOnThePicker_OnlyWhenTheRangeStartsWellBeforeIt()
    {
        var range = TimeRangeSpec.Relative(TimeSpan.FromDays(2));
        Assert.True(range.TryResolve(Now, Eastern, out var resolved, out _));

        Assert.NotNull(TimeRangeNotes.DataStartNote(resolved!, Now.AddHours(-3)));
        Assert.Null(TimeRangeNotes.DataStartNote(resolved!, Now.AddDays(-3)));
    }

    /* ---- the duration trends' tier clock (#5725) ---- */

    /// <summary>The regression: a live "past 95 hours" window starts exactly on the raw boundary (96 h less the 1 h margin). The viewer
    /// hands the tier decision the window's own end, so the window routes Raw however long the read takes to start; the old second
    /// <c>DateTime.UtcNow</c> read a few milliseconds LATER flipped the same window to the hourly tier.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(250)]
    public void ALive95HourWindow_RoutesRawEveryTime_WhenTheTierIsDecidedAtTheWindowEnd(int millisecondsLater)
    {
        var now = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.Relative(TimeSpan.FromHours(95)), now, TimeZoneInfo.Utc);
        Assert.True(live);
        Assert.Equal(TimeSpan.FromHours(95), end - start);
        Assert.Equal(TimescaleSupport.RawRetentionSpan - DurationTrendRouting.RawTierMargin, end - start);

        var tierNow = ViewerTimeRangeWindow.TierNowUtc(live, end);

        Assert.Equal(end, tierNow);
        Assert.True(DurationTrendRouting.ShouldUseRawTier(start, tierNow!.Value));
        Assert.Equal(RetentionTier.Raw, DurationTrendRouting.ResolveTier(start, tierNow.Value, hourlyAvailable: true, default));

        /* The old behaviour, for contrast (the revert check): a clock read after the window was computed flips the same window. */
        if (millisecondsLater > 0)
        {
            var late = end.AddMilliseconds(millisecondsLater);
            Assert.False(DurationTrendRouting.ShouldUseRawTier(start, late));
            Assert.Equal(RetentionTier.Hourly, DurationTrendRouting.ResolveTier(start, late, hourlyAvailable: true, default));
        }
    }

    [Fact]
    public void AFixedRange_KeepsTheWallClock_AndNeverUsesItsPastEndAsTheClock()
    {
        var now = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);
        var (_, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.FixedRange(now.AddDays(-10), now.AddDays(-9)), now, TimeZoneInfo.Utc);

        Assert.False(live);
        Assert.Null(ViewerTimeRangeWindow.TierNowUtc(live, end));
    }

    [Fact]
    public void TheSharedTierClock_IsOneDefinition_TheMcpToolsAndTheViewerBothUse()
    {
        var wall = new DateTime(2026, 10, 12, 12, 0, 3, DateTimeKind.Utc);
        var end = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);

        Assert.Equal(end, DurationTrendRouting.TierClock(true, end, () => wall));
        Assert.Equal(wall, DurationTrendRouting.TierClock(false, end, () => wall));
        Assert.Equal(end, PerformanceMonitor.Darling.Service.Mcp.DarlingMcpTrendTools.TierClock(null, end, () => wall));
        Assert.Equal(wall, PerformanceMonitor.Darling.Service.Mcp.DarlingMcpTrendTools.TierClock("2026-09-12T12:00:00Z", end, () => wall));

        var tools = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTrendTools.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("DurationTrendRouting.TierClock(string.IsNullOrWhiteSpace(asOf), windowEndUtc, wallClock)", tools, StringComparison.Ordinal);
        Assert.DoesNotContain("wallClock?.Invoke()", tools, StringComparison.Ordinal);
    }

    /// <summary>Source pin: the Performance Trends load reads its window with the tier clock and hands it to both duration trends; the
    /// clock is the window's end only for a live range (<see cref="ViewerTimeRangeWindow.TierNowUtc"/> returns null for a fixed one).</summary>
    [Fact]
    public void ThePerformanceTrendsLoad_PassesTheTierClock_ToBothDurationTrends()
    {
        static string Read(string file) => RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file).Replace("\r\n", "\n", StringComparison.Ordinal);

        var trends = Read("ViewerServerTab.QueryTrends.cs");
        Assert.Contains("GetQueryDurationTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter, nowUtc: tierNowUtc)", trends, StringComparison.Ordinal);
        Assert.Contains("GetProcedureDurationTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter, nowUtc: tierNowUtc)", trends, StringComparison.Ordinal);
        Assert.Contains("LoadPerformanceTrendsAsync(DateTime startUtc, DateTime endUtc, DateTime? tierNowUtc)", trends, StringComparison.Ordinal);

        var queries = Read("ViewerServerTab.Queries.cs");
        Assert.Contains("var (startUtc, endUtc, tierNowUtc) = GetWindowUtcWithTierClock();", queries, StringComparison.Ordinal);
        Assert.Contains("await LoadPerformanceTrendsAsync(startUtc, endUtc, tierNowUtc);", queries, StringComparison.Ordinal);

        var range = Read("ViewerServerTab.TimeRange.cs");
        Assert.Contains("return (startUtc, endUtc, ViewerTimeRangeWindow.TierNowUtc(isLive, endUtc));", range, StringComparison.Ordinal);

        var window = Read("ViewerTimeRangeWindow.cs");
        Assert.Contains("isLive ? DurationTrendRouting.TierClock(windowEndIsNow: true, endUtc) : null;", window, StringComparison.Ordinal);
    }
}
