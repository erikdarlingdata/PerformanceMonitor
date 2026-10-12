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
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4296, #4766: the Overview tab's "Compare to" ghost lines are "the same wall-clock hours N days earlier" on the
/// clock of the server being monitored: not in the display zone, and not a fixed number of real hours. The current
/// window is the one the axis spans (naive-UTC instants); the reference window is worked out from it once
/// (<see cref="CorrelatedTimelineLanesControl.GetOverviewComparisonRange"/> through
/// <see cref="TimeWindows.OverviewReference"/>) and the reads take it as it is; each ghost row goes back onto the
/// current axis by the same wall-clock rule (<see cref="TimeWindows.GhostX"/>). On a server across a clock change
/// that is 23 or 25 real hours, on a server with a fixed offset it is exactly N * 24 hours, and the same current
/// window on a server on UTC is always exactly N * 24 hours.
/// </summary>
public sealed class OverviewComparisonWindowOffsetTests
{
    private static readonly DateTime FixedUtcNow = new(2026, 3, 10, 16, 0, 0, DateTimeKind.Utc);

    /* US Eastern, whose 2026 spring change is 8 March at 07:00 UTC (02:00 EST jumps to 03:00 EDT) and whose autumn
       change is 1 November at 06:00 UTC (02:00 EDT falls back to 01:00 EST). The winter offset rides along as the
       fallback a server with no zone id would have; the zone wins. */
    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    /* A naive-UTC instant: the frame a window is held in from the moment it is made to the read that fetches it. */
    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// The X-axis window every lane is pinned to: a preset range is hoursBack REAL hours ending now, so across the
    /// spring change at 09:00 UTC on 8 March a 6-hour axis is 03:00 to 09:00 UTC. Wall-clock arithmetic in a
    /// zone (the local end minus six hours) would start it an hour late and cut the first real hour off the axis
    /// while its samples are still plotted.
    /// </summary>
    [Fact]
    public void GetXAxisWindow_PresetRange_AcrossSpringForward_IsHoursBackRealHoursEndingNow()
    {
        var utcNow = new DateTime(2026, 3, 8, 9, 0, 0, DateTimeKind.Utc);

        var (from, to) = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, utcNow);

        Assert.Equal(Utc(2026, 3, 8, 3, 0), from);
        Assert.Equal(Utc(2026, 3, 8, 9, 0), to);
        Assert.Equal(TimeSpan.FromHours(6), to - from);
    }

    /// <summary>
    /// A custom range pins the axis to its own two bounds, which are instants already and pass through untouched;
    /// with only one bound supplied the axis falls back to the preset window, as it always did.
    /// </summary>
    [Fact]
    public void GetXAxisWindow_CustomRange_IsBothBoundsAsHeld_AndOneBoundFallsBackToThePreset()
    {
        var from = Utc(2026, 3, 1, 9, 0);
        var to = Utc(2026, 3, 1, 17, 0);

        Assert.Equal((from, to), CorrelatedTimelineLanesControl.GetXAxisWindow(6, from, to, FixedUtcNow));

        var preset = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, FixedUtcNow);
        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(6, from, null, FixedUtcNow));
        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, to, FixedUtcNow));
    }

    /// <summary>
    /// On a server with a fixed offset (a server with no zone id) the offset never moves, so the reference window
    /// is the current one shifted back exactly 1 day (Yesterday) or 7 days (Last week / Same day last week), east
    /// and west of UTC, and the day count comes back for the ghost rows to use.
    /// </summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetOverviewComparisonRange_PresetRange_OnAFixedOffsetServer_ShiftsTheCurrentWindowByExactDays(int utcOffsetMinutes)
    {
        var clock = ServerClock.FixedOffset(utcOffsetMinutes);
        var (currentFrom, currentTo) = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, FixedUtcNow);

        var yesterday = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(1, hoursBack: 6, null, null, FixedUtcNow, clock);
        Assert.NotNull(yesterday);
        Assert.Equal(currentFrom.AddDays(-1), yesterday!.Value.FromUtc);
        Assert.Equal(currentTo.AddDays(-1), yesterday.Value.ToUtc);
        Assert.Equal(1, yesterday.Value.Days);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(2, hoursBack: 6, null, null, FixedUtcNow, clock);
        Assert.NotNull(lastWeek);
        Assert.Equal(currentFrom.AddDays(-7), lastWeek!.Value.FromUtc);
        Assert.Equal(currentTo.AddDays(-7), lastWeek.Value.ToUtc);
        Assert.Equal(7, lastWeek.Value.Days);

        var sameDayLastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(3, hoursBack: 6, null, null, FixedUtcNow, clock);
        Assert.Equal(lastWeek, sameDayLastWeek);

        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(0, 6, null, null, FixedUtcNow, clock));
        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(-1, 6, null, null, FixedUtcNow, clock));
        Assert.Null(CorrelatedTimelineLanesControl.GetOverviewComparisonRange(4, 6, null, null, FixedUtcNow, clock));
    }

    /// <summary>
    /// A custom range is the same window shifted the same way: its two bounds are the current window as held.
    /// </summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-420)]
    public void GetOverviewComparisonRange_CustomRange_OnAFixedOffsetServer_ShiftsTheHeldWindowByExactDays(int utcOffsetMinutes)
    {
        var from = Utc(2026, 3, 10, 9, 0);
        var to = Utc(2026, 3, 10, 17, 0);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
            2, hoursBack: 6, from, to, FixedUtcNow, ServerClock.FixedOffset(utcOffsetMinutes));

        Assert.NotNull(lastWeek);
        Assert.Equal(from.AddDays(-7), lastWeek!.Value.FromUtc);
        Assert.Equal(to.AddDays(-7), lastWeek.Value.ToUtc);
        Assert.Equal(7, lastWeek.Value.Days);
    }

    /// <summary>
    /// The same wall-clock hours yesterday across the spring change. On 8 March 2026 13:00 to 17:00 UTC is 09:00 to
    /// 13:00 EDT; yesterday's 09:00 to 13:00 was on EST, so it is 14:00 to 18:00 UTC on 7 March: 23 real hours
    /// earlier, not 24. A preset range that ends at 17:00 UTC is the same window. The ghost row at 14:00 UTC goes
    /// back to 13:00 UTC, and the ends of the reference go to the ends of the current window.
    /// </summary>
    [Fact]
    public void GetOverviewComparisonRange_Yesterday_AcrossSpringForward_IsTheSameWallHoursOnTheServersClock()
    {
        var from = Utc(2026, 3, 8, 13, 0);
        var to = Utc(2026, 3, 8, 17, 0);
        var utcNow = new DateTime(2026, 3, 8, 17, 0, 0, DateTimeKind.Utc);

        var custom = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(1, 4, from, to, utcNow, Eastern());
        var preset = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(1, 4, null, null, utcNow, Eastern());

        Assert.NotNull(custom);
        Assert.Equal(Utc(2026, 3, 7, 14, 0), custom!.Value.FromUtc);
        Assert.Equal(Utc(2026, 3, 7, 18, 0), custom.Value.ToUtc);
        Assert.Equal(1, custom.Value.Days);
        Assert.Equal(TimeSpan.FromHours(23), from - custom.Value.FromUtc);
        Assert.Equal(custom, preset);

        var zone = Eastern().AsTimeZone();
        Assert.Equal(Utc(2026, 3, 8, 13, 0), TimeWindows.GhostX(Utc(2026, 3, 7, 14, 0), custom.Value.Days, zone));
        Assert.Equal(from, TimeWindows.GhostX(custom.Value.FromUtc, custom.Value.Days, zone));
        Assert.Equal(to, TimeWindows.GhostX(custom.Value.ToUtc, custom.Value.Days, zone));
    }

    /// <summary>
    /// The same window across the autumn change. On 1 November 2026 14:00 to 18:00 UTC is 09:00 to 13:00 EST;
    /// yesterday's 09:00 to 13:00 was on EDT, so it is 13:00 to 17:00 UTC on 31 October: 25 real hours earlier.
    /// </summary>
    [Fact]
    public void GetOverviewComparisonRange_Yesterday_AcrossFallBack_IsTheSameWallHoursOnTheServersClock()
    {
        var from = Utc(2026, 11, 1, 14, 0);
        var to = Utc(2026, 11, 1, 18, 0);

        var yesterday = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(1, 4, from, to, FixedUtcNow, Eastern());

        Assert.NotNull(yesterday);
        Assert.Equal(Utc(2026, 10, 31, 13, 0), yesterday!.Value.FromUtc);
        Assert.Equal(Utc(2026, 10, 31, 17, 0), yesterday.Value.ToUtc);
        Assert.Equal(TimeSpan.FromHours(25), from - yesterday.Value.FromUtc);

        var zone = Eastern().AsTimeZone();
        Assert.Equal(from, TimeWindows.GhostX(yesterday.Value.FromUtc, yesterday.Value.Days, zone));
        Assert.Equal(to, TimeWindows.GhostX(yesterday.Value.ToUtc, yesterday.Value.Days, zone));
    }

    /// <summary>
    /// Last week keeps the wall hours too: 9 March 2026 13:00 to 17:00 UTC is 09:00 to 13:00 EDT, and the same hours
    /// a week earlier were on EST, 14:00 to 18:00 UTC on 2 March: 167 real hours, not 168. Same day last week is
    /// the same window.
    /// </summary>
    [Fact]
    public void GetOverviewComparisonRange_LastWeek_AcrossSpringForward_IsTheSameWallHoursOnTheServersClock()
    {
        var from = Utc(2026, 3, 9, 13, 0);
        var to = Utc(2026, 3, 9, 17, 0);

        var lastWeek = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(2, 4, from, to, FixedUtcNow, Eastern());

        Assert.NotNull(lastWeek);
        Assert.Equal(Utc(2026, 3, 2, 14, 0), lastWeek!.Value.FromUtc);
        Assert.Equal(Utc(2026, 3, 2, 18, 0), lastWeek.Value.ToUtc);
        Assert.Equal(7, lastWeek.Value.Days);
        Assert.Equal(TimeSpan.FromHours(167), from - lastWeek.Value.FromUtc);
        Assert.Equal(lastWeek, CorrelatedTimelineLanesControl.GetOverviewComparisonRange(3, 4, from, to, FixedUtcNow, Eastern()));

        Assert.Equal(from, TimeWindows.GhostX(lastWeek.Value.FromUtc, lastWeek.Value.Days, Eastern().AsTimeZone()));
    }

    /// <summary>
    /// "The same hours" are the MONITORED server's, so the clock passed in decides the reference: the current window
    /// of the spring change day gives 14:00 to 18:00 UTC on a US Eastern server and exactly 24 hours earlier
    /// (13:00 to 17:00 UTC) on a server on UTC.
    /// </summary>
    [Fact]
    public void GetOverviewComparisonRange_FollowsTheServersClock_NotTheDisplayZone()
    {
        var from = Utc(2026, 3, 8, 13, 0);
        var to = Utc(2026, 3, 8, 17, 0);

        var eastern = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(1, 4, from, to, FixedUtcNow, Eastern());
        var onUtc = CorrelatedTimelineLanesControl.GetOverviewComparisonRange(1, 4, from, to, FixedUtcNow, ServerClock.Utc);

        Assert.Equal(Utc(2026, 3, 7, 14, 0), eastern!.Value.FromUtc);
        Assert.Equal(Utc(2026, 3, 7, 13, 0), onUtc!.Value.FromUtc);
        Assert.Equal(Utc(2026, 3, 7, 17, 0), onUtc.Value.ToUtc);
    }

    /// <summary>
    /// The words beside a ghost line are the day count: 1 is "yesterday", 7 is "last week", anything else "Nd ago".
    /// </summary>
    [Theory]
    [InlineData(1, "yesterday")]
    [InlineData(7, "last week")]
    [InlineData(3, "3d ago")]
    [InlineData(14, "14d ago")]
    public void ComparisonLabel_WordsTheDayCount(int days, string expected)
    {
        Assert.Equal(expected, CorrelatedTimelineLanesControl.ComparisonLabel(days));
    }

    /// <summary>
    /// The call sites. RefreshOverviewAsync derives the range with GetOverviewComparisonRange, not the shared
    /// GetComparisonRange (ServerTab.Comparison.cs), whose preset fallback is a raw UTC now and which serves the
    /// Queries tab. It reads the tab's own server clock once and hands the same one to the range and to RefreshAsync,
    /// not ServerTimeHelper.ActiveServerClock, which follows the selected tab and would put another server's hours
    /// on this one. RefreshAsync moves no ghost row by a shift of its own and words the ghost line from the day count.
    /// </summary>
    [Fact]
    public void TheComparisonCallSites_UseTheTabsOwnClock_AndTheDayCount()
    {
        var refresh = CodeOnly(File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")));
        var overview = MethodBody(refresh, "private async System.Threading.Tasks.Task RefreshOverviewAsync(");

        Assert.Contains("CorrelatedTimelineLanesControl.GetOverviewComparisonRange(", overview, StringComparison.Ordinal);
        Assert.Contains("var clock = _serverClock;", overview, StringComparison.Ordinal);
        Assert.Contains("hoursBack, fromDate, toDate, DateTime.UtcNow, clock)", overview, StringComparison.Ordinal);
        Assert.Contains("CorrelatedLanes.RefreshAsync(hoursBack, fromDate, toDate, clock, comparison)", overview, StringComparison.Ordinal);
        Assert.DoesNotContain("ActiveServerClock", overview, StringComparison.Ordinal);
        Assert.DoesNotContain("GetComparisonRange(", overview, StringComparison.Ordinal);

        var lanes = CodeOnly(File.ReadAllText(ControlsFile("CorrelatedTimelineLanesControl.xaml.cs")));
        Assert.DoesNotContain("timeShift", lanes, StringComparison.Ordinal);
        Assert.DoesNotContain("CurrentFrom", lanes, StringComparison.Ordinal);
        Assert.Contains("var days = comparisonRange.Value.Days;", lanes, StringComparison.Ordinal);
        Assert.Contains("ComparisonLabel(days)", lanes, StringComparison.Ordinal);
    }

    /// <summary>
    /// The five ghost reads take the comparison window as it arrives (UTC bounds worked out once by
    /// GetOverviewComparisonRange); nothing converts it through a clock a second time, and the CPU read asks for the
    /// UTC frame so its rows carry the instant the ghost X is worked from.
    /// </summary>
    [Fact]
    public void TheGhostReads_TakeTheComparisonWindowAsItArrives()
    {
        var lanes = CodeOnly(File.ReadAllText(ControlsFile("CorrelatedTimelineLanesControl.xaml.cs")));

        Assert.Contains("var refFromUtc = comparisonRange.Value.FromUtc;", lanes, StringComparison.Ordinal);
        Assert.Contains("var refToUtc = comparisonRange.Value.ToUtc;", lanes, StringComparison.Ordinal);
        Assert.Contains("_dataService.GetCpuUtilizationAsync(_serverId, 0, refFromUtc, refToUtc, frame: CpuTimeFrame.Utc)", lanes, StringComparison.Ordinal);
        foreach (var read in new[] { "GetTotalWaitTrendAsync", "GetBlockingTrendAsync", "GetMemoryTrendAsync", "GetFileIoLatencyTrendAsync" })
        {
            Assert.Contains($"_dataService.{read}(_serverId, 0, refFromUtc, refToUtc)", lanes, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("(_serverId, 0, refFrom, refTo)", lanes, StringComparison.Ordinal);
        Assert.DoesNotContain("serverClock.ToUtc(", lanes, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4320: the baseline reference time is a UTC instant, because <c>BaselineLocalClock.LocalKey</c> expects UTC
    /// and converts it to server-local itself. A custom range's fromDate is a UTC instant already (#4766), so it
    /// goes through unchanged: converting it again shifted the baseline's local-hour lookup a second time.
    /// </summary>
    [Fact]
    public void GetBaselineReferenceTimeUtc_CustomRange_IsTheUtcBoundUnchanged()
    {
        var from = Utc(2026, 3, 10, 9, 0);

        var referenceTime = CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(hoursBack: 6, from, FixedUtcNow);

        Assert.Equal(from, referenceTime);
    }

    /// <summary>
    /// A custom fromDate on either side of a clock change is the same instant it was, winter or summer.
    /// </summary>
    [Fact]
    public void GetBaselineReferenceTimeUtc_CustomRange_IsTheUtcBoundInEitherSeason()
    {
        Assert.Equal(Utc(2026, 3, 1, 10, 0), CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(6, Utc(2026, 3, 1, 10, 0), FixedUtcNow));
        Assert.Equal(Utc(2026, 3, 9, 10, 0), CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(6, Utc(2026, 3, 9, 10, 0), FixedUtcNow));
    }

    /// <summary>
    /// A preset baseline reference time is the UTC instant hoursBack ago, across a change too.
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
    /// Under a PRESET range (fromDate null) the baseline reference time is utcNow.AddHours(-hoursBack), unchanged:
    /// it is UTC already and needs no conversion, whatever the server's offset.
    /// </summary>
    [Fact]
    public void GetBaselineReferenceTimeUtc_PresetRange_UsesUtcNowUnchanged()
    {
        var referenceTime = CorrelatedTimelineLanesControl.GetBaselineReferenceTimeUtc(hoursBack: 6, null, FixedUtcNow);

        Assert.Equal(FixedUtcNow.AddHours(-6), referenceTime);
    }

    /// <summary>
    /// Source pin: RefreshAsync takes its baseline reference time from <c>GetBaselineReferenceTimeUtc</c>, not from
    /// fromDate directly (a preset range has no fromDate).
    /// </summary>
    [Fact]
    public void BaselineReferenceTime_ComesFromGetBaselineReferenceTimeUtc_NotFromDateDirectly()
    {
        var lanesSource = File.ReadAllText(ControlsFile("CorrelatedTimelineLanesControl.xaml.cs"));

        Assert.False(lanesSource.Contains("var referenceTime = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);"),
            "RefreshAsync's referenceTime falls back to a raw fromDate again (#4320).");
        Assert.Contains(
            "var referenceTime = GetBaselineReferenceTimeUtc(hoursBack, fromDate, DateTime.UtcNow);",
            lanesSource);
    }

    /* The text of the method that starts at the signature, up to its closing brace: these files indent members by four
       spaces, so the first "\n    }\n" after the signature is the end of that member. */
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' is no longer in the source; update this pin.");
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        return source[start..end];
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ControlsFile(string name) => Path.Combine(ControlsDir(), name);

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
