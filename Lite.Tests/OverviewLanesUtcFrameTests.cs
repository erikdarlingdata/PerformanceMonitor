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
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the Overview lanes and the server tab's CPU chart plot the naive-UTC instant of each sample as their X
/// value. The display mode (UTC, this machine's zone, or the server's own clock) is applied only to TEXT: the tick
/// labels, with the ticks themselves at whole wall-clock times of the display zone, and the crosshair time. Switching
/// the mode therefore relabels the lanes and moves no point, a sample from before a clock change sits where it
/// happened, and the two readings of the repeated autumn hour are two points.
///
/// <para>The lanes and the tab are WPF controls this suite does not instantiate, so the frame is held by source pins
/// with comments stripped first, so a sentence that names the old shape cannot fail a pin. The window arithmetic is
/// tested on its own in <c>OverviewComparisonWindowOffsetTests</c> and <c>TimeWindowsTests</c>.</para>
/// </summary>
public sealed class OverviewLanesUtcFrameTests
{
    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// The lanes convert nothing through the server's clock: no <c>ToServerTime(</c>, no <c>ToServerLocal(</c>, no
    /// server-local window, no axis whose ticks sit on the plotted X (<c>DateTimeTicksBottomDateChange(</c>), and no
    /// reading of the selected tab's clock (<c>ActiveServerClock</c>, which is another server's when this tab is not
    /// the selected one).
    /// </summary>
    [Fact]
    public void TheOverviewLanes_ConvertNothingThroughTheServersClock()
    {
        var code = CodeOnly(ReadControl("CorrelatedTimelineLanesControl.xaml.cs"));

        Assert.DoesNotContain("ToServerTime(", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ToServerLocal(", code, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTimeTicksBottomDateChange(", code, StringComparison.Ordinal);
        Assert.DoesNotContain("GetCurrentWindowServerLocal", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ActiveServerClock", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both CPU reads (the window and the comparison window) ask for the UTC frame, so each row carries the instant
    /// it was collected at, and both plot that instant: no lane reads the server-local <c>SampleTime</c>.
    /// </summary>
    [Fact]
    public void BothCpuReads_AskForTheUtcFrame_AndTheLanesPlotTheInstant()
    {
        var code = CodeOnly(ReadControl("CorrelatedTimelineLanesControl.xaml.cs"));

        var reads = Regex.Matches(code, @"GetCpuUtilizationAsync\s*\(([^;]*)\)\s*\)\s*;");
        Assert.Equal(2, reads.Count);
        foreach (Match read in reads)
        {
            Assert.Contains("frame: CpuTimeFrame.Utc", read.Groups[1].Value, StringComparison.Ordinal);
        }

        Assert.DoesNotMatch(@"\.SampleTime\b", code);
        Assert.Contains("d.SampleTimeUtc.ToOADate(), (double)d.SqlServerCpu", code, StringComparison.Ordinal);
        Assert.Contains("d.SampleTimeUtc.ToOADate(), (double)d.TotalCpu", code, StringComparison.Ordinal);
        Assert.Contains("TimeWindows.GhostX(d.SampleTimeUtc, days, zone)", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every other lane plots the sample's instant as the read returned it (<c>CollectionTime</c> and <c>Time</c> are
    /// UTC), the file-I/O lane groups on it and plots the group's instant, and the ghost lines of all five lanes go
    /// back onto the current axis through <c>TimeWindows.GhostX</c> on the server's zone, with no shift added.
    /// </summary>
    [Fact]
    public void EveryLaneX_IsTheSamplesOwnInstant_AndEveryGhostSeriesGoesThroughGhostX()
    {
        var code = CodeOnly(ReadControl("CorrelatedTimelineLanesControl.xaml.cs"));
        var refresh = MethodBody(code, "public async Task RefreshAsync(");

        Assert.Contains("(d.CollectionTime.ToOADate(), d.WaitTimeMsPerSecond)", refresh, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(refresh, Regex.Escape("(d.Time.ToOADate(), (double)d.Count)")).Count);
        Assert.Contains("(d.CollectionTime.ToOADate(), d.BufferPoolMb)", refresh, StringComparison.Ordinal);
        Assert.Contains("(g.Key.ToOADate(), g.Average(x => x.AvgReadLatencyMs))", refresh, StringComparison.Ordinal);

        Assert.Contains("var zone = serverClock.AsTimeZone();", refresh, StringComparison.Ordinal);
        Assert.Equal(5, Regex.Matches(refresh, Regex.Escape("TimeWindows.GhostX(")).Count);
        Assert.Equal(5, Regex.Matches(refresh, @"TimeWindows\.GhostX\([^,()]+, days, zone\)\.ToOADate\(\)").Count);
        Assert.DoesNotContain(".Add(", refresh, StringComparison.Ordinal);
    }

    /// <summary>
    /// All four axes the lanes draw (the blocking, CPU and generic lanes, and the empty lane) use the UTC-instant
    /// ticks, in the zone the tab hands <c>Initialize</c>, which is read again on each render so a mode switch
    /// relabels the axis.
    /// </summary>
    [Fact]
    public void EveryLaneAxis_DrawsItsTicksInTheDisplayZone()
    {
        var code = CodeOnly(ReadControl("CorrelatedTimelineLanesControl.xaml.cs"));

        Assert.Equal(4, Regex.Matches(code, Regex.Escape(".Plot.Axes.DateTimeTicksBottomUtc(_displayZone);")).Count);
        Assert.Contains("private Func<TimeZoneInfo> _displayZone", code, StringComparison.Ordinal);
        Assert.Contains("_displayZone = displayZone;", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// Initialize takes the display zone and gives it to the crosshair manager BEFORE the first lane is added: the
    /// manager words the tooltip time from it, and a lane wired first would read X as the app's server-time value.
    /// The tab passes its own picker zone, so a tab that is not the selected one words its lanes in its own server's
    /// zone when the mode is Server Time.
    /// </summary>
    [Fact]
    public void Initialize_TakesTheDisplayZone_AndSetsTheCrosshairZoneBeforeAnyLaneIsAdded()
    {
        var code = CodeOnly(ReadControl("CorrelatedTimelineLanesControl.xaml.cs"));

        Assert.Contains("public void Initialize(LocalDataService dataService, int serverId, Func<TimeZoneInfo> displayZone)", code, StringComparison.Ordinal);
        var zone = code.IndexOf("DisplayZoneProvider = displayZone", StringComparison.Ordinal);
        var firstLane = code.IndexOf("_crosshairManager.AddLane(", StringComparison.Ordinal);
        Assert.True(zone > 0 && firstLane > zone, "the crosshair manager has to be given the display zone before its first lane.");

        var tab = CodeOnly(ReadControl("ServerTab.xaml.cs"));
        Assert.Contains("CorrelatedLanes.Initialize(_dataService, _serverId, GetPickerZone);", tab, StringComparison.Ordinal);
    }

    /// <summary>
    /// The drill from a lane: the clicked X is the instant, with no conversion, and it reaches the host as it is.
    /// The two readings of the repeated autumn hour (05:30 and 06:30 UTC on 1 November 2026 both read 01:30 on a US
    /// Eastern clock) stay two instants an hour apart, so a drill on each opens its own window.
    /// </summary>
    [Fact]
    public void TheDrill_IsTheClickedInstant_AndTheRepeatedHourKeepsItsTwoOccurrences()
    {
        var first = Utc(2026, 11, 1, 5, 30);
        var second = Utc(2026, 11, 1, 6, 30);

        Assert.Equal(first, CorrelatedTimelineLanesControl.DrillInstant(first.ToOADate()));
        Assert.Equal(second, CorrelatedTimelineLanesControl.DrillInstant(second.ToOADate()));
        Assert.NotEqual(
            TimeWindows.Drill(CorrelatedTimelineLanesControl.DrillInstant(first.ToOADate()), 30, 30),
            TimeWindows.Drill(CorrelatedTimelineLanesControl.DrillInstant(second.ToOADate()), 30, 30));

        var code = CodeOnly(ReadControl("CorrelatedTimelineLanesControl.xaml.cs"));
        Assert.Contains("var t = DrillInstant(chart.Plot.GetCoordinates(pixel).X);", code, StringComparison.Ordinal);
        Assert.Contains("ShowActiveQueriesRequested?.Invoke(t);", code, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(code, Regex.Escape("FromOADate(")));

        var drill = MethodBody(CodeOnly(ReadControl("ServerTab.DrillDown.cs")), "private async void OnActiveQueriesDrillDown(");
        Assert.Contains("GetDrillWindow(time, 30, 30)", drill, StringComparison.Ordinal);
        Assert.DoesNotContain("ToUtc", drill, StringComparison.Ordinal);
        Assert.DoesNotContain("ToServerLocal", drill, StringComparison.Ordinal);
    }

    /// <summary>
    /// The server tab's CPU chart is the same read as the Overview's CPU lane: it asks for the UTC frame and plots
    /// each row's instant, so the two readings of a repeated hour are two points and the chart agrees with the lane.
    /// The chart's text (its ticks and its hover) is worded in the display zone from that X, and nothing in the chart
    /// reads the server-local <c>SampleTime</c>.
    /// </summary>
    [Fact]
    public void TheServerTabCpuChart_AsksForTheUtcFrame_AndPlotsTheInstant()
    {
        var refresh = CodeOnly(ReadControl("ServerTab.Refresh.cs"));
        var read = MethodBody(refresh, "private async System.Threading.Tasks.Task RefreshCpuAsync(");
        Assert.Contains("GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate, frame: CpuTimeFrame.Utc)", read, StringComparison.Ordinal);

        var charts = CodeOnly(ReadControl("ServerTab.Charts.cs"));
        var chart = MethodBody(charts, "private void UpdateCpuChart(");
        Assert.Contains("d.SampleTimeUtc.ToOADate()", chart, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\.SampleTime\b", chart);
        Assert.Contains("DateTimeTicksBottomUtc(GetPickerZone)", chart, StringComparison.Ordinal);
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

    private static string ReadControl(string file, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", file)));
}
