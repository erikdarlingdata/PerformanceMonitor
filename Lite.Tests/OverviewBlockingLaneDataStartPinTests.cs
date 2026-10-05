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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// How the Overview's blocking chart is wired to say where its data starts (#4966), read from the source because the control needs a
/// window to run. The choice is in <c>OverviewBlockingLaneDataStartTests</c>, the notice in <c>OverviewBlockingLaneDataStartLiveTests</c>.
/// </summary>
public sealed class OverviewBlockingLaneDataStartPinTests
{
    private static string Code() => Read("CorrelatedTimelineLanesControl.xaml.cs");

    private static string Xaml() => Read("CorrelatedTimelineLanesControl.xaml");

    private static string Step() => Read("LiteBlockingLaneDataStart.cs");

    private static string Between(string source, string from, string to)
    {
        var start = source.IndexOf(from, StringComparison.Ordinal);
        Assert.True(start >= 0, from + " was not found");
        var end = source.IndexOf(to, start, StringComparison.Ordinal);
        Assert.True(end > start, to + " was not found after " + from);
        return source[start..end];
    }

    [Fact]
    public void RefreshAsync_RunsTheNoteAfterTheLanesAreDrawn_OutsideTheReadsWhenAll()
    {
        var refresh = Between(Code(), "public async Task RefreshAsync(", "private async Task ShowBlockingLaneDataStartAsync(");

        var whenAll = Between(refresh, "await Task.WhenAll(", ");");
        Assert.DoesNotContain("DataStart", whenAll, StringComparison.Ordinal);

        var drawn = refresh.IndexOf("UpdateBlockingLane(blockingData", StringComparison.Ordinal);
        var ghost = refresh.IndexOf("AddGhostLine(BlockingChart", StringComparison.Ordinal);
        var axes = refresh.IndexOf("SyncXAxes(hoursBack", StringComparison.Ordinal);
        var note = refresh.IndexOf("await ShowBlockingLaneDataStartAsync(", StringComparison.Ordinal);
        Assert.True(drawn > 0 && ghost > drawn && axes > ghost && note > axes, "the lanes, ghost lines and axes come before the note");
        Assert.Single(Regex.Matches(refresh, "ShowBlockingLaneDataStartAsync"));
    }

    [Fact]
    public void TheWindow_IsTheOneTheReadsTake_AndTheNoteIsFedFromBothSeriesBars()
    {
        var refresh = Between(Code(), "public async Task RefreshAsync(", "private async Task ShowBlockingLaneDataStartAsync(");
        Assert.Contains("var (windowStartUtc, windowEndUtc) = LocalDataService.GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null);", refresh, StringComparison.Ordinal);
        Assert.Contains("GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate)", refresh, StringComparison.Ordinal);
        Assert.Contains("GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate)", refresh, StringComparison.Ordinal);

        var call = Between(refresh, "await ShowBlockingLaneDataStartAsync(", ");");
        Assert.Contains("windowStartUtc, windowEndUtc", call, StringComparison.Ordinal);
        Assert.Contains("blockingTask.Result", call, StringComparison.Ordinal);
        Assert.Contains("deadlockTask.Result", call, StringComparison.Ordinal);

        var wrapper = Between(Code(), "private async Task ShowBlockingLaneDataStartAsync(", "private void UpdateBlockingLane(");
        Assert.Contains("LiteBlockingLaneDataStart.ShowAsync(", wrapper, StringComparison.Ordinal);
        Assert.Contains("BlockingLaneDataStartBanner", wrapper, StringComparison.Ordinal);
        Assert.Contains("GetQueryWindowFloorAsync(relation, _serverId, startUtc, endUtc)", wrapper, StringComparison.Ordinal);
        Assert.Contains("_displayZone()", wrapper, StringComparison.Ordinal);
        /* #5098: a blocking read that drew the XE reports names the XE collector's start alone. */
        Assert.Contains("HasBlockedProcessReportsInWindowAsync(_serverId, startUtc, endUtc)", wrapper, StringComparison.Ordinal);
        Assert.Contains("GetQueryWindowFloorAsync(QueryWindowRelation.BlockedProcessReports, _serverId, startUtc, endUtc, includeAlsoCovered: false)", wrapper, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStep_ProbesBothRelations_ThroughTheShortWindowGuard_AndWordsTheBanner()
    {
        var step = Between(Step(), "internal static async Task ShowAsync(", "/// <summary>One series' start:");
        Assert.DoesNotContain("ProbeWindowFloorOrNullAsync", step, StringComparison.Ordinal);
        Assert.True(step.IndexOf("McpQueryTools.CanWindowBeTruncated(startUtc, endUtc)", StringComparison.Ordinal)
            < step.IndexOf("ProbeBlockingAsync(floorOf, blockingReadTookXe, xeOnlyBlockingFloorOf)", StringComparison.Ordinal));
        Assert.Contains("Probe(floorOf, QueryWindowRelation.Deadlocks)", step, StringComparison.Ordinal);
        Assert.Contains("StartAsync(floorOf, startUtc, endUtc, blockingBars, deadlockBars, blockingReadTookXe, xeOnlyBlockingFloorOf)", step, StringComparison.Ordinal);
        Assert.Contains("ChooseAsync(blockingProbe, deadlockProbe, blockingBars, deadlockBars)", step, StringComparison.Ordinal);
        Assert.Contains("ServerTab.ApplyWindowFloorToBanner(banner, start, startUtc, zone)", step, StringComparison.Ordinal);
        Assert.Contains("bar.Count > 0", Step(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheBanner_IsDeclaredInAnAutoRowAboveTheChart_AndOnlyOnTheBlockingLane()
    {
        var chart = Between(Xaml(), "<!-- Lane 3: Blocking -->", "<!-- Lane 4");
        Assert.Contains("<RowDefinition Height=\"Auto\"/>", chart, StringComparison.Ordinal);
        Assert.Matches(@"x:Name=""BlockingLaneDataStartBanner""\s+Visibility=""Collapsed""", chart);
        Assert.Contains("Foreground=\"{DynamicResource ForegroundBrush}\"", chart, StringComparison.Ordinal);
        Assert.Contains("<ScottPlot:WpfPlot Grid.Row=\"1\" Grid.Column=\"1\" x:Name=\"BlockingChart\"/>", chart, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(Xaml(), "DataStartBanner"));
    }

    private static string Read(string name, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", name))).ReplaceLineEndings("\n");
}
