/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// How the Overview's blocking chart is wired to say where its data starts (#4966), read from the source because the control needs a
/// window to run: the probes sit outside the charts' main <c>Task.WhenAll</c>, the note is fed from both series, and the banner is
/// declared in an Auto row. The choice itself is in <c>ViewerOverviewBlockingLaneDataStartTests</c>.
/// </summary>
public sealed class ViewerOverviewBlockingLaneDataStartPinTests
{
    private static string Code() => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "CorrelatedTimelineLanesControl.xaml.cs").ReplaceLineEndings("\n");

    private static string Xaml() => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "CorrelatedTimelineLanesControl.xaml").ReplaceLineEndings("\n");

    private static string Between(string source, string from, string to)
    {
        var start = source.IndexOf(from, StringComparison.Ordinal);
        Assert.True(start >= 0, from + " was not found");
        var end = source.IndexOf(to, start, StringComparison.Ordinal);
        Assert.True(end > start, to + " was not found after " + from);
        return source[start..end];
    }

    [Fact]
    public void RefreshAsync_RunsTheNoteAfterTheLanesAreDrawn_AndKeepsTheProbesOutOfTheMainWhenAll()
    {
        var refresh = Between(Code(), "public async Task RefreshAsync(", "private async Task ShowBlockingLaneDataStartAsync(");

        var whenAll = Between(refresh, "await Task.WhenAll(", ");");
        Assert.DoesNotContain("DataStart", whenAll, StringComparison.Ordinal);
        Assert.Contains("blockingTask", whenAll, StringComparison.Ordinal);

        var drawn = refresh.IndexOf("UpdateBlockingLane(blockingData", StringComparison.Ordinal);
        var note = refresh.IndexOf("await ShowBlockingLaneDataStartAsync(", StringComparison.Ordinal);
        var axes = refresh.IndexOf("SyncXAxes(hoursBack", StringComparison.Ordinal);
        Assert.True(drawn > 0 && drawn < note && note < axes, "the bars are drawn before the note's probes start");
        Assert.DoesNotContain("DataStartAsync(_serverId", refresh, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNote_IsFedFromBothSeries_ThroughTheProbesAndTheBarsDrawn_AndSkipsTheProbeOnAShortWindow()
    {
        var code = Code();
        var call = Between(code, "await ShowBlockingLaneDataStartAsync(", ");");
        Assert.Contains("blockingTask.Result.Select(d => d.Time)", call, StringComparison.Ordinal);
        Assert.Contains("deadlockTask.Result.Select(d => d.Time)", call, StringComparison.Ordinal);

        var step = Between(code, "private async Task ShowBlockingLaneDataStartAsync(", "private void UpdateBlockingLane(");
        Assert.Contains("endUtc - startUtc > DurationTrendRouting.TruncationSlack", step, StringComparison.Ordinal);
        Assert.Contains("_dataService!.GetBlockedProcessReportsDataStartAsync(_serverId, startUtc, endUtc)", step, StringComparison.Ordinal);
        Assert.Contains("_dataService!.GetDeadlocksDataStartAsync(_serverId, startUtc, endUtc)", step, StringComparison.Ordinal);
        Assert.Contains("ViewerBlockingLaneDataStart.ChooseAsync(blockingProbe, deadlockProbe, blockingBars, deadlockBars)", step, StringComparison.Ordinal);
        Assert.Contains("ViewerServerTab.UpdateTruncationBanner(BlockingLaneDataStartBanner, start, startUtc)", step, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBanner_IsDeclaredInAnAutoRowAboveTheChart_StyledLikeTheTabsNotes()
    {
        var chart = Between(Xaml(), "<!-- Lane 3: Blocking -->", "<!-- Lane 4");
        Assert.Contains("<RowDefinition Height=\"Auto\"/>", chart, StringComparison.Ordinal);
        Assert.Matches(@"x:Name=""BlockingLaneDataStartBanner""\s+Visibility=""Collapsed""", chart);
        Assert.Contains("Background=\"#22FFAA00\"", chart, StringComparison.Ordinal);
        Assert.Contains("<ScottPlot:WpfPlot Grid.Row=\"1\" Grid.Column=\"1\" x:Name=\"BlockingChart\"/>", chart, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOtherLanes_GetNoNote()
    {
        Assert.Single(Regex.Matches(Xaml(), "DataStartBanner"));
        Assert.Equal(2, Regex.Matches(Code(), @"DataStartAsync\(_serverId").Count);
    }
}
