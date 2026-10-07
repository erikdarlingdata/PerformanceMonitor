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
/// How the Blocking tab's Trends and Blocking Stats charts are wired to say where their data starts (#4966): one probe per floor, started
/// beside the reads and kept out of any join, one banner per floor, each fed from its own floor, and the fan-out width declared.
/// </summary>
public sealed class ViewerBlockingChartsDataStartPinTests
{
    private static string Tab => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Blocking.cs").ReplaceLineEndings("\n");

    private static string Xaml => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml").ReplaceLineEndings("\n");

    private static string Case(string name)
    {
        var load = Tab[Tab.IndexOf("private async Task LoadBlockingAsync()", StringComparison.Ordinal)..];
        var start = load.IndexOf($"case {name}:", StringComparison.Ordinal);
        Assert.True(start >= 0, name);
        var end = load.IndexOf("            case ", start + 10, StringComparison.Ordinal);
        return load[start..end];
    }

    [Fact]
    public void Trends_ProbesEachFloor_OutsideAnyJoin_AndFeedsEachBannerFromItsOwnProbe()
    {
        var trends = Case("BlockingTrendsSubTabIndex");

        Assert.Contains("ViewerReadFanOut.Of(6)", trends, StringComparison.Ordinal);
        /* Every probe is watched by the helper, and none is inside the join. */
        Assert.Matches(@"Task\.WhenAll\(lockWaitTask,\s*blockingTask,\s*deadlockTask\),\s*lockWaitStartTask,\s*""Lock Wait Trend""\)", trends);
        Assert.Contains("blockingStartTask, \"Blocking Trend\")", trends, StringComparison.Ordinal);
        Assert.Contains("deadlockStartTask, \"Deadlock Trend\")", trends, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"WhenAll\([^)]*StartTask", trends);
        /* Lock Wait is a rate series: its note names the coverage alone, never the drawn points. */
        Assert.Contains("UpdateTruncationBanner(LockWaitTrendTruncationBanner, await DataStartOrNullAsync(lockWaitStartTask, \"Lock Wait Trend\"), startUtc)", trends, StringComparison.Ordinal);
        Assert.DoesNotContain("LockWaitTimesDrawn", trends, StringComparison.Ordinal);
        /* Released at the join, before any banner await. */
        Assert.True(trends.IndexOf("readFanOut.Release()", StringComparison.Ordinal) is var rel && rel > trends.IndexOf("AwaitReadWatchingProbeAsync(", StringComparison.Ordinal)
            && rel < trends.IndexOf("await DataStartOrNullAsync", StringComparison.Ordinal));
        Assert.Matches(@"ShowEventDataStartAsync\(BlockingTrendTruncationBanner, blockingStartTask,", trends);
        Assert.Matches(@"ShowEventDataStartAsync\(DeadlockTrendTruncationBanner, deadlockStartTask,", trends);
        Assert.Contains("lockWaitStartTask = _dataService.GetLockWaitTrendDataStartAsync(", trends, StringComparison.Ordinal);
        Assert.Contains("blockingStartTask = _dataService.GetBlockingChartDataStartAsync(", trends, StringComparison.Ordinal);
        Assert.Contains("deadlockStartTask = _dataService.GetDeadlocksDataStartAsync(", trends, StringComparison.Ordinal);
        Assert.True(trends.IndexOf("GetLockWaitTrendDataStartAsync", StringComparison.Ordinal) < trends.IndexOf("await AwaitReadWatchingProbeAsync", StringComparison.Ordinal),
            "the probes start before the reads are awaited");
    }

    [Fact]
    public void Stats_HasOneBannerPerFloor_FedFromItsOwnProbe_OutsideAnyJoin()
    {
        var stats = Case("BlockingStatsSubTabIndex");

        Assert.Contains("ViewerReadFanOut.Of(5)", stats, StringComparison.Ordinal);
        Assert.Matches(@"Task\.WhenAll\(durationStatsTask,\s*deadlockCountTask,\s*deadlockSeverityTask\),\s*blockingStartTask,\s*""Blocking Stats \(blocking\)""\)", stats);
        Assert.Contains("deadlockStartTask, \"Blocking Stats (deadlocks)\")", stats, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"WhenAll\([^)]*StartTask", stats);
        Assert.True(stats.IndexOf("readFanOut.Release()", StringComparison.Ordinal) is var rel && rel > stats.IndexOf("AwaitReadWatchingProbeAsync(", StringComparison.Ordinal)
            && rel < stats.IndexOf("ShowEventDataStartAsync", StringComparison.Ordinal));
        Assert.Matches(@"BlockingStatsBlockingTruncationBanner, blockingStartTask,", stats);
        Assert.Matches(@"BlockingStatsDeadlockTruncationBanner, deadlockStartTask,", stats);
        Assert.Contains("blockingStartTask = _dataService.GetBlockingChartDataStartAsync(", stats, StringComparison.Ordinal);
        Assert.Contains("deadlockStartTask = _dataService.GetDeadlocksDataStartAsync(", stats, StringComparison.Ordinal);
    }

    [Fact]
    public void TheXaml_DeclaresFiveBannersInAutoRows_AboveTheirCharts()
    {
        foreach (var name in new[] { "LockWaitTrend", "BlockingTrend", "DeadlockTrend", "BlockingStatsBlocking", "BlockingStatsDeadlock" })
        {
            var m = Regex.Match(Xaml, $@"<TextBlock Grid\.Row=""(\d)""[^>]*x:Name=""{name}TruncationBanner"" Visibility=""Collapsed""[^>]*Background=""#22FFAA00""/>", RegexOptions.Singleline);
            Assert.True(m.Success, name);
        }
    }
}
