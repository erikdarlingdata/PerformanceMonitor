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
        Assert.DoesNotContain("WhenAll", trends, StringComparison.Ordinal);
        Assert.Matches(@"ShowEventDataStartAsync\(LockWaitTrendTruncationBanner, lockWaitStartTask,", trends);
        Assert.Matches(@"ShowEventDataStartAsync\(BlockingTrendTruncationBanner, blockingStartTask,", trends);
        Assert.Matches(@"ShowEventDataStartAsync\(DeadlockTrendTruncationBanner, deadlockStartTask,", trends);
        Assert.Contains("lockWaitStartTask = _dataService.GetLockWaitTrendDataStartAsync(", trends, StringComparison.Ordinal);
        Assert.Contains("blockingStartTask = _dataService.GetBlockedProcessReportsDataStartAsync(", trends, StringComparison.Ordinal);
        Assert.Contains("deadlockStartTask = _dataService.GetDeadlocksDataStartAsync(", trends, StringComparison.Ordinal);
        Assert.True(trends.IndexOf("GetLockWaitTrendDataStartAsync", StringComparison.Ordinal) < trends.IndexOf("await lockWaitTask", StringComparison.Ordinal),
            "the probes start before the reads are awaited");
    }

    [Fact]
    public void Stats_HasOneBannerPerFloor_FedFromItsOwnProbe_OutsideAnyJoin()
    {
        var stats = Case("BlockingStatsSubTabIndex");

        Assert.Contains("ViewerReadFanOut.Of(5)", stats, StringComparison.Ordinal);
        Assert.DoesNotContain("WhenAll", stats, StringComparison.Ordinal);
        Assert.Matches(@"BlockingStatsBlockingTruncationBanner, blockingStartTask,", stats);
        Assert.Matches(@"BlockingStatsDeadlockTruncationBanner, deadlockStartTask,", stats);
        Assert.Contains("blockingStartTask = _dataService.GetBlockedProcessReportsDataStartAsync(", stats, StringComparison.Ordinal);
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
