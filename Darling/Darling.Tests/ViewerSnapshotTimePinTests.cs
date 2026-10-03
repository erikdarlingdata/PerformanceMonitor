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

/// <summary>How the CPU Scheduler, Latch, Spinlock and Running Jobs grids are wired to show their snapshot's time (#4966). These read
/// the source, as the other data-start wiring pins do; the label control's behavior is the STA fact in
/// <c>ViewerSnapshotTimeLabelStaTests</c>.</summary>
public sealed class ViewerSnapshotTimePinTests
{
    private static string ViewerFile(string file) =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file).ReplaceLineEndings("\n");

    [Theory]
    [InlineData("ViewerServerTab.CpuScheduler.cs", "ShowSnapshotTime(CpuSchedulerSnapshotTime, snapshotTask.Result?.CollectionTime)")]
    [InlineData("ViewerServerTab.RunningJobs.cs", "ShowSnapshotTime(RunningJobsSnapshotTime, jobs.Count == 0 ? null : jobs[0].CollectionTime)")]
    [InlineData("ViewerServerTab.LatchSpinlock.cs", "ShowSnapshotTime(LatchStatsSnapshotTime, latchSnapshotTask.Result.Count == 0 ? null : latchSnapshotTask.Result[0].CollectionTime)")]
    [InlineData("ViewerServerTab.LatchSpinlock.cs", "ShowSnapshotTime(SpinlockStatsSnapshotTime, spinlockSnapshotTask.Result.Count == 0 ? null : spinlockSnapshotTask.Result[0].CollectionTime)")]
    public void EachLoad_SetsItsLabelFromTheSnapshotItRendered(string file, string call)
    {
        var source = ViewerFile(file);
        Assert.Contains(call, source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLatchLoad_ProbesBothCollectors_OutsideTheJoin_AfterItsScopeIsReleased_UnderTheirOwnScope()
    {
        var source = ViewerFile("ViewerServerTab.LatchSpinlock.cs");
        Assert.Contains("ViewerReadFanOut.Of(4)", source, StringComparison.Ordinal);
        Assert.Contains("ViewerReadFanOut.Of(2)", source, StringComparison.Ordinal);
        var join = Regex.Match(source, @"await Task\.WhenAll\(([^;]*)\);").Groups[1].Value;
        Assert.DoesNotContain("DataStartTask", join, StringComparison.Ordinal);
        Assert.True(source.IndexOf("GetLatchStatsDataStartAsync", StringComparison.Ordinal) > source.IndexOf("readFanOut.Release()", StringComparison.Ordinal));
        Assert.True(source.IndexOf("readFanOut.Release()", StringComparison.Ordinal) > source.IndexOf("await Task.WhenAll", StringComparison.Ordinal));
        Assert.Contains("DataStartOrNullAsync(latchDataStartTask", source, StringComparison.Ordinal);
        Assert.Contains("DataStartOrNullAsync(spinlockDataStartTask", source, StringComparison.Ordinal);
        var data = ViewerFile("ViewerDataService.LatchSpinlock.cs");
        Assert.Contains("ForCollectorTable(\"latch_stats\")", data, StringComparison.Ordinal);
        Assert.Contains("ForCollectorTable(\"spinlock_stats\")", data, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("LatchSnapshotSql")]
    [InlineData("SpinlockSnapshotSql")]
    public void TheSnapshotSql_ReturnsTheCollectionTimeAsItsLastColumn(string constant)
    {
        var sql = constant == "LatchSnapshotSql" ? PerformanceMonitor.Darling.Viewer.ViewerDataService.LatchSnapshotSql : PerformanceMonitor.Darling.Viewer.ViewerDataService.SpinlockSnapshotSql;
        Assert.Matches(@"sample_interval_seconds,\s+collection_time\s+FROM v_", sql);
    }

    [Fact]
    public void TheXaml_DeclaresAMutedLabelAboveEachGrid()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");
        foreach (var name in new[] { "CpuSchedulerSnapshotTime", "RunningJobsSnapshotTime", "LatchStatsSnapshotTime", "SpinlockStatsSnapshotTime" })
        {
            Assert.Matches($@"<TextBlock Grid\.Row=""\d"" x:Name=""{name}"" Visibility=""Collapsed""[^>]*FontSize=""11""\s+Foreground=""\{{DynamicResource ForegroundMutedBrush\}}""", xaml);
        }
    }
}
