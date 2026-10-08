/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>The CPU Scheduler, Latch, Spinlock and Running Jobs grids end in a "Collected" column that says when their snapshot
/// was collected (#4966): the display-zone time to the second, sorted on the stored DateTime, as the viewer's other Collected columns.</summary>
[Collection("viewer-time-statics")]
public sealed class ViewerSnapshotCollectedColumnTests
{
    private static string Xaml() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml").ReplaceLineEndings("\n");

    private static string ViewerFile(string file) =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file).ReplaceLineEndings("\n");

    [Theory]
    [InlineData("CpuSchedulerGrid")]
    [InlineData("LatchStatsGrid")]
    [InlineData("SpinlockStatsGrid")]
    [InlineData("RunningJobsGrid")]
    public void TheGrid_EndsInACollectedColumn_BoundToTheDisplayTime_SortedOnTheStoredDateTime(string grid)
    {
        var xaml = Xaml();
        var start = Regex.Match(xaml, @"<DataGrid\b[^>]*?x:Name=""" + grid + @"""").Index;
        var end = xaml.IndexOf("</DataGrid.Columns>", start, StringComparison.Ordinal);
        var last = Regex.Matches(xaml[start..end], @"<DataGridTextColumn\b(?:""[^""]*""|[^>""])*>").Last().Value;
        Assert.Contains("Header=\"Collected\"", last, StringComparison.Ordinal);
        Assert.Contains("Binding=\"{Binding CollectionTimeLocal}\"", last, StringComparison.Ordinal);
        Assert.Contains("SortMemberPath=\"CollectionTime\"", last, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOldSnapshotLabel_IsGone()
    {
        Assert.DoesNotContain("SnapshotTime\"", Xaml(), StringComparison.Ordinal);
        Assert.DoesNotContain("Snapshot at", Xaml(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheCpuSchedulerLoad_WrapsItsRowsWithTheSnapshotsCollectionTime()
    {
        var source = ViewerFile("ViewerServerTab.CpuScheduler.cs");
        Assert.Contains("CpuSchedulerGrid.ItemsSource = CpuSchedulerGridRow.Build(snapshotTask.Result);", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDisplayText_IsTheDisplayZonesWallTime_ToTheSecond()
    {
        var saved = CultureInfo.CurrentCulture;
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var at = new DateTime(2026, 10, 3, 14, 5, 9, DateTimeKind.Unspecified);
            // A literal, so the format string itself is pinned.
            var expected = "2026-10-03 14:05:09";
            Assert.Equal(expected, HistoryTime.CollectionLocal(at));
            Assert.Equal(expected, new RunningJobRow { CollectionTime = at }.CollectionTimeLocal);
            Assert.Equal(expected, new LatchStatsSnapshotRow("L", 0, 0, 0, null, null, null, at).CollectionTimeLocal);
            Assert.Equal(expected, new SpinlockStatsSnapshotRow("S", 0, 0, 0, 0, 0, null, null, null, at).CollectionTimeLocal);
            Assert.Equal(expected, new CpuSchedulerGridRow { CollectionTime = at }.CollectionTimeLocal);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            CultureInfo.CurrentCulture = saved;
        }
    }
}
