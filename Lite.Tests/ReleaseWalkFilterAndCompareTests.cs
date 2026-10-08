/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Release walk V12a, V12c and V12d, in Lite and the Darling Viewer (the Viewer's tabs are a copy of Lite's, so the source
/// pins read both). V12a: an empty Long Queries grid on a server whose opt-in trace is off says so and says where the switch
/// is. V12c: "Select All" in the Databases filter means All (no filter), not a real filter naming every database collected
/// so far. V12d: a Compare window the store holds nothing for says so, on the Overview lanes and on the comparison grids.
/// </summary>
[Trait("Reads", "Darling")]
public sealed class ReleaseWalkFilterAndCompareTests
{
    private static string RepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath.Replace('/', Path.DirectorySeparatorChar)));
    }

    // ---- V12c ---------------------------------------------------------------------------------------------------

    [Fact]
    public void DatabaseFilter_EveryBoxTicked_IsAll_NotATwelveDatabaseFilter()
    {
        var items = Enumerable.Range(1, 12).Select(i => ($"Db{i}", true)).ToList();

        Assert.Empty(DatabaseFilterSelection.Stored(items, items.Select(i => i.Item1).ToList()));
    }

    [Fact]
    public void DatabaseFilter_SomeBoxesTicked_StoresThoseNames()
    {
        var items = new List<(string Name, bool IsSelected)> { ("A", true), ("B", false), ("C", true) };

        Assert.Equal(new[] { "A", "C" }, DatabaseFilterSelection.Stored(items, new[] { "A", "B", "C" }));
    }

    [Fact]
    public void DatabaseFilter_NoBoxTicked_AndNoDatabasesListed_AreAll()
    {
        Assert.Empty(DatabaseFilterSelection.Stored(new List<(string, bool)> { ("A", false), ("B", false) }, new[] { "A", "B" }));
        Assert.Empty(DatabaseFilterSelection.Stored(new List<(string, bool)>(), new[] { "A", "B" }));
        Assert.Empty(DatabaseFilterSelection.Stored(new List<(string, bool)>(), null));
    }

    /// <summary>
    /// #5554: a sticky filter for two databases, the collected-name read failed (null) or came back empty: the list holds
    /// only the sticky names, every one ticked, and that is not "every database ticked".
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DatabaseFilter_StickyNamesOnly_WhenTheCollectedReadFailed_StayTheFilter(bool emptyRead)
    {
        var items = new List<(string Name, bool IsSelected)> { ("Sales", true), ("Billing", true) };

        var stored = DatabaseFilterSelection.Stored(items, emptyRead ? new List<string>() : null);

        Assert.Equal(new[] { "Sales", "Billing" }, stored);
    }

    /// <summary>
    /// #5554: every listed box is ticked, but one collected database is not on the list the user ticked (it was collected
    /// after the list was built, or the sticky names are all that is ticked): not All. Ticking every collected name, with
    /// a sticky name beside them, is All.
    /// </summary>
    [Fact]
    public void DatabaseFilter_AllIsEveryCollectedNameTicked_NotEveryListedBox()
    {
        var onlySticky = new List<(string Name, bool IsSelected)> { ("Sales", true), ("Ghost", true) };
        Assert.Equal(new[] { "Sales", "Ghost" }, DatabaseFilterSelection.Stored(onlySticky, new[] { "Sales", "Billing" }));

        var withSticky = new List<(string Name, bool IsSelected)> { ("Billing", true), ("Ghost", true), ("Sales", true) };
        Assert.Empty(DatabaseFilterSelection.Stored(withSticky, new[] { "sales", "BILLING" }));
    }

    [Theory]
    [InlineData("Lite/Controls/ServerTab.DatabaseFilter.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.DatabaseFilter.cs")]
    public void DatabaseFilter_SyncStoresThroughTheSharedRule(string path)
    {
        var source = RepoFile(path);

        Assert.Contains("DatabaseFilterSelection.Stored(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("if (item.IsSelected)", source, StringComparison.Ordinal);
    }

    // ---- #5554: a failed Running Jobs read is not "no jobs are running" -------------------------------------------

    [Theory]
    [InlineData("Lite/Controls/ServerTab.Refresh.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.RunningJobs.cs")]
    public void RunningJobsLoad_DoesNotTurnAFailedReadIntoAnEmptyList(string path)
    {
        var source = RepoFile(path);

        /* SafeQueryAsync answers a failed read with an empty list, and the empty grid then says "No SQL Agent jobs are
           running." about a read that never answered. The load catches the failure itself and says the read failed. */
        Assert.DoesNotContain("SafeQueryAsync(() => _dataService.GetRunningJobsAsync", source, StringComparison.Ordinal);
        Assert.Contains("RunningJobsReadFailedText(ex)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void RunningJobsReadFailedText_NamesTheCause_AndNeverSaysNoJobsAreRunning()
    {
        var text = ServerTab.RunningJobsReadFailedText(new InvalidOperationException("msdb is offline"));

        Assert.Contains("could not be read", text, StringComparison.Ordinal);
        Assert.Contains("msdb is offline", text, StringComparison.Ordinal);
        Assert.DoesNotContain("No SQL Agent jobs are running", text, StringComparison.Ordinal);
    }

    // ---- V12d ---------------------------------------------------------------------------------------------------

    [Fact]
    public void OverviewCompare_NoRowsInAnyLane_IsNoData_AndAnyRowIsNot()
    {
        Assert.True(CorrelatedTimelineLanesControl.ComparisonHasNoData(0, 0, 0, 0, 0));
        Assert.False(CorrelatedTimelineLanesControl.ComparisonHasNoData(0, 0, 0, 0, 1));
        Assert.False(CorrelatedTimelineLanesControl.ComparisonHasNoData(5, 0, 0, 0, 0));
    }

    [Theory]
    [InlineData(1, "No comparison line: no data was collected for the same hours yesterday")]
    [InlineData(7, "No comparison line: no data was collected for the same hours last week")]
    [InlineData(3, "No comparison line: no data was collected for the same hours 3 days earlier")]
    public void OverviewCompare_EmptyText_NamesThePeriod(int days, string expected)
    {
        Assert.Equal(expected, CorrelatedTimelineLanesControl.ComparisonEmptyText(days));
    }

    [Fact]
    public void OverviewCompare_RefreshShowsTheBanner_OnlyWhenEveryReadCameBackEmpty()
    {
        var source = RepoFile("Lite/Controls/CorrelatedTimelineLanesControl.xaml.cs");

        Assert.Contains("ComparisonEmptyBanner.Text = ComparisonEmptyText(days);", source, StringComparison.Ordinal);
        Assert.Contains("refIoTask.IsCompletedSuccessfully", source, StringComparison.Ordinal);
        Assert.Contains("ComparisonEmptyBanner.Visibility = Visibility.Collapsed;", source, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"ComparisonEmptyBanner\"", RepoFile("Lite/Controls/CorrelatedTimelineLanesControl.xaml"), StringComparison.Ordinal);
    }

    private sealed class Item : ComparisonItemBase { }

    [Fact]
    public void ComparisonGrids_BaselineWithNoRows_SaysSo()
    {
        var onlyNew = new List<ComparisonItemBase> { new Item { ExecutionCount = 4, BaselineExecutionCount = 0 } };
        var withBaseline = new List<ComparisonItemBase> { new Item { ExecutionCount = 4, BaselineExecutionCount = 2 } };
        const string banner = "Comparing against baseline: a → b";

        Assert.Equal(banner + ComparisonBaselineNote.NoBaselineData, ComparisonBaselineNote.Banner(banner, onlyNew));
        Assert.Equal(banner + ComparisonBaselineNote.NoBaselineData, ComparisonBaselineNote.Banner(banner, new List<ComparisonItemBase>()));
        Assert.Equal(banner, ComparisonBaselineNote.Banner(banner, withBaseline));
    }

    [Theory]
    [InlineData("Lite/Controls/ServerTab.Comparison.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.QueriesComparison.cs")]
    public void ComparisonGrids_AllThreeLoadsAddTheNoBaselineNote(string path)
    {
        var source = RepoFile(path);

        foreach (var banner in new[] { "QueryStatsComparisonBanner", "ProcStatsComparisonBanner", "QueryStoreComparisonBanner" })
        {
            Assert.Contains($"{banner}.Text = ComparisonBaselineNote.Banner({banner}.Text, items);", source, StringComparison.Ordinal);
        }
    }

    // ---- V12a ---------------------------------------------------------------------------------------------------

    [Fact]
    public void LongQueries_TraceOff_EmptyTextSaysWhyAndWhereToTurnItOn()
    {
        var off = LongQueriesEmptyText.Text(false, "Settings → Collector Schedules → Edit");
        var on = LongQueriesEmptyText.Text(true, "Settings → Collector Schedules → Edit");

        Assert.Contains("trace is OFF", off, StringComparison.Ordinal);
        Assert.Contains("Settings → Collector Schedules → Edit", off, StringComparison.Ordinal);
        Assert.Contains("long_query_completions", off, StringComparison.Ordinal);
        Assert.DoesNotContain("OFF", on, StringComparison.Ordinal);
        Assert.NotEqual(EmptyState.DefaultGridText, off);
        Assert.NotEqual(EmptyState.DefaultGridText, on);
    }

    [Theory]
    [InlineData("Lite/Controls/ServerTab.LongQueries.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.LongQueries.cs")]
    public void LongQueries_TheGridsEmptyTextFollowsTheTraceSetting(string path)
    {
        var source = RepoFile(path);

        Assert.Contains("EmptyState.SetText(LongQueryCompletionsGrid", source, StringComparison.Ordinal);
        Assert.Contains("LongQueriesEmptyText.Text(traceEnabled, LongQueriesSwitchLocation)", source, StringComparison.Ordinal);
    }
}
