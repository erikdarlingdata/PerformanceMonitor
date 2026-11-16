/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static PerformanceMonitor.Darling.Storage.DarlingStoreMaintenanceReadinessReader;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;

namespace Darling.Tests;

/// <summary>
/// #8819 and #8820: the Viewer's Store Health tab on a refresh. A refresh over rows already on screen keeps the grids and the summary lines readable and
/// says "Refreshing…" beside the button, one trigger runs one load (a trigger that queued behind a running load does not run the slow reads again), a
/// slow load writes a SlowLoad line, and the vacuum and analyze times read on the clock of the stamp beside them. The three reads are scripted through the
/// tab's test seams, so no store is needed.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerStoreHealthRefreshTests
{
    private static readonly DateTime Vacuumed = new(2026, 11, 15, 15, 13, 0, DateTimeKind.Utc);

    private static StoreTableHealthRow TableRow(string name, DateTime? vacuum = null) =>
        new(
            "collect", name, KindTable, 2_000_000_000, 1_999_999_900, 60, 40, 1234, 0, 0, 1_500_000_000, 0.8, 1, 1, null,
            500, 100, vacuum, null, null, null, false, null, false, 10, 5, null, 50.0, 1000.0, 0, null, 0, 0, 100, 10, 0, 0, null, 0, 0, 0,
            null, StatsSourceReset, 24, false, 0, 0, 0, null);

    private static StoreIndexHealthRow IndexRow(string index) =>
        new("collect", "store_things", KindTable, index, false, 1, 0, 1000, 1000, 0, 0.5, false, false, true, false, 0, false,
            null, StatsSourceReset, null, null, 0, "below_size_floor");

    private static StoreMaintenanceReadiness Readiness() =>
        new(true, "Safe", [], [], [], [], [], new DarlingStoreJobCrashReader.StoreJobCrashRead(true, null, []), true, 0, DateTime.UtcNow);

    /// <summary>The three scripted reads. Each holds until <see cref="Gate"/> is released when it is set, and counts its calls.</summary>
    private sealed class ScriptedReads
    {
        public int Tables;
        public int Indexes;
        public int Readiness;
        public TaskCompletionSource? Gate;
        public bool FailTables;

        public void Wire(StoreHealthTab tab)
        {
            tab.TablesRead = async () =>
            {
                Tables++;
                if (Gate is { } g) await g.Task;
                if (FailTables) throw new InvalidOperationException("scripted failure");
                return [TableRow("store_things")];
            };
            tab.IndexesRead = async () =>
            {
                Indexes++;
                if (Gate is { } g) await g.Task;
                return new StoreIndexHealth(
                    [IndexRow("ix_things")],
                    [new StoreIndexCoverageRow("collect", "parts", "ix_parts", true, 2, 2, 0, [], 0, [])]);
            };
            tab.ReadinessRead = async () =>
            {
                Readiness++;
                if (Gate is { } g) await g.Task;
                return Readiness();
            };
        }
    }

    private static void RunTab(Func<StoreHealthTab, ScriptedReads, Task> body) => StaTestThread.Run(async () =>
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary
            {
                Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
            };
            app.Resources.MergedDictionaries.Add(added);
        }

        var tab = new StoreHealthTab();
        var reads = new ScriptedReads();
        reads.Wire(tab);
        var window = new Window { Content = tab, Width = 1600, Height = 900 };
        StaTestThread.ShowOffScreen(window);
        try
        {
            await body(tab, reads);
        }
        finally
        {
            window.Close();
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    });

    [Fact]
    public void ARefreshOverFilledGrids_KeepsTheRowsAndTheSummaryLine_AndSaysRefreshing()
    {
        RunTab(async (tab, reads) =>
        {
            await tab.RefreshAsync(force: true);
            var tablesHeader = tab.HeaderText.Text;
            var indexesHeader = tab.IndexHeaderText.Text;
            var verdict = tab.ReadinessTextBlock.Text;
            Assert.DoesNotContain("Loading", tablesHeader, StringComparison.Ordinal);
            Assert.Equal(Visibility.Collapsed, tab.TablesMessage.Visibility);
            Assert.Equal(Visibility.Collapsed, tab.IndexesMessage.Visibility);
            Assert.Equal(Visibility.Collapsed, tab.CoverageMessage.Visibility);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var refresh = tab.RefreshAsync(force: true);

            /* The reads are out and held: this is what a reader sees for the seconds the load takes. */
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Single(tab.IndexesDataGrid.Items);
            Assert.Single(tab.CoverageDataGrid.Items);
            Assert.Equal(Visibility.Collapsed, tab.TablesMessage.Visibility);
            Assert.Equal(Visibility.Collapsed, tab.IndexesMessage.Visibility);
            Assert.Equal(Visibility.Collapsed, tab.CoverageMessage.Visibility);
            Assert.Equal(tablesHeader, tab.HeaderText.Text);
            Assert.Equal(indexesHeader, tab.IndexHeaderText.Text);
            Assert.Equal(verdict, tab.ReadinessTextBlock.Text);
            Assert.StartsWith("Refreshing… (last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            reads.Gate.SetResult();
            await refresh;
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void TheFirstLoad_ShowsTheLoadingText_BecauseNothingIsOnScreen()
    {
        RunTab(async (tab, reads) =>
        {
            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var refresh = tab.RefreshAsync(force: true);

            Assert.Equal(Visibility.Visible, tab.TablesMessage.Visibility);
            Assert.Equal("Loading store tables…", tab.HeaderText.Text);
            Assert.Equal(Visibility.Visible, tab.IndexesMessage.Visibility);
            Assert.Equal(Visibility.Visible, tab.CoverageMessage.Visibility);
            Assert.Equal("Loading…", tab.LastRefreshedIndicator.Text);

            reads.Gate.SetResult();
            await refresh;
            Assert.Equal(Visibility.Collapsed, tab.TablesMessage.Visibility);
        });
    }

    [Fact]
    public void AFailedRefresh_DoesNotLeaveTheStripSayingRefreshing()
    {
        RunTab(async (tab, reads) =>
        {
            await tab.RefreshAsync(force: true);
            var stamp = tab.LastRefreshedIndicator.Text;
            reads.FailTables = true;

            await tab.RefreshAsync(force: true);

            Assert.Equal(stamp, tab.LastRefreshedIndicator.Text);
        });
    }

    [Fact]
    public void ATriggerQueuedBehindARunningLoad_DoesNotRunTheReadsAgainWhenItEnds()
    {
        RunTab(async (tab, reads) =>
        {
            /* The shell's path: a timer tick that lands while a load runs asks the running loop to go round once more. */
            var lanes = new ViewerRefreshLanes();
            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var first = lanes.RunLaneAsync(() => false, tab.RefreshAsync);
            await lanes.RunLaneAsync(() => false, tab.RefreshAsync);
            reads.Gate.SetResult();
            await first;

            Assert.Equal(1, reads.Tables);
            Assert.Equal(1, reads.Indexes);
            Assert.Equal(1, reads.Readiness);
        });
    }

    [Fact]
    public void ARefreshButtonClickDuringALoad_StartsNoSecondLoad()
    {
        RunTab(async (tab, reads) =>
        {
            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var first = tab.RefreshAsync(force: true);
            await tab.RefreshAsync(force: true);
            reads.Gate.SetResult();
            await first;

            Assert.Equal(1, reads.Tables);
        });
    }

    [Fact]
    public void ARefreshAfterTheFreshnessWindow_LoadsAgain_AndTheRefreshButtonAlwaysDoes()
    {
        RunTab(async (tab, reads) =>
        {
            await tab.RefreshAsync();
            Assert.Equal(1, reads.Tables);

            await tab.RefreshAsync();
            Assert.Equal(1, reads.Tables);

            await tab.RefreshAsync(force: true);
            Assert.Equal(2, reads.Tables);

            tab.FreshFor = TimeSpan.Zero;
            await tab.RefreshAsync();
            Assert.Equal(3, reads.Tables);
        });
    }

    [Fact]
    public void ATabPick_SaysRefreshingAtOnce_WhenTheLoadIsDue_AndNothingWhenTheLastLoadIsFresh()
    {
        RunTab(async (tab, reads) =>
        {
            await tab.RefreshAsync(force: true);
            var stamp = tab.LastRefreshedIndicator.Text;

            tab.ShowRefreshPending();
            Assert.Equal(stamp, tab.LastRefreshedIndicator.Text);

            tab.FreshFor = TimeSpan.Zero;
            tab.ShowRefreshPending();
            Assert.StartsWith("Refreshing… (last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Equal(1, reads.Tables);
        });
    }

    [Fact]
    public void ASlowLoad_WritesASlowLoadLine_NamingTheThreeReads()
    {
        var lines = new List<string>();
        RunTab(async (tab, reads) =>
        {
            tab.SlowLoadSink = lines.Add;
            tab.SlowLoadThresholdMs = 0;

            tab.BeginFirstOpen();
            tab.FirstOpenRefreshStarted();
            await tab.RefreshAsync();
            await tab.RefreshAsync(force: true);
        });

        Assert.Equal(2, lines.Count);
        Assert.Contains("first open > Store Health", lines[0], StringComparison.Ordinal);
        Assert.DoesNotContain("first open", lines[1], StringComparison.Ordinal);
        foreach (var line in lines)
        {
            Assert.Contains("slow load: ", line, StringComparison.Ordinal);
            Assert.Contains("Store Health", line, StringComparison.Ordinal);
            Assert.Contains("store tables read ", line, StringComparison.Ordinal);
            Assert.Contains("store indexes read ", line, StringComparison.Ordinal);
            Assert.Contains("store maintenance check ", line, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AQuickLoad_WritesNoSlowLoadLine()
    {
        var lines = new List<string>();
        RunTab(async (tab, reads) =>
        {
            tab.SlowLoadSink = lines.Add;
            tab.SlowLoadThresholdMs = 600_000;
            await tab.RefreshAsync(force: true);
        });

        Assert.Empty(lines);
    }

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void TheVacuumAndAnalyzeTimes_AreOnTheClockOfTheRefreshStamp_WithTheirZone(TimeDisplayMode mode)
    {
        var previous = ViewerTimeHelper.CurrentDisplayMode;
        ViewerTimeHelper.CurrentDisplayMode = mode;
        try
        {
            var row = StoreHealthRows.Table(TableRow("store_things", Vacuumed) with { LastAnalyze = Vacuumed });

            var expected = ViewerTimeHelper.FormatFleetWide(Vacuumed, "yyyy-MM-dd HH:mm", mode);
            Assert.Equal(expected, row.LastVacuum);
            Assert.Equal(expected, row.LastAnalyze);
            var stampZone = ViewerTimeHelper.RefreshedStamp(Vacuumed, mode);
            Assert.EndsWith(stampZone[(stampZone.IndexOf(' ') + 1)..], row.LastVacuum, StringComparison.Ordinal);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = previous;
        }
    }

    [Fact]
    public void TheVacuumTime_InServerTimeMode_ReadsUtcWithItsLabel_AndATableNeverVacuumedShowsNothing()
    {
        var previous = ViewerTimeHelper.CurrentDisplayMode;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        try
        {
            Assert.Equal("2026-11-15 15:13 UTC", StoreHealthRows.Table(TableRow("store_things", Vacuumed)).LastVacuum);
            Assert.Equal("", StoreHealthRows.Table(TableRow("store_things")).LastVacuum);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = previous;
        }
    }
}
