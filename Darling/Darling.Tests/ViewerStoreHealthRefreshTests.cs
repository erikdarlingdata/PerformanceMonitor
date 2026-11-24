/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
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

        /// <summary>The time every scripted tables and indexes answer says the store gave it: a fixed one, so the strip's text does not
        /// depend on when a test runs.</summary>
        public DateTime ReadAt = new(2026, 11, 15, 15, 0, 0, DateTimeKind.Utc);

        public void Wire(StoreHealthTab tab)
        {
            tab.TablesRead = async _ =>
            {
                Tables++;
                if (Gate is { } g) await g.Task;
                if (FailTables) throw new InvalidOperationException("scripted failure");
                return new StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>([TableRow("store_things")], ReadAt);
            };
            tab.IndexesRead = async _ =>
            {
                Indexes++;
                if (Gate is { } g) await g.Task;
                return new StoreIndexHealth(
                    [IndexRow("ix_things")],
                    [new StoreIndexCoverageRow("collect", "parts", "ix_parts", true, 2, 2, 0, [], 0, [])],
                    ReadAtUtc: ReadAt);
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

    /// <summary>#8924: the tab over a store health cache whose three store reads count their calls, on a clock the test steps. The refresh
    /// the shell's timer runs every 30 seconds shows the held rows and sends the store only the maintenance check, the strip says when the
    /// store answered the rows on screen (not when the tab last refreshed), the Refresh button reads the store again, and so does a timer
    /// refresh once the lifetime has passed. The data source is never opened: the reads are scripted.</summary>
    [Fact]
    public void ATimerRefresh_ShowsTheHeldRows_SendsTheStoreNoTableOrIndexRead_AndTheStripSaysWhenTheyWereRead()
    {
        RunTab(async (tab, reads) =>
        {
            var now = new DateTime(2026, 11, 15, 15, 0, 0, DateTimeKind.Utc);
            int tables = 0, indexes = 0, coverage = 0;
            var held = new DarlingStoreTableHealthCache(
                () => now,
                (_, _) => { Interlocked.Increment(ref tables); return Task.FromResult(new List<StoreTableHealthRow> { TableRow("store_things") }); },
                (_, _) => { Interlocked.Increment(ref indexes); return Task.FromResult(new List<StoreIndexHealthRow> { IndexRow("ix_things") }); },
                (_, _) =>
                {
                    Interlocked.Increment(ref coverage);
                    return Task.FromResult(new List<StoreIndexCoverageRow> { new("collect", "parts", "ix_parts", true, 2, 2, 0, [], 0, []) });
                });
            await using var store = NpgsqlDataSource.Create("Host=127.0.0.1;Database=never_opened");
            tab.TablesRead = fresh => held.GetTablesAsync(store, fresh);
            tab.IndexesRead = fresh => ViewerDataService.ReadStoreIndexHealthAsync(held, store, fresh, CancellationToken.None);
            tab.FreshFor = TimeSpan.Zero;
            string ReadAt() => ", tables and indexes read " + ViewerTimeHelper.RefreshedStamp(now, ViewerTimeHelper.CurrentDisplayMode);

            await tab.RefreshAsync();
            Assert.Equal((1, 1, 1), (tables, indexes, coverage));
            Assert.Equal(1, reads.Readiness);
            var firstRead = ReadAt();
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
            Assert.EndsWith(firstRead, tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            /* Two timer refreshes, two minutes on: the rows on screen are the held ones and the strip still gives the time the store answered them. */
            now = now.AddMinutes(2);
            await tab.RefreshAsync();
            await tab.RefreshAsync();
            Assert.Equal((1, 1, 1), (tables, indexes, coverage));
            Assert.Equal(3, reads.Readiness);
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Single(tab.IndexesDataGrid.Items);
            Assert.Single(tab.CoverageDataGrid.Items);
            Assert.NotEqual(firstRead, ReadAt());
            Assert.EndsWith(firstRead, tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            /* The Refresh button asks for a new read of all three parts, and the strip moves to its time. */
            await tab.RefreshAsync(force: true);
            Assert.Equal((2, 2, 2), (tables, indexes, coverage));
            Assert.EndsWith(ReadAt(), tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            /* #9224: past the lifetime a timer refresh shows the held rows and their own read time while a new read runs behind it, and the
               refresh after that shows the new read. */
            var forcedRead = ReadAt();
            now = now + DarlingStoreTableHealthCache.Lifetime + TimeSpan.FromSeconds(1);
            await tab.RefreshAsync();
            Assert.EndsWith(forcedRead, tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
            await held.WhenIdleAsync();
            Assert.Equal((3, 3, 3), (tables, indexes, coverage));
            await tab.RefreshAsync();
            Assert.Equal((3, 3, 3), (tables, indexes, coverage));
            Assert.EndsWith(ReadAt(), tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

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
            Assert.Contains(", tables and indexes read ", stamp, StringComparison.Ordinal);
            reads.FailTables = true;

            await tab.RefreshAsync(force: true);

            /* The strip is back on the last refresh that worked. The table rows are gone, so the read time it gives is the index rows' alone. */
            Assert.Equal(stamp.Replace(", tables and indexes read ", ", indexes read ", StringComparison.Ordinal), tab.LastRefreshedIndicator.Text);
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

    /// <summary>#8950: the autovacuum settings line under the header. The scripted settings read answers five minutes before the tables read,
    /// so the strip gives the older of the two answers. A read past its limit leaves the line saying so and the tables still show, any other
    /// failure says it failed, and with no settings read at all the line stays hidden.</summary>
    [Fact]
    public void TheAutovacuumSettingsLine_ShowsTheSettings_SaysWhenTheyWereLeftOut_AndStaysHiddenWithNoRead()
    {
        RunTab(async (tab, reads) =>
        {
            await tab.RefreshAsync(force: true);
            Assert.Equal(Visibility.Collapsed, tab.AutovacuumSettingsTextBlock.Visibility);
            Assert.Equal("", tab.AutovacuumSettingsTextBlock.Text);

            var settings = Settings(true, 3, 1, 60, -1, 200, 2, 0, -1, 65536, 20, 8192, 4, 1);
            var settingsAt = reads.ReadAt.AddMinutes(-5);
            var calls = new List<bool>();
            Exception? failure = null;
            tab.AutovacuumSettingsRead = fresh =>
            {
                calls.Add(fresh);
                return failure is { } ex
                    ? Task.FromException<StoreHealthRead<StoreAutovacuumSettings>>(ex)
                    : Task.FromResult(new StoreHealthRead<StoreAutovacuumSettings>(settings, settingsAt));
            };

            await tab.RefreshAsync(force: true);
            Assert.Equal([true], calls);
            Assert.Equal(Visibility.Visible, tab.AutovacuumSettingsTextBlock.Visibility);
            Assert.Equal(StoreHealthRows.AutovacuumSettingsText(settings), tab.AutovacuumSettingsTextBlock.Text);
            Assert.EndsWith(
                ", tables and indexes read " + ViewerTimeHelper.RefreshedStamp(settingsAt, ViewerTimeHelper.CurrentDisplayMode),
                tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            failure = new StoreTableHealthTimeoutException(new TimeoutException());
            await tab.RefreshAsync(force: true);
            Assert.Equal(Visibility.Visible, tab.AutovacuumSettingsTextBlock.Visibility);
            Assert.Equal("The autovacuum settings were left out. " + failure.Message, tab.AutovacuumSettingsTextBlock.Text);
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.EndsWith(
                ", tables and indexes read " + ViewerTimeHelper.RefreshedStamp(reads.ReadAt, ViewerTimeHelper.CurrentDisplayMode),
                tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            failure = new InvalidOperationException("scripted settings failure");
            await tab.RefreshAsync(force: true);
            Assert.Equal("The store autovacuum settings did not load.", tab.AutovacuumSettingsTextBlock.Text);
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Equal(3, calls.Count);
        });
    }

    /// <summary>#8950: the grid keeps the reader's order, which puts the tables behind on autovacuum first, and flags them.</summary>
    [Fact]
    public void TheTablesBehindOnAutovacuum_ComeFirstInTheGrid_AndAreFlagged()
    {
        RunTab(async (tab, reads) =>
        {
            tab.TablesRead = _ => Task.FromResult(new StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>(
                [
                    TableRow("behind") with { Behind = true, BehindHours = 30, RelationsPastTrigger = 1 },
                    TableRow("cancelled") with { AutovacuumCancels = 3 },
                    TableRow("quiet"),
                ],
                reads.ReadAt));

            await tab.RefreshAsync(force: true);

            var rows = new List<StoreTableRow>();
            foreach (var item in tab.TablesDataGrid.Items)
            {
                rows.Add((StoreTableRow)item);
            }

            Assert.Equal(["collect.behind", "collect.cancelled", "collect.quiet"], rows.ConvertAll(r => r.Table));
            Assert.Equal("Behind on autovacuum", rows[0].Flag);
            Assert.Equal("Behind, no vacuum in 30 h", rows[0].Behind);
            Assert.Equal("Autovacuum cancelled repeatedly", rows[1].Flag);
            Assert.Equal("No", rows[2].Behind);
            Assert.Contains("1 table behind on autovacuum", tab.HeaderText.Text, StringComparison.Ordinal);
            Assert.Contains("1 table with autovacuum cancelled 2 or more times in 24 hours", tab.HeaderText.Text, StringComparison.Ordinal);
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

    /// <summary>#9364: the slow-load line says how many relations each read sized and whether the read was taken for this open (stamped after the
    /// call began) or held from earlier (stamped before it, so the line gives its age). The maintenance check carries no such facts and keeps its text.</summary>
    [Fact]
    public void ASlowLoad_NamesTheRelationsEachReadSized_AndWhetherTheyWereMeasuredLiveOrHeld()
    {
        var lines = new List<string>();
        RunTab(async (tab, reads) =>
        {
            tab.SlowLoadSink = lines.Add;
            tab.SlowLoadThresholdMs = 0;
            tab.TablesRead = _ => Task.FromResult(new StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>(
                [TableRow("a"), TableRow("b"), TableRow("c")], DateTime.UtcNow.AddSeconds(1)));
            tab.IndexesRead = _ => Task.FromResult(new StoreIndexHealth([IndexRow("ix_things")], [], ReadAtUtc: DateTime.UtcNow.AddSeconds(-130)));
            tab.ReadinessRead = () => Task.FromResult(Readiness());

            await tab.RefreshAsync(force: true);
        });

        var line = Assert.Single(lines);
        Assert.Matches(@"store tables read \d+ ms \(sized 3 relations, measured live\)", line);
        Assert.Matches(@"store indexes read \d+ ms \(sized 1 relation, from the held measurement taken 2 min \d+ s ago\)", line);
        Assert.Matches(@"store maintenance check \d+ ms(?! \()", line);
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

    /* #9374: the first open from the store's last measured sizes. */

    private static readonly DateTime SizesMeasured = new(2026, 11, 14, 9, 30, 0, DateTimeKind.Utc);

    private static string MeasuredStamp() => ViewerTimeHelper.RefreshedStamp(SizesMeasured, ViewerTimeHelper.CurrentDisplayMode);

    /// <summary>The store's last measured sizes as the data service answers them: two tables (the live read answers one). There are no index rows.</summary>
    private static StoreLastSizesHealth LastSizes(ScriptedReads reads) =>
        new(
            new StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>([TableRow("store_things"), TableRow("old_things")], reads.ReadAt),
            SizesMeasured);

    private static async Task UntilAsync(Func<bool> condition)
    {
        for (var i = 0; i < 300 && !condition(); i++)
        {
            await Task.Delay(10);
        }

        Assert.True(condition());
    }

    [Fact]
    public void TheFirstOpen_ShowsTheLastMeasuredSizesAtOnce_ThenSwapsInTheLiveRows_KeepingTheSelection()
    {
        RunTab(async (tab, reads) =>
        {
            var lastCalls = 0;
            tab.LastSizesRead = () => { lastCalls++; return Task.FromResult<StoreLastSizesHealth?>(LastSizes(reads)); };
            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => tab.TablesDataGrid.Items.Count == 2);

            /* The live read is held at its gate: what is on screen is the store's last measurement, and the strip says so. */
            Assert.Equal(1, lastCalls);
            Assert.Empty(tab.IndexesDataGrid.Items);
            Assert.Equal(
                "Sizes are from the store's last measurement, taken " + MeasuredStamp() + ". A live read is running…",
                tab.LastRefreshedIndicator.Text);
            var before = tab.TablesDataGrid.Items.Cast<StoreTableRow>().First(r => r.Table.Contains("store_things", StringComparison.Ordinal));
            tab.TablesDataGrid.SelectedItem = before;

            reads.Gate.SetResult();
            await refresh;

            Assert.Equal(1, reads.Tables);
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("last measurement", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
            Assert.Equal("ix_things", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
            Assert.Equal(1, lastCalls);
            /* The live rows are new objects, and the table that was selected is still the selected one. */
            var after = Assert.IsType<StoreTableRow>(tab.TablesDataGrid.SelectedItem);
            Assert.Equal(before.Table, after.Table);
            Assert.NotSame(before, after);
        });
    }

    /// <summary>#9374: the live reads start beside the last sizes read, so the first open costs the longer of the two and not their sum.</summary>
    [Fact]
    public void TheFirstOpen_StartsTheLiveReads_BeforeTheLastSizesReadFinishes()
    {
        RunTab(async (tab, reads) =>
        {
            var lastRelease = new TaskCompletionSource<StoreLastSizesHealth?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var lastCalls = 0;
            tab.LastSizesRead = () => { lastCalls++; return lastRelease.Task; };
            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var refresh = tab.RefreshAsync();

            /* The last sizes read is still running, and the live tables, indexes and maintenance reads are already under way. */
            await UntilAsync(() => reads.Tables == 1 && reads.Indexes == 1 && reads.Readiness == 1);
            Assert.Equal(1, lastCalls);
            Assert.False(refresh.IsCompleted);

            lastRelease.SetResult(LastSizes(reads));
            await UntilAsync(() => tab.TablesDataGrid.Items.Count == 2);
            reads.Gate.SetResult();
            await refresh;

            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Equal((1, 1), (reads.Tables, reads.Indexes));
        });
    }

    /// <summary>#9374: whichever read finishes first, a live result is never written over by the last sizes.</summary>
    [Fact]
    public void ALiveResultThatLandsBeforeTheLastSizes_IsNeverOverwrittenByThem()
    {
        RunTab(async (tab, reads) =>
        {
            var lastRelease = new TaskCompletionSource<StoreLastSizesHealth?>(TaskCreationOptions.RunContinuationsAsynchronously);
            tab.LastSizesRead = () => lastRelease.Task;

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => tab.TablesDataGrid.Items.Count == 1 && tab.IndexesDataGrid.Items.Count == 1);

            lastRelease.SetResult(LastSizes(reads));
            await refresh;

            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Equal("store_things", ((StoreTableRow)tab.TablesDataGrid.Items[0]).Table.Split('.')[^1]);
            Assert.Equal("ix_things", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("last measurement", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    /// <summary>#9374: the first open reads the indexes once, and the tables rows from the last sizes show while the indexes read is still running.</summary>
    [Fact]
    public void TheFirstOpen_ReadsTheIndexesOnce_AndShowsTheLastSizesTablesWithoutWaitingForThem()
    {
        RunTab(async (tab, reads) =>
        {
            tab.LastSizesRead = () => Task.FromResult<StoreLastSizesHealth?>(LastSizes(reads));
            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => tab.TablesDataGrid.Items.Count == 2);

            /* The live indexes read is held: the tables show their last sizes, and the indexes grid waits for the one read of them. */
            Assert.Equal(1, reads.Indexes);
            Assert.Empty(tab.IndexesDataGrid.Items);

            reads.Gate.SetResult();
            await refresh;

            Assert.Equal(1, reads.Indexes);
            Assert.Equal("ix_things", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
        });
    }

    [Fact]
    public void TheFirstOpen_WithNoLastSizes_RunsOneLiveRead_AsItAlwaysDid()
    {
        RunTab(async (tab, reads) =>
        {
            var lastCalls = 0;
            tab.LastSizesRead = () => { lastCalls++; return Task.FromResult<StoreLastSizesHealth?>(null); };

            await tab.RefreshAsync();

            Assert.Equal(1, lastCalls);
            Assert.Equal((1, 1), (reads.Tables, reads.Indexes));
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void ARefreshClickedDuringTheLiveRead_StartsNoSecondRead_AndTheLiveRowsLand()
    {
        RunTab(async (tab, reads) =>
        {
            tab.LastSizesRead = () => Task.FromResult<StoreLastSizesHealth?>(LastSizes(reads));
            reads.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var refresh = tab.RefreshAsync();
            await UntilAsync(() => tab.TablesDataGrid.Items.Count == 2 && reads.Tables == 1);

            /* The running refresh answers the click, so the last measured rows are not replaced by a stale live read. */
            await tab.RefreshAsync(force: true);
            Assert.Equal(1, reads.Tables);

            reads.Gate.SetResult();
            await refresh;
            Assert.Equal(1, reads.Tables);
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void AFailedLiveRead_KeepsTheLastMeasuredRowsAndText_ReportsTheFailure_AndALaterRefreshReplacesThem()
    {
        RunTab(async (tab, reads) =>
        {
            var status = new List<string>();
            tab.StatusChanged += status.Add;
            tab.LastSizesRead = () => Task.FromResult<StoreLastSizesHealth?>(LastSizes(reads));
            tab.FreshFor = TimeSpan.Zero;
            reads.FailTables = true;

            await tab.RefreshAsync();

            Assert.Equal(2, tab.TablesDataGrid.Items.Count);
            Assert.Equal(
                "Sizes are from the store's last measurement, taken " + MeasuredStamp() + ". The live read failed.",
                tab.LastRefreshedIndicator.Text);
            Assert.Contains("The store tables did not load.", status);
            Assert.DoesNotContain(status, m => m.StartsWith("store health: refreshed", StringComparison.Ordinal));

            /* The refresh the timer runs next reads live again: the last measured rows are not read again, and the live rows replace them. */
            reads.FailTables = false;
            await tab.RefreshAsync();
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void AReopenWithAHeldRead_SkipsTheLastMeasuredSizes_AndSoDoesARefreshOfLoadedRows()
    {
        RunTab(async (tab, reads) =>
        {
            var lastCalls = 0;
            tab.LastSizesRead = () => { lastCalls++; return Task.FromResult<StoreLastSizesHealth?>(LastSizes(reads)); };
            tab.FreshFor = TimeSpan.Zero;
            var held = true;
            tab.HoldsTablesRead = () => held;

            await tab.RefreshAsync();
            Assert.Equal(0, lastCalls);
            Assert.Equal(1, reads.Tables);

            /* Nothing held, but rows are on screen from the first load: the timer's refresh does not go back to the last measured sizes. */
            held = false;
            await tab.RefreshAsync();
            Assert.Equal(0, lastCalls);
            Assert.Equal(2, reads.Tables);
        });
    }

    [Fact]
    public void TheFirstOpensSlowLoadLine_NamesTheStoresLastMeasurementAndItsAge()
    {
        RunTab(async (tab, reads) =>
        {
            var lines = new List<string>();
            tab.SlowLoadSink = lines.Add;
            tab.SlowLoadThresholdMs = 0;
            tab.LastSizesRead = () => Task.FromResult<StoreLastSizesHealth?>(LastSizes(reads));

            await tab.RefreshAsync();

            var line = Assert.Single(lines);
            Assert.Contains("store last sizes read", line, StringComparison.Ordinal);
            Assert.Contains("sized 2 relations, from the store's last measurement taken ", line, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void TheLastSizesNote_SaysHowOldTheMeasurementIs()
    {
        var now = new DateTime(2026, 11, 15, 12, 0, 0, DateTimeKind.Utc);
        Assert.Equal("sized 1 relation, from the store's last measurement taken 45 s ago", StoreHealthRows.LastSizesNote(1, now.AddSeconds(-45), now));
        Assert.Equal("sized 3 relations, from the store's last measurement taken 2 min 5 s ago", StoreHealthRows.LastSizesNote(3, now.AddSeconds(-125), now));
        Assert.Equal("sized 3 relations, from the store's last measurement taken 26 h 30 min ago", StoreHealthRows.LastSizesNote(3, now.AddMinutes(-1590), now));
    }
}
