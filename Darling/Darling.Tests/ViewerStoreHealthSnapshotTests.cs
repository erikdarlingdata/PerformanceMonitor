/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static PerformanceMonitor.Darling.Storage.DarlingStoreMaintenanceReadinessReader;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;

namespace Darling.Tests;

/// <summary>
/// #9364: the Store Health tab's first open in a new process shows the rows the Viewer saved after its last live read of the store, with their
/// age, while the live read runs, and swaps the live rows in when it ends. The live reads are scripted through the tab's seams and held at a gate,
/// so each test asserts what is on screen and how many reads ran at the moment the live read is still out, never how long anything took.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerStoreHealthSnapshotTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "store-health-snapshot-" + Guid.NewGuid().ToString("N"));

    private string FilePath => Path.Combine(_dir, "store-health.json");

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
        }
    }

    private static StoreTableHealthRow TableRow(string name) =>
        new(
            "collect", name, KindTable, 2_000_000_000, 1_999_999_900, 60, 40, 1234, 0, 0, 1_500_000_000, 0.8, 1, 1, null,
            500, 100, new DateTime(2026, 11, 15, 15, 13, 0, DateTimeKind.Utc), null, null, null, false, null, false, 10, 5, null, 50.0, 1000.0, 0, null, 0, 0, 100, 10, 0, 0, null, 0, 0, 0,
            null, StatsSourceReset, 24, false, 0, 0, 0, null);

    private static StoreIndexHealthRow IndexRow(string index) =>
        new("collect", "store_things", KindTable, index, false, 1, 0, 1000, 1000, 0, 0.5, false, false, true, false, 0, false,
            null, StatsSourceReset, null, null, 0, "below_size_floor");

    private static StoreIndexCoverageRow CoverageRow() => new("collect", "parts", "ix_parts", true, 2, 2, 0, [], 0, []);

    private static StoreMaintenanceReadiness Readiness() =>
        new(true, "Safe", [], [], [], [], [], new DarlingStoreJobCrashReader.StoreJobCrashRead(true, null, []), true, 0, DateTime.UtcNow);

    /// <summary>The live reads: each counts its calls (and the fresh flag it was asked with) and waits at <see cref="Gate"/> when it is set.</summary>
    private sealed class LiveReads
    {
        public int Tables;
        public int Indexes;
        public int LastSizes;
        public readonly List<bool> TablesFresh = [];
        public TaskCompletionSource? Gate;
        public DateTime ReadAt = DateTime.UtcNow;

        /// <summary>What the store's last sizes read answers (#9374): null is a store with no last sizes.</summary>
        public StoreLastSizesHealth? LastSizesAnswer;

        public void Wire(StoreHealthTab tab)
        {
            tab.TablesRead = async fresh =>
            {
                Tables++;
                TablesFresh.Add(fresh);
                if (Gate is { } g) await g.Task;
                return new StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>([TableRow("live_things")], ReadAt);
            };
            tab.IndexesRead = async _ =>
            {
                Indexes++;
                if (Gate is { } g) await g.Task;
                return new StoreIndexHealth([IndexRow("ix_live")], [CoverageRow()], ReadAtUtc: ReadAt);
            };
            tab.ReadinessRead = async () =>
            {
                if (Gate is { } g) await g.Task;
                return Readiness();
            };
            tab.LastSizesRead = () =>
            {
                LastSizes++;
                return Task.FromResult(LastSizesAnswer);
            };
        }
    }

    private void RunTab(Func<StoreHealthTab, LiveReads, StoreHealthSnapshotStore, Task> body) => StaTestThread.Run(async () =>
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
        var reads = new LiveReads { Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
        reads.Wire(tab);
        var store = new StoreHealthSnapshotStore(FilePath);
        tab.Snapshots = store;
        var window = new Window { Content = tab, Width = 1600, Height = 900 };
        StaTestThread.ShowOffScreen(window);
        try
        {
            await body(tab, reads, store);
            await tab.SnapshotSaves;
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

    private static async Task UntilAsync(Func<bool> condition)
    {
        for (var i = 0; i < 300 && !condition(); i++)
        {
            await Task.Delay(10);
        }

        Assert.True(condition());
    }

    private static void Save(StoreHealthSnapshotStore store, DateTime readAt)
    {
        store.SaveTables([TableRow("saved_things"), TableRow("saved_more")], readAt);
        store.SaveIndexes(new StoreIndexHealth([IndexRow("ix_saved")], [CoverageRow()], ReadAtUtc: readAt));
    }

    [Fact]
    public void TheFirstOpen_WithASavedFile_ShowsItsRowsAndTheirAgeBeforeTheLiveReadEnds_ThenTheLiveRows()
    {
        RunTab(async (tab, reads, store) =>
        {
            Save(store, DateTime.UtcNow.AddMinutes(-12).AddSeconds(-10));

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => tab.TablesDataGrid.Items.Count == 2 && tab.IndexesDataGrid.Items.Count == 1);

            /* The live reads are out at their gate: what is on screen is the saved file, with its age. The store has no last sizes to give. */
            Assert.Equal((1, 1, 1), (reads.Tables, reads.Indexes, reads.LastSizes));
            Assert.Equal(["saved_more", "saved_things"], TableNames(tab));
            Assert.Equal("ix_saved", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
            Assert.StartsWith("Tables and indexes are from the last measurement, 12 min ago. A live read is running", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            reads.Gate!.SetResult();
            await refresh;

            Assert.Equal((1, 1), (reads.Tables, reads.Indexes));
            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Equal("ix_live", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    private static StoreLastSizesHealth LastSizesOf(DateTime measuredAt, params string[] tables) =>
        new(new StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>([.. tables.Select(TableRow)], DateTime.UtcNow), measuredAt);

    private static string[] TableNames(StoreHealthTab tab) =>
        [.. tab.TablesDataGrid.Items.Cast<StoreTableRow>().Select(r => r.Table.Split('.').Last()).Order(StringComparer.Ordinal)];

    private static string[] TableNamesContaining(StoreHealthTab tab, string text) =>
        [.. tab.TablesDataGrid.Items.Cast<StoreTableRow>().Where(r => r.Table.Contains(text, StringComparison.Ordinal)).Select(r => r.Table)];

    private static string StampOf(DateTime utc) => ViewerTimeHelper.RefreshedStamp(utc, ViewerTimeHelper.CurrentDisplayMode);

    [Fact]
    public void TheFirstOpen_WithAFileOlderThanTheStoresLastSizes_ShowsTheStoresTablesAndTheFilesIndexes()
    {
        RunTab(async (tab, reads, store) =>
        {
            var storeMeasured = DateTime.UtcNow.AddMinutes(-3);
            Save(store, DateTime.UtcNow.AddMinutes(-12).AddSeconds(-10));
            reads.LastSizesAnswer = LastSizesOf(storeMeasured, "store_a", "store_b", "store_c");

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => TableNamesContaining(tab, "store_").Length == 3 && tab.IndexesDataGrid.Items.Count == 1);

            /* The live reads are out at their gate: the tables are the store's last sizes, the indexes are the file's, and the strip says where each is from. */
            Assert.Empty(TableNamesContaining(tab, "saved_"));
            Assert.Equal("ix_saved", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
            Assert.Equal(
                "Sizes are from the store's last measurement, taken " + StampOf(storeMeasured) + ". Indexes are from the last measurement, 12 min ago. A live read is running…",
                tab.LastRefreshedIndicator.Text);

            reads.Gate!.SetResult();
            await refresh;

            Assert.Equal(["live_things"], TableNames(tab));
            Assert.Equal("ix_live", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void TheFirstOpen_WithAFileMeasuredAtTheSameTimeAsTheStoresLastSizes_ShowsTheStoresTables()
    {
        RunTab(async (tab, reads, store) =>
        {
            var measured = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, 0, 0, 0, DateTimeKind.Utc).AddMinutes(-15);
            Save(store, measured);
            reads.LastSizesAnswer = LastSizesOf(measured, "store_a", "store_b", "store_c");

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => TableNamesContaining(tab, "store_").Length == 3 && tab.IndexesDataGrid.Items.Count == 1);

            Assert.Empty(TableNamesContaining(tab, "saved_"));
            Assert.StartsWith("Sizes are from the store's last measurement, taken " + StampOf(measured) + ". Indexes are from the last measurement,", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            reads.Gate!.SetResult();
            await refresh;
            Assert.Equal(["live_things"], TableNames(tab));
        });
    }

    [Fact]
    public void TheFirstOpen_WithAFileNewerThanTheStoresLastSizes_ShowsTheFilesTables()
    {
        RunTab(async (tab, reads, store) =>
        {
            Save(store, DateTime.UtcNow.AddMinutes(-3).AddSeconds(-10));
            reads.LastSizesAnswer = LastSizesOf(DateTime.UtcNow.AddMinutes(-12), "store_a", "store_b", "store_c");

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => reads.LastSizes == 1 && tab.TablesDataGrid.Items.Count > 0 && tab.IndexesDataGrid.Items.Count == 1);
            await Task.Delay(50);

            /* The store's last sizes were read and are older than the file, so they never reach the screen. */
            Assert.Equal(["saved_more", "saved_things"], TableNames(tab));
            Assert.StartsWith("Tables and indexes are from the last measurement, 3 min ago. A live read is running", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);

            reads.Gate!.SetResult();
            await refresh;

            Assert.Equal(["live_things"], TableNames(tab));
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void TheFirstOpen_WithNoFile_WaitsForTheLiveRead_AsItAlwaysDid()
    {
        RunTab(async (tab, reads, store) =>
        {
            Assert.False(File.Exists(FilePath));

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => reads.Tables == 1 && reads.Indexes == 1);
            await Task.Delay(50);

            Assert.Empty(tab.TablesDataGrid.Items);
            Assert.Empty(tab.IndexesDataGrid.Items);
            Assert.Equal("Loading…", tab.LastRefreshedIndicator.Text);

            reads.Gate!.SetResult();
            await refresh;

            Assert.Single(tab.TablesDataGrid.Items);
            Assert.Equal(1, reads.LastSizes);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void ADamagedOrForeignFile_IsTreatedAsNoFile()
    {
        foreach (var content in new[] { "{ not json", "", "{\"Version\":99,\"Tables\":[],\"TablesReadAtUtc\":null}", "{\"Version\":1,\"Tables\":[null],\"TablesReadAtUtc\":\"2026-11-15T15:00:00Z\"}" })
        {
            Directory.CreateDirectory(_dir);
            File.WriteAllText(FilePath, content);
            Assert.Null(new StoreHealthSnapshotStore(FilePath).Load());
        }

        RunTab(async (tab, reads, store) =>
        {
            File.WriteAllText(FilePath, "{ not json");

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => reads.Tables == 1 && reads.Indexes == 1);
            await Task.Delay(50);

            Assert.Empty(tab.TablesDataGrid.Items);
            Assert.Empty(tab.IndexesDataGrid.Items);

            reads.Gate!.SetResult();
            await refresh;

            Assert.Single(tab.TablesDataGrid.Items);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void Refresh_MeasuresLive_AndShowsNoSavedRows()
    {
        RunTab(async (tab, reads, store) =>
        {
            Save(store, DateTime.UtcNow.AddMinutes(-5));

            var refresh = tab.RefreshAsync(force: true);
            await UntilAsync(() => reads.Tables == 1 && reads.Indexes == 1);
            await Task.Delay(50);

            Assert.Equal([true], reads.TablesFresh);
            Assert.Empty(tab.TablesDataGrid.Items);
            Assert.Empty(tab.IndexesDataGrid.Items);

            reads.Gate!.SetResult();
            await refresh;

            Assert.Equal("live_things", ((StoreTableRow)tab.TablesDataGrid.Items[0]).Table.Split('.').Last());
        });
    }

    [Fact]
    public void AFailedLiveRead_KeepsTheSavedRowsAndSaysSo()
    {
        RunTab(async (tab, reads, store) =>
        {
            Save(store, DateTime.UtcNow.AddMinutes(-3).AddSeconds(-5));
            tab.TablesRead = async _ =>
            {
                await reads.Gate!.Task;
                throw new InvalidOperationException("scripted failure");
            };

            var refresh = tab.RefreshAsync();
            await UntilAsync(() => tab.TablesDataGrid.Items.Count == 2);
            reads.Gate!.SetResult();
            await refresh;

            /* The tables read failed: its rows stay, as saved. The indexes read landed live. */
            Assert.Equal(2, tab.TablesDataGrid.Items.Count);
            Assert.Equal("ix_live", ((StoreIndexRow)tab.IndexesDataGrid.Items[0]).Index);
            Assert.StartsWith("Tables are from the last measurement, 3 min ago. The live read failed.", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void ALiveRead_IsSavedForTheNextOpen_KeepingTheOtherPart()
    {
        RunTab(async (tab, reads, store) =>
        {
            reads.Gate!.SetResult();
            await tab.RefreshAsync();
            await tab.SnapshotSaves;

            var saved = Assert.IsType<StoreHealthSnapshot>(store.Load());
            Assert.Equal("live_things", Assert.Single(saved.Tables!).TableName);
            Assert.Equal("ix_live", Assert.Single(saved.Indexes!).IndexName);
            Assert.Equal(reads.ReadAt, saved.TablesReadAtUtc);
            Assert.Single(saved.Coverage!);
        });
    }

    [Fact]
    public void AFailedSave_ShowsNothing_AndTheOpenCompletes()
    {
        RunTab(async (tab, reads, store) =>
        {
            /* A file where the folder should be: every write fails. */
            Directory.CreateDirectory(_dir);
            File.WriteAllText(FilePath, "x");
            tab.Snapshots = new StoreHealthSnapshotStore(Path.Combine(FilePath, "inner", "store-health.json"));
            var status = new List<string>();
            tab.StatusChanged += status.Add;
            reads.Gate!.SetResult();

            await tab.RefreshAsync();
            await tab.SnapshotSaves;

            Assert.Single(tab.TablesDataGrid.Items);
            Assert.StartsWith("Last refreshed ", tab.LastRefreshedIndicator.Text, StringComparison.Ordinal);
            Assert.DoesNotContain(status, text => text.Contains("saved", StringComparison.OrdinalIgnoreCase) || text.Contains("failed", StringComparison.OrdinalIgnoreCase));
        });
    }

    [Fact]
    public void ASavedPart_IsNotWrittenOverByAnOlderRead_AndBothPartsSurviveASaveOfOne()
    {
        var store = new StoreHealthSnapshotStore(FilePath);
        var newer = new DateTime(2026, 11, 15, 16, 0, 0, DateTimeKind.Utc);
        Save(store, newer);

        store.SaveTables([TableRow("older_things")], newer.AddMinutes(-30));
        var saved = Assert.IsType<StoreHealthSnapshot>(store.Load());
        Assert.Equal(2, saved.Tables!.Count);

        store.SaveTables([TableRow("newest")], newer.AddMinutes(1));
        saved = Assert.IsType<StoreHealthSnapshot>(store.Load());
        Assert.Equal("newest", Assert.Single(saved.Tables!).TableName);
        Assert.Equal("ix_saved", Assert.Single(saved.Indexes!).IndexName);
        Assert.Equal(newer, saved.IndexesReadAtUtc);
    }

    [Fact]
    public void TheSavedFile_IsKeyedByTheStoreIdentity_AndHoldsNoPassword()
    {
        var a = StoreHealthSnapshotStore.ForStore(ViewerPasswordKey.StoreIdentityOf("Host=db1;Port=5432;Database=pm;Username=u;Password=secret1"));
        var b = StoreHealthSnapshotStore.ForStore(ViewerPasswordKey.StoreIdentityOf("Host=DB1;Port=5432;Database=pm;Username=other;Password=secret2"));
        var c = StoreHealthSnapshotStore.ForStore(ViewerPasswordKey.StoreIdentityOf("Host=db2;Port=5432;Database=pm;Username=u;Password=secret1"));

        Assert.Equal(a.FilePath, b.FilePath);
        Assert.NotEqual(a.FilePath, c.FilePath);
        Assert.DoesNotContain("secret", a.FilePath, StringComparison.Ordinal);
        Assert.Equal("PerformanceMonitorDarling", Path.GetFileName(Path.GetDirectoryName(a.FilePath)));
    }
}
