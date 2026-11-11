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
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Object Growth Heatmap's popup over a store with a collection gap. Two objects are seeded over five days: one has a sample
/// on the first, second and fifth day, the other on the second and fifth, and neither on the third or fourth. The tab's own
/// loader reads the store and draws the chart, and the mouse is then pointed at drawn cells. A cell on a day neither object has
/// a sample for reads that day and "no sample collected"; a blank cell on a day the other object has a sample for shows nothing;
/// a cell with a sample reads its object and size.
/// </summary>
/* Sets ServerTimeHelper.CurrentDisplayMode and ActiveServerClock, process-wide statics, and loads the theme at application
   scope; joins the collection every other class that writes them uses, which runs alone. */
[Collection("server-time-helper")]
public sealed class FinOpsObjectHeatmapGapDayHoverTests : IDisposable
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const int ServerId = 8531;
    private const string DatabaseName = "GapDb";
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "FinOpsHeatmapHover_" + Guid.NewGuid().ToString("N")[..8]);
    private long _nextId = 1;

    public FinOpsObjectHeatmapGapDayHoverTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task PointingAtAGapDay_SaysNoSampleCollected_AndABlankCellOnASampledDaySaysNothing()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_dir, "hover.duckdb"));
        await duckDb.InitializeFromTemplateAsync();
        /* Noon UTC on five whole days that have ended, so each sample's day is the same whatever the wall clock reads. */
        var fifth = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-1).AddHours(12), DateTimeKind.Unspecified);
        await SeedObjectAsync(duckDb, "Orders", 1001, fifth.AddDays(-4), 100m);
        await SeedObjectAsync(duckDb, "Orders", 1001, fifth.AddDays(-3), 110m);
        await SeedObjectAsync(duckDb, "Orders", 1001, fifth, 150m);
        await SeedObjectAsync(duckDb, "Lines", 1002, fifth.AddDays(-3), 40m);
        await SeedObjectAsync(duckDb, "Lines", 1002, fifth, 45m);

        var popups = HoverEveryCell(duckDb, columns: 5, rows: 2);

        /* The third and fourth days: no sample for either object, in either row. */
        foreach (var (column, day) in new[] { (2, fifth.AddDays(-2)), (3, fifth.AddDays(-1)) })
        {
            for (var row = 0; row < 2; row++)
            {
                Assert.Equal($"{day:M/d}  |  no sample collected", popups[(column, row)]);
            }
        }

        /* The first day: Orders has a sample and Lines has none, so one row reads Orders' size and the other shows nothing. */
        var firstDay = new[] { popups[(0, 0)], popups[(0, 1)] };
        Assert.Single(firstDay, text => text is null);
        Assert.Single(firstDay, text => text is not null && text.StartsWith("dbo.Orders  |  ", StringComparison.Ordinal) && text.EndsWith(" MB reserved", StringComparison.Ordinal));

        /* The second and fifth days: both objects have a sample. */
        foreach (var column in new[] { 1, 4 })
        {
            for (var row = 0; row < 2; row++)
            {
                Assert.EndsWith(" MB reserved", popups[(column, row)]);
            }
        }
    }

    /// <summary>Runs the tab's own object-growth loader over the store, points the mouse at every drawn cell and returns what the
    /// popup read at each: its text, or null when it was closed.</summary>
    private static Dictionary<(int Column, int Row), string?> HoverEveryCell(DuckDbInitializer duckDb, int columns, int rows)
    {
        var dataService = new LocalDataService(duckDb);
        var popups = new Dictionary<(int, int), string?>();
        StaTestThread.Run(async () =>
        {
            ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            ServerTimeHelper.ActiveServerClock = ServerClock.Resolve("UTC", 0);
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("DataGridCellText"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }
            var tab = new FinOpsTab();
            typeof(FinOpsTab).GetField("_dataService", Instance)!.SetValue(tab, dataService);
            var outer = new TabControl();
            outer.Items.Add(new TabItem { Header = "FinOps", Content = tab });
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(outer), Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var chart = (WpfPlot)tab.FindName("ObjectGrowthHeatmapChart");
                for (var node = (DependencyObject)chart; node is not null; node = LogicalTreeHelper.GetParent(node))
                {
                    if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
                    {
                        tabs.SelectedItem = item;
                    }
                }

                /* The drill the tab makes when a database is opened: the object view, then its load over the store. */
                var level = typeof(FinOpsTab).GetNestedType("StorageDrillLevel", BindingFlags.NonPublic)!;
                typeof(FinOpsTab).GetMethod("ShowStorageView", Instance)!.Invoke(tab, new[] { Enum.Parse(level, "Objects") });
                ChartLayoutProbe.Layout(window);
                await (Task)typeof(FinOpsTab).GetMethod("LoadObjectGrowthAsync", Instance)!.Invoke(tab, [ServerId, DatabaseName])!;
                ChartLayoutProbe.Settle(window, new[] { chart });

                var lastHover = typeof(FinOpsTab).GetField("_lastObjHeatmapHover", Instance)!;
                try
                {
                    for (var column = 0; column < columns; column++)
                    {
                        for (var row = 0; row < rows; row++)
                        {
                            /* The handler answers one move in fifty milliseconds; each cell's move starts from a hover that has aged out. */
                            lastHover.SetValue(tab, default(DateTime));
                            HeatmapHoverProbe.MoveTo(window, chart, HeatmapHoverProbe.CellPoint(chart, column, row));
                            var popup = (Popup?)typeof(FinOpsTab).GetField("_objHeatmapPopup", Instance)!.GetValue(tab);
                            var text = (TextBlock?)typeof(FinOpsTab).GetField("_objHeatmapPopupText", Instance)!.GetValue(tab);
                            popups[(column, row)] = popup is { IsOpen: true } ? text?.Text : null;
                        }
                    }
                }
                finally
                {
                    if (typeof(FinOpsTab).GetField("_objHeatmapPopup", Instance)!.GetValue(tab) is Popup open) open.IsOpen = false;
                    HeatmapHoverProbe.Leave(window);
                }
            }
            finally
            {
                window.Close();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });
        return popups;
    }

    /// <summary>One object's one index at <paramref name="at"/>: the heatmap's series sums an object's indexes per day.</summary>
    private async Task SeedObjectAsync(DuckDbInitializer duckDb, string table, int objectId, DateTime at, decimal reservedMb)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO index_object_stats
    (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
     schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
     user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, 'heatmap-hover', $4, $5, 7, 'dbo', $6, $7, 1, 'PK', 'CLUSTERED', $8, $8, 1000, 0, 0, 0, 0, 0, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = at.AddDays(-30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = DatabaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectId });
        cmd.Parameters.Add(new DuckDBParameter { Value = table });
        cmd.Parameters.Add(new DuckDBParameter { Value = reservedMb });
        await cmd.ExecuteNonQueryAsync();
    }
}
