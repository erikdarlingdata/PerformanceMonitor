/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Object Growth Heatmap popup over a store with a collection gap. Two objects are seeded over five days: one has a
/// sample on the first, second and fifth day, the other on the second and fifth, and neither on the third or fourth. The tab's
/// own loader reads the store and draws the chart, and the mouse is then pointed at drawn cells. A cell on a day neither object
/// has a sample for reads that day and "no sample collected"; a blank cell on a day the other object has a sample for shows
/// nothing; a cell with a sample reads its object and size.
/// </summary>
/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
/* Writes ViewerTimeHelper.CurrentDisplayMode, a process-wide static, and loads the Viewer's theme at application scope. The
   collection runs alone, which is what the application-theme guard (#8395) asks of a theme writer. */
[Collection("viewer-time-statics")]
public sealed class ViewerFinOpsObjectHeatmapGapDayHoverTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string ServerName = "darling-finops-heatmap-hover";
    private const string DatabaseName = "GapDb";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task PointingAtAGapDay_SaysNoSampleCollected_AndABlankCellOnASampledDaySaysNothing()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live FinOps heatmap hover test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* Noon UTC on five whole days that have ended, so each sample's day is the same whatever the wall clock reads. */
        var fifth = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-1).AddHours(12), DateTimeKind.Unspecified);
        await SeedObjectAsync(connection, "Orders", 1001, fifth.AddDays(-4), 100m, ct);
        await SeedObjectAsync(connection, "Orders", 1001, fifth.AddDays(-3), 110m, ct);
        await SeedObjectAsync(connection, "Orders", 1001, fifth, 150m, ct);
        await SeedObjectAsync(connection, "Lines", 1002, fifth.AddDays(-3), 40m, ct);
        await SeedObjectAsync(connection, "Lines", 1002, fifth, 45m, ct);

        var popups = HoverEveryCell(scratch.ConnectionString, columns: 5, rows: 2);

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
    private static Dictionary<(int Column, int Row), string?> HoverEveryCell(string connectionString, int columns, int rows)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var popups = new Dictionary<(int, int), string?>();
        StaTestThread.Run(async () =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("NumericCell"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }
            await using var dataService = new ViewerDataService(connectionString);
            var tab = new FinOpsTab();
            tab.Initialize(dataService);
            tab.SetServers([new DarlingServer(ServerId, ServerName, ServerName, true, null)], ServerId);
            var outer = new TabControl();
            outer.Items.Add(new TabItem { Header = "FinOps", Content = tab });
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(outer), Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var chart = (WpfPlot)tab.FindName("FinOpsObjectGrowthHeatmapChart");
                for (var node = (DependencyObject)chart; node is not null; node = LogicalTreeHelper.GetParent(node))
                {
                    if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
                    {
                        tabs.SelectedItem = item;
                    }
                }

                /* The drill the tab makes when a database is opened: the object view, then its load over the store. */
                var level = typeof(FinOpsTab).GetNestedType("FinOpsStorageDrillLevel", BindingFlags.NonPublic)!;
                typeof(FinOpsTab).GetMethod("ShowFinOpsStorageView", Instance)!.Invoke(tab, new[] { Enum.Parse(level, "Objects") });
                ChartLayoutProbe.Layout(window);
                await (Task)typeof(FinOpsTab).GetMethod("LoadFinOpsObjectGrowthAsync", Instance)!.Invoke(tab, [DatabaseName])!;
                ChartLayoutProbe.Settle(window, new[] { chart });

                var lastHover = typeof(FinOpsTab).GetField("_finopsLastObjHeatmapHover", Instance)!;
                try
                {
                    for (var column = 0; column < columns; column++)
                    {
                        for (var row = 0; row < rows; row++)
                        {
                            /* The handler answers one move in fifty milliseconds; each cell's move starts from a hover that has aged out. */
                            lastHover.SetValue(tab, default(DateTime));
                            HeatmapHoverProbe.MoveTo(window, chart, HeatmapHoverProbe.CellPoint(chart, column, row));
                            var popup = (Popup?)typeof(FinOpsTab).GetField("_finopsObjHeatmapPopup", Instance)!.GetValue(tab);
                            var text = (TextBlock?)typeof(FinOpsTab).GetField("_finopsObjHeatmapPopupText", Instance)!.GetValue(tab);
                            popups[(column, row)] = popup is { IsOpen: true } ? text?.Text : null;
                        }
                    }
                }
                finally
                {
                    if (typeof(FinOpsTab).GetField("_finopsObjHeatmapPopup", Instance)!.GetValue(tab) is Popup open) open.IsOpen = false;
                    HeatmapHoverProbe.Leave(window);
                }
            }
            finally
            {
                window.Close();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
                ViewerTimeHelper.CurrentDisplayMode = savedMode;
            }
        });
        return popups;
    }

    /// <summary>One object's one index at <paramref name="at"/>: the heatmap's series sums an object's indexes per day.</summary>
    private static Task SeedObjectAsync(NpgsqlConnection connection, string table, int objectId, DateTime at, decimal reservedMb, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, object_id, schema_name, table_name, index_id, index_name, reserved_mb, used_mb, total_rows, row_lock_wait_in_ms, index_lock_promotion_count) VALUES ($1, $2, $3, $4, $5, $6, 'dbo', $7, 1, 'PK', $8, $8, 1000, 0, 0)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, DatabaseName, objectId, table, reservedMb);
}
