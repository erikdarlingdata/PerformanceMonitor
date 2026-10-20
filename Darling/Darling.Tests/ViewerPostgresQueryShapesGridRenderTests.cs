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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using System.Windows.Media;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6225: the PostgreSQL Activity tab's Query Shapes sub-tab. The real server tab is built on the shared STA thread, themed as
/// the app themes it, laid out in a window, and the text its grid cells actually show is read back from the visual tree, so
/// these fail on a grid whose rows exist but whose cells are blank, and on a grid that prints a raw double.
/// </summary>
public sealed class ViewerPostgresQueryShapesGridRenderTests
{
    private static string ViewerDir([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "") =>
        System.IO.Path.GetFullPath(System.IO.Path.Combine(System.IO.Path.GetDirectoryName(thisFile)!, "..", "..", "Darling", "PerformanceMonitor.Darling.Viewer"));

    private static ResourceDictionary Theme() =>
        (ResourceDictionary)XamlReader.Parse(System.IO.File.ReadAllText(System.IO.Path.Combine(ViewerDir(), "Themes", "DarkTheme.xaml")));

    /// <summary>Builds the PostgreSQL server tab, opens Activity then Query Shapes, hands the body the grid, and lays the window out.</summary>
    private static T OnQueryShapes<T>(Action<ViewerServerTab> fill, Func<ViewerServerTab, T> read, int subTab = 2) =>
        StaTestThread.Run(() =>
        {
            /* The tab's XAML resolves NumericCell and the rest from the application's merged theme (see ViewerPostgresGridNumericSortTests.OnTab);
               the dictionary this body added is taken back out when it ends (#6175). */
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

            try
            {
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "pg-shapes-test", "pg-shapes-test", true, null, engineKind: "postgres"));
                var window = new Window { Content = tab, Width = 1800, Height = 1000 };
                window.Show();
                try
                {
                    /* Switching tabs starts the tab's own database read on a thread-pool continuation; the viewer's own drill-down skips it the same way. */
                    var suppress = typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!;
                    suppress.SetValue(tab, true);
                    ((TabControl)tab.FindName("InnerTabs")).SelectedItem = tab.FindName("PgActivityTab");
                    ((TabControl)tab.FindName("PgActivitySubTabs")).SelectedIndex = subTab;
                    fill(tab);
                    ClippedTextProbe.Settle(window);
                    return read(tab);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                if (added is not null)
                {
                    app.Resources.MergedDictionaries.Remove(added);
                }
            }
        });

    /// <summary>The text each realised cell of the grid's first row shows, by column header.</summary>
    private static Dictionary<string, string> FirstRowCells(DataGrid grid)
    {
        var row = ClippedTextProbe.Descendants(grid).OfType<System.Windows.Controls.DataGridRow>().First();
        var cells = ClippedTextProbe.Descendants(row).OfType<DataGridCell>().ToList();
        var shown = new Dictionary<string, string>();
        foreach (var cell in cells)
        {
            var header = cell.Column.Header?.ToString() ?? "";
            var blocks = ClippedTextProbe.Descendants(cell).OfType<TextBlock>().Select(b => b.Text).ToList();
            shown[header] = string.Join("", blocks);
        }

        return shown;
    }

    private static DarlingPgStatementReader.PgStatementRow Statement(long queryId, string? text) =>
        new(queryId, 16384, 100, 5000, 250, 91.54, 10, 5, null, null, 4, 2, 8192, null, text);

    /// <summary>A statement as pg_stat_statements keeps it: the text as written, with its line breaks.</summary>
    private static string LongMultiLineStatement() =>
        string.Join(Environment.NewLine, new[] { "SELECT o.id," }.Concat(Enumerable.Range(0, 300).Select(i => $"       o.col_{i},")).Concat(new[] { "       o.last", "FROM orders o" }));

    [Fact]
    public void TheStatementGrid_ShowsTheValuesOfItsRows_InEveryCell()
    {
        var shown = OnQueryShapes(
            tab => tab.PgTopQueriesGrid.ItemsSource = new[] { PgDisplay.Statement(Statement(7_000_000_000_000_000_001L, "select 1")) },
            tab => FirstRowCells(tab.PgTopQueriesGrid));

        Assert.Equal("7000000000000000001", shown["Query ID"]);
        Assert.Equal("100", shown["Calls"]);
        Assert.Equal("5,000", shown["Total (ms)"]);
        Assert.Equal("select 1", shown["Statement"]);
    }

    /// <summary>
    /// A statement kept with its line breaks used to make its row as tall as the statement. Every other cell is centred in its row, so the
    /// numbers sat far below the visible part of the grid and the row read as blank, while the scrollbar showed rows were there (#6225).
    /// </summary>
    [Fact]
    public void TheStatementGrid_KeepsEveryValueInView_WhenAStatementHasHundredsOfLines()
    {
        var (rowHeight, gridHeight, callsBottom) = OnQueryShapes(
            tab => tab.PgTopQueriesGrid.ItemsSource = new[]
            {
                PgDisplay.Statement(Statement(7_000_000_000_000_000_001L, LongMultiLineStatement())),
                PgDisplay.Statement(Statement(7_000_000_000_000_000_002L, "select 2")),
            },
            tab =>
            {
                var grid = tab.PgTopQueriesGrid;
                var row = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>().First();
                var calls = ClippedTextProbe.Descendants(row).OfType<DataGridCell>().Single(c => c.Column.Header?.ToString() == "Calls");
                var block = ClippedTextProbe.Descendants(calls).OfType<TextBlock>().Single();
                return (row.ActualHeight, grid.ActualHeight, block.TranslatePoint(new Point(0, block.ActualHeight), grid).Y);
            });

        Assert.True(gridHeight > 100, $"the grid was not laid out ({gridHeight}).");
        Assert.True(callsBottom <= gridHeight, $"the first row is {rowHeight:0} px tall in a {gridHeight:0} px grid and its Calls value sits at {callsBottom:0} px, below what the grid shows.");
    }

    [Fact]
    public void TheOsCpuGrid_ShowsMillisecondsRounded_AndDiskBytesAsSizes()
    {
        var shown = OnQueryShapes(
            tab => tab.PgKernelStatsGrid.ItemsSource = new[]
            {
                PgDisplay.KernelStat(new DarlingPgKernelStatsReader.PgKernelStatRow("appdb", 7_000_000_000_000_000_001L, 130.67499999999, 97.628999999999, 33.045999999991, 0, 3 * 1024 * 1024, 2, false, DateTime.UtcNow)),
            },
            tab => FirstRowCells(tab.PgKernelStatsGrid));

        Assert.Equal("130.7", shown["CPU (ms)"]);
        Assert.Equal("97.6", shown["User (ms)"]);
        Assert.Equal("33", shown["System (ms)"]);
        Assert.Equal("0 B", shown["Disk Read"]);
        Assert.Equal("3.0 MB", shown["Disk Write"]);
    }

    [Fact]
    public void TheCapturedPlansGrid_ShowsDurationsRounded()
    {
        var shown = OnQueryShapes(
            tab => tab.PgCapturedPlansGrid.ItemsSource = new[]
            {
                PgDisplay.CapturedPlan(new DarlingPgPlanCaptureReader.PgPlanCaptureRow(7, "h", "Seq Scan", 3, 5, 130.67499999999, 61.250000000001, 26.134999999998, "{}", DateTime.UtcNow)),
            },
            tab => FirstRowCells(tab.PgCapturedPlansGrid));

        Assert.Equal("130.7", shown["Total (ms)"]);
        Assert.Equal("61.3", shown["Max (ms)"]);
        Assert.Equal("26.1", shown["Avg (ms)"]);
    }

    /// <summary>The log-event and deadlock grids hold free text with line breaks too (a message, a detail, a victim statement); their rows stay a few lines tall for the same reason (#6225).</summary>
    [Fact]
    public void TheLogEventsGrid_KeepsEveryValueInView_WhenAMessageHasHundredsOfLines()
    {
        var (rowHeight, gridHeight, pidBottom) = OnQueryShapes(
            tab => tab.PgLogEventsGrid.ItemsSource = new[]
            {
                new DarlingPgLogEventReader.PgLogEventRow(DateTime.UtcNow, "error", "ERROR", "23505", "appdb", "app", "psql", 4242, LongMultiLineStatement(), null, null, 1, default),
            },
            tab =>
            {
                var grid = tab.PgLogEventsGrid;
                var row = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>().First();
                var pid = ClippedTextProbe.Descendants(row).OfType<DataGridCell>().Single(c => c.Column.Header?.ToString() == "PID");
                var block = ClippedTextProbe.Descendants(pid).OfType<TextBlock>().Single();
                return (row.ActualHeight, grid.ActualHeight, block.TranslatePoint(new Point(0, block.ActualHeight), grid).Y);
            },
            subTab: 0);

        Assert.True(gridHeight > 100, $"the grid was not laid out ({gridHeight}).");
        Assert.True(pidBottom <= gridHeight, $"the row is {rowHeight:0} px tall in a {gridHeight:0} px grid and its PID sits at {pidBottom:0} px, below what the grid shows.");
    }
}
