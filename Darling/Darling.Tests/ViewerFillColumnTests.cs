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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Markup;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7383: a grid with a fill (<c>Width="*"</c>) column took width back from its fixed columns once they did not fit, down to 20 px, so the
/// PostgreSQL Vacuum tab's PID column read "Bacl" and "Us" and there was no horizontal scrollbar. These lay real grids out on the shared
/// STA thread, narrower and wider than their columns, and read back the width each column was actually given. Lite's twin, for Lite's
/// themes, is FillColumnTests in Lite.Tests.
/// </summary>
public sealed class ViewerFillColumnTests
{
    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "Darling", "PerformanceMonitor.Darling.Viewer"));

    private static ResourceDictionary Theme(string name = "DarkTheme") =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(ViewerDir(), "Themes", name + ".xaml")));

    private static ScrollViewer ScrollOf(DataGrid grid) => (ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid);

    /// <summary>What a layout pass gave a grid: the width each column declared, the width it got, and how far the grid scrolls sideways.</summary>
    private sealed record Laid(List<(string Header, double Declared, double Actual, bool Fill)> Columns, double Scrollable, double Viewport, double Total)
    {
        public override string ToString() =>
            $"viewport {Viewport}, scrolls {Scrollable}, columns total {Total}: " + string.Join(", ", Columns.Take(6).Select(c => $"{c.Header} {c.Declared}->{c.Actual}")) + ", fill " + string.Join("", Columns.Where(c => c.Fill).Select(c => c.Actual));
    }

    private static Laid Read(DataGrid grid, Dictionary<DataGridColumn, (double Declared, bool Fill)> declared)
    {
        var scroll = ScrollOf(grid);
        var columns = grid.Columns.Select(c => (c.Header?.ToString() ?? "", declared[c].Declared, c.ActualWidth, declared[c].Fill)).ToList();
        return new Laid(columns, scroll.ScrollableWidth, scroll.ViewportWidth, grid.Columns.Sum(c => c.ActualWidth));
    }

    private static Dictionary<DataGridColumn, (double Declared, bool Fill)> Declared(DataGrid grid) =>
        grid.Columns.ToDictionary(c => c, c => (c.Width.IsStar ? 0 : c.Width.Value, c.Width.IsStar));

    private static void AssertFixedColumnsKeepTheirWidth(Laid laid)
    {
        /* #8197: a column may grow to its header's width (DataGridHeaderFit), never shrink below what it was given. */
        var squeezed = laid.Columns.Where(c => !c.Fill && c.Declared > 0 && c.Actual < c.Declared - 0.5)
            .Select(c => $"{c.Header} declared {c.Declared} got {c.Actual}").ToList();
        Assert.True(squeezed.Count == 0, "fixed columns lost width: " + string.Join("; ", squeezed));
    }

    /// <summary>Builds the PostgreSQL server tab at <paramref name="width"/>, opens the inner tab <paramref name="tabName"/>, and hands the body its grid <paramref name="gridName"/> with its declared widths.</summary>
    private static T OnServerGrid<T>(double width, string tabName, string gridName, Func<ViewerServerTab, Window, DataGrid, Dictionary<DataGridColumn, (double Declared, bool Fill)>, T> body) =>
        StaTestThread.Run(() =>
        {
            /* The tab's XAML resolves its cell styles from the application's merged theme (see ViewerPostgresQueryShapesGridRenderTests). */
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
                    new DarlingServer(1, "pg-fill-test", "pg-fill-test", true, null, engineKind: "postgres"));
                var grid = (DataGrid)tab.FindName(gridName);
                var declared = Declared(grid);
                /* A grid with no rows has no content to scroll over, so it never scrolls sideways; one empty row stands in for the data. */
                grid.ItemsSource = new[] { new object() };
                var window = new Window { Content = tab, Width = width, Height = 1000 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                    var inner = (TabItem)tab.FindName(tabName);
                    inner.Visibility = Visibility.Visible;
                    StaTestThread.Reveal(inner);
                    ClippedTextProbe.Settle(window);
                    return body(tab, window, grid, declared);
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

    [Fact]
    public void TheVacuumSessionGrid_KeepsEveryFixedColumnAtItsWidth_AndScrollsSideways_WhenTheWindowIsNarrowerThanItsColumns()
    {
        var laid = OnServerGrid(1400, "PgVacuumTab", "PgSessionStatesGrid", (_, _, grid, declared) => Read(grid, declared));

        Assert.True(laid.Columns.Sum(c => c.Declared) > 2500, "the grid should declare far more than the window holds");
        AssertFixedColumnsKeepTheirWidth(laid);
        Assert.True(laid.Columns.Single(c => c.Fill).Actual >= DataGridFillColumn.MinFillWidth, "the fill column keeps its floor");
        Assert.True(laid.Scrollable > 1000, $"the grid should scroll sideways: {laid}");
    }

    /// <summary>A plain grid of twelve 100 px columns and a fill column, in a canvas <paramref name="width"/> wide, in a window themed as the app themes it.</summary>
    private static T OnPlainGrid<T>(string theme, double width, Func<Window, DataGrid, Dictionary<DataGridColumn, (double Declared, bool Fill)>, T> body) =>
        StaTestThread.Run(() =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false, ItemsSource = new[] { new object() } };
            for (var i = 0; i < 12; i++)
            {
                grid.Columns.Add(new DataGridTextColumn { Header = "Col" + i, Width = 100 });
            }

            grid.Columns.Add(new DataGridTextColumn { Header = "Fill", Width = new DataGridLength(1, DataGridLengthUnitType.Star) });
            var declared = Declared(grid);
            /* The grid is given its width by the canvas, not by the window: Windows clamps a window to the screen, and a small CI screen made a "wide" 1500 px window about 1000 px. */
            grid.Width = width;
            grid.Height = 300;
            var window = new Window { Content = new Canvas { Children = { grid } }, Width = 600, Height = 400 };
            window.Resources.MergedDictionaries.Add(Theme(theme));
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                return body(window, grid, declared);
            }
            finally
            {
                window.Close();
            }
        });

    /// <summary>
    /// #8101: the newest error and its time are the two columns after Status, and at 1400 wide both are on screen, so a HEALTHY
    /// row with Errors 1 says what the error was and when without a sideways scroll.
    /// </summary>
    [Fact]
    public void TheCollectionHealthGrid_ShowsLastErrorAtAndLastError_RightAfterStatus_WithoutScrolling()
    {
        var (keys, rightEdges, viewport) = OnServerGrid(1400, "CollectionHealthTab", "CollectionHealthGrid", (_, _, grid, _) =>
        {
            var ordered = grid.Columns.OrderBy(c => c.DisplayIndex).ToList();
            var edges = new List<double>();
            var right = 0.0;
            foreach (var column in ordered)
            {
                right += column.ActualWidth;
                edges.Add(right);
            }

            return (ordered.Select(ColumnKey).ToList(), edges, ScrollOf(grid).ViewportWidth);
        });

        var status = keys.IndexOf("DisplayStatus");
        Assert.True(status >= 0, "the grid has a Status column: " + string.Join(", ", keys));
        Assert.Equal("LastErrorTime", keys[status + 1]);
        Assert.Equal("LastError", keys[status + 2]);
        Assert.True(rightEdges[status + 2] <= viewport, $"Last Error ends at {rightEdges[status + 2]} in a viewport of {viewport}");
    }

    /// <summary>What a column shows: its sort member, else the path its cells bind to, else its header.</summary>
    private static string ColumnKey(DataGridColumn column) =>
        !string.IsNullOrEmpty(column.SortMemberPath) ? column.SortMemberPath
        : column is DataGridBoundColumn { Binding: System.Windows.Data.Binding binding } ? binding.Path.Path
        : column.Header?.ToString() ?? "";

    /// <summary>The grid whose one fill column, Note, has its own MinWidth: it keeps that floor and its fixed columns keep their width, the 320-wide Last Error column beside Status among them.</summary>
    [Fact]
    public void TheCollectionHealthGrid_KeepsItsFixedColumns_AndGivesItsFillColumnAtLeastItsMinWidth()
    {
        var laid = OnServerGrid(1400, "CollectionHealthTab", "CollectionHealthGrid", (_, _, grid, declared) => Read(grid, declared));

        AssertFixedColumnsKeepTheirWidth(laid);
        var fill = Assert.Single(laid.Columns, c => c.Fill);
        Assert.True(fill.Actual >= 160, $"the fill column fell under its MinWidth: {laid}");
        Assert.True(laid.Scrollable > 0, $"the grid should scroll sideways: {laid}");
    }

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void AnyGridInAThemedWindow_KeepsItsFixedColumns_AndScrollsSideways_WhenItsColumnsDoNotFit(string theme)
    {
        var laid = OnPlainGrid(theme, 700, (_, grid, declared) => Read(grid, declared));

        AssertFixedColumnsKeepTheirWidth(laid);
        Assert.True(laid.Scrollable > 500, $"the grid should scroll sideways: {laid}");
    }

    [Fact]
    public void AFillColumn_TakesTheRoomTheFixedColumnsLeave_AndAColumnDraggedNarrowerGivesItsWidthToTheFillColumn()
    {
        var (wide, dragged, pidWidth) = OnPlainGrid("DarkTheme", 1500, (window, grid, declared) =>
        {
            var wide = Read(grid, declared);
            var first = grid.Columns[0];
            /* #8197: the header floor keeps a drag from going below the header; this test is about the fill column taking the difference, so it lifts the floor. */
            first.MinWidth = 20;
            first.Width = new DataGridLength(40);
            grid.RaiseEvent(new DragCompletedEventArgs(-60, 0, false) { RoutedEvent = Thumb.DragCompletedEvent, Source = grid });
            ClippedTextProbe.Settle(window);
            return (wide, Read(grid, declared), first.ActualWidth);
        });

        AssertFixedColumnsKeepTheirWidth(wide);
        Assert.True(wide.Columns.Single(c => c.Fill).Actual > 200, $"the fill column should take the extra room: {wide}");
        Assert.True(wide.Scrollable < 1, $"nothing to scroll when the columns fit: {wide}");
        Assert.True(Math.Abs(wide.Total - wide.Viewport) < 2, $"the columns should fill the viewport: {wide}");
        Assert.Equal(40, pidWidth, 0.5);
        Assert.Equal(wide.Columns.Single(c => c.Fill).Actual + (wide.Columns[0].Actual - 40), dragged.Columns.Single(c => c.Fill).Actual, 1.5);
        Assert.True(Math.Abs(dragged.Total - dragged.Viewport) < 2, $"the columns should still fill the viewport: {dragged}");
    }
}
