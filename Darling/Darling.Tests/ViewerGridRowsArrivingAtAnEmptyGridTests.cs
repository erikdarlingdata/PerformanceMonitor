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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8534: the Viewer's grids draw only the rows in view when rows arrive at an empty grid, as Lite's do (GridRowsArrivingAtAnEmptyGridTests in
/// Lite.Tests is the twin). The two apps share the helper that lets an empty grid wider than its viewport scroll by pixel (#8223) and puts it back
/// to scrolling by row when rows arrive; put back too late, the rows presenter is given unbounded height and builds a container for every row.
/// Each test shows a real grid empty on the shared STA thread, hands it rows the way a load does, lays it out once and counts the row
/// containers built.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerGridRowsArrivingAtAnEmptyGridTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>What a grid built for the rows it was handed.</summary>
    private sealed record Built(int Rows, int Containers, int RowsInView, bool EmptyGridScrolledByPixel, bool ScrollsByRow);

    /// <summary>
    /// Hands <paramref name="grid"/>, empty and shown, <paramref name="rows"/>, lays the window out once and counts. The count is taken
    /// straight after that one layout pass, with nothing pumped in between, so it is what the pass built.
    /// </summary>
    private static Built HandRows(Window window, DataGrid grid, System.Collections.IList rows)
    {
        ClippedTextProbe.Settle(window);
        var scroll = (ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid);
        var emptyScrolledByPixel = grid.Items.Count == 0 && !scroll.CanContentScroll;

        grid.ItemsSource = rows;
        window.UpdateLayout();

        var containers = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>().ToList();
        var shortest = containers.Count == 0 ? 1 : containers.Min(r => r.ActualHeight);
        return new Built(rows.Count, containers.Count, (int)Math.Ceiling(scroll.ActualHeight / Math.Max(1, shortest)), emptyScrolledByPixel, scroll.CanContentScroll);
    }

    /// <summary>
    /// The premise held (the empty grid was wider than its viewport, so it scrolled by pixel), the grid scrolls by row again, and it built no
    /// more row containers than the rows in view and the panel's cache beside them: four screens of rows is far above any cache and far below
    /// every row.
    /// </summary>
    private static void AssertBuiltOnlyTheRowsInView(Built built, string screen)
    {
        Assert.True(built.EmptyGridScrolledByPixel, $"{screen}: the empty grid should be wider than its viewport and scroll by pixel, or this test hands rows to a grid that never left scrolling by row");
        Assert.True(built.ScrollsByRow, $"{screen}: the grid scrolls by row once it has rows");
        Assert.True(built.Containers > 0, $"{screen}: the grid built no row at all");
        Assert.True(built.Containers <= 4 * built.RowsInView + 4,
            $"{screen}: {built.Rows} rows arrived at the empty grid and it built {built.Containers} row containers, where about {built.RowsInView} rows are in view");
    }

    private static Uri ThemeUri(string theme) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml");

    public static IEnumerable<object[]> Themes() =>
        new[] { "DarkTheme", "LightTheme", "CoolBreezeTheme" }.Select(t => new object[] { t });

    [Theory]
    [MemberData(nameof(Themes))]
    public void AThemedGrid_BuildsOnlyTheRowsInView_WhenRowsArriveAtItEmpty(string theme)
    {
        var built = StaTestThread.Run(() =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false, IsReadOnly = true, Width = 700, Height = 300 };
            for (var i = 0; i < 12; i++)
            {
                grid.Columns.Add(new DataGridTextColumn { Header = "Column " + i, Binding = new System.Windows.Data.Binding("."), Width = 130 });
            }

            var window = new Window { Content = new Canvas { Children = { grid } }, Width = 800, Height = 400 };
            window.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });
            grid.Style = (Style)window.FindResource(typeof(DataGrid));
            StaTestThread.ShowOffScreen(window);
            try
            {
                return HandRows(window, grid, Enumerable.Range(0, 200).Select(i => "row " + i).ToList());
            }
            finally
            {
                window.Close();
            }
        });

        AssertBuiltOnlyTheRowsInView(built, $"a {theme} grid");
    }

    [Fact]
    public void BlockedProcessReports_BuildsOnlyTheRowsInView_WhenReportsArriveAtTheEmptyGrid()
    {
        var rows = Enumerable.Range(0, 200).Select(i => new ViewerBlockedProcessRow
        {
            CollectionTime = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-i),
            EventTime = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-i),
            DatabaseName = "db" + (i % 7),
            BlockedSpid = 50 + i,
            BlockingSpid = 100 + (i % 11),
            WaitTimeMs = 5000 + i,
            LockMode = "U",
            BlockedSqlText = "UPDATE dbo.orders SET status = @p0 WHERE order_id = " + i,
            BlockingSqlText = "SELECT o.* FROM dbo.orders AS o WHERE o.customer_id = " + i,
        }).ToList();

        AssertBuiltOnlyTheRowsInView(OnTheServerTab("BlockedProcessReportGrid", rows), "Blocking > Blocked Process Reports at 900x600");
    }

    [Fact]
    public void MemoryBroker_BuildsOnlyTheRowsInView_WhenNotificationsArriveAtTheEmptyGrid()
    {
        var rows = Enumerable.Range(0, 1500).Select(i => new MemoryBrokerRow(new MemoryBrokerRecord
        {
            EventTime = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-i),
            BrokerId = i % 4096,
            Broker = "MEMORYBROKER_FOR_CACHE",
            Notification = "SHRINK",
            PoolMetadataId = 2,
            DeltaTime = 10,
            MemoryRatio = 80,
            NewTarget = 4000000 + i,
            Overall = 12000000,
            Rate = -100,
            CurrentlyPredicated = 3000000,
            CurrentlyAllocated = 3100000,
            PreviouslyAllocated = 3050000,
        })).ToList();

        AssertBuiltOnlyTheRowsInView(OnTheServerTab("MemoryBrokerGrid", rows), "System Events > Memory Broker at 900x600");
    }

    [Fact]
    public void JobHistory_BuildsOnlyTheRowsInView_WhenRunsArriveAtTheEmptyGrid()
    {
        var rows = Enumerable.Range(0, 2000).Select(i => new ViewerJobHistoryRow
        {
            ServerId = 1 + (i % 5),
            ServerName = "SERVER" + (i % 5),
            InstanceId = i,
            JobName = "DatabaseBackup - USER_DATABASES - LOG",
            CategoryName = "Database Maintenance",
            StepId = i % 2,
            StepName = "(Job outcome)",
            RunStatus = 1,
            RunStatusDesc = "Succeeded",
            RunDateTimeUtc = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-30 * i),
            RunDurationSeconds = i % 400,
            Message = "The job succeeded.  The Job was invoked by Schedule 12 (every few minutes).  The last step to run was step 1 (Run the procedure).",
        }).ToList();

        var built = RunThemed(() =>
        {
            var tab = new JobHistoryTab();
            var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                return HandRows(window, (DataGrid)tab.FindName("JobHistoryDataGrid"), rows);
            }
            finally
            {
                window.Close();
            }
        });

        AssertBuiltOnlyTheRowsInView(built, "Job History at 900x600");
    }

    /// <summary>The named grid of a real server tab in a 900x600 window (the Viewer's minimum), shown empty on its own sub-tab and then handed <paramref name="rows"/>.</summary>
    private static Built OnTheServerTab(string gridName, System.Collections.IList rows) => RunThemed(() =>
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "rows-arrive-test", "rows-arrive-test", true, null));
        /* Switching to a tab would start that tab's load against a store that is not there. */
        typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", Instance)!.SetValue(tab, true);
        var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
        StaTestThread.ShowOffScreen(window);
        try
        {
            var grid = (DataGrid)tab.FindName(gridName);
            StaTestThread.Reveal(grid);
            return HandRows(window, grid, rows);
        }
        finally
        {
            window.Close();
        }
    });

    /// <summary>Runs the body on the shared STA thread with the Viewer's Dark theme merged at application level, as App.xaml supplies it (the tabs' static resources need it), and takes it back out.</summary>
    private static T RunThemed<T>(Func<T> body) => StaTestThread.Run(() =>
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var theme = new ResourceDictionary { Source = ThemeUri("DarkTheme") };
        app.Resources.MergedDictionaries.Add(theme);
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Remove(theme);
        }
    });
}
