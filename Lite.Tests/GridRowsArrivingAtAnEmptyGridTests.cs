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
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8534: rows that arrive at an empty grid are drawn as any other rows are, only the ones in view. A grid that is empty and wider than its
/// viewport scrolls by pixel, so its last column can be reached (#8223), and scrolls by row again when rows arrive. It has to do that before
/// the rows are laid out: a rows presenter with no scroll owner is given unbounded height and builds a container for every row, 200 blocked
/// process reports, 1,500 broker notifications or 2,000 job runs where a dozen are in view, and the screen takes seconds to draw. Each test
/// shows a real grid empty on the shared STA thread, hands it rows the way a load does, lays it out once and counts the row containers built.
/// </summary>
[Collection("credential-manager")]
public sealed class GridRowsArrivingAtAnEmptyGridTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>What a grid built for the rows it was handed.</summary>
    private sealed record Built(int Rows, int Containers, int RowsInView, bool EmptyGridScrolledByPixel, bool ScrollsByRow, bool UnloadedWhenTheRowsArrived = false);

    /// <summary>
    /// Hands <paramref name="grid"/>, empty and shown, <paramref name="rows"/>, lays the window out once and counts. The count is taken
    /// straight after that one layout pass, with nothing pumped in between, so it is what the pass built.
    /// </summary>
    private static Built HandRows(Window window, DataGrid grid, System.Collections.IList rows, bool whileItsTabIsDeselected = false)
    {
        ClippedTextProbe.Settle(window);
        var scroll = (ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid);
        var emptyScrolledByPixel = grid.Items.Count == 0 && !scroll.CanContentScroll;
        var unloaded = false;

        if (whileItsTabIsDeselected)
        {
            /* A reader who leaves the tab before its read lands: the rows arrive at a grid that is not loaded, and the reader comes back. */
            var (tabs, own) = OwnTab(grid);
            StaTestThread.WithDispatcherContext(() => tabs.SelectedItem = tabs.Items.OfType<TabItem>().First(item => !ReferenceEquals(item, own)));
            ClippedTextProbe.Settle(window);
            unloaded = !grid.IsLoaded;
            grid.ItemsSource = rows;
            StaTestThread.WithDispatcherContext(() => tabs.SelectedItem = own);
        }
        else
        {
            grid.ItemsSource = rows;
        }

        window.UpdateLayout();

        var containers = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>().ToList();
        var shortest = containers.Count == 0 ? 1 : containers.Min(r => r.ActualHeight);
        return new Built(rows.Count, containers.Count, (int)Math.Ceiling(scroll.ActualHeight / Math.Max(1, shortest)), emptyScrolledByPixel, scroll.CanContentScroll, unloaded);
    }

    /// <summary>The tab <paramref name="element"/> sits on, and the tab control that tab belongs to.</summary>
    private static (TabControl Tabs, TabItem Own) OwnTab(DependencyObject element)
    {
        for (var at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
            {
                return (owner, item);
            }
        }

        throw new Xunit.Sdk.XunitException("the grid is on no tab");
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
            window.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
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
        var rows = Enumerable.Range(0, 200).Select(i => new BlockedProcessReportRow
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

    /// <summary>
    /// The reader leaves Blocked Process Reports before its read lands and comes back: the reports arrive while the sub-tab is deselected,
    /// so the grid is not loaded when it gets them. It still builds only the rows in view when it is shown again.
    /// </summary>
    [Fact]
    public void BlockedProcessReports_BuildsOnlyTheRowsInView_WhenReportsArriveWhileItsSubTabIsDeselected()
    {
        var rows = Enumerable.Range(0, 200).Select(i => new BlockedProcessReportRow
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

        var built = OnTheServerTab("BlockedProcessReportGrid", rows, whileItsTabIsDeselected: true);

        Assert.True(built.UnloadedWhenTheRowsArrived, "the grid of a deselected sub-tab should not be loaded, or this test hands rows to a grid that is on screen");
        AssertBuiltOnlyTheRowsInView(built, "Blocking > Blocked Process Reports at 900x600, filled while another sub-tab was selected");
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
        var rows = Enumerable.Range(0, 2000).Select(i => new JobHistoryRow
        {
            ServerId = 1000 + (i % 5),
            ServerName = "SERVER" + (i % 5),
            InstanceId = i,
            JobName = "DatabaseBackup - USER_DATABASES - LOG",
            CategoryName = "Database Maintenance",
            StepId = i % 2,
            StepName = "(Job outcome)",
            RunStatus = 1,
            RunStatusDesc = "Succeeded",
            RunDateTime = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-30 * i),
            RunDateTimeUtc = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-30 * i),
            RunDurationSeconds = i % 400,
            Message = "The job succeeded.  The Job was invoked by Schedule 12 (every few minutes).  The last step to run was step 1 (Run the procedure).",
        }).ToList();

        var built = StaTestThread.Run(() => WithDarkTheme(() =>
        {
            var tab = new JobHistoryTab();
            /* Beside the 40 px sidebar rail of a 900 px wide window. */
            var window = new Window { Content = tab, Width = 860, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                return HandRows(window, (DataGrid)tab.FindName("JobHistoryDataGrid"), rows);
            }
            finally
            {
                window.Close();
            }
        }));

        AssertBuiltOnlyTheRowsInView(built, "Job History beside the sidebar of a 900x600 window");
    }

    /// <summary>
    /// Top Queries by Duration holds the read's top 50 queries in 47 columns, wider than any window, so its rows always arrive at a grid
    /// that scrolls by pixel. It builds the rows in view, not all 50 of 47 cells each.
    /// </summary>
    [Fact]
    public void TopQueriesByDuration_BuildsOnlyTheRowsInView_WhenQueriesArriveAtTheEmptyGrid()
    {
        var rows = Enumerable.Range(0, 50).Select(i => new QueryStatsRow
        {
            DatabaseName = "db" + (i % 7),
            QueryHash = "0x" + (0x1A2B3C4D5E6F7000L + i).ToString("X16"),
            QueryText = "SELECT o.order_id, o.status FROM dbo.orders AS o WHERE o.customer_id = " + i,
            TotalExecutions = 1000 + i,
            TotalCpuUs = 5_000_000L + i,
            TotalElapsedUs = 9_000_000L + i,
            TotalLogicalReads = 120_000 + i,
            LastExecutionTime = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-i),
        }).ToList();

        var built = OnTheServerTab("QueryStatsGrid", rows);

        AssertBuiltOnlyTheRowsInView(built, "Queries > Top Queries by Duration at 900x600");
        Assert.True(built.Containers < built.Rows, $"Top Queries by Duration built a row container for each of its {built.Rows} rows, where about {built.RowsInView} are in view");
    }

    /// <summary>Query Store by Duration holds the read's top 50 queries in 57 columns. It builds the rows in view, not all 50 of 57 cells each.</summary>
    [Fact]
    public void QueryStoreByDuration_BuildsOnlyTheRowsInView_WhenQueriesArriveAtTheEmptyGrid()
    {
        var rows = Enumerable.Range(0, 50).Select(i => new QueryStoreRow
        {
            DatabaseName = "db" + (i % 7),
            QueryId = 4000 + i,
            PlanId = 9000 + i,
            QueryHash = "0x" + (0x1A2B3C4D5E6F7000L + i).ToString("X16"),
            QueryText = "SELECT o.order_id, o.status FROM dbo.orders AS o WHERE o.customer_id = " + i,
            TotalExecutions = 1000 + i,
            AvgDurationMs = 12.5 * (i + 1),
            LastExecutionTime = new DateTime(2026, 11, 10, 12, 0, 0).AddSeconds(-i),
        }).ToList();

        var built = OnTheServerTab("QueryStoreGrid", rows);

        AssertBuiltOnlyTheRowsInView(built, "Queries > Query Store by Duration at 900x600");
        Assert.True(built.Containers < built.Rows, $"Query Store by Duration built a row container for each of its {built.Rows} rows, where about {built.RowsInView} are in view");
    }

    /// <summary>The named grid of a real server tab in a 900x600 window, shown empty on its own sub-tab and then handed <paramref name="rows"/>.</summary>
    private static Built OnTheServerTab(string gridName, System.Collections.IList rows, bool whileItsTabIsDeselected = false)
    {
        var dir = Path.Combine(Path.GetTempPath(), "GridRowsArrive_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithDarkTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "rows-arrive-test", DisplayName = "rows-arrive-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var grid = (DataGrid)tab.FindName(gridName);
                    StaTestThread.Reveal(grid);
                    return HandRows(window, grid, rows, whileItsTabIsDeselected);
                }
                finally
                {
                    window.Close();
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static T WithDarkTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
