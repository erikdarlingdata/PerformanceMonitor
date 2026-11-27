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
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9604: the Store Health window's housekeeping history keeps every column inside the grid. The real window is shown off screen at its
/// default size over a history of three rows: a compaction pass with every part of its summary, an archive pass whose figures are as long
/// as a figure can be, and a failed retention pass with a long message. Each cell's place is read from the rendered grid.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class StoreHealthHistoryLayoutTests
{
    private static readonly DateTime T0 = new(2026, 11, 25, 22, 17, 0, DateTimeKind.Utc);

    /// <summary>A compaction pass that did every part of its work, so its summary has every part.</summary>
    private static readonly CompactionPassDetail FullPass = new(
        T0.AddMinutes(-9), 56.1, GroupsMerged: 12, FilesIn: 2_345, FilesOut: 123, BytesIn: 24_000_000_000, BytesOut: 23_400_000_000,
        SortedAtSmallGroupBatches: 3, UnsortedFallbackBatches: 2, FilesRecut: 40, RecutSkippedLiveJournal: 1, RecutSkippedTimeBudget: 5,
        RecutSkips: new Dictionary<string, int> { [ArchiveService.RecutSkipReason.OverBatchBudget.ToString()] = 1, [ArchiveService.RecutSkipReason.LockTimeout.ToString()] = 2 },
        ViewRebuildMs: 768, ProcessPeakWorkingSetBytes: 1_434_200_000, DuckDbPeakMemoryBytes: 488_800_000);

    [Fact]
    public void EveryRowsInStoreTimesAndMessageCells_StartInsideTheGrid_AtTheDefaultWindowSize()
    {
        var dir = Path.Combine(Path.GetTempPath(), "StoreHealthLayout_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        var report = new List<string>();
        double viewport = 0;
        double windowWidth = 0;
        var defaultWidth = 0.0;
        var rows = 0;
        try
        {
            var dbPath = Path.Combine(dir, "test.duckdb");
            var history = new StoreHousekeepingHistory(StoreHousekeepingHistory.PathFor(dbPath));
            history.Record(new StoreHousekeepingEvent(T0, StoreHousekeepingHistory.KindCompaction, StoreHousekeepingHistory.OutcomeOk, UsedMb: 321, Compaction: FullPass));
            history.Record(new StoreHousekeepingEvent(T0.AddMinutes(-30), StoreHousekeepingHistory.KindArchive, StoreHousekeepingHistory.OutcomeOk,
                Rows: long.MaxValue, Files: int.MaxValue, UsedMb: 320));
            history.Record(new StoreHousekeepingEvent(T0.AddMinutes(-60), StoreHousekeepingHistory.KindRetention, StoreHousekeepingHistory.OutcomeFailed,
                Message: string.Join(" ", Enumerable.Repeat("The process cannot access the file because it is being used by another process.", 6))));
            using var duckDb = new DuckDbInitializer(dbPath);
            var reader = new StoreHealthReader(duckDb, Path.Combine(dir, "archive"), history);

            StaTestThread.Run(async () =>
            {
                var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                ResourceDictionary? added = null;
                if (!app.Resources.Contains("NumericCell"))
                {
                    added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
                    app.Resources.MergedDictionaries.Add(added);
                }

                var window = new StoreHealthWindow(reader);
                try
                {
                    defaultWidth = window.Width;
                    StaTestThread.ShowOffScreen(window);
                    var grid = window.HistoryGrid;
                    Assert.True(await PumpUntilAsync(() => grid.Items.Count == 3 && CellsAreRealized(grid)), "the history rows never reached the grid");
                    window.UpdateLayout();

                    windowWidth = window.ActualWidth;
                    viewport = FindScrollViewer(grid)?.ViewportWidth ?? grid.ActualWidth;
                    rows = grid.Items.Count;
                    foreach (var column in grid.Columns.Where(c => (string)c.Header is "In store (MB)" or "Times" or "Message"))
                    {
                        for (var i = 0; i < grid.Items.Count; i++)
                        {
                            var cell = column.GetCellContent(grid.Items[i]);
                            var place = cell.TransformToAncestor(grid).TransformBounds(new Rect(0, 0, cell.ActualWidth, cell.ActualHeight));
                            report.Add($"{column.Header}|{i}|{place.Left:F0}|{place.Right:F0}");
                        }
                    }
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
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* Best-effort cleanup */ }
        }

        Assert.Equal(3, rows);
        /* The window is laid out at the width it declares, so the figures below are the default-size ones. */
        Assert.True(Math.Abs(windowWidth - defaultWidth) < 1, $"the window is {windowWidth:F0} wide, its default is {defaultWidth:F0}");
        Assert.Equal(9, report.Count);
        foreach (var line in report)
        {
            var part = line.Split('|');
            var left = double.Parse(part[2], System.Globalization.CultureInfo.InvariantCulture);
            var right = double.Parse(part[3], System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(left < viewport && right <= viewport + 1,
                $"the {part[0]} cell of row {part[1]} spans {left:F0} to {right:F0} in a grid whose visible width is {viewport:F0}");
            if (part[0] == "Message")
            {
                Assert.True(right - left >= 150, $"the Message cell of row {part[1]} is {right - left:F0} wide, too narrow to read");
            }
        }
    }

    private static bool CellsAreRealized(DataGrid grid) =>
        grid.Columns.Count > 0 && grid.Items.Cast<object>().All(item => grid.Columns.All(c => c.GetCellContent(item) is { ActualWidth: > 0 }));

    private static ScrollViewer? FindScrollViewer(DependencyObject root)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is ScrollViewer viewer)
            {
                return viewer;
            }

            if (FindScrollViewer(child) is { } found)
            {
                return found;
            }
        }

        return null;
    }

    /// <summary>Lets the dispatcher run until <paramref name="condition"/> holds, looking at most <paramref name="polls"/> times, 20 ms apart.
    /// The window's read finishes on the dispatcher, so the wait yields to it instead of blocking the STA thread.</summary>
    private static async Task<bool> PumpUntilAsync(Func<bool> condition, int polls = 500)
    {
        for (var i = 0; i < polls && !condition(); i++)
        {
            await Task.Delay(20);
        }

        return condition();
    }
}
