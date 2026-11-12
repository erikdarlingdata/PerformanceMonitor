/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7175: the history windows' per-row plan actions name a snapshot. The Download button sits in a cell whose
/// DataContext is its row, so the button's row is the one asked about even when the grid's CurrentItem still points
/// at another row. The context menu has no row of its own and reads the grid's CurrentItem. Runs on an STA thread
/// like the other Viewer grid tests, because the controls are WPF.
/// </summary>
public sealed class HistoryRowPlanTargetTests
{
    private static ViewerQueryStatsHistoryRow Row(string planHash) => new() { QueryPlanHash = planHash };

    private static DataGrid GridOn(ViewerQueryStatsHistoryRow current, params ViewerQueryStatsHistoryRow[] rows)
    {
        var grid = new DataGrid { ItemsSource = new List<ViewerQueryStatsHistoryRow>(rows) };
        grid.CurrentItem = current;
        return grid;
    }

    [Fact]
    public void DownloadButton_NamesItsOwnRow_NotTheGridsCurrentRow()
    {
        StaTestThread.Run(() =>
        {
            var oldRow = Row("0xOLD");
            var newRow = Row("0xNEW");
            var grid = GridOn(newRow, newRow, oldRow);
            var button = new Button { DataContext = oldRow };

            Assert.Same(oldRow, HistoryRowPlanTarget.Resolve<ViewerQueryStatsHistoryRow>(button, grid));
            return true;
        });
    }

    [Fact]
    public void ContextMenuItem_NamesTheGridsCurrentRow()
    {
        StaTestThread.Run(() =>
        {
            var oldRow = Row("0xOLD");
            var newRow = Row("0xNEW");
            var grid = GridOn(oldRow, newRow, oldRow);

            Assert.Same(oldRow, HistoryRowPlanTarget.Resolve<ViewerQueryStatsHistoryRow>(new MenuItem(), grid));
            return true;
        });
    }

    [Fact]
    public void NoRow_ResolvesToNull_SoTheReadFallsBackToTheNewestPlan()
    {
        StaTestThread.Run(() =>
        {
            var grid = new DataGrid();

            Assert.Null(HistoryRowPlanTarget.Resolve<ViewerQueryStatsHistoryRow>(new Button(), grid));
            return true;
        });
    }
}
