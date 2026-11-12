/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A refresh hands the grid a new list of row objects, and the selected row is one of the old ones. The grid manager finds the new row
/// that shows the same value in every bound column and selects it again, so a refresh of Active Queries keeps the row the user is
/// reading. A row the new list no longer holds leaves the grid with no selection.
/// </summary>
public sealed class DataGridFilterManagerSelectionTests
{
    private sealed class Row
    {
        public int SessionId { get; set; }
        public string QueryText { get; set; } = "";
        public long CpuMs { get; set; }
    }

    private static DataGrid NewGrid()
    {
        var grid = new DataGrid { AutoGenerateColumns = false, Name = "QuerySnapshotsGrid" };
        foreach (var path in new[] { "SessionId", "QueryText", "CpuMs" })
            grid.Columns.Add(new DataGridTextColumn { Header = path, Binding = new Binding(path) });
        return grid;
    }

    private static List<Row> Rows(params (int Session, string Text, long Cpu)[] rows) =>
        rows.Select(r => new Row { SessionId = r.Session, QueryText = r.Text, CpuMs = r.Cpu }).ToList();

    [Fact]
    public void UpdateData_SelectsTheNewRowThatShowsTheSameValues()
    {
        var selected = StaTestThread.Run(() =>
        {
            var grid = NewGrid();
            var manager = new DataGridFilterManager<Row>(grid);
            manager.UpdateData(Rows((51, "SELECT 1", 10), (52, "SELECT 2", 20), (53, "SELECT 3", 30)));
            grid.SelectedItem = grid.Items[1];

            var refreshed = Rows((54, "SELECT 4", 40), (52, "SELECT 2", 20), (53, "SELECT 3", 30));
            manager.UpdateData(refreshed);

            return (Expected: refreshed[1], Actual: grid.SelectedItem as Row);
        });

        Assert.Same(selected.Expected, selected.Actual);
    }

    [Fact]
    public void UpdateData_LeavesNoSelection_WhenTheRowIsGone_OrShowsOtherValues()
    {
        var (gone, changed) = StaTestThread.Run(() =>
        {
            var grid = NewGrid();
            var manager = new DataGridFilterManager<Row>(grid);
            manager.UpdateData(Rows((51, "SELECT 1", 10), (52, "SELECT 2", 20)));
            grid.SelectedItem = grid.Items[1];
            manager.UpdateData(Rows((51, "SELECT 1", 10)));
            var afterGone = grid.SelectedItem;

            manager.UpdateData(Rows((51, "SELECT 1", 10), (52, "SELECT 2", 20)));
            grid.SelectedItem = grid.Items[1];
            manager.UpdateData(Rows((51, "SELECT 1", 10), (52, "SELECT 2", 25)));
            return (afterGone, grid.SelectedItem);
        });

        Assert.Null(gone);
        Assert.Null(changed);
    }
}
