/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7210: the drill handlers (<c>*Grid_MouseDoubleClick</c>) act on <c>SelectedItem</c>, and WPF raises that event for a
/// double-click anywhere in the grid. <see cref="DataGridRowHit"/> is what tells them the click was on a row.
/// </summary>
public sealed class ViewerDataGridRowHitTests
{
    public sealed class Item
    {
        public string Name { get; set; } = "";
    }

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var deeper in Descendants<T>(child))
            {
                yield return deeper;
            }
        }
    }

    [Fact]
    public void OnlyAClickInsideARow_IsOnARow_NotAHeaderTheScrollbarOrTheGridItself()
    {
        var (header, cell, scrollBar, grid, nothing) = StaTestThread.Run(() =>
        {
            var dataGrid = new DataGrid { AutoGenerateColumns = false, IsReadOnly = true };
            dataGrid.Columns.Add(new DataGridTextColumn { Header = "Name", Binding = new Binding("Name") });
            dataGrid.ItemsSource = Enumerable.Range(0, 200).Select(i => new Item { Name = "row " + i }).ToList();
            var window = new Window { Content = dataGrid, Width = 400, Height = 200 };
            StaTestThread.Settle(window);

            var headerElement = Descendants<DataGridColumnHeader>(dataGrid).First();
            var cellText = Descendants<System.Windows.Controls.TextBlock>(dataGrid.ItemContainerGenerator.ContainerFromIndex(0)!).First();
            var bar = Descendants<ScrollBar>(dataGrid).First(b => b.Orientation == Orientation.Vertical);
            var track = Descendants<Track>(bar).First();

            var result = (
                DataGridRowHit.IsOnRow(headerElement),
                DataGridRowHit.IsOnRow(cellText),
                DataGridRowHit.IsOnRow(track),
                DataGridRowHit.IsOnRow(dataGrid),
                DataGridRowHit.IsOnRow(null));
            window.Close();
            return result;
        });

        Assert.False(header, "a double-click on a column header (to auto-size or sort twice) is not a drill");
        Assert.True(cell, "a double-click on a cell is a drill");
        Assert.False(scrollBar, "a double-click on the scrollbar track is not a drill");
        Assert.False(grid, "a double-click on the grid's own empty area is not a drill");
        Assert.False(nothing);
    }
}
