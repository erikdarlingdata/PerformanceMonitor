/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Manage Servers at its default size shows every column, Last Connected included, with no horizontal scroll bar: Display Name
/// and Server take the room the fixed columns leave instead of fixed widths that push the last column past the edge.
/// </summary>
[Collection("credential-manager")]
public sealed class ManageServersWindowFitTests
{
    [Fact]
    public void AtTheDefaultSize_EveryColumnIsInsideTheGrid_WithNoHorizontalScrollBar()
    {
        var dir = DialogWalkTestHost.ScratchDir("ManageServersFit");
        try
        {
            var (scrollBar, lastColumnRight, gridWidth, columns) = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
            {
                var servers = new ServerManager(dir);
                servers.AddServer(new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "sql-prod-01", DisplayName = "Production", IsEnabled = true });
                servers.AddServer(new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "sql-reporting-02.corp.example", DisplayName = "Reporting replica", IsEnabled = true });
                var window = new ManageServersWindow(servers, new ProfileManager(servers), _ => Task.CompletedTask);
                DialogWalkTestHost.Show(window);
                var grid = (DataGrid)window.FindName("ServersGrid");
                var scroll = ClippedTextProbe.Descendants(grid).OfType<ScrollViewer>().First();
                var last = ClippedTextProbe.Descendants(grid).OfType<DataGridColumnHeader>().Single(h => h.Content as string == "Last Connected");
                var widths = string.Join(", ", grid.Columns.Select(c => $"{c.Header}={c.ActualWidth:0}"));
                var result = (scroll.ComputedHorizontalScrollBarVisibility, Math.Round(DialogWalkTestHost.RightIn(last, grid), 1), Math.Round(grid.ActualWidth, 1), widths);
                window.Close();
                return result;
            }));

            Assert.True(scrollBar == Visibility.Collapsed, $"the grid needs a horizontal scroll bar at its default size (Last Connected ends at {lastColumnRight} in a {gridWidth} grid; columns: {columns})");
            Assert.True(lastColumnRight <= gridWidth + 0.5, $"Last Connected ends at {lastColumnRight}, past the grid's {gridWidth}");
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }
}
