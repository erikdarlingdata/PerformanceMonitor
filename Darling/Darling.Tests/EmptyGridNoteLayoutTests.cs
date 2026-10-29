/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6907: the Agent Alerts, Database Protection and Identity Capacity tabs draw a grid's empty-state sentence in the same cell as the
/// grid, and it used to start inside the column header row, so the sentence and the header names were drawn over each other and
/// neither could be read. These tests lay the real tab out and read where the sentence and the header row land.
/// </summary>
public sealed class EmptyGridNoteLayoutTests
{
    [Theory]
    [InlineData(typeof(AgentAlertsTab), "GapsDataGrid", "GapsEmptyMessage")]
    [InlineData(typeof(AgentAlertsTab), "AlertsDataGrid", "AlertsEmptyMessage")]
    [InlineData(typeof(DatabaseProtectionTab), "DatabasesDataGrid", "EmptyMessage")]
    [InlineData(typeof(IdentityCapacityTab), "ColumnsDataGrid", "ColumnsEmptyMessage")]
    [InlineData(typeof(IdentityCapacityTab), "ScanDataGrid", "ScanEmptyMessage")]
    public void TheEmptyMessage_StartsBelowTheColumnHeaderRow(System.Type tabType, string gridName, string messageName)
    {
        StaTestThread.Run(() =>
        {
            var tab = (UserControl)System.Activator.CreateInstance(tabType)!;
            var grid = (DataGrid)tab.FindName(gridName);
            var message = (TextBlock)tab.FindName(messageName);
            message.Text = "No data for this server.";
            message.Visibility = Visibility.Visible;

            tab.Measure(new Size(1000, 700));
            tab.Arrange(new Rect(0, 0, 1000, 700));
            tab.UpdateLayout();

            var headers = FindHeadersPresenter(grid);
            Assert.NotNull(headers);
            Assert.True(headers!.ActualHeight > 0, "the column header row did not lay out");

            var headerBottom = headers.TransformToAncestor(tab).Transform(new Point(0, headers.ActualHeight)).Y;
            var messageTop = message.TransformToAncestor(tab).Transform(new Point(0, 0)).Y;
            Assert.True(messageTop >= headerBottom,
                $"{messageName} starts at {messageTop} but the column headers end at {headerBottom}");
        });
    }

    private static DataGridColumnHeadersPresenter? FindHeadersPresenter(DependencyObject root)
    {
        if (root is DataGridColumnHeadersPresenter found)
        {
            return found;
        }

        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            if (FindHeadersPresenter(VisualTreeHelper.GetChild(root, i)) is { } hit)
            {
                return hit;
            }
        }

        return null;
    }
}
