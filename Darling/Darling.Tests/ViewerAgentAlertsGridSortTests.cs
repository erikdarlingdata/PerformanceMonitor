/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7243: the Agent Alerts grid writes "Last fired (server clock)" in the culture's date and time format, where "9:00:00 AM" sorts after
/// "1:00:00 PM" as text. This builds the real tab, takes the column the user clicks and sorts the rows the way the grid's collection view does,
/// so it fails when the column goes back to sorting by its text.
/// </summary>
public sealed class ViewerAgentAlertsGridSortTests
{
    [Fact]
    public void LastFiredColumn_SortsByTheTime_NotByTheFormattedText()
    {
        StaTestThread.Run(() =>
        {
            var tab = new AgentAlertsTab();
            var column = ((DataGrid)tab.FindName("AlertsDataGrid")).Columns.OfType<DataGridTextColumn>()
                .Single(c => c.Header is string header && header.StartsWith("Last fired", StringComparison.Ordinal));
            var morning = new DateTime(2025, 12, 7, 9, 0, 0);
            var afternoon = new DateTime(2025, 12, 7, 13, 0, 0);
            var rows = new[]
            {
                Row("afternoon", "12/7/2025, 1:00:00 PM", afternoon),
                Row("morning", "12/7/2025, 9:00:00 AM", morning),
            };

            var view = new ListCollectionView(rows);
            view.SortDescriptions.Add(new SortDescription(column.SortMemberPath, ListSortDirection.Ascending));

            Assert.Equal(new[] { "morning", "afternoon" }, view.Cast<ViewerAgentAlertRow>().Select(r => r.Name));
        });
    }

    private static ViewerAgentAlertRow Row(string name, string lastFired, DateTime at) =>
        new(name, "yes", "0", "0", "", "0", "yes", "1", lastFired, at);
}
