/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7160: Dismiss All counts the rows on screen, but the window write dismisses every alert in the window. The tab sends the
/// window write only while the grid shows the whole window: no column filter set and the read under its row cap.
/// </summary>
public sealed class AlertHistoryDismissAllScopeTests
{
    private static List<AlertHistoryRow> Rows(int count)
    {
        var rows = new List<AlertHistoryRow>();
        for (var i = 0; i < count; i++)
        {
            rows.Add(new AlertHistoryRow { ServerId = 1, ServerName = i < 3 ? "example-sql-01" : "example-sql-02", MetricName = "CPU" });
        }

        return rows;
    }

    [Fact]
    public void ANarrowedGrid_DismissesTheRowsOnScreen_AndAClearedGridGoesBackToTheWindow()
    {
        StaTestThread.Run(() =>
        {
            var tab = new AlertsHistoryTab();
            tab.Initialize(null!, _ => null);
            tab.ShowAlerts(Rows(10));
            Assert.False(tab.DismissAllUsesShownRows());

            tab.ApplyColumnFilter(new ColumnFilterState { ColumnName = "ServerName", Operator = FilterOperator.Equals, Value = "example-sql-01" });
            Assert.True(tab.DismissAllUsesShownRows());

            tab.ApplyColumnFilter(new ColumnFilterState { ColumnName = "ServerName", Operator = FilterOperator.Contains, Value = "" });
            Assert.False(tab.DismissAllUsesShownRows());
        });
    }

    [Fact]
    public void AReadThatHitTheRowCap_DismissesTheRowsOnScreen()
    {
        StaTestThread.Run(() =>
        {
            var tab = new AlertsHistoryTab();
            tab.Initialize(null!, _ => null);
            tab.ShowAlerts(Rows(AlertsHistoryTab.RowCap));
            Assert.True(tab.DismissAllUsesShownRows());

            tab.ShowAlerts(Rows(AlertsHistoryTab.RowCap - 1));
            Assert.False(tab.DismissAllUsesShownRows());
        });
    }
}
