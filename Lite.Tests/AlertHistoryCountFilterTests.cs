/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5542 L6: Lite's Alert History count text follows a column filter, as the Darling Viewer's tab does. The read is cut at 500 rows, the
/// count says how many the grid shows now, and the "showing the newest 500" label belongs to the read, so a filter that narrows the
/// grid changes the number and keeps the label.
/// </summary>
public sealed class AlertHistoryCountFilterTests
{
    [Fact]
    public void AColumnFilter_RebuildsTheCountTextFromWhatTheGridNowShows_AndKeepsTheCapLabel()
    {
        StaTestThread.Run(() =>
        {
            var tab = new AlertsHistoryTab();
            tab.Initialize(null!, _ => null);
            var rows = new List<AlertHistoryRow>();
            for (var i = 0; i < AlertsHistoryTab.RowCap; i++)
            {
                rows.Add(new AlertHistoryRow { ServerId = 1, ServerName = i < 7 ? "example-sql-01" : "example-sql-02", MetricName = "CPU" });
            }

            tab.ShowAlerts(rows);
            Assert.Equal("500 alert(s) (showing the newest 500)", tab.AlertCountIndicator.Text);

            tab.ApplyColumnFilter(new ColumnFilterState { ColumnName = "ServerName", Operator = FilterOperator.Equals, Value = "example-sql-01" });
            Assert.Equal("7 alert(s) (showing the newest 500)", tab.AlertCountIndicator.Text);

            /* The popup's Clear button raises FilterApplied with an empty filter: the full count comes back. */
            tab.ApplyColumnFilter(new ColumnFilterState { ColumnName = "ServerName", Operator = FilterOperator.Contains, Value = "" });
            Assert.Equal("500 alert(s) (showing the newest 500)", tab.AlertCountIndicator.Text);
        });
    }

}
