/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every user-facing blocked-process-report and deadlock read windows on the EVENT column, carries the
/// <c>collection_time</c> floor (<see cref="EventWindowFloor"/>), and has NO upper bound on
/// <c>collection_time</c>. An upper bound would drop an event collected after its window had passed (the
/// catch-up after an outage), which is exactly the row the event-time window exists to keep.
///
/// <para>The check is scoped to the statement that reads the event table, from its <c>FROM</c> to the next
/// <c>FROM</c> or <c>UNION</c>, so a DMV-snapshot arm beside it (whose <c>event_time</c> IS its collection
/// time and which stays on <c>collection_time</c>) is not mistaken for the event read.</para>
/// </summary>
public sealed class EventTimeWindowSqlShapeTests
{
    public static TheoryData<string, string, string, string> Statements() => new()
    {
        { "Viewer BlockedProcessReportsSql", ViewerDataService.BlockedProcessReportsSql, "FROM blocked_process_reports", "event_time" },
        { "Viewer BlockingSlicerSql (XE)", ViewerDataService.BlockingSlicerSql, "FROM v_blocked_process_reports", "event_time" },
        { "Viewer DeadlockSlicerSql", ViewerDataService.DeadlockSlicerSql, "FROM v_deadlocks", "deadlock_time" },
        { "Viewer RecentDeadlocksSql", ViewerDataService.RecentDeadlocksSql, "FROM deadlocks", "deadlock_time" },
        { "MCP BlockedProcessReportsSql", DarlingBlockingReader.BlockedProcessReportsSql, "FROM blocked_process_reports", "event_time" },
        { "MCP BlockedProcessReportsWithXmlSql", DarlingBlockingReader.BlockedProcessReportsWithXmlSql, "FROM blocked_process_reports", "event_time" },
        { "MCP RecentDeadlocksSql", DarlingBlockingReader.RecentDeadlocksSql, "FROM deadlocks", "deadlock_time" },
        { "MCP RecentDeadlocksWithGraphSql", DarlingBlockingReader.RecentDeadlocksWithGraphSql, "FROM deadlocks", "deadlock_time" },
        { "MCP DeadlockTrendSql", DarlingBlockingTrendReader.DeadlockTrendSql, "FROM v_deadlocks", "deadlock_time" },
        { "MCP DeadlockSeverityGraphsSql", DarlingDataReader.DeadlockSeverityGraphsSql, "FROM v_deadlocks", "deadlock_time" },
        { "DailySummarySql.RangeSql deadlocks", DailySummarySql.RangeSql, "FROM v_deadlocks", "deadlock_time" },
        { "DailySummarySql.RangeSql blocked-process reports", DailySummarySql.RangeSql, "FROM v_blocked_process_reports", "event_time" },
    };

    [Theory]
    [MemberData(nameof(Statements))]
    public void TheEventRead_WindowsOnTheEventColumn_WithTheFloor_AndNoUpperBoundOnCollectionTime(
        string label, string sql, string fromClause, string eventColumn)
    {
        var start = sql.IndexOf(fromClause, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{label}: '{fromClause}' not found");

        var statement = sql[start..];
        var end = NextBoundary(statement, fromClause.Length);
        statement = statement[..end];

        Assert.Contains($"{eventColumn} >= $2", statement, StringComparison.Ordinal);
        Assert.Matches(@"collection_time >= \$\d", statement);
        Assert.DoesNotContain("collection_time <", statement, StringComparison.Ordinal);
    }

    /// <summary>The offset in <paramref name="statement"/> where the next statement starts: the next
    /// <c>FROM</c> or <c>UNION</c> after the opening one, or the end.</summary>
    private static int NextBoundary(string statement, int searchFrom)
    {
        var end = statement.Length;
        foreach (var marker in new[] { "FROM ", "UNION" })
        {
            var at = statement.IndexOf(marker, searchFrom, StringComparison.Ordinal);
            if (at >= 0 && at < end)
            {
                end = at;
            }
        }

        return end;
    }
}
