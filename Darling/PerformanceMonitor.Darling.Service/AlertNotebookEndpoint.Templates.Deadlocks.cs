/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>Deadlocks template version (#4222 slice b). Bumped only if this template's SHAPE changes.</summary>
    internal const int DeadlocksTemplateVersion = 1;

    /// <summary>Deadlocks Detected (spec §3): header, status, <c>get_deadlock_detail</c> (limit 3), a
    /// deadlocks timeline with a blocking annotation, deadlocks by database (the <c>deadlock-postmortem</c>
    /// panel specs, ported), then <c>get_deadlock_trend</c> over 24h.</summary>
    private static JsonArray BuildDeadlockCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            AuthoredReadCell("get_deadlock_detail", "Deadlock detail", serverName, asOf,
                ("hours", "24"), ("limit", "3")),
            TimelinePanel(
                "Deadlocks over time", "deadlocks", "deadlock_count",
                windowStart, windowEnd, annotation: "blocked_process_reports", databaseFilter: incident?.Database),
            RankedPanel(
                "Deadlocks by database", "deadlocks", "deadlock_count",
                windowStart, windowEnd, "database_name", databaseFilter: null),
            AuthoredReadCell("get_deadlock_trend", "Deadlock trend", serverName, asOf,
                ("hours", "24")),
        };

        return cells;
    }
}
