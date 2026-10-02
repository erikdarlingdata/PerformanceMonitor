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
    /// <summary>Blocking template version (#4222 slice b). Bumped only if this template's SHAPE changes.</summary>
    internal const int BlockingTemplateVersion = 1;

    /// <summary>Blocking Detected / Blocking Wait Time (spec §3): header, status, <c>get_blocking</c> (limit
    /// 20), a blocked-process-reports timeline with a deadlock annotation, most-blocked objects / by database
    /// / lock modes (the <c>blocking-rca</c> panel specs, ported), then <c>get_active_queries
    /// blocking_only</c>.</summary>
    private static JsonArray BuildBlockingCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            AuthoredReadCell("get_blocking", "Blocking chains", serverName, asOf,
                ("hours", "24"), ("limit", "20")),
            TimelinePanel(
                "Blocked-process reports over time", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, annotation: "deadlocks", databaseFilter: incident?.Database),
            RankedPanel(
                "Most-blocked objects", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, "contentious_object", databaseFilter: incident?.Database),
            RankedPanel(
                "Blocking by database", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, "database_name", databaseFilter: null),
            RankedPanel(
                "Lock modes", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, "lock_mode", databaseFilter: incident?.Database),
            AuthoredReadCell("get_active_queries", "Active blocking queries", serverName, asOf,
                ("hours", "1"), ("blocking_only", "true"), ("limit", "25")),
        };

        return cells;
    }
}
