/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>Blocking template version (#4222 slice b). Bumped only if this template's SHAPE changes. 2: the cells follow
    /// the server's engine (#8584).</summary>
    internal const int BlockingTemplateVersion = 2;

    /// <summary>Blocking Detected / Blocking Wait Time (spec §3), then the resolved server's own engine's cells (#8584,
    /// <see cref="AuthoredContext.EngineKind"/>, the #8323 High CPU pattern). SQL Server: header, status, <c>get_blocking</c>
    /// (limit 20), a blocked-process-reports timeline with a deadlock annotation, most-blocked objects / by database / lock
    /// modes (the <c>blocking-rca</c> panel specs, ported), then <c>get_active_queries blocking_only</c>. PostgreSQL (Aurora
    /// included): <see cref="PgBlockingCells"/>, the PostgreSQL blocking chains and the sessions holding a transaction open,
    /// and no SQL Server cell. With no engine (a fleet-level store with no server, or a registry row no connect has
    /// stamped) the SQL Server cells and then the PostgreSQL ones are emitted, and the wrong engine's cells answer empty or
    /// not collected. A cell for a read the engine cannot answer is never drawn for a known engine: it would say "does not
    /// apply" or "No data in this window" about blocking the service itself detected.</summary>
    private static JsonArray BuildBlockingCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status, AuthoredContext context)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
        };

        var engine = MonitoredEngineKind.EngineOf(context.EngineKind);
        if (engine is null or CollectorTargetEngine.SqlServer)
        {
            cells.Add(AuthoredReadCell("get_blocking", "Blocking chains", serverName, asOf,
                ("hours", "24"), ("limit", "20")));
            cells.Add(TimelinePanel(
                "Blocked-process reports over time", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, annotation: "deadlocks", databaseFilter: incident?.Database));
            cells.Add(RankedPanel(
                "Most-blocked objects", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, "contentious_object", databaseFilter: incident?.Database));
            cells.Add(RankedPanel(
                "Blocking by database", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, "database_name", databaseFilter: null));
            cells.Add(RankedPanel(
                "Lock modes", "blocked_process_reports", "bpr_wait_time_ms",
                windowStart, windowEnd, "lock_mode", databaseFilter: incident?.Database));
            cells.Add(AuthoredReadCell("get_active_queries", "Active blocking queries", serverName, asOf,
                ("hours", "1"), ("blocking_only", "true"), ("limit", "25")));
        }

        if (engine is null or CollectorTargetEngine.PostgreSql)
        {
            PgBlockingCells(cells, serverName, asOf, chainHours: "24", sessionHours: "1");
        }

        return cells;
    }

    /// <summary>The PostgreSQL blocking cells (#8584): <c>get_pg_blocking</c>, the sampled chains with their root blocker, then
    /// <c>get_pg_session_states</c>, the sessions holding a transaction open (an idle-in-transaction root is the usual
    /// cause). Both the firing and the cleared notebooks use it.</summary>
    private static void PgBlockingCells(JsonArray cells, string? serverName, string? asOf, string chainHours, string sessionHours)
    {
        cells.Add(AuthoredReadCell("get_pg_blocking", "Blocking chains", serverName, asOf,
            ("hours", chainHours), ("limit", "20")));
        cells.Add(AuthoredReadCell("get_pg_session_states", "Sessions holding a transaction open", serverName, asOf,
            ("hours", sessionHours), ("limit", "25")));
    }
}
