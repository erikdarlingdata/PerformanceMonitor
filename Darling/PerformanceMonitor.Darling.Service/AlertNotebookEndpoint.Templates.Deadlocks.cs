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
    /// <summary>Deadlocks template version (#4222 slice b). Bumped only if this template's SHAPE changes. 3: the cells follow
    /// the server's engine (#8553).</summary>
    internal const int DeadlocksTemplateVersion = 3;

    /// <summary>The metric the Deadlocks Cleared notebook's engine pick keys on (#8553): the alert engine's recovery title
    /// resolves through <see cref="ResolvedMetricOf"/> to this firing metric.</summary>
    private const string DeadlocksFiringMetric = "Deadlocks Detected";

    /// <summary>Deadlocks Detected (spec §3), then the resolved server's own engine's cells (#8553,
    /// <see cref="AuthoredContext.EngineKind"/>, the #8323 High CPU pattern). SQL Server: header, status,
    /// <c>get_deadlock_detail</c> (limit 3), a deadlocks timeline with a blocking annotation, deadlocks by database (the
    /// <c>deadlock-postmortem</c> panel specs, ported), then <c>get_deadlock_trend</c> over 24h. PostgreSQL (Aurora included):
    /// <see cref="PgDeadlockCells"/>, the PostgreSQL deadlock list and graphs, and no SQL Server cell. With no engine (a
    /// fleet-level store with no server, or a registry row no connect has stamped) the SQL Server cells and then the
    /// PostgreSQL ones are emitted, and the wrong engine's cells answer empty or not collected. A cell for a read the engine
    /// cannot answer is never drawn for a known engine: it would say "does not apply" or "No data in this window" about a
    /// deadlock the service itself stored.</summary>
    private static JsonArray BuildDeadlockCells(
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
            cells.Add(AuthoredReadCell("get_deadlock_detail", "Deadlock detail", serverName, asOf,
                ("hours", "24"), ("limit", "3")));
            cells.Add(TimelinePanel(
                "Deadlocks over time", "deadlocks", "deadlock_count",
                windowStart, windowEnd, annotation: "blocked_process_reports", databaseFilter: incident?.Database));
            cells.Add(RankedPanel(
                "Deadlocks by database", "deadlocks", "deadlock_count",
                windowStart, windowEnd, "database_name", databaseFilter: null));
            cells.Add(AuthoredReadCell("get_deadlock_trend", "Deadlock trend", serverName, asOf,
                ("hours", "24")));
        }

        if (engine is null or CollectorTargetEngine.PostgreSql)
        {
            PgDeadlockCells(cells, serverName, asOf, listHours: "24");
        }

        return cells;
    }

    /// <summary>The PostgreSQL deadlock cells (#8553): <c>get_pg_deadlocks</c>, the reports in the window ending at the alert,
    /// then <c>get_pg_deadlock_detail</c>, the newest full wait graphs. The graph read takes no window (no <c>hours</c>, no
    /// <c>as_of</c>: it answers the newest graphs the store holds, or one by hash), so its cell binds the server and a limit
    /// only, and its title says it is the newest graphs, not the alert's own. Both the firing and the cleared notebook use it.</summary>
    private static void PgDeadlockCells(JsonArray cells, string? serverName, string? asOf, string listHours)
    {
        cells.Add(AuthoredReadCell("get_pg_deadlocks", "Deadlocks", serverName, asOf,
            ("hours", listHours), ("limit", "10")));

        var detailParams = new JsonObject();
        if (!string.IsNullOrWhiteSpace(serverName))
        {
            detailParams["server"] = serverName;
        }

        detailParams["limit"] = "3";
        cells.Add(new JsonObject
        {
            ["type"] = "read",
            ["read"] = "get_pg_deadlock_detail",
            ["params"] = detailParams,
            ["viz"] = ReadVizFor("get_pg_deadlock_detail"),
            ["title"] = "Newest deadlock graphs",
        });
    }
}
