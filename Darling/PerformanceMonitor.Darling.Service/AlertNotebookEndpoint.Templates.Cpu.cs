/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>High CPU template version (#4223). Bumped only if this template's SHAPE changes.</summary>
    internal const int CpuTemplateVersion = 1;

    /// <summary>High CPU (spec §3): header, status, a SQL Server CPU timeline and a PostgreSQL CPU timeline
    /// (the metric fires on both engines since #2719, so both panels are always present — the wrong-engine
    /// panel's own read simply comes back empty, the same "not applicable" shape
    /// <see cref="DarlingTriageEndpoint.SectionsByMetric"/>'s "High CPU" row already uses), the top-queries and
    /// top-procedures-by-CPU drill-downs, then scheduler pressure for the SQL Server-side follow-up.</summary>
    private static JsonArray BuildCpuCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            CpuGaugeTimelinePanel("SQL Server CPU", "cpu_utilization_stats", "sqlserver_cpu_utilization", windowStart, windowEnd),
            CpuGaugeTimelinePanel("PostgreSQL CPU", "pg_cpu_utilization", "pg_acu_utilization_pct", windowStart, windowEnd),
            AuthoredReadCell("get_top_queries_by_cpu", "Top queries by CPU", serverName, asOf,
                ("hours", "24"), ("top", "10")),
            AuthoredReadCell("get_top_procedures_by_cpu", "Top procedures by CPU", serverName, asOf,
                ("hours", "24"), ("top", "10")),
            AuthoredReadCell("get_cpu_scheduler_pressure", "Scheduler pressure", serverName, asOf),
        };

        return cells;
    }

    /// <summary>Both CPU measures (<c>sqlserver_cpu_utilization</c>, <c>pg_acu_utilization_pct</c>) are Gauge
    /// archetype (point-in-time percent readings, per <c>MeasureCatalog</c>'s own grain-trap note) — their
    /// <c>ValidAggs</c> is avg/min/max, never <c>count</c>. <see cref="TimelinePanel"/> hardcodes
    /// <c>aggregate: "count"</c> for the per-event deadlock/blocking sources it was built for, so a CPU
    /// timeline needs its own cell rather than reusing that helper (widening it to a per-family aggregate
    /// would change what every OTHER family's already-shipped panel emits). No annotation overlay: neither
    /// deadlocks nor blocked-process reports nor a long-query completion is a CPU-specific marker, and the
    /// spec doesn't ask for one here — see the PR body for why this template carries no core-helper
    /// change.</summary>
    private static JsonObject CpuGaugeTimelinePanel(string title, string source, string measure, DateTime windowStart, DateTime windowEnd) => new()
    {
        ["type"] = "panel",
        ["title"] = title,
        ["source"] = source,
        ["measure"] = measure,
        ["aggregate"] = "avg",
        ["viz"] = "line",
        ["timeBucket"] = "hour",
        ["range"] = new JsonObject
        {
            ["windowStart"] = windowStart.ToString("o", CultureInfo.InvariantCulture),
            ["windowEnd"] = windowEnd.ToString("o", CultureInfo.InvariantCulture),
        },
    };
}
