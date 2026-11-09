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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>High CPU template version (#4223). Bumped only if this template's SHAPE changes. 2: the cells follow
    /// the server's engine (#8323).</summary>
    internal const int CpuTemplateVersion = 2;

    /// <summary>High CPU (spec §3): header, status, then the resolved server's own engine's cells (#8323,
    /// <see cref="AuthoredContext.EngineKind"/>). The metric fires on both engines (#2719). SQL Server: the SQL Server CPU
    /// timeline, the top-queries and top-procedures-by-CPU drill-downs, then scheduler pressure. PostgreSQL (Aurora
    /// included): the PostgreSQL CPU timeline and <c>get_pg_top_queries</c>, titled by its measure (total execution time). With no engine (a fleet-level store with
    /// no server, or a registry row no connect has stamped) both timelines and every drill-down are emitted, the shape
    /// <see cref="DarlingTriageEndpoint.SectionsByMetric"/>'s "High CPU" row uses, and the wrong engine's cells answer
    /// empty or not collected. A panel for a measure the engine cannot have is never drawn for a known engine: it would
    /// say "No data in this window", a true-negative sentence for a measure that was never collected.</summary>
    private static JsonArray BuildCpuCells(
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
            cells.Add(CpuGaugeTimelinePanel("SQL Server CPU", "cpu_utilization_stats", "sqlserver_cpu_utilization", windowStart, windowEnd));
        }

        if (engine is null or CollectorTargetEngine.PostgreSql)
        {
            cells.Add(CpuGaugeTimelinePanel("PostgreSQL CPU", "pg_cpu_utilization", "pg_acu_utilization_pct", windowStart, windowEnd));
        }

        if (engine is null or CollectorTargetEngine.SqlServer)
        {
            cells.Add(AuthoredReadCell("get_top_queries_by_cpu", "Top queries by CPU", serverName, asOf,
                ("hours", "24"), ("top", "10")));
            cells.Add(AuthoredReadCell("get_top_procedures_by_cpu", "Top procedures by CPU", serverName, asOf,
                ("hours", "24"), ("top", "10")));
            cells.Add(AuthoredReadCell("get_cpu_scheduler_pressure", "Scheduler pressure", serverName, asOf));
        }
        else
        {
            /* get_pg_top_queries ranks query shapes by total execution time; pg_stat_statements has no CPU column. */
            cells.Add(AuthoredReadCell("get_pg_top_queries", "Top queries by execution time", serverName, asOf,
                ("hours", "24"), ("limit", "10")));
        }

        return cells;
    }

    /// <summary>Both CPU measures (<c>sqlserver_cpu_utilization</c>, <c>pg_acu_utilization_pct</c>) are Gauge
    /// archetype (point-in-time percent readings, per <c>MeasureCatalog</c>'s own grain-trap note) — their
    /// <c>ValidAggs</c> is avg/min/max, never <c>count</c>. The aggregate is <c>max</c>: the panel buckets by
    /// hour over 24 h, and an alert fires on a few minutes over the bar (#7085), so an hourly average would
    /// flatten a 10-minute spike at 95% in a 20% hour to about 32% and show nothing where the alert fired
    /// (#7203). The same helper draws the Runnable Queue panels, whose alert is also a short excursion over a bar. <see cref="TimelinePanel"/> hardcodes
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
        ["aggregate"] = "max",
        ["viz"] = "line",
        ["timeBucket"] = "hour",
        ["range"] = new JsonObject
        {
            ["windowStart"] = windowStart.ToString("o", CultureInfo.InvariantCulture),
            ["windowEnd"] = windowEnd.ToString("o", CultureInfo.InvariantCulture),
        },
    };
}
