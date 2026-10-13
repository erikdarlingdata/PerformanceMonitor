/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>Runnable Queue template version (#5641). Bumped only if this template's SHAPE changes.</summary>
    internal const int RunnableQueueTemplateVersion = 1;

    /// <summary>Runnable Queue (#5641): header, status, then the runnable-task count from
    /// <c>cpu_scheduler_stats</c> over the window, with the scheduler-pressure read beside it, and only then the
    /// CPU context. The lead cell plots <c>total_runnable_tasks_count</c>, the runnable tasks summed across every
    /// visible online scheduler. The alert grades runnable tasks per scheduler (each collection's sum divided by its
    /// scheduler count, averaged over the window; the bars are on
    /// <see cref="PerformanceMonitor.Alerting.RunnableQueueEvaluator"/>). The catalog has no per-scheduler measure,
    /// so the cell is titled "(all schedulers)": divide its value by the scheduler count to compare it with the bars.
    /// It is its own template rather than a route to <c>authored/cpu</c> because the High CPU
    /// cells are tied to CPU percent: a PostgreSQL CPU timeline this SQL Server-only alert can never have, a
    /// per-procedure drill-down, and the scheduler read last. A queue of runnable tasks is work waiting for CPU,
    /// which can time clients out before CPU percent reaches its bar, so the scheduler evidence comes first and
    /// the SQL Server CPU timeline and the top queries by CPU follow as the "who is queueing" corroboration.</summary>
    private static JsonArray BuildRunnableQueueCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            CpuGaugeTimelinePanel("Runnable tasks (all schedulers)", "cpu_scheduler_stats", "scheduler_runnable_tasks", windowStart, windowEnd),
            AuthoredReadCell("get_cpu_scheduler_pressure", "Scheduler pressure", serverName, asOf),
            CpuGaugeTimelinePanel("SQL Server CPU", "cpu_utilization_stats", "sqlserver_cpu_utilization", windowStart, windowEnd),
            AuthoredReadCell("get_top_queries_by_cpu", "Top queries by CPU", serverName, asOf,
                ("hours", "24"), ("top", "10")),
        };

        return cells;
    }
}
