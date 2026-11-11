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
    /// <summary>Long-Running Query template version (#4223). Bumped only if this template's SHAPE changes. 2: the cells follow
    /// the server's engine (#8584).</summary>
    internal const int LongRunningQueryTemplateVersion = 2;

    /// <summary>Forced Plan Failing template version (#4223). Bumped only if this template's SHAPE changes.</summary>
    internal const int ForcedPlanFailingTemplateVersion = 1;

    /// <summary>Long-Running Query (#4223): header, status, <c>get_active_queries</c> (limit 25), a note
    /// that the plan for the still-running session is one click away in that row (<c>get_plan_xml</c> needs a
    /// concrete <c>query_hash</c>, which neither <see cref="AlertIncident"/> nor the persisted history row
    /// carries as data — <see cref="AlertContextBuilders.LongRunningQueryIncidents"/> hashes it into the
    /// fingerprint's dedup key rather than exposing it, so there is no value to bind an auto-run cell to), a
    /// completion-duration timeline, then <c>get_long_query_completions</c> over 24h. Then the resolved server's own engine's
    /// cells (#8584, <see cref="AuthoredContext.EngineKind"/>, the #8323 High CPU pattern): on PostgreSQL (Aurora included)
    /// the one read the PostgreSQL alert itself is built on, <c>get_pg_session_states</c> (the same sampled table
    /// <c>EvaluatePgLongRunningQueryAsync</c> reads), and no SQL Server cell: PostgreSQL has no completed-query log, so the
    /// completions timeline and read would say "does not apply" or "No data in this window" about a query the service
    /// itself caught. With no engine, the SQL Server cells and then the PostgreSQL one are emitted.</summary>
    private static JsonArray BuildLongRunningQueryCells(
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
            cells.Add(AuthoredReadCell("get_active_queries", "Active queries", serverName, asOf,
                ("hours", "1"), ("limit", "25")));
            cells.Add(new JsonObject
            {
                ["type"] = "markdown",
                ["text"] = "The execution plan for a still-running session is one click away from its row " +
                    "above (Active queries) — this notebook cannot bind `get_plan_xml` without a concrete " +
                    "`query_hash`, which this alert does not carry as data.",
            });
            cells.Add(TimelinePanel(
                "Long-query completions over time", "long_query_completions", "lqc_duration_us",
                windowStart, windowEnd, annotation: "long_query_completions", databaseFilter: incident?.Database));
            cells.Add(AuthoredReadCell("get_long_query_completions", "Completed long queries", serverName, asOf,
                ("hours", "24"), ("limit", "20")));
        }

        if (engine is null or CollectorTargetEngine.PostgreSql)
        {
            PgLongRunningQueryCells(cells, serverName, asOf, hours: "1");
        }

        return cells;
    }

    /// <summary>The PostgreSQL long-running-query cell (#8584): <c>get_pg_session_states</c>, the sampled sessions with their
    /// transaction age and last statement. Both the firing and the cleared notebooks use it.</summary>
    private static void PgLongRunningQueryCells(JsonArray cells, string? serverName, string? asOf, string hours) =>
        cells.Add(AuthoredReadCell("get_pg_session_states", "Long-running sessions", serverName, asOf,
            ("hours", hours), ("limit", "25")));

    /// <summary>Forced Plan Failing (#4223): header, status, <c>get_plan_corrections</c> (limit 25, full
    /// text), then <c>plan_correction_captures</c> by <c>recommendation_state</c> as the corrections
    /// timeline (no marker overlay — <see cref="MeasureCatalog.AnnotationSources"/> has no plan-correction
    /// entry, and <see cref="TimelinePanel"/> requires a known annotation key, so this panel is built
    /// directly rather than through that helper). <c>get_query_store_regressions</c> stays OFF this notebook
    /// entirely: the issue calls it click-to-run, and the notebook schema (<see
    /// cref="DarlingWebEndpoints.ValidateNotebookDefinition"/>) has no non-auto-run cell type to bind an
    /// eager read to — every <c>read</c> cell runs on open, so putting the regressions read here would run it
    /// eagerly against the issue's own intent.</summary>
    private static JsonArray BuildForcedPlanFailingCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var timelineCell = new JsonObject
        {
            ["type"] = "panel",
            ["title"] = "Plan corrections over time",
            ["source"] = "plan_correction",
            ["measure"] = "plan_correction_captures",
            ["aggregate"] = "count",
            ["viz"] = "line",
            ["timeBucket"] = "hour",
            ["groupBy"] = new JsonArray { "recommendation_state" },
            ["range"] = new JsonObject
            {
                ["windowStart"] = windowStart.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                ["windowEnd"] = windowEnd.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
            },
        };

        if (!string.IsNullOrWhiteSpace(incident?.Database))
        {
            timelineCell["filters"] = new JsonArray
            {
                new JsonObject
                {
                    ["dimension"] = "database_name",
                    ["op"] = "eq",
                    ["value"] = incident!.Database,
                },
            };
        }

        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            AuthoredReadCell("get_plan_corrections", "Plan corrections", serverName, asOf,
                ("hours", "24"), ("limit", "25"), ("full_text", "true")),
            timelineCell,
        };

        return cells;
    }
}
