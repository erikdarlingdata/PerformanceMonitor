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
    /// <summary>Poison Wait template version (#4223). Bumped only if this template's SHAPE changes.</summary>
    internal const int PoisonWaitTemplateVersion = 1;

    /// <summary>The one wait type this alert fires on, RESOURCE_SEMAPHORE (ordinal, case-insensitive) — the
    /// only type whose remedy is "raise a memory grant/worker ceiling", so its two supporting reads
    /// (<see cref="BuildPoisonWaitCells"/>'s memory-grant cells) are wasted panels on every other poison
    /// type and are gated on this string rather than shown unconditionally.</summary>
    internal const string ResourceSemaphoreWaitType = "RESOURCE_SEMAPHORE";

    /// <summary>Poison Wait (#4223, both engines): header, status, <c>get_wait_stats</c> /
    /// <c>get_pg_wait_stats</c> (each engine's own top-waits read, titled by engine so the wrong-engine read
    /// answers "not applicable" on its own — the mechanical map's #2719 convention), <c>get_waiting_tasks</c>,
    /// <c>get_resource_semaphore</c> + <c>get_memory_grants</c> ONLY when the firing wait type is
    /// RESOURCE_SEMAPHORE, then <c>get_wait_trend</c> for the wait type the alert names — or, when no
    /// incident/row carries a wait type, a note cell in its place rather than a trend call that would 400 on
    /// a missing required <c>wait_type</c>.
    ///
    /// <para>The wait type rides the alert's context as structured data (#4223 step 1):
    /// <see cref="AlertContext.WaitType"/>, set at both fire sites
    /// (<c>AlertEngine.CheckPoisonWaitsAsync</c>'s worst-graded <see cref="PoisonWaitEvaluator.SqlServerFinding.WaitType"/>,
    /// <c>DarlingWorker.EvaluatePgPoisonWaitAsync</c>'s <c>finding.Subject</c>) — the same value each engine's
    /// mute context already keys on (<c>AlertMuteContext.WaitType</c>), so this template reuses the field
    /// rather than re-deriving it from prose.</para>
    /// </summary>
    private static JsonArray BuildPoisonWaitCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        /* The wait type rides the ROW's persisted context (AlertContext.WaitType), not the incident —
           AlertIncident carries no wait-type member; the two engines' Poison Wait fire sites set the
           context-level field directly (see this method's doc comment). */
        var waitType = row?.ContextJson is string contextJson
            ? AlertContextSerializer.TryReadWaitType(contextJson)
            : null;

        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            AuthoredReadCell("get_wait_stats", "Top waits (SQL Server)", serverName, asOf,
                ("hours", "24"), ("limit", "20")),
            AuthoredReadCell("get_pg_wait_stats", "Top waits (PostgreSQL)", serverName, asOf,
                ("hours", "24"), ("limit", "20")),
            AuthoredReadCell("get_waiting_tasks", "Waiting tasks", serverName, asOf,
                ("hours", "1"), ("limit", "30")),
        };

        if (string.Equals(waitType, ResourceSemaphoreWaitType, StringComparison.OrdinalIgnoreCase))
        {
            cells.Add(AuthoredReadCell("get_resource_semaphore", "Resource semaphore", serverName, asOf,
                ("hours", "24")));
            cells.Add(AuthoredReadCell("get_memory_grants", "Memory grants", serverName, asOf,
                ("hours", "1")));
        }

        /* A timeline on wait_stats/wait_time_ms, filtered to the firing wait type
           when one is known -- built inline (not through TimelinePanel, which only knows a database_name
           filter) rather than widening that shared helper for one family's dimension. */
        var wallPanel = new JsonObject
        {
            ["type"] = "panel",
            ["title"] = "Wait time over time",
            ["source"] = "wait_stats",
            ["measure"] = "wait_time_ms",
            ["aggregate"] = "sum",
            ["viz"] = "line",
            ["timeBucket"] = "hour",
            ["range"] = new JsonObject
            {
                ["windowStart"] = windowStart.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                ["windowEnd"] = windowEnd.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
            },
        };
        if (!string.IsNullOrWhiteSpace(waitType))
        {
            wallPanel["filters"] = new JsonArray
            {
                new JsonObject
                {
                    ["dimension"] = "wait_type",
                    ["op"] = "eq",
                    ["value"] = waitType,
                },
            };
        }
        cells.Add(wallPanel);

        if (string.IsNullOrWhiteSpace(waitType))
        {
            /* get_wait_trend REQUIRES wait_type (PReqText) — emitting the cell with none would 400. A
               markdown cell is the notebook's own prose slot (ValidateNotebookDefinition's "markdown" arm),
               so the degrade reads as a note rather than a missing panel. */
            cells.Add(new JsonObject
            {
                ["type"] = "markdown",
                ["text"] = "No matched incident carries a wait type, so the wait trend (which requires one) is omitted.",
            });
        }
        else
        {
            cells.Add(AuthoredReadCell("get_wait_trend", "Wait trend", serverName, asOf,
                ("hours", "24"), ("wait_type", waitType)));
        }

        return cells;
    }
}
