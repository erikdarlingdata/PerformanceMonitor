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
    /// <summary>Self-monitor family (Collection Stopped / Capture Down / Collector Cost Regression) template
    /// version (#4223). Bumped only if this template's SHAPE changes.</summary>
    internal const int SelfMonitorTemplateVersion = 1;

    /// <summary>Collection Stopped / Capture Down / Collector Cost Regression: the three per-server
    /// self-monitor alerts (<see cref="DarlingTriageEndpoint.PerServerSelfMonitorMetrics"/>) — each about
    /// the MONITOR's own health on ONE real server, not a measured metric with a chart, so this family is
    /// read cells only (<c>get_collection_health</c>, <c>get_collection_log</c> and, for Cost Regression,
    /// <c>get_collector_cost</c>/<c>get_collector_stall_probes</c>). None of those four reads carries a
    /// <see cref="ComposeSpec"/> source, so no timeline/panel cell is possible here — unlike every other
    /// authored family, which plots the condition it fired on.
    ///
    /// <para><b>Per-metric status filter on <c>get_collection_log</c>:</b>
    /// <list type="bullet">
    /// <item><b>Collection Stopped</b> — NO <c>status</c> filter. <c>ApplyCollectionStoppedAsync</c> fires
    /// off <c>ReadCollectionSignalsAsync</c>'s "no SUCCESS/SKIPPED run recently" signal, so the recent runs
    /// span whatever actually happened — <c>ERROR</c>, <c>PERMISSIONS</c>, <c>EXTENSION_MISSING</c>, or
    /// simply no rows at all — and a single status value would hide the others.</item>
    /// <item><b>Capture Down</b> — <c>status=SESSION_MISSING</c>. <c>ApplyCaptureDownAsync</c>
    /// (<c>DarlingSelfAlertEvaluator.cs</c> ~1420) only ever fires on rows
    /// <see cref="DarlingSelfAlertEvaluator.MissingCaptureSessionsSql"/> (~6637) returns, and that statement's
    /// own <c>WHERE x.status = 'SESSION_MISSING'</c> clause (~6655) is the ONLY predicate feeding
    /// <c>missing</c> — no other status can put a collector in that list, so one status filter covers every
    /// case that can fire this alert.</item>
    /// <item><b>Collector Cost Regression</b> — <c>collector_name</c>-scoped, no status filter (the log for
    /// that one collector is small enough to read unfiltered).</item>
    /// </list>
    /// </para>
    ///
    /// <para>Cost Regression's collector name rides the alert's persisted context as structured data
    /// (<see cref="AlertContext.CollectorName"/>, set at the fire site in
    /// <see cref="DarlingSelfAlertEvaluator.ApplyCostRegressionsAsync"/>), read from
    /// <paramref name="row"/>'s <c>ContextJson</c> the same way <see cref="BuildPoisonWaitCells"/> reads
    /// <see cref="AlertContext.WaitType"/> — never re-derived from prose, and never sent as an empty
    /// <c>collector_name</c> value. A row written before this member existed (or the incident's own
    /// evaluator instance that has since restarted) carries no collector name; that case gets a note cell
    /// in place of the two collector-scoped reads plus the unfiltered log read, rather than a read with a
    /// blank required-looking parameter.</para>
    /// </summary>
    private static JsonArray BuildSelfMonitorCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            ServerOnlyReadCell("get_collection_health", "Collection health", serverName),
        };

        if (string.Equals(metric, "Collection Stopped", StringComparison.OrdinalIgnoreCase))
        {
            /* No status filter (see doc comment): the recent runs a stopped collection covers can be
               ERROR, PERMISSIONS, EXTENSION_MISSING, or simply absent, and any single value would hide
               the others. */
            cells.Add(AuthoredReadCell("get_collection_log", "Collection log", serverName, asOf,
                ("hours", "2"), ("limit", "50")));
            return cells;
        }

        if (string.Equals(metric, "Capture Down", StringComparison.OrdinalIgnoreCase))
        {
            cells.Add(AuthoredReadCell("get_collection_log", "Collection log", serverName, asOf,
                ("hours", "24"), ("limit", "50"), ("status", "SESSION_MISSING")));
            return cells;
        }

        /* Collector Cost Regression: the collector name rides the row's persisted context, not the
           incident (AlertIncident carries no collector-name member). */
        var collectorName = row?.ContextJson is string contextJson
            ? AlertContextSerializer.TryReadCollectorName(contextJson)
            : null;

        if (string.IsNullOrWhiteSpace(collectorName))
        {
            cells.Add(new JsonObject
            {
                ["type"] = "markdown",
                ["text"] = "This alert predates the collector name being recorded; the collection log below "
                    + "is for every collector.",
            });
            cells.Add(AuthoredReadCell("get_collection_log", "Collection log", serverName, asOf,
                ("hours", "24"), ("limit", "50")));
            return cells;
        }

        cells.Add(AuthoredReadCell("get_collection_log", "Collection log", serverName, asOf,
            ("hours", "24"), ("limit", "50"), ("collector_name", collectorName)));
        cells.Add(CollectorCostReadCell(collectorName));
        cells.Add(CollectorStallProbesReadCell(serverName));

        return cells;
    }

    /// <summary>A read cell for <c>get_collector_cost</c> scoped to one collector (14-day trend): the read
    /// declares only <c>days_back</c> and <c>collector_name</c> (<see cref="DarlingWebEndpoints"/>'s own
    /// catalog entry) — no <c>server</c>, <c>hours</c>, <c>as_of</c> or <c>limit</c> param exists on it, so
    /// neither <see cref="AuthoredReadCell"/> nor <see cref="ServerOnlyReadCell"/> can build this cell
    /// without tripping <c>ValidateReadPanelSpec</c>'s unknown-parameter check. <c>get_collector_cost</c> is
    /// registered in <see cref="s_authoredLimitlessTrendReads"/> because it declares no <c>limit</c> param
    /// at all.</summary>
    private static JsonObject CollectorCostReadCell(string collectorName) => new()
    {
        ["type"] = "read",
        ["read"] = "get_collector_cost",
        ["params"] = new JsonObject
        {
            ["collector_name"] = collectorName,
            ["days_back"] = "14",
        },
        ["viz"] = "table",
        ["title"] = "Collector cost trend",
    };

    /// <summary>A read cell for <c>get_collector_stall_probes</c>: the read declares <c>server</c>,
    /// <c>days_back</c> and <c>limit</c> (<see cref="DarlingWebEndpoints"/>'s own catalog entry) — no
    /// <c>hours</c> or <c>as_of</c>, so <see cref="AuthoredReadCell"/> cannot build this cell without
    /// forcing an undeclared <c>hours</c> onto it. The declared <c>limit</c> is carried explicitly at its
    /// catalog default (<see cref="DarlingMcpStallProbeTools.DefaultLimit"/>) so the shared budget theory's
    /// "every read cell carries a limit" rule holds without adding this read to
    /// <see cref="s_authoredLimitlessTrendReads"/>.</summary>
    private static JsonObject CollectorStallProbesReadCell(string? serverName)
    {
        var parameters = new JsonObject
        {
            ["days_back"] = "7",
            ["limit"] = DarlingMcpStallProbeTools.DefaultLimit.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (!string.IsNullOrWhiteSpace(serverName))
        {
            parameters["server"] = serverName;
        }

        return new JsonObject
        {
            ["type"] = "read",
            ["read"] = "get_collector_stall_probes",
            ["params"] = parameters,
            ["viz"] = "table",
            ["title"] = "Collector stall probes",
        };
    }
}
