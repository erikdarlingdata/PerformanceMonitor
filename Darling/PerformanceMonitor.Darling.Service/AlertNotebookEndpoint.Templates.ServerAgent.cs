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
    /// <summary>Server Unreachable / Server Restored template version (#4223). Bumped only if this
    /// template's SHAPE changes.</summary>
    internal const int ServerConnectTemplateVersion = 1;

    /// <summary>Agent-job family (Failed Agent Job / Long-Running Job / Agent Not Running) template version
    /// (#4223). Bumped only if this template's SHAPE changes.</summary>
    internal const int AgentJobTemplateVersion = 1;

    /// <summary>Server Unreachable / Server Restored (spec §3, the connect edge — <c>DarlingTriageEndpoint</c>
    /// V20): header, status, <c>get_collection_health</c> to tell whether one collector or the whole box
    /// dropped, then <c>get_collection_log</c> filtered to <c>status=ERROR</c> so the failing runs are the
    /// ones on the page instead of whatever a plain newest-first tail happened to catch.</summary>
    private static JsonArray BuildServerConnectCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            ServerOnlyReadCell("get_collection_health", "Collection health", serverName),
            AuthoredReadCell("get_collection_log", "Collection log (failures)", serverName, asOf,
                ("hours", "24"), ("limit", "50"), ("status", "ERROR")),
        };

        return cells;
    }

    /// <summary>Failed Agent Job / Long-Running Job / Agent Not Running (spec §3): header, status,
    /// <c>get_running_jobs</c>, then a markdown note that per-job run history is not available in this
    /// notebook — <c>get_running_jobs</c> only ever answers what is running RIGHT NOW, and this family has no
    /// history read to fall back on (unlike, say, the deadlock family's trend). The note says the history is
    /// unavailable HERE, not that it does not exist: SQL Server Agent's own job-history log still has it.</summary>
    private static JsonArray BuildAgentJobCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            ServerOnlyReadCell("get_running_jobs", "Running jobs", serverName),
            AgentJobHistoryNoteCell(),
        };

        return cells;
    }

    /// <summary>A read cell for a catalog entry that declares NOTHING but <c>server</c> (#4223:
    /// <c>get_collection_health</c>, <c>get_running_jobs</c>) — <see cref="AuthoredReadCell"/> cannot be
    /// reused here because it always forces <c>hours</c>, <c>limit</c> and (when supplied) <c>as_of</c> onto
    /// the params object, and none of those three are declared params on either read; sending one would trip
    /// <c>ValidateReadPanelSpec</c>'s unknown-parameter check. <c>viz</c> is "table", same as every other
    /// drill-down read cell.</summary>
    private static JsonObject ServerOnlyReadCell(string read, string title, string? serverName)
    {
        var parameters = new JsonObject();
        if (!string.IsNullOrWhiteSpace(serverName))
        {
            parameters["server"] = serverName;
        }

        return new JsonObject
        {
            ["type"] = "read",
            ["read"] = read,
            ["params"] = parameters,
            ["viz"] = "table",
            ["title"] = title,
        };
    }

    /// <summary>The Agent-job family's one markdown cell (#4223): states plainly that per-job run history
    /// is out of scope for this notebook, without implying the history is empty — it says where to look
    /// instead. <see cref="DarlingWebEndpoints.ValidateNotebookDefinition"/>'s <c>markdown</c> arm is the
    /// only shape this can take: a <c>type</c> discriminator plus a string <c>text</c>, nothing else.</summary>
    private static JsonObject AgentJobHistoryNoteCell() => new()
    {
        ["type"] = "markdown",
        ["text"] = "Per-job run history isn't available in this notebook — `get_running_jobs` only shows jobs "
            + "running right now. Check SQL Server Agent's own job history (SSMS: Agent > Job Activity Monitor, "
            + "or a job's History) for what ran before this alert fired.",
    };
}
