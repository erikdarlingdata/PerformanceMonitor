/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4223's "Server Unreachable"/"Server Restored" and "Failed Agent Job"/"Long-Running
/// Job"/"Agent Not Running" authored alert-notebook templates (<c>AlertNotebookEndpoint.BuildServerConnectCells</c>
/// / <c>BuildAgentJobCells</c>). The shared theories in <see cref="AlertNotebookAuthoredTemplateTests"/> already
/// cover both families' validation, dispatch names, budget and compose catalog once their row is registered;
/// this file pins each family's own shape: routing, the exact cell list (including the two server-only reads
/// that carry no <c>hours</c>/<c>limit</c>), the <c>status=ERROR</c> filter on the connect family's log read,
/// and the Agent-job family's note cell text.
/// </summary>
public sealed class AlertNotebookTemplateServerAgentTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private static AlertIncident IncidentWithDatabase(string database) =>
        new("dedup-key", new[] { "obj1" }, Database: database);

    /* ═══════════════════════════ routing ═══════════════════════════ */

    [Theory]
    [InlineData("Server Unreachable")]
    [InlineData("Server Restored")]
    public void AuthoredTemplate_ServerConnect_RoutesToItsOwnTemplate(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);

        Assert.NotNull(template);
        Assert.Equal("authored/server-connect", template!.Value.Id);
    }

    [Theory]
    [InlineData("Failed Agent Job")]
    [InlineData("Long-Running Job")]
    [InlineData("Agent Not Running")]
    public void AuthoredTemplate_AgentJob_RoutesToItsOwnTemplate(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);

        Assert.NotNull(template);
        Assert.Equal("authored/agent-job", template!.Value.Id);
    }

    /* ═══════════════════════════ Server Unreachable/Restored: exact cells ═══════════════════════════ */

    [Theory]
    [InlineData("Server Unreachable")]
    [InlineData("Server Restored")]
    public void ServerConnect_BuildsHeaderStatusCollectionHealthAndFilteredLog(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(4, cells.Count);

        Assert.Equal("header", (string?)cells[0]!.AsObject()["type"]);
        Assert.Equal("status", (string?)cells[1]!.AsObject()["type"]);

        var health = cells[2]!.AsObject();
        Assert.Equal("read", (string?)health["type"]);
        Assert.Equal("get_collection_health", (string?)health["read"]);
        Assert.Equal("SRV1", (string?)health["params"]!["server"]);
        // get_collection_health declares no hours/limit/as_of -- ServerOnlyReadCell must not add any.
        Assert.Null(health["params"]!["hours"]);
        Assert.Null(health["params"]!["limit"]);
        Assert.Null(health["params"]!["as_of"]);

        var log = cells[3]!.AsObject();
        Assert.Equal("read", (string?)log["type"]);
        Assert.Equal("get_collection_log", (string?)log["read"]);
        Assert.Equal("ERROR", (string?)log["params"]!["status"]);
        Assert.Equal("50", (string?)log["params"]!["limit"]);
        Assert.Equal("24", (string?)log["params"]!["hours"]);
        Assert.Equal(AsOf, (string?)log["params"]!["as_of"]);
    }

    /* ═══════════════════════════ Failed Agent Job / Long-Running Job / Agent Not Running: exact cells ═══════════════════════════ */

    [Theory]
    [InlineData("Failed Agent Job")]
    [InlineData("Long-Running Job")]
    [InlineData("Agent Not Running")]
    public void AgentJob_BuildsHeaderStatusRunningJobsAndHistoryNote(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(4, cells.Count);

        Assert.Equal("header", (string?)cells[0]!.AsObject()["type"]);
        Assert.Equal("status", (string?)cells[1]!.AsObject()["type"]);

        var runningJobs = cells[2]!.AsObject();
        Assert.Equal("read", (string?)runningJobs["type"]);
        Assert.Equal("get_running_jobs", (string?)runningJobs["read"]);
        Assert.Equal("SRV1", (string?)runningJobs["params"]!["server"]);
        Assert.Null(runningJobs["params"]!["hours"]);
        Assert.Null(runningJobs["params"]!["limit"]);
        Assert.Null(runningJobs["params"]!["as_of"]);

        // The job-history note (#4223): a markdown cell, present with its exact text, and it must not
        // imply the history is empty -- it says the history is unavailable HERE and where else to look.
        var note = cells[3]!.AsObject();
        Assert.Equal("markdown", (string?)note["type"]);
        var text = (string?)note["text"];
        Assert.NotNull(text);
        Assert.Contains("isn't available in this notebook", text);
        Assert.Contains("get_running_jobs", text);
        Assert.Contains("Job Activity Monitor", text);
        Assert.DoesNotContain("no history", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("empty", text, StringComparison.OrdinalIgnoreCase);
    }

    /* ═══════════════════════════ degrade: no matched incident, same cell count ═══════════════════════════ */

    [Theory]
    [InlineData("Server Unreachable")]
    [InlineData("Failed Agent Job")]
    public void NoMatchedIncident_StillDegradesToTheSameCellCount(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var withIncident = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");
        var withoutIncident = template.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, null, null, "Unknown");

        Assert.Equal(withIncident.Count, withoutIncident.Count);

        var definition = new JsonObject { ["kind"] = "notebook", ["cells"] = withoutIncident };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);
        Assert.True(validation.IsValid, validation.Error);
    }
}
