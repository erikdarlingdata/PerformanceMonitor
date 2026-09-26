/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4223's authored Poison Wait alert-notebook template
/// (<see cref="AlertNotebookEndpoint.AuthoredTemplate"/> routing "Poison Wait" to
/// <c>authored/poison-wait</c> / <c>BuildPoisonWaitCells</c>). Follows the same shape as
/// <c>AlertNotebookTemplateBlockingTests</c>/<c>Deadlocks</c>' shared theories, which already cover this
/// family (<c>AlertNotebookAuthoredTemplateTests</c>' <c>AllAuthoredMetrics</c> enumerates
/// <see cref="AlertNotebookEndpoint.s_authoredTemplates"/> itself); this file pins the shape only this
/// family owns: the RESOURCE_SEMAPHORE gate on the two memory-grant reads, and the wait-type binding on
/// <c>get_wait_trend</c>.
/// </summary>
public sealed class AlertNotebookTemplatePoisonWaitTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private static DarlingAlertReader.AlertHistoryReadRow RowWithWaitType(string? waitType) =>
        new(WindowEnd, 1, "SRV1", "Poison Wait", 1, 1, true, "email", null, false, null, false,
            ContextJson: waitType is null
                ? null
                : AlertContextSerializer.Serialize(new AlertContext { WaitType = waitType }));

    /* ═══════════════════════════ routing ═══════════════════════════ */

    [Fact]
    public void AuthoredTemplate_PoisonWait_RoutesToItsOwnTemplate()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait");

        Assert.NotNull(template);
        Assert.Equal("authored/poison-wait", template!.Value.Id);
        Assert.Equal(AlertNotebookEndpoint.PoisonWaitTemplateVersion, template.Value.Version);
    }

    /// <summary>RED on dev: before this PR, "Poison Wait" has no registered row, so
    /// <see cref="AlertNotebookEndpoint.AuthoredTemplate"/> returns null and this assertion fails.</summary>
    [Fact]
    public void AuthoredTemplate_PoisonWait_IsNotNull()
    {
        Assert.NotNull(AlertNotebookEndpoint.AuthoredTemplate("Poison Wait"));
    }

    /* ═══════════════════════════ cell list ═══════════════════════════ */

    [Fact]
    public void BuildPoisonWaitCells_ResourceSemaphore_IncludesBothSemaphoreReads()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;
        var row = RowWithWaitType("RESOURCE_SEMAPHORE");

        var cells = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null, row, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var reads = cells.OfType<JsonObject>()
            .Where(c => (string?)c["type"] == "read")
            .Select(c => (string)c["read"]!)
            .ToArray();

        Assert.Contains("get_resource_semaphore", reads);
        Assert.Contains("get_memory_grants", reads);
        Assert.Contains("get_wait_stats", reads);
        Assert.Contains("get_pg_wait_stats", reads);
        Assert.Contains("get_waiting_tasks", reads);
        Assert.Contains("get_wait_trend", reads);
    }

    /// <summary>Case-insensitive ordinal match.</summary>
    [Fact]
    public void BuildPoisonWaitCells_ResourceSemaphoreLowercase_StillIncludesBothSemaphoreReads()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;
        var row = RowWithWaitType("resource_semaphore");

        var cells = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null, row, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var reads = cells.OfType<JsonObject>()
            .Where(c => (string?)c["type"] == "read")
            .Select(c => (string)c["read"]!)
            .ToArray();

        Assert.Contains("get_resource_semaphore", reads);
        Assert.Contains("get_memory_grants", reads);
    }

    [Theory]
    [InlineData("THREADPOOL")]
    [InlineData("RESOURCE_SEMAPHORE_QUERY_COMPILE")]
    public void BuildPoisonWaitCells_OtherWaitType_ExcludesBothSemaphoreReads(string waitType)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;
        var row = RowWithWaitType(waitType);

        var cells = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null, row, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var reads = cells.OfType<JsonObject>()
            .Where(c => (string?)c["type"] == "read")
            .Select(c => (string)c["read"]!)
            .ToArray();

        Assert.DoesNotContain("get_resource_semaphore", reads);
        Assert.DoesNotContain("get_memory_grants", reads);
    }

    [Fact]
    public void BuildPoisonWaitCells_WaitTrend_WaitTypeParamEqualsTheIncidentsExactly()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;
        var row = RowWithWaitType("THREADPOOL");

        var cells = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null, row, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var trend = cells.OfType<JsonObject>()
            .Single(c => (string?)c["type"] == "read" && (string?)c["read"] == "get_wait_trend");
        var parameters = Assert.IsType<JsonObject>(trend["params"]);

        Assert.Equal("THREADPOOL", (string?)parameters["wait_type"]);
    }

    [Fact]
    public void BuildPoisonWaitCells_NoWaitType_OmitsTrend_AddsANoteInstead()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;

        var cells = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null, null, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var reads = cells.OfType<JsonObject>()
            .Where(c => (string?)c["type"] == "read")
            .Select(c => (string)c["read"]!)
            .ToArray();

        Assert.DoesNotContain("get_wait_trend", reads);
        Assert.Contains(cells.OfType<JsonObject>(), c => (string?)c["type"] == "markdown");
    }

    /* ═══════════════════════════ degrade ═══════════════════════════ */

    [Fact]
    public void BuildPoisonWaitCells_NoMatchedIncidentOrRow_DegradesWithTheSameCellCount()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;

        var withRow = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null,
            RowWithWaitType("THREADPOOL"), "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);
        var withoutRow = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null, null, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        /* Both degrade to "no wait type" cell counts identically -- the with-row case here carries a wait
           type (adds the trend read), the without-row case adds the note markdown cell instead: same count,
           different content. */
        Assert.Equal(withRow.Count, withoutRow.Count);
    }

    /* ═══════════════════════════ validates ═══════════════════════════ */

    [Fact]
    public void BuildPoisonWaitCells_PassesValidateNotebookDefinition()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;
        var cells = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null,
            RowWithWaitType("RESOURCE_SEMAPHORE"), "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var definition = new JsonObject { ["kind"] = "notebook", ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);

        Assert.True(validation.IsValid, validation.Error);
    }

    [Fact]
    public void BuildPoisonWaitCells_NoWaitType_AlsoPassesValidateNotebookDefinition()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait")!.Value;
        var cells = template.Invoke(
            "Poison Wait", "SRV1", AsOf, WindowStart, WindowEnd, null, null, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var definition = new JsonObject { ["kind"] = "notebook", ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);

        Assert.True(validation.IsValid, validation.Error);
    }

    /* ═══════════════════════════ step 1: the context builder pin ═══════════════════════════ */

    [Fact]
    public void PoisonWaitContext_SerializesAndRehydratesTheWaitType()
    {
        var context = new AlertContext { WaitType = "RESOURCE_SEMAPHORE" };

        var json = AlertContextSerializer.Serialize(context);

        Assert.True(AlertContextSerializer.TryDeserialize(json, out var rehydrated));
        Assert.Equal("RESOURCE_SEMAPHORE", rehydrated.WaitType);
        Assert.Equal("RESOURCE_SEMAPHORE", AlertContextSerializer.TryReadWaitType(json));
    }

    [Fact]
    public void PoisonWaitContext_LegacyJsonWithNoWaitTypeMember_RehydratesNull()
    {
        const string legacyJson = "{\"Details\":[]}";

        Assert.Null(AlertContextSerializer.TryReadWaitType(legacyJson));
    }
}
