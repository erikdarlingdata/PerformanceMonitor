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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4223's authored self-monitor alert-notebook template (Collection Stopped / Capture Down /
/// Collector Cost Regression -&gt; <c>authored/self-monitor</c> / <c>BuildSelfMonitorCells</c>). Follows the
/// same shape as <c>AlertNotebookTemplatePoisonWaitTests</c> — the shared <c>AllAuthoredMetrics</c> theories
/// in <c>AlertNotebookAuthoredTemplateTests</c> already cover routing/validation/budget for this family the
/// moment its row lands; this file pins the shape only this family owns: the per-metric status filter, the
/// collector-name binding read from the row's persisted context, and the note-cell degrade when no
/// collector name is recorded.
/// </summary>
public sealed class AlertNotebookTemplateSelfMonitorTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private static DarlingAlertReader.AlertHistoryReadRow RowWithCollectorName(string metric, string? collectorName) =>
        new(WindowEnd, 1, "SRV1", metric, 1, 1, true, "email", null, false, null, false,
            ContextJson: collectorName is null
                ? null
                : AlertContextSerializer.Serialize(new AlertContext { CollectorName = collectorName }));

    /* ═══════════════════════════ routing ═══════════════════════════ */

    [Theory]
    [InlineData("Collection Stopped")]
    [InlineData("Capture Down")]
    [InlineData("Collector Cost Regression")]
    public void AuthoredTemplate_SelfMonitorMetrics_RouteToOneTemplate(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);

        Assert.NotNull(template);
        Assert.Equal("authored/self-monitor", template!.Value.Id);
        Assert.Equal(AlertNotebookEndpoint.SelfMonitorTemplateVersion, template.Value.Version);
    }

    /// <summary>RED on dev: before this PR, none of the three metrics has a registered row, so
    /// <see cref="AlertNotebookEndpoint.AuthoredTemplate"/> returns null and this assertion fails.</summary>
    [Theory]
    [InlineData("Collection Stopped")]
    [InlineData("Capture Down")]
    [InlineData("Collector Cost Regression")]
    public void AuthoredTemplate_SelfMonitorMetrics_AreNotNull(string metric)
    {
        Assert.NotNull(AlertNotebookEndpoint.AuthoredTemplate(metric));
    }

    /* ═══════════════════════════ status filter, per metric ═══════════════════════════ */

    [Fact]
    public void BuildSelfMonitorCells_CollectionStopped_LogCellHasNoStatusFilter()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Collection Stopped")!.Value;
        var row = RowWithCollectorName("Collection Stopped", null);

        var cells = template.Invoke(
            "Collection Stopped", "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: row,
            status: "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);

        var logCell = FindReadCell(cells, "get_collection_log");
        var parameters = Assert.IsType<JsonObject>(logCell["params"]);
        Assert.False(parameters.ContainsKey("status"), "Collection Stopped's log cell must not filter by status");
        Assert.Equal("2", (string)parameters["hours"]!);
    }

    [Fact]
    public void BuildSelfMonitorCells_CaptureDown_LogCellFiltersSessionMissing()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Capture Down")!.Value;
        var row = RowWithCollectorName("Capture Down", null);

        var cells = template.Invoke(
            "Capture Down", "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: row,
            status: "Firing", AlertNotebookEndpoint.AuthoredContext.Empty);

        var logCell = FindReadCell(cells, "get_collection_log");
        var parameters = Assert.IsType<JsonObject>(logCell["params"]);
        Assert.Equal("SESSION_MISSING", (string)parameters["status"]!);
        Assert.Equal("24", (string)parameters["hours"]!);
    }

    /* ═══════════════════════════ collector name from ROW context, not a literal ═══════════════════════════ */

    [Theory]
    [InlineData("nvme_health")]
    [InlineData("query_store")]
    public void BuildSelfMonitorCells_CostRegression_CollectorNameComesFromRowContext(string collectorName)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Collector Cost Regression")!.Value;
        var row = RowWithCollectorName("Collector Cost Regression", collectorName);

        var cells = template.Invoke(
            "Collector Cost Regression", "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: row,
            status: "Firing", AlertNotebookEndpoint.AuthoredContext.Empty);

        var logCell = FindReadCell(cells, "get_collection_log");
        Assert.Equal(collectorName, (string)((JsonObject)logCell["params"]!)["collector_name"]!);

        var costCell = FindReadCell(cells, "get_collector_cost");
        Assert.Equal(collectorName, (string)((JsonObject)costCell["params"]!)["collector_name"]!);

        var stallCell = FindReadCell(cells, "get_collector_stall_probes");
        Assert.Equal("SRV1", (string)((JsonObject)stallCell["params"]!)["server"]!);

        Assert.DoesNotContain(cells, c => ((JsonObject)c!)["type"]?.ToString() == "markdown");
    }

    [Fact]
    public void BuildSelfMonitorCells_CostRegression_NoCollectorName_UsesNoteInsteadOfScopedReads()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Collector Cost Regression")!.Value;
        var row = RowWithCollectorName("Collector Cost Regression", null);

        var cells = template.Invoke(
            "Collector Cost Regression", "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: row,
            status: "Firing", AlertNotebookEndpoint.AuthoredContext.Empty);

        Assert.Contains(cells, c => ((JsonObject)c!)["type"]?.ToString() == "markdown");
        Assert.DoesNotContain(cells, c => c is JsonObject o && o["type"]?.ToString() == "read"
            && o["read"]?.ToString() == "get_collector_cost");
        Assert.DoesNotContain(cells, c => c is JsonObject o && o["type"]?.ToString() == "read"
            && o["read"]?.ToString() == "get_collector_stall_probes");

        var logCell = FindReadCell(cells, "get_collection_log");
        Assert.False(((JsonObject)logCell["params"]!).ContainsKey("collector_name"));
    }

    /// <summary>No cell of any of the three metrics ever carries an empty/absent-value <c>collector_name</c>
    /// -- either the read is scoped with a real name, or it is omitted (Collection Stopped/Capture Down never
    /// pass one; the no-collector-name Cost Regression case degrades to the unscoped log read).</summary>
    [Theory]
    [InlineData("Collection Stopped")]
    [InlineData("Capture Down")]
    [InlineData("Collector Cost Regression")]
    public void BuildSelfMonitorCells_NeverEmitsAnEmptyCollectorNameParam(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric)!.Value;
        var row = RowWithCollectorName(metric, null);

        var cells = template.Invoke(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: row,
            status: "Firing", AlertNotebookEndpoint.AuthoredContext.Empty);

        foreach (var cellNode in cells)
        {
            var cell = Assert.IsType<JsonObject>(cellNode);
            if ((string?)cell["type"] != "read")
            {
                continue;
            }

            var parameters = Assert.IsType<JsonObject>(cell["params"]);
            if (parameters.TryGetPropertyValue("collector_name", out var value))
            {
                Assert.False(string.IsNullOrEmpty((string?)value), "collector_name must never be empty");
            }
        }
    }

    /* ═══════════════════════════ no panel cell for this family ═══════════════════════════ */

    [Theory]
    [InlineData("Collection Stopped")]
    [InlineData("Capture Down")]
    [InlineData("Collector Cost Regression")]
    public void BuildSelfMonitorCells_EmitsNoPanelCell(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric)!.Value;
        var row = RowWithCollectorName(metric, "some_collector");

        var cells = template.Invoke(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: row,
            status: "Firing", AlertNotebookEndpoint.AuthoredContext.Empty);

        Assert.DoesNotContain(cells, c => ((JsonObject)c!)["type"]?.ToString() == "panel");
    }

    /* ═══════════════════════════ every param each read cell passes is declared ═══════════════════════════ */

    [Theory]
    [InlineData("Collection Stopped")]
    [InlineData("Capture Down")]
    [InlineData("Collector Cost Regression")]
    public void BuildSelfMonitorCells_EveryReadCellParamIsDeclaredInItsCatalogEntry(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric)!.Value;
        var row = RowWithCollectorName(metric, "some_collector");

        var cells = template.Invoke(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: row,
            status: "Firing", AlertNotebookEndpoint.AuthoredContext.Empty);

        var descriptors = DarlingWebEndpoints.CatalogDescriptors;

        foreach (var cellNode in cells)
        {
            var cell = Assert.IsType<JsonObject>(cellNode);
            if ((string?)cell["type"] != "read")
            {
                continue;
            }

            var read = (string)cell["read"]!;
            Assert.True(descriptors.TryGetValue(read, out var descriptor), $"'{read}' has no catalog entry");
            var declared = descriptor.Params.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);

            var parameters = Assert.IsType<JsonObject>(cell["params"]);
            foreach (var property in parameters)
            {
                Assert.True(declared.Contains(property.Key),
                    $"read '{read}' passes undeclared parameter '{property.Key}'");
            }
        }
    }

    /* ═══════════════════════════ AlertContext round trip ═══════════════════════════ */

    [Fact]
    public void SelfMonitorContext_SerializesAndRehydratesTheCollectorName()
    {
        var context = new AlertContext { CollectorName = "query_store" };

        var json = AlertContextSerializer.Serialize(context);

        Assert.True(AlertContextSerializer.TryDeserialize(json, out var rehydrated));
        Assert.Equal("query_store", rehydrated.CollectorName);
        Assert.Equal("query_store", AlertContextSerializer.TryReadCollectorName(json));
    }

    [Fact]
    public void SelfMonitorContext_LegacyJsonWithNoCollectorNameMember_RehydratesNull()
    {
        const string legacyJson = "{\"Details\":[]}";

        Assert.Null(AlertContextSerializer.TryReadCollectorName(legacyJson));
    }

    /* ═══════════════════════════ helper ═══════════════════════════ */

    private static JsonObject FindReadCell(JsonArray cells, string read) =>
        (JsonObject)cells.Single(c => c is JsonObject o
            && (string?)o["type"] == "read"
            && (string?)o["read"] == read)!;
}
