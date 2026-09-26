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
/// Pins for #4223's "Long-Running Query" and "Forced Plan Failing" authored alert-notebook templates
/// (<c>AlertNotebookEndpoint.BuildLongRunningQueryCells</c> / <c>BuildForcedPlanFailingCells</c>). The shared
/// theories in <see cref="AlertNotebookAuthoredTemplateTests"/> already cover both metrics' validation,
/// dispatch names and compose catalog once their row is registered; this file pins the two families' own
/// shapes: routing, the exact cell list, the <c>query_hash</c> non-binding note, and that
/// <c>get_query_store_regressions</c> never appears as an auto-run cell.
/// </summary>
public sealed class AlertNotebookTemplateQueryPlansTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private static AlertIncident IncidentWithDatabase(string database) =>
        new("dedup-key", new[] { "obj1" }, Database: database);

    /* ═══════════════════════════ routing ═══════════════════════════ */

    [Fact]
    public void AuthoredTemplate_LongRunningQuery_RoutesToItsOwnTemplate()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Long-Running Query");

        Assert.NotNull(template);
        Assert.Equal("authored/long-running-query", template!.Value.Id);
    }

    [Fact]
    public void AuthoredTemplate_ForcedPlanFailing_RoutesToItsOwnTemplate()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Forced Plan Failing");

        Assert.NotNull(template);
        Assert.Equal("authored/forced-plan-failing", template!.Value.Id);
    }

    /* ═══════════════════════════ Long-Running Query: exact cells ═══════════════════════════ */

    [Fact]
    public void LongRunningQuery_BuildsHeaderStatusActiveQueriesNoteAndCompletions()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Long-Running Query");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "Long-Running Query", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(6, cells.Count);

        Assert.Equal("header", (string?)cells[0]!.AsObject()["type"]);
        Assert.Equal("status", (string?)cells[1]!.AsObject()["type"]);

        var activeQueries = cells[2]!.AsObject();
        Assert.Equal("read", (string?)activeQueries["type"]);
        Assert.Equal("get_active_queries", (string?)activeQueries["read"]);
        Assert.Equal("25", (string?)activeQueries["params"]!["limit"]);
        Assert.Equal("1", (string?)activeQueries["params"]!["hours"]);

        // The query_hash note (#4223): a markdown cell, never a get_plan_xml read cell.
        var note = cells[3]!.AsObject();
        Assert.Equal("markdown", (string?)note["type"]);
        Assert.Contains("query_hash", (string?)note["text"], StringComparison.Ordinal);

        var timeline = cells[4]!.AsObject();
        Assert.Equal("panel", (string?)timeline["type"]);
        Assert.Equal("long_query_completions", (string?)timeline["source"]);

        var completions = cells[5]!.AsObject();
        Assert.Equal("read", (string?)completions["type"]);
        Assert.Equal("get_long_query_completions", (string?)completions["read"]);
        Assert.Equal("20", (string?)completions["params"]!["limit"]);
        Assert.Equal("24", (string?)completions["params"]!["hours"]);
    }

    [Fact]
    public void LongRunningQuery_NeverEmitsAGetPlanXmlCell()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Long-Running Query");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "Long-Running Query", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");

        var planXmlCells = cells.OfType<JsonObject>()
            .Where(c => (string?)c["type"] == "read" && (string?)c["read"] == "get_plan_xml")
            .ToArray();

        Assert.Empty(planXmlCells);
    }

    [Fact]
    public void LongRunningQuery_NoMatchedIncident_StillDegradesWithSameCellCount()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Long-Running Query");
        Assert.NotNull(template);

        var withIncident = template!.Value.BuildCells(
            "Long-Running Query", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");
        var withoutIncident = template.Value.BuildCells(
            "Long-Running Query", "SRV1", AsOf, WindowStart, WindowEnd,
            null, null, "Unknown");

        Assert.Equal(withIncident.Count, withoutIncident.Count);
    }

    /* ═══════════════════════════ Forced Plan Failing: exact cells ═══════════════════════════ */

    [Fact]
    public void ForcedPlanFailing_BuildsHeaderStatusPlanCorrectionsAndTimeline()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Forced Plan Failing");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "Forced Plan Failing", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(4, cells.Count);

        Assert.Equal("header", (string?)cells[0]!.AsObject()["type"]);
        Assert.Equal("status", (string?)cells[1]!.AsObject()["type"]);

        var corrections = cells[2]!.AsObject();
        Assert.Equal("read", (string?)corrections["type"]);
        Assert.Equal("get_plan_corrections", (string?)corrections["read"]);
        Assert.Equal("25", (string?)corrections["params"]!["limit"]);
        Assert.Equal("24", (string?)corrections["params"]!["hours"]);
        Assert.Equal("true", (string?)corrections["params"]!["full_text"]);

        var timeline = cells[3]!.AsObject();
        Assert.Equal("panel", (string?)timeline["type"]);
        Assert.Equal("plan_correction", (string?)timeline["source"]);
        Assert.Equal("plan_correction_captures", (string?)timeline["measure"]);
        Assert.Equal("line", (string?)timeline["viz"]);
    }

    [Fact]
    public void ForcedPlanFailing_NeverEmitsGetQueryStoreRegressionsAsAnAutoRunCell()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Forced Plan Failing");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "Forced Plan Failing", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");

        // get_query_store_regressions must never appear as a "read" (auto-run) cell -- the issue keeps it
        // click-to-run, and the notebook schema has no non-auto-run read cell type to bind it to instead.
        var regressionReads = cells.OfType<JsonObject>()
            .Where(c => (string?)c["type"] == "read" && (string?)c["read"] == "get_query_store_regressions")
            .ToArray();

        Assert.Empty(regressionReads);
    }

    [Fact]
    public void ForcedPlanFailing_NoMatchedIncident_StillDegradesWithSameCellCount()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Forced Plan Failing");
        Assert.NotNull(template);

        var withIncident = template!.Value.BuildCells(
            "Forced Plan Failing", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");
        var withoutIncident = template.Value.BuildCells(
            "Forced Plan Failing", "SRV1", AsOf, WindowStart, WindowEnd,
            null, null, "Unknown");

        Assert.Equal(withIncident.Count, withoutIncident.Count);
    }
}
