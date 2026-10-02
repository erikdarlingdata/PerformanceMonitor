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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4223's analysis-finding authored template (<c>Analysis: &lt;category&gt; [&lt;hash8&gt;]</c> -&gt;
/// <c>authored/analysis-finding</c>): exact-vs-prefix routing (a <c>Custom:</c> row must not be intercepted),
/// a found finding's summary cell plus its evidence read cell, the aged-out/superseded degrade note (#2710:
/// never a throw), and <c>get_analysis_findings</c>' catalog/dispatch parity (the read cell's own
/// <c>include_drilldown</c>/<c>full_text</c>/<c>limit</c> params must be declared or
/// <see cref="DarlingWebEndpoints.ValidateNotebookDefinition"/> reds every evidence cell).
/// </summary>
public sealed class AlertNotebookTemplateAnalysisFindingsTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";
    private const string Metric = "Analysis: query-regression [abcd1234]";

    private static AnalysisFinding SampleFinding() => new()
    {
        FindingId = 7,
        AnalysisTime = WindowEnd,
        ServerId = 1,
        ServerName = "SRV1",
        DatabaseName = "SalesDb",
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
        Severity = 82.5,
        Confidence = 0.75,
        Category = "query-regression",
        StoryPath = "high_cpu -> query_regression",
        StoryPathHash = "abcd12349999999999999999",
        StoryText = "Query regression detected on SalesDb.",
        FactCount = 2,
    };

    private static AlertNotebookEndpoint.AuthoredContext ContextFor(AnalysisFinding? finding, bool missing) =>
        new(null, false, finding, missing);

    private static JsonArray InvokeAnalysisFindingTemplate(AlertNotebookEndpoint.AuthoredContext context)
    {
        var resolved = AlertNotebookEndpoint.ResolveAuthored(Metric);
        Assert.NotNull(resolved);

        return resolved!.Value.Entry.Invoke(
            Metric, "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: null,
            status: "Firing", context);
    }

    /* ═══════════════════════════ routing ═══════════════════════════ */

    [Fact]
    public void ResolveAuthored_AnalysisPrefix_RoutesToAnalysisFindingTemplate()
    {
        var resolved = AlertNotebookEndpoint.ResolveAuthored(Metric);

        Assert.NotNull(resolved);
        Assert.Equal("authored/analysis-finding", resolved!.Value.Entry.Id);
        Assert.Equal(AlertNotebookEndpoint.AuthoredContextKind.AnalysisFinding, resolved.Value.Kind);
    }

    [Fact]
    public void ResolveAuthored_CustomPrefix_StillRoutesToCustomRule_NotAnalysis()
    {
        var resolved = AlertNotebookEndpoint.ResolveAuthored("Custom:5");

        Assert.NotNull(resolved);
        Assert.Equal("authored/custom-rule", resolved!.Value.Entry.Id);
        Assert.Equal(AlertNotebookEndpoint.AuthoredContextKind.CustomRule, resolved.Value.Kind);
    }

    [Fact]
    public void ResolveAuthored_UnrelatedPrefix_StaysMechanical()
    {
        Assert.Null(AlertNotebookEndpoint.ResolveAuthored("Some Other Metric"));
    }

    /* ═══════════════════════════ found ═══════════════════════════ */

    [Fact]
    public void FoundFinding_SummaryCell_CarriesTheFindingsFields()
    {
        var finding = SampleFinding();
        var cells = InvokeAnalysisFindingTemplate(ContextFor(finding, missing: false));

        var summary = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "markdown");
        var text = (string?)summary["text"];

        Assert.Contains("query-regression", text, StringComparison.Ordinal);
        Assert.Contains("82.5", text, StringComparison.Ordinal);
        Assert.Contains("high_cpu -> query_regression", text, StringComparison.Ordinal);
        Assert.Contains("SalesDb", text, StringComparison.Ordinal);
        Assert.Contains("Query regression detected on SalesDb.", text, StringComparison.Ordinal);
    }

    [Fact]
    public void FoundFinding_EvidenceReadCell_CarriesIncludeDrilldownAndFullText()
    {
        var finding = SampleFinding();
        var cells = InvokeAnalysisFindingTemplate(ContextFor(finding, missing: false));

        var read = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "read");
        Assert.Equal("get_analysis_findings", (string?)read["read"]);

        var parameters = Assert.IsType<JsonObject>(read["params"]);
        Assert.Equal("true", (string?)parameters["include_drilldown"]);
        Assert.Equal("true", (string?)parameters["full_text"]);
        Assert.Equal("SRV1", (string?)parameters["server"]);
        Assert.Equal(AsOf, (string?)parameters["as_of"]);
    }

    [Fact]
    public void FoundFinding_PassesValidateNotebookDefinition()
    {
        var finding = SampleFinding();
        var cells = InvokeAnalysisFindingTemplate(ContextFor(finding, missing: false));

        var notebook = new JsonObject { ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(notebook);

        Assert.True(validation.IsValid, validation.Error);
    }

    /* ═══════════════════════════ missing ═══════════════════════════ */

    [Fact]
    public void MissingFinding_ProducesNoteCell_NeverAnError()
    {
        var cells = InvokeAnalysisFindingTemplate(ContextFor(null, missing: true));

        var note = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "markdown");
        Assert.Contains("no longer", (string?)note["text"], StringComparison.Ordinal);

        // header + status + the note, nothing else -- no evidence read was ever attempted.
        Assert.Equal(3, cells.Count);
    }

    [Fact]
    public void MissingFinding_PassesValidateNotebookDefinition()
    {
        var cells = InvokeAnalysisFindingTemplate(ContextFor(null, missing: true));

        var notebook = new JsonObject { ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(notebook);

        Assert.True(validation.IsValid, validation.Error);
    }

    /* ═══════════════════════════ catalog/dispatch parity ═══════════════════════════ */

    /// <summary>#4223: <c>get_analysis_findings</c>' catalog row (<see cref="DarlingWebEndpoints.CatalogDescriptors"/>)
    /// used to declare only <c>server</c>/<c>hours</c>/<c>as_of</c>, while the dispatch row also read
    /// <c>limit</c>, <c>include_drilldown</c> and <c>full_text</c> off the request — an evidence read cell
    /// passing any of those three failed <c>ValidateNotebookDefinition</c>'s undeclared-parameter check.</summary>
    [Fact]
    public void GetAnalysisFindings_CatalogDeclaresLimitIncludeDrilldownAndFullText()
    {
        var descriptor = DarlingWebEndpoints.CatalogDescriptors["get_analysis_findings"];
        var names = descriptor.Params.Select(p => p.Name).ToArray();

        Assert.Contains("limit", names);
        Assert.Contains("include_drilldown", names);
        Assert.Contains("full_text", names);
    }
}
