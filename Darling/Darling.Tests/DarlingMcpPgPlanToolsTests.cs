/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Wire shape for <c>get_pg_plans</c> (#2567), asserted against the real projection rather than a
/// re-implementation of it — a guard that rebuilt the shape would keep passing while the shipped one drifted.
/// </summary>
public class DarlingMcpPgPlanToolsTests
{
    /// <summary>A queryid past 2^53, which is where a JSON number stops being able to hold one exactly.</summary>
    private const long BigQueryId = -8126435036642491494;

    private static List<DarlingPgPlanCaptureReader.PgPlanCaptureRow> Rows() => new()
    {
        new DarlingPgPlanCaptureReader.PgPlanCaptureRow(
            QueryId: BigQueryId,
            PlanHash: "ABC123",
            TopNodeType: "Seq Scan",
            NodeCount: 3,
            Captures: 4,
            TotalDurationMs: 120.5,
            MaxDurationMs: 42.25,
            AvgDurationMs: 30.125,
            PlanJson: """{"Plan":{"Node Type":"Seq Scan","Relation Name":"orders","Filter":"(id > ?)"}}""",
            LastSeen: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
    };

    /// <summary>
    /// <b>queryid is a STRING on the wire.</b> It is a signed int8 spread over the whole 64-bit range, so
    /// most ids are past 2^53 and any consumer decoding JSON numbers as IEEE-754 doubles rounds one. It is
    /// also an equality join key, so a rounded value does not approximate the answer — it matches nothing.
    /// </summary>
    [Fact]
    public void QueryId_IsSerializedAsAString_AndSurvivesExactly()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0];
        var queryId = plan.GetProperty("queryid");

        Assert.Equal(JsonValueKind.String, queryId.ValueKind);
        Assert.Equal("-8126435036642491494", queryId.GetString());

        /* The point of the string, demonstrated: the same value through a double loses its identity. */
        Assert.NotEqual(BigQueryId, (long)(double)BigQueryId);
    }

    /// <summary>
    /// The plan is returned as parsed JSON, not as an opaque string and not as an id to fetch later. #2538
    /// is explicit: an agent has no viewer to follow a reference into, so a pointer is not an answer.
    /// </summary>
    [Fact]
    public void ThePlanItself_IsReturnedAsNavigableJson()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0].GetProperty("plan");

        Assert.Equal(JsonValueKind.Object, plan.ValueKind);
        Assert.Equal("Seq Scan", plan.GetProperty("Plan").GetProperty("Node Type").GetString());
        Assert.Equal("orders", plan.GetProperty("Plan").GetProperty("Relation Name").GetString());
    }

    /// <summary>
    /// <c>captures</c> is named for what it counts. The collector reads an overlapping tail of the server
    /// log, so one execution can be seen twice — calling it <c>executions</c> would invite a reader to
    /// divide by it.
    /// </summary>
    [Fact]
    public void CaptureCount_IsNotPresentedAsAnExecutionCount()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0];

        Assert.Equal(4, plan.GetProperty("captures").GetInt64());
        Assert.False(plan.TryGetProperty("executions", out _));
        Assert.False(plan.TryGetProperty("calls", out _));
    }

    /// <summary>
    /// The response says the plans are redacted. A consumer that does not know this could reasonably read a
    /// placeholder-bearing filter as the literal the query used.
    /// </summary>
    [Fact]
    public void TheResponseSaysThePlansAreRedacted()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var note = doc.RootElement.GetProperty("note").GetString();

        Assert.Contains("REDACTED", note, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("get_pg_top_queries", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// A plan that will not re-parse is still shown, as its raw text. Dropping it would hide the one row
    /// somebody needs to explain, and throwing would take out the whole response for it.
    /// </summary>
    [Fact]
    public void AnUnparseablePlan_IsShownRatherThanDropped()
    {
        var rows = new List<DarlingPgPlanCaptureReader.PgPlanCaptureRow>
        {
            Rows()[0] with { PlanJson = "{not valid json" },
        };

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, rows, 10));

        var plan = doc.RootElement.GetProperty("plans")[0].GetProperty("plan");

        Assert.Equal(JsonValueKind.String, plan.ValueKind);
        Assert.Equal("{not valid json", plan.GetString());
    }

    /* ── get_pg_plan_capture_readiness (#3070) ── */

    /// <summary>
    /// The facets as the shared reader hands them over: causal order, one unsatisfied facet in the middle
    /// of the sequence and one at the end, so a projection that sorted by satisfaction would show.
    /// </summary>
    private static List<DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow> Facets() => new()
    {
        new(Facet: "extension_available", IsSatisfied: true, Observed: "(loaded, and therefore available)",
            Detail: "auto_explain is available on this server.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "library_loaded", IsSatisfied: true, Observed: "auto_explain",
            Detail: "auto_explain must be listed in shared_preload_libraries.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "capture_threshold", IsSatisfied: false, Observed: "-1",
            Detail: "auto_explain IS loaded but log_min_duration is -1, so it captures NOTHING.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "plan_text_setting", IsSatisfied: true, Observed: "json",
            Detail: "The format auto_explain writes plans in.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "plan_attribution", IsSatisfied: false, Observed: "%m [%p] ",
            Detail: "log_line_prefix does NOT carry %Q, so every captured plan is an orphan.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
    };

    /// <summary>
    /// <b><c>detail</c> reaches the wire on every row.</b> It is the per-facet remedy and the reason this
    /// read exists: before #3070 its only reader was the WPF tab, so on a Linux host and to every agent the
    /// remedy did not exist. A projection that carried facet and observed only would look complete.
    ///
    /// <para>The satisfied facets are asserted present for the same reason. The other path to these rows,
    /// <c>UnsatisfiedFacetsAsync</c>, filters to <c>is_satisfied IS NOT TRUE</c> and cannot answer "what is
    /// the state of this server's plan capture" at all — which is the question somebody has when the plans
    /// are missing, and the gap this tool closes rather than duplicating.</para>
    ///
    /// <para>And the ORDER is the reader's, not re-sorted here: the facets have a causal sequence
    /// (<c>library_loaded</c> gates <c>capture_threshold</c>) and the remedies only make sense read in it.
    /// </para>
    /// </summary>
    [Fact]
    public void EveryReadinessFacet_CarriesItsRemedy_InTheReadersCausalOrder()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, Facets(), 25));
        var root = doc.RootElement;

        Assert.Equal(5, root.GetProperty("facet_count").GetInt32());
        Assert.False(root.GetProperty("truncated").GetBoolean());

        var names = new List<string>();
        foreach (var facet in root.GetProperty("facets").EnumerateArray())
        {
            names.Add(facet.GetProperty("facet").GetString()!);
            Assert.False(string.IsNullOrWhiteSpace(facet.GetProperty("detail").GetString()));
            Assert.False(string.IsNullOrWhiteSpace(facet.GetProperty("observed").GetString()));
            Assert.True(facet.TryGetProperty("is_satisfied", out _));
            Assert.True(facet.TryGetProperty("last_observed", out _));
        }

        Assert.Equal(
            new[] { "extension_available", "library_loaded", "capture_threshold", "plan_text_setting", "plan_attribution" },
            names);
    }

    /// <summary>
    /// The unsatisfied facets are NAMED rather than reduced to a verdict. There is deliberately no
    /// ready/not-ready boolean: an unmet <c>plan_attribution</c> still captures plans and merely orphans
    /// them, and <c>message_locale</c> is about every target-side log read rather than about capture, so one
    /// flag would have to pick a meaning and be wrong under the other — the exact collapse the collector
    /// splits its rows to avoid.
    /// </summary>
    [Fact]
    public void TheUnsatisfiedFacetsAreNamed_AndThereIsNoSingleVerdict()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, Facets(), 25));
        var root = doc.RootElement;

        var unmet = root.GetProperty("unsatisfied_facets");
        Assert.Equal(JsonValueKind.Array, unmet.ValueKind);
        Assert.Equal(new[] { "capture_threshold", "plan_attribution" },
            unmet.EnumerateArray().Select(f => f.GetString()).ToArray());

        Assert.False(root.TryGetProperty("ready", out _));
        Assert.False(root.TryGetProperty("is_ready", out _));
    }

    /// <summary>
    /// #2629's lesson, on a read whose row count makes it look unnecessary: a summary taken over a CAPPED
    /// result describes the page and reads as a fact about the server. <c>limit</c> is the caller's, so the
    /// unsatisfied summary is withheld with a sentence saying why rather than computed over what arrived.
    /// </summary>
    [Fact]
    public void ACappedResult_WithholdsTheUnsatisfiedSummary_RatherThanDescribingThePage()
    {
        var firstTwo = Facets().GetRange(0, 2);

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, firstTwo, 2));
        var root = doc.RootElement;

        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("unsatisfied_facets").ValueKind);
        Assert.Contains("TRUNCATED", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }
}
