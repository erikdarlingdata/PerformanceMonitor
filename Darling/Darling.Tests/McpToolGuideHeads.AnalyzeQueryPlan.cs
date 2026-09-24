/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for <c>analyze_query_plan</c> (wave E, lane e1, D9 finish pass). The head is the same
/// guardrail block the plan-tool truth pins share with <c>analyze_plan_xml</c> (#4115) and
/// <c>analyze_procedure_plan</c> (#4117, see <see cref="McpToolGuideHeadsAnalyzeProcedurePlanTests"/> for the
/// twin pattern this file follows). The generic
/// <see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>
/// already proves the served heads match byte for byte across SKUs, so this file pins only the tool's own
/// guardrail facts and that every moved detail is still reachable in the tail. Lite's twin is
/// <c>Lite.Tests/McpToolGuideHeadsAnalyzeQueryPlanTests</c>.
/// </summary>
public sealed class McpToolGuideHeadsAnalyzeQueryPlanTests
{
    private const string Tool = "analyze_query_plan";

    /// <summary>The guardrail facts the head must state: what "latest" means (the lookup's own ORDER BY, not
    /// a query-store-style history), the two miss statuses in the order <c>AnalyzeQueryPlan</c> checks them
    /// (not_collected before unavailable -- see the tool body), the missing-index guardrails the plan-tool
    /// truth pins keep in every plan tool's head (the impact_basis label; corroboration, never a diagnosis; the
    /// fixed caveat's regression-risk clause), the operator cut's basis and counts, and the null-not-zero rule
    /// for actual_* fields (#3541 A12).</summary>
    private static readonly string[] HeadFacts =
    [
        "Analyzes query_hash's latest plan.",
        "No plan: not_collected if the engine can't collect query_stats, else unavailable.",
        "labelled impact_basis",
        "corroboration for a statement already measured slow, never a diagnosis",
        "every row carries the fixed caveat (regression risk for other plans, write cost)",
        "top_operators by operators_ranked_by, with operators_returned / total_operators / truncated.",
        "actual_* are null, not 0, without runtime stats.",
    ];

    [Fact]
    public void Head_CarriesEveryGuardrailFact_AndStaysUnderTheCap()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"{Tool}: served head {served.Served.Length} is over the 620 target");
        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{Tool}.{p.Parameter}: {p.Length} > 200"));

        foreach (var fact in HeadFacts)
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D9: nothing dropped. Every sentence of the pre-#3898 description, including Darling's own
    /// product-specific first sentence, survives verbatim in the tail get_tool_guide serves.</summary>
    [Fact]
    public void Tail_KeepsEveryOriginalSentence_IncludingDarlingsOwnFirstOne()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.StartsWith("Analyzes a stored execution plan captured from query stats by query_hash.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Use after get_top_queries_by_cpu to understand why a query is expensive.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("operators_returned / total_operators / truncated", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("an index is a per-table commitment with write cost and regression risk for other plans, so test it", served.Tail!, StringComparison.Ordinal);
    }
}
