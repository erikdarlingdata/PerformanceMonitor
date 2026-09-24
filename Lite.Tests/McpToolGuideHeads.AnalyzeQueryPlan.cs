/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 D3 head pins for <c>analyze_query_plan</c> (lane e1, wave E), a twin on Darling
/// (<c>DarlingMcpPlanTools.cs</c>). The generic
/// <see cref="Darling.Tests.McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>
/// (Darling.Tests only; it reads both SKUs' source) already proves the served heads match byte for byte, so
/// this file pins only the tool's own guardrail facts and that every moved detail is still reachable in the
/// tail.
/// </summary>
public sealed class McpToolGuideHeadsAnalyzeQueryPlanTests
{
    private const string Tool = "analyze_query_plan";

    [Fact]
    public void Head_StatesTheOperatorCap_TheRankingFallback_AndTheNullNotZeroRule()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head {served.Served.Length} is over the 620 target");

        /* top_operators: the cap (10), the ranking fallback (measured elapsed, else the optimizer's cost
           share), and that the cut is never silent. */
        Assert.Contains("up to 10/statement, ranked by actual_elapsed_ms, else the optimizer's cost_percent estimate", served.Served, StringComparison.Ordinal);
        /* #3541 A12: null is "not measured" here, never "measured as zero" -- actual_elapsed_ms and its
           siblings are null, not 0, on a statement with no runtime statistics. */
        Assert.Contains("actual_* fields are null, not 0, without runtime stats.", served.Served, StringComparison.Ordinal);
        /* missing_indexes: impact_basis is a per-statement estimate, not an additive score across statements. */
        Assert.Contains("impact_basis: a per-statement optimizer estimate, not additive", served.Served, StringComparison.Ordinal);
        /* The "never a diagnosis" advisory caveat every missing-index row carries. */
        Assert.Contains("create_statement corroborates, never a diagnosis", served.Served, StringComparison.Ordinal);

        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{p.Parameter}: {p.Length}"));
    }

    /// <summary>D9: the two miss statuses this tool actually returns, and that neither reads as the other.
    /// <c>not_collected</c> is a permanent per-engine capability gap (checked before the hash lookup even
    /// runs); <c>unavailable</c> is a real miss for this one hash. Status routes: AnalyzeQueryPlan checks
    /// <c>McpEngineCapability.NotCollectedStatusAsync</c> first and falls back to
    /// <c>McpHelpers.Status("unavailable", ...)</c> only when that returns null.</summary>
    [Fact]
    public void Head_StatesBothMissStatuses_AndTheirGroundTruth()
    {
        var served = McpToolGuideTests.Served(Tool).Served;
        Assert.Contains("not_collected: engine never captures query_stats.", served, StringComparison.Ordinal);
        Assert.Contains("unavailable: no stored plan", served, StringComparison.Ordinal);
        Assert.Contains("never captured, aged out, or evicted.", served, StringComparison.Ordinal);
    }

    /// <summary>D3: nothing in Lite's original prose was dropped, only moved. The one sentence that differs
    /// from Darling's (the source of the plan) opens Lite's own tail, and every other original sentence --
    /// including the fragments <c>McpDescriptionTruthPinTests</c> already pins against the whole compiled
    /// description -- is still there verbatim.</summary>
    [Fact]
    public void Tail_CarriesLitesOwnOriginalSentences_Verbatim()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail;
        Assert.NotNull(tail);
        Assert.StartsWith("Analyzes an execution plan from the plan cache by query_hash.", tail, StringComparison.Ordinal);
        Assert.Contains("Use after get_top_queries_by_cpu to understand why a query is expensive.", tail, StringComparison.Ordinal);
        Assert.Contains("operators_returned / total_operators / truncated", tail, StringComparison.Ordinal);
        Assert.Contains("operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.", tail, StringComparison.Ordinal);
        Assert.Contains("an index is a per-table commitment with write cost and regression risk for other plans, so test it", tail, StringComparison.Ordinal);
    }
}
