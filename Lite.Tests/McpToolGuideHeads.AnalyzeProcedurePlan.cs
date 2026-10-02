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
/// #3898 D3 head pin for <c>analyze_procedure_plan</c>'s Lite twin (wave E, lane e2). Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsAnalyzeProcedurePlanTests</c>, which also holds the D6 rationale: the key
/// PARAMETER differs by product (sql_handle on Darling, plan_handle on Lite), so the head names both in one
/// clause rather than picking a SKU-specific word.
/// </summary>
public sealed class McpToolGuideHeadsAnalyzeProcedurePlanTests
{
    private const string Tool = "analyze_procedure_plan";

    private static readonly string[] HeadFacts =
    [
        "by sql_handle (Darling) or plan_handle (Lite)",
        "No plan: not_collected if the engine can't collect procedure_stats, else unavailable.",
        "labelled impact_basis",
        "corroboration for a statement already measured slow, never a diagnosis",
        "every row carries the fixed caveat (regression risk for other plans, write cost)",
        "top_operators by operators_ranked_by, with operators_returned / total_operators / truncated.",
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

    /// <summary>D9: nothing dropped. Every sentence of the pre-#3898 description, including Lite's
    /// product-specific first sentence naming plan_handle, survives verbatim in the tail get_tool_guide serves.</summary>
    [Fact]
    public void Tail_KeepsEveryOriginalSentence_IncludingTheProductSpecificFirstOne()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.Contains("Analyzes an execution plan from procedure stats by plan_handle.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Use after get_top_procedures_by_cpu to understand why a procedure is expensive.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("operators_returned / total_operators / truncated", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.", served.Tail!, StringComparison.Ordinal);
    }
}
