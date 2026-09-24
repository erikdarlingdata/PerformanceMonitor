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
/// #3898 D3 head pin for <c>analyze_procedure_plan</c> (wave E, lane e2): a twin whose key PARAMETER differs by
/// product (sql_handle on Darling, plan_handle on Lite), so per D6 the head states both in one clause rather than
/// picking a SKU-specific word. Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>. Lite's
/// twin is <c>Lite.Tests/McpToolGuideHeadsAnalyzeProcedurePlanTests</c>.
/// </summary>
public sealed class McpToolGuideHeadsAnalyzeProcedurePlanTests
{
    private const string Tool = "analyze_procedure_plan";

    /// <summary>The guardrail facts the head must state: the differing key (D6), the two miss statuses in the
    /// order <c>AnalyzeProcedurePlan</c> checks them (not_collected before unavailable — see the tool body), the
    /// missing-index guardrails the plan-tool truth pins keep in every plan tool's head (the impact_basis label;
    /// corroboration, never a diagnosis; the fixed caveat's regression-risk clause), and the operator cut's basis
    /// and counts.</summary>
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

    /// <summary>D9: nothing dropped. Every sentence of the pre-#3898 description, including Darling's
    /// product-specific first sentence naming sql_handle, survives verbatim in the tail get_tool_guide serves.</summary>
    [Fact]
    public void Tail_KeepsEveryOriginalSentence_IncludingTheProductSpecificFirstOne()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.Contains("Analyzes a stored execution plan captured from procedure stats by sql_handle.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Use after get_top_procedures_by_cpu to understand why a procedure is expensive.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("operators_returned / total_operators / truncated", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.", served.Tail!, StringComparison.Ordinal);
    }
}
