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
/// #3898 D3 head pins for the "SqlTail" lane's Lite twins: <c>get_query_duration_trend</c> and
/// <c>get_procedure_duration_trend</c>. Darling's twin is <c>Darling.Tests/McpToolGuideHeadsSqlTailDurationTrendTests</c>,
/// which also holds the cross-SKU lockstep pin. <c>get_query_store_clutter</c> and
/// <c>get_oversized_plan_backlog</c> (this lane's other drop) are Darling-only and have no entry here. Follows
/// the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_query_duration_trend",
        "get_procedure_duration_trend",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_query_duration_trend", "unrated_collections counts it, and a point left with nothing else carries null rates (unrated_points), never zero"),
        ("get_query_duration_trend", "status empty means a quiet window that has collected before; unavailable means query_stats has never been collected here"),
        ("get_query_duration_trend", "window_truncated is the store's retention floor, not a page cut"),
        ("get_procedure_duration_trend", "charged to the whole call rather than smeared across its statements"),
        ("get_procedure_duration_trend", "the empty/unavailable split and window_truncated (a retention floor, not a page cut) all follow get_query_duration_trend exactly"),
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }
    }

    /// <summary>Lite's tail keeps the original opening sentence and Lite's own "one tier" wording; it never
    /// carries Darling's hourly-rollup-route clause, which lives only in Darling's own tail.</summary>
    [Fact]
    public void QueryDurationTrend_TailKeepsOriginalSentence_AndLitesOwnOneTierDetail()
    {
        var served = McpToolGuideTests.Served("get_query_duration_trend");
        Assert.Contains("Gets a time-series of average query duration over time.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Lite has one tier - nothing is rolled up.", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("hourly rollup route", served.Tail!, StringComparison.Ordinal);
    }
}
