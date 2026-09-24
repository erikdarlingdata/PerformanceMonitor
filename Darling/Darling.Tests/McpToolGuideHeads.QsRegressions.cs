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
/// #3898 D3 head pins for <c>get_query_store_regressions</c> and its Lite twin. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsQsRegressionsTests
{
    private const string Tool = "get_query_store_regressions";

    /// <summary>Guardrail facts the head must carry: the disambiguator against get_query_store_top, the CPU
    /// gate, the null-vs-0% percent semantics tied to the RIGHT fields (cpu_regression_percent's own WHERE
    /// gate means it is never null in a returned row, so only duration and io are named), the ranking key, and
    /// all three status words zero rows can arrive under.</summary>
    private static readonly string[] HeadFacts =
    [
        "get_query_store_top ranks EXPENSIVE, this ranks CHANGED.",
        "Gated: average CPU regressed over 25%.",
        "duration_regression_percent and io_regression_percent are null, not 0%, when their baseline is 0",
        "severity is null with the former",
        "additional_duration_ms is the ranking key.",
        "empty: no regression (all clear), or a baseline with nothing yet in the window.",
        "unavailable: no baseline exists yet.",
        "not_collected: this server's engine cannot run Query Store.",
    ];

    [Fact]
    public void Head_CarriesEveryGuardrailFact_AndThePointer_UnderBudget()
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

    /// <summary>Nothing dropped: the original first sentence, the get_query_store_top disambiguator, the
    /// undefined_percents reasoning and the four-rung empty-regressions explanation all still ride, verbatim,
    /// in the tail get_tool_guide serves.</summary>
    [Fact]
    public void Tail_CarriesOriginalProse_Verbatim()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail!;
        Assert.StartsWith("Finds queries whose Query Store performance got WORSE, by comparing each (database, query_id) group's averages", tail, StringComparison.Ordinal);
        Assert.Contains("get_query_store_top answers what is EXPENSIVE; the most expensive query is usually the one that always was.", tail, StringComparison.Ordinal);
        Assert.Contains("never as 0, which would read as no change when the truth is the largest possible one", tail, StringComparison.Ordinal);
        Assert.Contains("so a null percent never sorts as 0.", tail, StringComparison.Ordinal);
    }
}
