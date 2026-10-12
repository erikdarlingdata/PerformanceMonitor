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
/// Head pins for the FinOps Recommendations list: <c>get_finops_recommendations</c>. Follows the pattern in
/// <see cref="McpToolGuideHeadsPvsTests"/>. The head and tail are read from the served tools/list.
/// </summary>
public sealed class McpToolGuideHeadsFinOpsRecommendationsTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_finops_recommendations",
    ];

    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_finops_recommendations", "High severity first"),
        ("get_finops_recommendations", "no hours_back, limit or as_of"),
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

    [Fact]
    public void ReadingGuidance_NotRepeatedInHead_StaysInTail()
    {
        var served = McpToolGuideTests.Served("get_finops_recommendations");
        Assert.DoesNotContain("skipped_checks", served.Served, StringComparison.Ordinal);
        Assert.Contains("skipped_checks names each check", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("est_savings_usd_month is a rough", served.Tail!, StringComparison.Ordinal);
    }
}
