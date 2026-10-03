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
/// Head pins for the "FinOps" family: <c>get_finops</c>, the grouped FinOps tool. Follows the pattern in
/// <see cref="McpToolGuideHeadsPvsTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsFinOpsTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_finops",
    ];

    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_finops", "Windowed over hours_back, UTC; no as_of."),
        ("get_finops", "high_impact:"),
        ("get_finops", "An unknown view is refused with the valid list."),
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
        var served = McpToolGuideTests.Served("get_finops");
        Assert.DoesNotContain("percent-rank", served.Served, StringComparison.Ordinal);
        Assert.Contains("percent-rank", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("has_plan only says whether a plan was captured", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("impact_band", served.Served, StringComparison.Ordinal);
        Assert.Contains("impact_band is high at 80 and above, medium at 60 and above, else low", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("impact_score 0-100", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("rounded to 0.1", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("No cost fields", served.Tail!, StringComparison.Ordinal);
    }
}
