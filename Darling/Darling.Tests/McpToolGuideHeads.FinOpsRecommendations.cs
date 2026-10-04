/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Reflection;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Head pins for the FinOps Recommendations list: <c>get_finops_recommendations</c>. Follows the pattern in
/// <see cref="McpToolGuideHeadsPvsTests"/>. The tool is not registered with the host yet, so the head and tail are
/// split from its description attribute rather than read from the served list; the head is measured with the
/// pointer the host appends.
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

    private static (string Served, string? Tail, string[] Parameters) Split(string tool)
    {
        Assert.Equal("get_finops_recommendations", tool);
        var method = typeof(DarlingMcpFinOpsRecommendationsTools).GetMethod(nameof(DarlingMcpFinOpsRecommendationsTools.GetFinOpsRecommendations))!;
        var (head, tail) = McpToolGuide.Split(method.GetCustomAttribute<DescriptionAttribute>()!.Description);
        var parameters = Array.ConvertAll(
            Array.FindAll(method.GetParameters(), p => p.GetCustomAttribute<DescriptionAttribute>() is not null),
            p => p.GetCustomAttribute<DescriptionAttribute>()!.Description);
        return (head + McpToolGuide.GuidePointer, tail, parameters);
    }

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = Split(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.Parameters, p => Assert.True(p.Length <= 200, $"{tool}: parameter description {p.Length} > 200"));
        }

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, Split(tool).Served, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ReadingGuidance_NotRepeatedInHead_StaysInTail()
    {
        var served = Split("get_finops_recommendations");
        Assert.DoesNotContain("skipped_checks", served.Served, StringComparison.Ordinal);
        Assert.Contains("skipped_checks names each check", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("est_savings_usd_month is a rough", served.Tail!, StringComparison.Ordinal);
    }
}
