/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for the "Trend" family: <c>get_query_store_duration_trend</c> and
/// <c>get_perfmon_trend</c>, converted out of <c>DarlingMcpTrendTools</c> (Darling) and their Lite twins.
/// Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsTrendTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_perfmon_trend",
        "get_query_store_duration_trend",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_perfmon_trend", "No points never returns empty"),
        ("get_perfmon_trend", "never delta_value alone or when the interval is 0"),
        ("get_perfmon_trend", "classify by name (ends in /sec = rate)"),
        ("get_query_store_duration_trend", "not_collected: engine cannot run Query Store."),
        ("get_query_store_duration_trend", "unavailable: never sampled here."),
        ("get_query_store_duration_trend", "empty: quiet on Lite always; on Darling, empty can also be a rollup coverage gap"),
        ("get_query_store_duration_trend", "each interval counted once, at the hour it ran"),
        ("get_query_store_duration_trend", "window_truncated marks the retention floor, not a page cut"),
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

    /// <summary>get_perfmon_trend's miss vocabulary is not_collected/unavailable only: unlike its trend
    /// siblings it never returns status empty (#4098-era D9 trap). The head must say so, and the tail (the
    /// original prose, unchanged) must still carry the four counter_kind branches behind it.</summary>
    [Fact]
    public void PerfmonTrend_NeverEmpty_AndTailKeepsAllFourCounterKinds()
    {
        var served = McpToolGuideTests.Served("get_perfmon_trend");
        Assert.Contains("No points never returns empty:", served.Served, StringComparison.Ordinal);
        Assert.Contains("unavailable: no counter at all collected in the window.", served.Served, StringComparison.Ordinal);
        Assert.StartsWith("Gets one performance counter over time in time buckets.", served.Tail!, StringComparison.Ordinal);
        Assert.EndsWith(BaselineDiscontinuities.DescriptionSentence, served.Tail!, StringComparison.Ordinal);
        foreach (var kind in new[] { "'gauge'", "'rate'", "'other'" })
        {
            Assert.Contains(kind, served.Tail!, StringComparison.Ordinal);
        }
    }

    /// <summary>get_query_store_duration_trend's empty answer means different things on the two products
    /// (Darling has a rollup tier that can leave a coverage gap; Lite has none, so its empty is always a
    /// genuinely quiet window): the shared head states both in one clause per D6, and each product's own
    /// tail (the original prose, unchanged) keeps its own routing detail.</summary>
    [Fact]
    public void QueryStoreDurationTrend_TailKeepsItsOwnRoutingDetail_DarlingSide()
    {
        var served = McpToolGuideTests.Served("get_query_store_duration_trend");
        Assert.StartsWith("Gets a time-series of Query Store duration per second and executions per second over time, summed across every query.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("A rollup point (an hourly bucket the corrected rollup has materialized) is rated over its bucket width", served.Tail!, StringComparison.Ordinal);
        Assert.EndsWith(BaselineDiscontinuities.DescriptionSentence, served.Tail!, StringComparison.Ordinal);
        Assert.Contains("window_truncated is true when the store did not hold the start of the window", served.Tail!, StringComparison.Ordinal);
    }
}
