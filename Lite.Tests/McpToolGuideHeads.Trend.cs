/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 D3 head pins for the "Trend" family's Lite twins: <c>get_query_store_duration_trend</c> and
/// <c>get_perfmon_trend</c>. Darling's twin is <c>Darling.Tests/McpToolGuideHeadsTrendTests</c>, which also
/// holds the cross-SKU lockstep pin.
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

    /// <summary>get_perfmon_trend's miss vocabulary is not_collected/unavailable only, byte-identical on both
    /// products: it never returns status empty. The tail (the original prose, unchanged) still carries the
    /// four counter_kind branches behind the head's terse summary.</summary>
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

    /// <summary>Lite has no rollup tier, so get_query_store_duration_trend's empty is always a genuinely
    /// quiet window there; the shared head says so in the same clause that states Darling's rollup-coverage-gap
    /// case (D6), and Lite's own tail (the original prose, unchanged) never mentions a rollup.</summary>
    [Fact]
    public void QueryStoreDurationTrend_TailKeepsItsOwnRoutingDetail_LiteSide()
    {
        var served = McpToolGuideTests.Served("get_query_store_duration_trend");
        Assert.StartsWith("Gets a time-series of Query Store duration per second and executions per second over time, summed across every query.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Every point is a rate over the gap since the PREVIOUS point", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("rollup", served.Tail!, StringComparison.Ordinal);
        Assert.EndsWith(BaselineDiscontinuities.DescriptionSentence, served.Tail!, StringComparison.Ordinal);
    }
}
