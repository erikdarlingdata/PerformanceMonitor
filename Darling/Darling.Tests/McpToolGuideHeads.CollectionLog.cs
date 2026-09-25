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
/// #3898 D3 head pins for the "CollectionLog" mini-family: <c>get_collection_log</c> and
/// <c>get_query_store_top</c>, converted out of <c>DarlingMcpDataTools</c> (Darling) and their Lite twins.
/// Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>. A separate file from
/// <see cref="McpToolGuideHeadsDataTests"/> so the two lanes converting Data-family tools in parallel never
/// conflict on one file.
/// </summary>
public sealed class McpToolGuideHeadsCollectionLogTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_collection_log",
        "get_query_store_top",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_collection_log", "NEWEST FIRST by default"),
        ("get_collection_log", "cost-ranked sample's age, not reach"),
        ("get_collection_log", "an unknown value is refused, never silently empty"),
        ("get_query_store_top", "window_truncated"),
        ("get_query_store_top", "not a page cut"),
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

    /// <summary>The three collection_log parameters over D2's 200-char cap keep their guardrail half on the
    /// wire and carry the rest into the tail, labelled by name so a reader of the guide can find them.</summary>
    [Fact]
    public void CollectionLogTail_CarriesTheThreeHeldBackParameterRemainders()
    {
        var tail = McpToolGuideTests.Served("get_collection_log").Tail!;
        Assert.Contains("collector_name: A name this server has never run returns the no-matches status", tail, StringComparison.Ordinal);
        Assert.Contains("min_duration_ms: Applied in SQL before the cap.", tail, StringComparison.Ordinal);
        Assert.Contains("status: THE FAILURE FILTER", tail, StringComparison.Ordinal);
    }

    /// <summary>#4231: get_query_store_top's window-floor clause (#2364/#3653) is now one shared, byte-identical
    /// head across both SKUs -- neither the old "Darling: " qualifier nor the false "Lite: no such floor" line
    /// survives.</summary>
    [Fact]
    public void QueryStoreTop_WindowFloorClause_IsSharedAcrossBothSkusInTheHead()
    {
        var served = McpToolGuideTests.Served("get_query_store_top");
        Assert.Contains("window_truncated", served.Served, StringComparison.Ordinal);
        Assert.Contains("not a page cut", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("Darling: window_truncated", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("Lite: no such floor", served.Served, StringComparison.Ordinal);
    }
}
