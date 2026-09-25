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
/// #3898 D3 head pins for the "CollectionLog" mini-family's Lite twins: <c>get_collection_log</c> (in
/// <c>McpHealthTools</c>) and <c>get_query_store_top</c> (in <c>McpQueryTools</c>). Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsCollectionLogTests</c>.
/// </summary>
public sealed class McpToolGuideHeadsCollectionLogTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_collection_log",
        "get_query_store_top",
    ];

    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_collection_log", "NEWEST FIRST by default"),
        ("get_collection_log", "cost-ranked sample's age, not reach"),
        ("get_collection_log", "an unknown value is refused, never silently empty"),
        ("get_query_store_top", "window_truncated"),
        ("get_query_store_top", "not a page cut"),
        ("get_query_store_top", "raw-tier retention floor"),
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
    public void CollectionLogTail_CarriesTheThreeHeldBackParameterRemainders()
    {
        var tail = McpToolGuideTests.Served("get_collection_log").Tail!;
        Assert.Contains("collector_name: A name this server has never run returns the no-matches status", tail, StringComparison.Ordinal);
        Assert.Contains("min_duration_ms: Applied in SQL before the cap.", tail, StringComparison.Ordinal);
        Assert.Contains("status: THE FAILURE FILTER", tail, StringComparison.Ordinal);
    }

    /// <summary>D4: the issue reference that used to sit beside the sql_duration_ms precision note is off the
    /// wire; the rule it carries (Lite never enables the deferred fetches, so none of Darling's store-probe
    /// share applies here) stays, in the tail.</summary>
    [Fact]
    public void CollectionLogTail_DropsTheIssueReference_ButKeepsTheRule()
    {
        var tail = McpToolGuideTests.Served("get_collection_log").Tail!;
        Assert.DoesNotContain("#3192", tail, StringComparison.Ordinal);
        Assert.Contains("this SKU never enables the deferred plan-XML or statement-text fetches", tail, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4231: Lite gained the same raw-tier window floor Darling already had, so the head no longer splits
    /// "Darling: has one. Lite: does not" -- it names the same disclosure both SKUs now carry.
    /// </summary>
    [Fact]
    public void QueryStoreTop_HeadNamesTheSameDisclosureAsDarling()
    {
        var served = McpToolGuideTests.Served("get_query_store_top");
        Assert.Contains("window_truncated", served.Served, StringComparison.Ordinal);
        Assert.Contains("Same disclosure as Darling's get_query_store_top", served.Served, StringComparison.Ordinal);
    }
}
