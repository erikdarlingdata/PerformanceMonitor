/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane: <c>get_query_store_clutter</c> and
/// <c>get_oversized_plan_backlog</c>, both Darling-only (no Lite twin). Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_query_store_clutter",
        "get_oversized_plan_backlog",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_query_store_clutter", "Clutter (read_cost/plan_churn/config) is per DATABASE"),
        ("get_query_store_clutter", "qs_overhead (waits/memory clerk) is per SERVER"),
        ("get_query_store_clutter", "No rows answers unavailable, never a clean bill"),
        ("get_query_store_clutter", "Verdict Unknown means unmeasured, never quietly Healthy"),
        ("get_query_store_clutter", "REPLICAS: excluded, verdict Unknown, never a defect"),
        ("get_query_store_clutter", "query_capture_mode null means never asked, never NONE"),
        ("get_query_store_clutter", "window_truncated is a retention floor, not a page cut"),
        ("get_query_store_clutter", "fleet_median is null unless include_fleet_median=true"),
        ("get_oversized_plan_backlog", "524288 bytes"),
        ("get_oversized_plan_backlog", "No time window: a worklist updated in place, not a series"),
        ("get_oversized_plan_backlog", "Read last_captured_at/last_expired_at FIRST"),
        ("get_oversized_plan_backlog", "pending+captured+expired=total_rows"),
        ("get_oversized_plan_backlog", "include_rows requires server_name"),
        ("get_oversized_plan_backlog", "Permanently empty below schema V121"),
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

    /// <summary>D4: the two issue-number references and the one numeric anecdote in
    /// <c>get_query_store_clutter</c>'s original text are off the wire's head; the rung-naming and cross-tool
    /// facts they carried survive in the tail (dropcheck.py's one reported "missing" chunk for this tool is
    /// this removal, folded into a longer run-on sentence its splitter cannot break).</summary>
    [Fact]
    public void QueryStoreClutter_D4Removals_DropTheIssueRefsAndAnecdote_KeepTheRules()
    {
        var served = McpToolGuideTests.Served("get_query_store_clutter");
        Assert.DoesNotContain("#3502", served.Served + served.Tail, StringComparison.Ordinal);
        Assert.DoesNotContain("#3796", served.Served + served.Tail, StringComparison.Ordinal);
        Assert.DoesNotContain("92-96%", served.Served + served.Tail, StringComparison.Ordinal);
        Assert.Contains("get_collection_health's verdict figure", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("predates the V137 rung that added the column", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D2: <c>include_fleet_median</c>'s parameter description was cut from 322 to 190 characters; its
    /// full original wording moves into the tool's own tail rather than being dropped.</summary>
    [Fact]
    public void QueryStoreClutter_IncludeFleetMedianParameter_IsCutToAGuardrail_WithTheRestInTheTail()
    {
        var served = McpToolGuideTests.Served("get_query_store_clutter");
        var param = served.ParameterDescriptionLengths.Single(p => p.Parameter == "include_fleet_median");
        Assert.True(param.Length is > 0 and <= 200, $"include_fleet_median: {param.Length}");
        Assert.Contains("include_fleet_median's own original wording, unabbreviated", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("it walks query_store_stats and wait_stats fleet-wide over the window", served.Tail!, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane's second drop: <c>get_query_duration_trend</c> and
/// <c>get_procedure_duration_trend</c>, twins on both products (D6). <c>get_query_store_duration_trend</c>
/// shares the same source file but is out of scope for this drop; see the Handoff note in the PR that added
/// this class. Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailDurationTrendTests
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

    /// <summary>The tail keeps the original opening sentence and the original wording Darling's rollup route
    /// carries; Lite's own tail says "Lite has one tier" instead, never this Darling-only clause. D6's generic
    /// cross-SKU check (<see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>)
    /// already pins that these two heads match Lite's byte for byte, so nothing family-specific is needed for
    /// that half here.</summary>
    [Fact]
    public void QueryDurationTrend_TailKeepsOriginalSentence_AndDarlingsRollupRouteDetail()
    {
        var served = McpToolGuideTests.Served("get_query_duration_trend");
        Assert.Contains("Gets a time-series of average query duration over time.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("The hourly rollup route divides by the bucket width and has no unrated point.", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("Lite has one tier", served.Tail!, StringComparison.Ordinal);
    }
}
