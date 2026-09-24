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
        ("get_query_duration_trend", "unavailable means query_stats was never collected here; empty means it was (the message says quiet window or rollup coverage gap)"),
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

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane's third drop: <c>get_query_store_health</c>, a twin on both
/// products (D6) whose original description was already byte-identical on Darling and Lite, so its tail is
/// too. Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailQueryStoreHealthTests
{
    /// <summary>The per-tool guardrail phrase the head must state.</summary>
    private static readonly string[] HeadFacts =
    [
        "desired READ_WRITE, actual READ_ONLY = the storage-cap failure",
        "ALL (2016/17 default) churns most",
        "AUTO (2019+ default) skips minor ones",
        "CUSTOM tunes AUTO",
        "NONE stops new capture",
        "wait_stats_capture_mode: ON default, OFF empties per-query waits",
        "null on either: pre-rung row, or pre-2017 engine for wait_stats - never OFF",
        "No verdict rendered",
        "No rows = unavailable or not_collected",
        "an unmatched database_name gives database_count 0",
        "LATEST IS A TIME: captured_at is the newest hourly capture",
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        var served = McpToolGuideTests.Served("get_query_store_health");
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head {served.Served.Length} is over the 620 target");
        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"get_query_store_health.{p.Parameter}: {p.Length} > 200"));

        foreach (var fact in HeadFacts)
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D4: the two issue-number references (#3796 on the V137/v64 clause, #3797 on the
    /// get_query_store_clutter cross-reference) and the one anecdote (a specific production store's plan count,
    /// the story of why the V137 rung was added) come off the wire; the rule the anecdote sat beside — ALL
    /// churns plans on an ad hoc workload, and was the 2016/2017 default — survives in the tail untouched.</summary>
    [Fact]
    public void QueryStoreHealth_D4Removals_DropTheIssueRefsAndAnecdote_KeepTheRules()
    {
        var served = McpToolGuideTests.Served("get_query_store_health");
        var wire = served.Served + served.Tail;
        Assert.DoesNotContain("#3796", wire, StringComparison.Ordinal);
        Assert.DoesNotContain("#3797", wire, StringComparison.Ordinal);
        Assert.DoesNotContain("755", wire, StringComparison.Ordinal);
        Assert.DoesNotContain("42 servers", wire, StringComparison.Ordinal);
        Assert.DoesNotContain("production store class", wire, StringComparison.Ordinal);
        Assert.Contains("ALL was the engine default on SQL Server 2016 and 2017.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("by get_query_store_clutter as its churn", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D6: the original description was already byte-identical on Darling and Lite, so the tail (the
    /// full original prose, D4 removals aside) opens and carries its central claim unchanged; the generic
    /// cross-SKU pin
    /// (<see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>)
    /// covers the head, and Lite's own copy of this class pins Lite's served tail directly.</summary>
    [Fact]
    public void QueryStoreHealth_TailKeepsOriginalOpeningSentence_AndCentralClaim()
    {
        var darlingTail = McpToolGuideTests.Served("get_query_store_health").Tail!;
        Assert.Contains("Gets per-database Query Store health", darlingTail, StringComparison.Ordinal);
        Assert.Contains("CAPTURE MODE IS THE PLAN-CHURN KNOB", darlingTail, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane's fourth drop: <c>get_default_trace_events</c>, a twin on both
/// products (D6) whose Darling description (866 served chars) crossed the 800-char conversion line; Lite's own
/// description (702) converts alongside it per D6. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>. No D4 removals: the original text on either product carried
/// no issue references or anecdotes.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailDefaultTraceTests
{
    /// <summary>The per-tool guardrail phrase the head must state.</summary>
    private static readonly string[] HeadFacts =
    [
        "file auto-grow/shrink stalls over 1 second",
        "ErrorLog writes at severity 16+ (a null severity also counts)",
        "over an event_time window ending at as_of, newest first",
        "Config-change events are excluded: use get_server_config_changes / get_database_config_changes / get_trace_flag_changes",
        "Empty: nothing significant in the window, or nothing collected in it",
        "not_collected means this engine has no default trace (Azure SQL Database)",
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        var served = McpToolGuideTests.Served("get_default_trace_events");
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head {served.Served.Length} is over the 620 target");
        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"get_default_trace_events.{p.Parameter}: {p.Length} > 200"));

        foreach (var fact in HeadFacts)
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }
    }

    /// <summary>Darling's tail keeps its own original sentence tying event_time to get_collection_log and
    /// list_servers, which Lite's twin never carried (Lite has no fleet-wide collection_time/last_collection
    /// concept on this surface); Lite's own tail is pinned in <c>Lite.Tests</c>.</summary>
    [Fact]
    public void DefaultTraceEvents_DarlingTailKeepsItsOwnCrossToolSentence()
    {
        var served = McpToolGuideTests.Served("get_default_trace_events");
        Assert.Contains("so it lines up directly against get_collection_log's collection_time and list_servers' last_collection", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("intentionally excluded here to avoid double-counting", served.Tail!, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane's fifth drop: <c>get_resource_semaphore</c> and
/// <c>get_memory_grants</c>, twins on both products (D6) whose original descriptions were already
/// byte-identical on Darling and Lite, so their tails are too. No D4 removals: the original text on either
/// tool carried no issue references or anecdotes. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailMemoryGrantTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_resource_semaphore",
        "get_memory_grants",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_resource_semaphore", "TWO READS, per semaphore+pool: grants[] is the NEWEST snapshot"),
        ("get_resource_semaphore", "window[] aggregates EVERY snapshot"),
        ("get_resource_semaphore", "No rows: unavailable (not_collected first)"),
        ("get_resource_semaphore", "sample_interval_seconds/interval_known null/false on a restart-marker or pre-column row"),
        ("get_memory_grants", "TWO READS: grants[] is the NEWEST snapshot"),
        ("get_memory_grants", "one row per pool"),
        ("get_memory_grants", "window[] aggregates EVERY snapshot in it, per pool"),
        ("get_memory_grants", "No rows: unavailable (or not_collected first)"),
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

    /// <summary>D6: the original descriptions were already byte-identical on Darling and Lite, so each tail
    /// (the full original prose, unchanged) opens with its own original first sentence; the generic cross-SKU
    /// pin (<see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>)
    /// covers the head, and Lite's own copy of this class pins Lite's served tail directly.</summary>
    [Fact]
    public void ResourceSemaphoreAndMemoryGrants_TailsKeepTheirOwnOriginalOpeningSentence()
    {
        var rsTail = McpToolGuideTests.Served("get_resource_semaphore").Tail!;
        var mgTail = McpToolGuideTests.Served("get_memory_grants").Tail!;
        Assert.Contains("Gets resource semaphore statistics showing granted vs available workspace memory against the target/max-target ceiling", rsTail, StringComparison.Ordinal);
        Assert.Contains("Gets resource semaphore statistics showing granted vs available workspace memory per resource pool", mgTail, StringComparison.Ordinal);
    }

    /// <summary>The closing sentence was identical, word for word, on both tools and both products before this
    /// PR (four verbatim copies of the same read-order guidance); it now lives once as a topic that each tail
    /// appends, so <c>get_tool_guide</c> still serves it for every one of the four, unchanged.</summary>
    [Fact]
    public void ResourceSemaphoreAndMemoryGrants_TailsCarryTheSharedReadOrderTopic()
    {
        foreach (var tool in ConvertedTools)
        {
            Assert.EndsWith(McpToolGuideTopics.MemoryGrantWindowReadOrder, McpToolGuideTests.Served(tool).Tail!, StringComparison.Ordinal);
        }
    }
}
