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
/// #3898 D3 head pins for the "SqlTail" lane's Lite twins: <c>get_query_duration_trend</c> and
/// <c>get_procedure_duration_trend</c>. Darling's twin is <c>Darling.Tests/McpToolGuideHeadsSqlTailDurationTrendTests</c>,
/// which also holds the cross-SKU lockstep pin. <c>get_query_store_clutter</c> and
/// <c>get_oversized_plan_backlog</c> (this lane's other drop) are Darling-only and have no entry here. Follows
/// the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailTests
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

    /// <summary>Lite's tail keeps the original opening sentence and Lite's own "one tier" wording; it never
    /// carries Darling's hourly-rollup-route clause, which lives only in Darling's own tail.</summary>
    [Fact]
    public void QueryDurationTrend_TailKeepsOriginalSentence_AndLitesOwnOneTierDetail()
    {
        var served = McpToolGuideTests.Served("get_query_duration_trend");
        Assert.Contains("Gets a time-series of average query duration over time.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Lite has one tier - nothing is rolled up.", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("hourly rollup route", served.Tail!, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane's third drop: <c>get_query_store_health</c>, a twin on both
/// products (D6) whose original description was already byte-identical on Darling and Lite, so its tail is
/// too. Darling's twin also pins the D4 removals (two issue refs and one anecdote). Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
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

    /// <summary>D4, Lite's side: the same two issue-number references and the one anecdote Darling's twin
    /// pins are gone from Lite's served tail too (the original text was byte-identical on both products).</summary>
    [Fact]
    public void QueryStoreHealth_D4Removals_DropTheIssueRefsAndAnecdote_KeepTheRules()
    {
        var served = McpToolGuideTests.Served("get_query_store_health");
        var wire = served.Served + served.Tail;
        Assert.DoesNotContain("#3796", wire, StringComparison.Ordinal);
        Assert.DoesNotContain("#3797", wire, StringComparison.Ordinal);
        Assert.DoesNotContain("755", wire, StringComparison.Ordinal);
        Assert.DoesNotContain("production store class", wire, StringComparison.Ordinal);
        Assert.Contains("ALL was the engine default on SQL Server 2016 and 2017.", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("by get_query_store_clutter as its churn", served.Tail!, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane's fourth drop: <c>get_default_trace_events</c>, a twin on both
/// products (D6). Lite's own description (702 served chars) converts because Darling's twin (866) crossed the
/// 800-char line. Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>. No D4 removals: the
/// original text on either product carried no issue references or anecdotes.
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

    /// <summary>Lite's tail keeps its own original, simpler wording; it never carries Darling's cross-tool
    /// sentence tying event_time to get_collection_log and list_servers (Darling's twin pins that sentence in
    /// its own tail).</summary>
    [Fact]
    public void DefaultTraceEvents_LiteTailKeepsItsOwnSimplerWording()
    {
        var served = McpToolGuideTests.Served("get_default_trace_events");
        Assert.Contains("event_time is UTC, the same frame as as_of", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("get_collection_log's collection_time", served.Tail!, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3898 D3 head pins for the "SqlTail" lane's fifth drop, Lite's twin: <c>get_resource_semaphore</c> and
/// <c>get_memory_grants</c>. Darling's twin is <c>Darling.Tests/McpToolGuideHeadsSqlTailMemoryGrantTests</c>,
/// which also holds the cross-SKU lockstep pin. No D4 removals: the original text on either tool carried no
/// issue references or anecdotes. Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
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

    /// <summary>D6, Lite's side: the original descriptions were already byte-identical on Darling and Lite, so
    /// each tail (the full original prose, unchanged) opens with its own original first sentence.</summary>
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

/// <summary>
/// #3898 D1 head pins for the "SqlTail" lane's sixth drop, Lite's twin: <c>get_plan_corrections</c>. Darling's
/// twin is <c>Darling.Tests/McpToolGuideHeadsSqlTailPlanCorrectionsTests</c>, which also holds the cross-SKU
/// lockstep pin. No D4 removals: the original text carried no issue references or anecdotes. Follows the
/// pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlTailPlanCorrectionsTests
{
    /// <summary>The per-tool guardrail phrase the head must state.</summary>
    private static readonly string[] HeadFacts =
    [
        "Rows recur per capture, not per distinct recommendation.",
        "THE PAGE IS BOUNDED BY limit, NOT hours_back: truncated means more rows existed; oldest/newest_returned_collection_time bound the page.",
        "automatic_tuning ignores the window: a latest snapshot, as_of says when.",
        "All timestamps are UTC.",
        "No rows and no automatic_tuning: empty (not_collected checked first).",
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        var served = McpToolGuideTests.Served("get_plan_corrections");
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head {served.Served.Length} is over the 620 target");
        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"get_plan_corrections.{p.Parameter}: {p.Length} > 200"));

        foreach (var fact in HeadFacts)
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D6: the original description was already byte-identical on Darling and Lite, so the tail (the
    /// full original prose, unchanged) opens with its own original first sentence and keeps the UTC-ordering
    /// claim closing it; the generic cross-SKU pin
    /// (<see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>)
    /// covers the head, and Darling's own copy of this class pins the cross-SKU byte-identical head.</summary>
    [Fact]
    public void PlanCorrections_TailKeepsOriginalOpeningSentence_AndClosingUtcClaim()
    {
        var served = McpToolGuideTests.Served("get_plan_corrections");
        Assert.Contains("Gets SQL Server automatic plan correction (APC) activity: the engine's FORCE_LAST_GOOD_PLAN recommendations and actions over the window, NEWEST CAPTURE FIRST", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Every timestamp here is UTC, including valid_since / last_refresh / execute_action_initiated_time / revert_action_initiated_time", served.Tail!, StringComparison.Ordinal);
    }
}
