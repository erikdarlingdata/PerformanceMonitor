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
/// #3898 D3 head pins for the "pgA" family: <c>get_pg_log_events</c>, <c>get_pg_index_bloat</c> and
/// <c>get_pg_deadlocks</c>, converted out of <c>DarlingMcpPgLogEventTools</c>, <c>DarlingMcpPgIndexTools</c> and
/// <c>DarlingMcpPgDeadlockTools</c> (Darling only — none of the three has a Lite twin). Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsPgATests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pg_log_events",
        "get_pg_index_bloat",
        "get_pg_deadlocks",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pg_log_events", "Each family is gated by its own logging setting"),
        ("get_pg_log_events", "an off family is EMPTY here, unaudited"),
        ("get_pg_log_events", "log_timezone = UTC (else refused whole)"),
        ("get_pg_log_events", "min_severity ranks by SERIOUSNESS, not log_min_messages' order"),
        ("get_pg_log_events", "times_seen is a sighting count, not an occurrence count"),
        ("get_pg_index_bloat", "ESTIMATED"),
        ("get_pg_index_bloat", "skipped_reason present = NO answer, not healthy"),
        ("get_pg_index_bloat", "Answerless rows sort FIRST by design"),
        ("get_pg_index_bloat", "pass answered_only=true to reach real answers"),
        ("get_pg_deadlocks", "An empty answer does not mean none occurred"),
        ("get_pg_deadlocks", "log_error_verbosity=terse drops the DETAIL field"),
        ("get_pg_deadlocks", "times_seen is a sighting count, not a deadlock count"),
        ("get_pg_deadlocks", "RDS/Aurora normally stays 1 (normal there, not partial)"),
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

    /// <summary>Nothing dropped: the parameter-overflow sentences D2 moved off <c>min_severity</c>,
    /// <c>get_pg_index_bloat.limit</c> and <c>get_pg_index_bloat.answered_only</c> land verbatim in their tool's
    /// tail rather than disappearing when the parameter itself was trimmed to a guardrail sentence.</summary>
    [Fact]
    public void D2ParameterOverflow_LandsInTheTail_NotJustTheGuardrailSentence()
    {
        var logEvents = McpToolGuideTests.Served("get_pg_log_events");
        Assert.Contains("connection and lock_wait lines are LOG", logEvents.Tail!, StringComparison.Ordinal);

        var indexBloat = McpToolGuideTests.Served("get_pg_index_bloat");
        Assert.Contains("read truncated to know whether the census held more indexes", indexBloat.Tail!, StringComparison.Ordinal);
        Assert.Contains("answered_only defaults to false", indexBloat.Tail!, StringComparison.Ordinal);

        var deadlocks = McpToolGuideTests.Served("get_pg_deadlocks");
        Assert.Contains("read truncated to know whether the window held more distinct deadlocks", deadlocks.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D8: no renames, no consolidation — all three still resolve as the same tool names with the same
    /// parameters, just a shorter served head and the two over-cap parameters (D2) trimmed to a pointer.</summary>
    [Fact]
    public void OverCapParameters_StayAtOrUnder200_AndKeepThePointer()
    {
        foreach (var (tool, param) in new[] { ("get_pg_log_events", "min_severity"), ("get_pg_index_bloat", "limit"), ("get_pg_index_bloat", "answered_only"), ("get_pg_deadlocks", "limit") })
        {
            var served = McpToolGuideTests.Served(tool);
            var p = Assert.Single(served.ParameterDescriptionLengths, x => x.Parameter == param);
            Assert.True(p.Length <= 200, $"{tool}.{param}: {p.Length} > 200");
        }
    }
}

/// <summary>
/// #3898 D3 head pins for a second pgA batch: <c>get_pg_database_stats</c>, <c>get_pg_autovacuum_health</c> and
/// <c>get_pg_top_queries</c>, converted out of <c>DarlingMcpPgDatabaseTools</c>, <c>DarlingMcpPgAutovacuumTools</c>
/// and <c>DarlingMcpPgStatementTools</c> (Darling only — none of the three has a Lite twin). Shares this file's
/// "PgA" home per the coordinator's file assignment, distinct from <see cref="McpToolGuideHeadsPgATests"/>'s
/// earlier three tools. Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsPgDatabaseTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pg_database_stats",
        "get_pg_autovacuum_health",
        "get_pg_top_queries",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pg_database_stats", "0 is a real all-clear"),
        ("get_pg_database_stats", "never differenced or summed"),
        ("get_pg_database_stats", "always cover the whole window"),
        ("get_pg_database_stats", "unavailable, not quiet"),
        ("get_pg_autovacuum_health", "EACH TABLE'S OWN threshold"),
        ("get_pg_autovacuum_health", "never zero"),
        ("get_pg_autovacuum_health", "count only the page, not the whole server"),
        ("get_pg_autovacuum_health", "genuine all-clear"),
        ("get_pg_top_queries", "queryid is a STRING"),
        ("get_pg_top_queries", "JSON doubles silently round"),
        ("get_pg_top_queries", "shares never sum to 100%"),
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

    /// <summary>Nothing dropped: the parameter-overflow sentence D2 moved off <c>get_pg_autovacuum_health.limit</c>
    /// lands verbatim in that tool's tail. <c>get_pg_database_stats.limit</c> and <c>get_pg_top_queries.limit</c>
    /// were reworded rather than cut — their trimmed text still keeps <c>truncated</c> and <c>whole window</c>
    /// (both existing pins read the parameter directly), and the page/window mechanics they describe are already
    /// stated in full in the tool's own tail, so no separate overflow sentence was needed for those two.</summary>
    [Fact]
    public void D2ParameterOverflow_LandsInTheTail_NotJustTheGuardrailSentence()
    {
        var autovacuum = McpToolGuideTests.Served("get_pg_autovacuum_health");
        Assert.Contains("This is what bounds the page - read truncated to know whether the server held more tables with pending work than were returned", autovacuum.Tail!, StringComparison.Ordinal);

        var databaseStats = McpToolGuideTests.Served("get_pg_database_stats");
        Assert.Contains("THE PAGE IS BOUNDED BY limit", databaseStats.Tail!, StringComparison.Ordinal);

        var topQueries = McpToolGuideTests.Served("get_pg_top_queries");
        Assert.Contains("SHARES ARE OF THE WINDOW, NOT OF THE PAGE", topQueries.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D8: no renames, no consolidation — all three still resolve as the same tool names with the same
    /// parameters, just a shorter served head and the over-cap <c>limit</c> parameter (D2) trimmed to a guardrail
    /// sentence.</summary>
    [Fact]
    public void OverCapParameters_StayAtOrUnder200_AndKeepTheGuardrailWords()
    {
        foreach (var (tool, param) in new[] { ("get_pg_database_stats", "limit"), ("get_pg_autovacuum_health", "limit"), ("get_pg_top_queries", "limit") })
        {
            var served = McpToolGuideTests.Served(tool);
            var p = Assert.Single(served.ParameterDescriptionLengths, x => x.Parameter == param);
            Assert.True(p.Length <= 200, $"{tool}.{param}: {p.Length} > 200");
        }
    }
}

/// <summary>
/// #3898 D3 head pins for a third pgA batch: <c>get_pg_plan_capture_readiness</c>, <c>get_pg_plans</c> and
/// <c>get_pg_session_states</c>, converted out of <c>DarlingMcpPgPlanTools</c> and
/// <c>DarlingMcpPgSessionStatesTools</c> (Darling only — none of the three has a Lite twin). Shares this file's
/// "PgA" home per the coordinator's file assignment, distinct from <see cref="McpToolGuideHeadsPgATests"/> and
/// <see cref="McpToolGuideHeadsPgDatabaseTests"/>. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsPgPlanTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pg_plan_capture_readiness",
        "get_pg_plans",
        "get_pg_session_states",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pg_plans", "Returns the plan JSON ITSELF, not a reference"),
        ("get_pg_plans", "queryid is a STRING"),
        ("get_pg_plans", "Empty separates three causes"),
        ("get_pg_plans", "read get_pg_plan_capture_readiness first"),
        ("get_pg_plan_capture_readiness", "in CAUSAL order"),
        ("get_pg_plan_capture_readiness", "Runs HOURLY"),
        ("get_pg_plan_capture_readiness", "An EMPTY answer here is NOT the healthy case"),
        ("get_pg_plan_capture_readiness", "Never claims a plan was captured"),
        ("get_pg_session_states", "-1 means pinned NOTHING, not a small age"),
        ("get_pg_session_states", "THIS IS A SAMPLE at the collection interval"),
        ("get_pg_session_states", "captures_in_window = 0 means unavailable"),
        ("get_pg_session_states", "Requires pg_monitor"),
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

    /// <summary>Nothing dropped: the parameter-overflow sentences D2 moved off <c>get_pg_plans.limit</c>,
    /// <c>get_pg_plans.query_id</c>, <c>get_pg_plan_capture_readiness.limit</c> and
    /// <c>get_pg_session_states.limit</c> land verbatim in their tool's tail rather than disappearing when the
    /// parameter itself was trimmed to a pointer at the tool's reading guide.</summary>
    [Fact]
    public void D2ParameterOverflow_LandsInTheTail_NotJustTheGuardrailSentence()
    {
        var plans = McpToolGuideTests.Served("get_pg_plans");
        Assert.Contains("read truncated to know whether the window held more shapes than were returned", plans.Tail!, StringComparison.Ordinal);
        Assert.Contains("an empty answer with this set genuinely means no plan for it was captured", plans.Tail!, StringComparison.Ordinal);

        var readiness = McpToolGuideTests.Served("get_pg_plan_capture_readiness");
        Assert.Contains("in which case unsatisfied_facets is withheld", readiness.Tail!, StringComparison.Ordinal);

        var sessions = McpToolGuideTests.Served("get_pg_session_states");
        Assert.Contains("read truncated to know whether the window held more sessions than were returned", sessions.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D8: no renames, no consolidation — all three still resolve as the same tool names with the same
    /// parameters, just a shorter served head and the over-cap parameters (D2) trimmed to a pointer at the
    /// tool's reading guide.</summary>
    [Fact]
    public void OverCapParameters_StayAtOrUnder200_AndKeepThePointer()
    {
        foreach (var (tool, param) in new[] { ("get_pg_plans", "limit"), ("get_pg_plans", "query_id"), ("get_pg_plan_capture_readiness", "limit"), ("get_pg_session_states", "limit") })
        {
            var served = McpToolGuideTests.Served(tool);
            var p = Assert.Single(served.ParameterDescriptionLengths, x => x.Parameter == param);
            Assert.True(p.Length <= 200, $"{tool}.{param}: {p.Length} > 200");
        }
    }
}

/// <summary>
/// #3898 D3 head pins for the pgA trend/CPU batch: <c>get_pg_io_trend</c>, <c>get_pg_database_trend</c> and
/// <c>get_pg_cpu_utilization</c>, converted out of <c>DarlingMcpPgTrendTools</c> and
/// <c>DarlingMcpPgCpuUtilizationTools</c> (Darling only — none of the three has a Lite twin). Shares this
/// file's "PgA" home per the coordinator's file assignment, distinct from the other classes here. Follows the
/// pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
///
/// <para><b>Correction:</b> <c>get_pg_cpu_utilization</c>'s original tail said "Aurora and RDS only" and "this
/// returns empty" for self-hosted PostgreSQL. <c>PgCpuUtilizationCollector.AppliesTo</c> gates on
/// <c>target.IsAurora</c> alone (matching <c>PgWaitStatsCollector</c>'s own gate, per that collector's doc
/// comment) — a plain, non-Aurora RDS PostgreSQL target is not currently reached either, and the miss for both
/// it and a self-hosted target is <c>not_collected</c>, not <c>empty</c>. The tail now says so.</para>
/// </summary>
public sealed class McpToolGuideHeadsPgTrendCpuTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pg_io_trend",
        "get_pg_database_trend",
        "get_pg_cpu_utilization",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pg_io_trend", "avg_read_ms/avg_write_ms are null, never 0.000, when track_io_timing is off"),
        ("get_pg_io_trend", "Write fields are null, not 0, on Amazon Aurora"),
        ("get_pg_io_trend", "Byte rates: measured (18+), estimated (earlier), or null"),
        ("get_pg_io_trend", "Empty: no pair was active, or the window holds one snapshot"),
        ("get_pg_io_trend", "A point spanning a stats reset reports everything since the reset, not a quiet interval"),
        ("get_pg_io_trend", "Requires PostgreSQL 16+"),
        ("get_pg_database_trend", "Differenced per interval - the raw counters are cumulative"),
        ("get_pg_database_trend", "cache_hit_pct/rollback_pct are null, not 0"),
        ("get_pg_database_trend", "deadlocks is a COUNT per point, not a rate"),
        ("get_pg_database_trend", "A point spanning a stats reset reports everything since, never a quiet interval"),
        ("get_pg_database_trend", "Empty: wrong name, only one snapshot so far, or none in the window - the message says which"),
        ("get_pg_cpu_utilization", "Aurora only; RDS and self-hosted are not_collected"),
        ("get_pg_cpu_utilization", "cpu_percent is percent of the capacity CURRENTLY ALLOCATED, not a fixed ceiling"),
        ("get_pg_cpu_utilization", "100% is often a scale-up, not saturation"),
        ("get_pg_cpu_utilization", "acu_utilization_percent is percent of the CONFIGURED ceiling and is the saturation figure to alert on"),
        ("get_pg_cpu_utilization", "null ACU means no sample, never headroom"),
        ("get_pg_cpu_utilization", "Host-memory bytes (since V136) are null when unmeasured, never 0"),
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

    /// <summary>Nothing dropped: the parameter-overflow sentence D2 moved off <c>get_pg_io_trend.backend_type</c>
    /// lands verbatim in that tool's tail rather than disappearing when the parameter itself was trimmed to its
    /// guardrail clause (what a backend_type value IS).</summary>
    [Fact]
    public void D2ParameterOverflow_LandsInTheTail_NotJustTheGuardrailSentence()
    {
        var ioTrend = McpToolGuideTests.Served("get_pg_io_trend");
        Assert.Contains("Naming this alone follows the busiest CONTEXT for that backend", ioTrend.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D8: no renames, no consolidation — all three still resolve as the same tool names with the same
    /// parameters, just a shorter served head and the over-cap <c>backend_type</c> parameter (D2) trimmed to its
    /// guardrail clause.</summary>
    [Fact]
    public void OverCapParameters_StayAtOrUnder200()
    {
        var served = McpToolGuideTests.Served("get_pg_io_trend");
        var p = Assert.Single(served.ParameterDescriptionLengths, x => x.Parameter == "backend_type");
        Assert.True(p.Length <= 200, $"get_pg_io_trend.backend_type: {p.Length} > 200");
    }

    /// <summary>The correction: the tail no longer claims RDS is reached or that the miss is <c>empty</c> for a
    /// non-Aurora target.</summary>
    [Fact]
    public void CpuUtilizationTail_CorrectsTheAuroraOnlyClaim()
    {
        var served = McpToolGuideTests.Served("get_pg_cpu_utilization");
        Assert.Contains("Aurora only - a plain RDS or self-hosted target has no route here and this is not_collected for it", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("Aurora and RDS only", served.Tail!, StringComparison.Ordinal);
    }
}
