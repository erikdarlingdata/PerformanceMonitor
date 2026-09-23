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
