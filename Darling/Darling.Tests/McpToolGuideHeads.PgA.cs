/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
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

    /// <summary><b>Two</b> readiness facets reach past plan capture, and the reading guide says so (#4735).
    /// <c>message_locale</c> is about every target-side log read; <c>log_line_prefix_readable</c> is about the
    /// stderr deadlock and log-event reads, which parse the prefix with no auto_explain in the picture. A tail that
    /// named only the locale facet would leave a quiet <c>get_pg_deadlocks</c> looking healthy while an unreadable
    /// prefix hides every report from it, so the sentence names both and says an unmet facet of either kind is the
    /// difference between a quiet server and a read that cannot see anything.</summary>
    [Fact]
    public void ReadinessTail_NamesBothFacetsThatReachPastPlanCapture()
    {
        var tail = McpToolGuideTests.Served("get_pg_plan_capture_readiness").Tail!;

        Assert.Contains("Two facets reach beyond plan capture", tail, StringComparison.Ordinal);
        Assert.Contains("message_locale reports whether the target writes its log messages in English", tail, StringComparison.Ordinal);
        Assert.Contains("log_line_prefix_readable reports whether the log line prefix can be parsed", tail, StringComparison.Ordinal);
        Assert.Contains("which the stderr deadlock and log-event reads need", tail, StringComparison.Ordinal);
        Assert.Contains("an unmet facet of either kind is the difference between a quiet server and a read that cannot see anything", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("One facet reaches beyond plan capture", tail, StringComparison.Ordinal);
    }

    /// <summary>The <c>get_pg_deadlocks</c> reading guide keeps step with those two facets and with the saved RDS
    /// position (#4735). Its list of reasons for an empty answer grows a fourth entry, the log line prefix that
    /// <c>log_line_prefix_readable</c> judges, and says the readiness tool reports both facets. Its sentence on how
    /// <c>times_seen</c> can exceed 1 on RDS and Aurora no longer claims the resume position lives in memory (#4708
    /// saves it): a restart resumes where the last read stopped, except while a deadlock report is split across
    /// two reads, when the saved position waits and that part is read again.</summary>
    [Fact]
    public void DeadlockTail_NamesThePrefixFacet_AndSaysTheReadPositionSurvivesARestart()
    {
        var tail = McpToolGuideTests.Served("get_pg_deadlocks").Tail!;

        Assert.Contains("There is a fourth precondition", tail, StringComparison.Ordinal);
        Assert.Contains("The log_line_prefix_readable facet judges it", tail, StringComparison.Ordinal);
        Assert.Contains("get_pg_plan_capture_readiness reports both facets", tail, StringComparison.Ordinal);

        Assert.Contains("the collector saves its resume position, so a restart resumes where the last read stopped", tail, StringComparison.Ordinal);
        Assert.Contains("a deadlock report split across two reads, where the saved position waits and a restart reads that part again", tail, StringComparison.Ordinal);
        Assert.Contains("a window whose write did not land is offered again", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("resume position in memory", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("re-reads a bounded tail", tail, StringComparison.Ordinal);
    }

    /// <summary>The <c>note</c> the <c>get_pg_deadlocks</c> RESPONSE carries says the RDS read position is saved,
    /// in the reading guide's own words (#4735). The guide and the note are two texts about one fact, and the guide
    /// was corrected first: the note went on saying the resume position "lives in the collector process, so a
    /// restart re-reads a bounded tail", which stopped being true when #4708 saved it. A caller that never opened
    /// the guide would still have been told to expect a re-read after every restart. Pinned on
    /// <see cref="DarlingMcpPgDeadlockTools.DeadlocksNote"/> and, because a pin on a constant proves nothing if the
    /// response stops using it, on the source line that puts it in the response.</summary>
    [Fact]
    public void DeadlocksResponseNote_SaysTheReadPositionIsSaved_InTheGuidesWords()
    {
        var note = DarlingMcpPgDeadlockTools.DeadlocksNote;

        Assert.Contains("the collector saves its resume position, so a restart resumes where the last read stopped", note, StringComparison.Ordinal);
        Assert.Contains("a deadlock report split across two reads, where the saved position waits and a restart reads that part again", note, StringComparison.Ordinal);
        Assert.Contains("a window whose write did not land is offered again", note, StringComparison.Ordinal);
        Assert.DoesNotContain("lives in the collector process", note, StringComparison.Ordinal);
        Assert.DoesNotContain("re-reads a bounded tail", note, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgDeadlockTools.cs");
        Assert.Contains("note = DeadlocksNote,", source, StringComparison.Ordinal);
    }

    /// <summary>An empty <c>get_pg_deadlocks</c> answer names the log line prefix beside the locale (#4735). The
    /// status text listed what makes an empty window mean something other than a quiet server and named the
    /// <c>message_locale</c> facet; an unreadable <c>log_line_prefix</c> on a stderr log target produces exactly
    /// the same empty answer (every line written under it is dropped), and the text sent the caller to the
    /// readiness read without saying that one of its facets is about the prefix.</summary>
    [Fact]
    public void NoDeadlocksText_NamesThePrefixFacet_BesideTheLocaleFacet()
    {
        var text = DarlingMcpPgDeadlockTools.NoDeadlocksText("target-a", 24);

        Assert.StartsWith("No deadlock was reported on target-a in the last 24 hour(s).", text, StringComparison.Ordinal);
        Assert.Contains("message_locale facet", text, StringComparison.Ordinal);
        Assert.Contains("log_line_prefix_readable facet", text, StringComparison.Ordinal);
        Assert.Contains("FOUR different things", text, StringComparison.Ordinal);
        Assert.DoesNotContain("THREE different things", text, StringComparison.Ordinal);
        Assert.Contains("get_pg_plan_capture_readiness", text, StringComparison.Ordinal);
        Assert.Contains("get_pg_database_stats", text, StringComparison.Ordinal);
    }

    /// <summary>The same for <c>get_pg_deadlock_detail</c>'s empty answer when no hash is given (#4735): it named the
    /// locale mismatch as the only way a read that runs can come back empty, so it now names the log line prefix
    /// too, and still points at the cumulative counter as the test that tells the healthy case from the others.</summary>
    [Fact]
    public void NoDeadlockGraphText_NamesThePrefixFacet_BesideTheLocaleMismatch()
    {
        var text = DarlingMcpPgDeadlockTools.NoDeadlockGraphText("target-a");

        Assert.StartsWith("No deadlock graph is stored for target-a.", text, StringComparison.Ordinal);
        Assert.Contains("non-English lc_messages", text, StringComparison.Ordinal);
        Assert.Contains("log_line_prefix_readable", text, StringComparison.Ordinal);
        Assert.Contains("get_pg_database_stats", text, StringComparison.Ordinal);
        Assert.Contains("which tells the healthy case from the others", text, StringComparison.Ordinal);
        Assert.DoesNotContain("from the other two", text, StringComparison.Ordinal);
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
        ("get_pg_cpu_utilization", "cpu_percent is percent of capacity CURRENTLY ALLOCATED, not a fixed ceiling"),
        ("get_pg_cpu_utilization", "100% is often a scale-up, not saturation"),
        ("get_pg_cpu_utilization", "acu_utilization_percent is percent of the CONFIGURED ceiling, the saturation figure"),
        ("get_pg_cpu_utilization", "null ACU means no sample, never headroom"),
        ("get_pg_cpu_utilization", "Host-memory bytes (since V136) are null when unmeasured"),
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
