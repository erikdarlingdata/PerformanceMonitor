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
/// #3898 D3 head pins for the "sqlCore" family: <c>analyze_server</c> and <c>compare_analysis</c>, converted out
/// of <c>DarlingMcpTools</c> (Darling) and their byte-identical Lite twins (both apps' bodies mirror each other
/// field-for-field, so the original prose was already byte-identical on both SKUs and the heads and tails stay
/// that way here too). <c>get_analysis_facts</c> and <c>get_analysis_findings</c> joined the same way in a later
/// lane, and <c>mute_analysis_finding</c> and <c>audit_config</c> joined in a later one still. Lite's twin is
/// <c>Lite.Tests/McpToolGuideHeadsSqlCoreTests</c>. Follows the pattern in
/// <see cref="McpToolGuideHeadsDataTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsSqlCoreTests
{
    private static readonly string[] ConvertedTools =
    [
        "analyze_server",
        "audit_config",
        "compare_analysis",
        "get_analysis_facts",
        "get_analysis_findings",
        "mute_analysis_finding",
    ];

    /// <summary>The four tools whose original prose carried no product-specific fact; their heads name neither
    /// SKU. <c>mute_analysis_finding</c> and <c>audit_config</c> are deliberately excluded (see
    /// <see cref="MuteHeadStatesTheSharedErrorStatus_AuditHeadNamesDarlingInOneClause"/>).</summary>
    private static readonly string[] ToolsWithNoProductQualifier =
    [
        "analyze_server",
        "compare_analysis",
        "get_analysis_facts",
        "get_analysis_findings",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("analyze_server", "confidence is evidence, not probability or diagnosis"),
        ("analyze_server", "Needs 24h+ history, else insufficient_data"),
        ("analyze_server", "unavailable: window unobserved (dead collector), no verdict either way"),
        ("analyze_server", "empty: window observed, nothing fired (a true all-clear)"),
        ("analyze_server", "Unanchored runs persist findings to the store; as_of runs are exploratory, not saved"),
        ("analyze_server", "remediation_command is advisory, never auto-executed"),
        ("compare_analysis", "must exceed hours_back, else refused"),
        ("compare_analysis", "N=1 vs N=1: never proves causation"),
        ("compare_analysis", "unavailable = BOTH windows had zero facts"),
        ("compare_analysis", "coverage_caveat flags a partly-collected side"),
        ("compare_analysis", "band_source/band_rules say which"),
        ("get_analysis_facts", "WITHOUT graph traversal: facts, not findings"),
        ("get_analysis_facts", "so a fact here did not necessarily produce a finding"),
        ("get_analysis_facts", "differs from a finding's confidence in analyze_server"),
        ("get_analysis_facts", "collection_caveats is absent on a clean read"),
        ("get_analysis_facts", "ranked is absent unless 2+ objects ride"),
        ("get_analysis_findings", "Persisted findings from PAST analysis runs, not a new analysis"),
        ("get_analysis_findings", "empty: no findings in the window, a true zero"),
        ("get_analysis_findings", "truncated (see truncation_note) flags a read cap, so occurrence stats may under-report"),
        ("get_analysis_findings", "confidence_basis flags rows persisted before this scoring existed as path-shape, not corroboration"),
        ("get_analysis_findings", "remediation_command/structured_remediation are advisory, never executed"),
        ("get_analysis_findings", "Recurrence fields are LABELS at unchanged severity"),
        ("audit_config", "NO check branches on it (MAXDOP is topology-based, the others are resource-based)"),
        ("audit_config", "Darling also audits PostgreSQL targets"),
        ("mute_analysis_finding", "not per-occurrence"),
        ("mute_analysis_finding", "registered: a NEW row was stored this call"),
        ("mute_analysis_finding", "already_muted: the scope already held the hash; nothing was written"),
        ("mute_analysis_finding", "matched_now: retained findings in scope carrying the hash now"),
        ("mute_analysis_finding", "muted_unmatched (registered, matched_now 0; maybe a mistyped hash)"),
        ("mute_analysis_finding", "error (the write failed; nothing is muted)"),
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

    /// <summary>D2: <c>baseline_hours_back</c>'s description was trimmed from 211 to 139 characters to clear the
    /// 200-character parameter cap; the sentence it dropped (the baseline period is always as long as the
    /// comparison period) rides on <c>compare_analysis</c>'s own tail instead of being lost.</summary>
    [Fact]
    public void CompareAnalysis_TailCarriesTheBaselineDurationRuleTrimmedFromTheParameter()
    {
        Assert.Contains("same duration as the comparison period", McpToolGuideTests.Served("compare_analysis").Tail!, StringComparison.Ordinal);
    }

    /// <summary>These four tools' original prose carried no product-specific fact (the two SKUs' tool bodies
    /// mirror each other field-for-field), so nothing needed to be kept off the shared head for D6 — heads and
    /// tails are byte-identical on both SKUs, which the generic cross-SKU pin in <c>McpToolGuideTests</c> also
    /// covers.</summary>
    [Fact]
    public void BothHeads_CarryNoProductQualifier()
    {
        foreach (var tool in ToolsWithNoProductQualifier)
        {
            var served = McpToolGuideTests.Served(tool).Served;
            Assert.DoesNotContain("Darling", served, StringComparison.Ordinal);
            Assert.DoesNotContain("Lite", served, StringComparison.Ordinal);
        }
    }

    /// <summary>D6: a failed mute write reports <c>status: \"error\"</c> on both products (Darling in its own
    /// payload, Lite through <c>McpHelpers.FormatError</c>), so the shared head states it once. Only
    /// <c>audit_config</c>'s fact differs by product (Darling also audits PostgreSQL targets), so its
    /// byte-identical head names Darling in one clause that is true whichever SKU serves it.</summary>
    [Fact]
    public void MuteHeadStatesTheSharedErrorStatus_AuditHeadNamesDarlingInOneClause()
    {
        Assert.Contains("error (the write failed; nothing is muted).", McpToolGuideTests.Served("mute_analysis_finding").Served, StringComparison.Ordinal);
        Assert.Contains("Darling also audits PostgreSQL targets.", McpToolGuideTests.Served("audit_config").Served, StringComparison.Ordinal);
    }

    /// <summary>The head's "error (the write failed; nothing is muted)" is backed by Darling's own tail, which still
    /// carries the original sentence in full: what triggers it and that nothing is muted when it does.</summary>
    [Fact]
    public void MuteAnalysisFinding_DarlingTailNamesTheErrorStatusInFull()
    {
        Assert.Contains("\"error\" when the row could not be written (nothing is muted)", McpToolGuideTests.Served("mute_analysis_finding").Tail!, StringComparison.Ordinal);
    }

    /// <summary>D2: two parameter descriptions were trimmed to clear the 200-character cap once their tools
    /// converted. <c>source</c>'s full accepted-value enumeration (was 515 chars) and <c>include_drilldown</c>'s
    /// examples and default-false rationale (was 235 chars) both ride on their tool's own tail instead of being
    /// lost — the same pattern <c>compare_analysis.baseline_hours_back</c> set above.</summary>
    [Fact]
    public void TrimmedParameterOverflows_RideOnTheirOwnTail()
    {
        Assert.Contains("source accepts: anomaly, bad_actor, blocking", McpToolGuideTests.Served("get_analysis_facts").Tail!, StringComparison.Ordinal);
        Assert.Contains("queries, sessions, tempdb, waits; omit it for all.", McpToolGuideTests.Served("get_analysis_facts").Tail!, StringComparison.Ordinal);
        Assert.Contains("parameter-sensitive plans or top spill queries behind the finding", McpToolGuideTests.Served("get_analysis_findings").Tail!, StringComparison.Ordinal);
    }

    /// <summary>The facts payload's total_facts/shown/filters were never documented; the coordinator added
    /// one tail sentence in #4083, so an agent reading shown: 0 can tell a filtered-out read from no facts.</summary>
    [Fact]
    public void FactsPayloadCounts_AreDocumentedInTheTail()
    {
        Assert.Contains("total_facts counts every scored fact before the source and min_severity filters; shown counts the facts that passed them", McpToolGuideTests.Served("get_analysis_facts").Tail!, StringComparison.Ordinal);
    }

    /// <summary>D4: <c>get_analysis_findings</c>' two issue references ("(pre-#3538)" naming the confidence-basis
    /// migration, "(#3653)" naming the recurrence-label change) are off the wire on both head and tail; the
    /// guardrail each carried (a pre-migration row is path-shape, not corroboration; the three recurrence fields
    /// and what they mean) stays, worded without the ref. get_analysis_facts carried no issue references.</summary>
    [Fact]
    public void GetAnalysisFindings_CarriesNoIssueReferences()
    {
        var full = McpToolGuideTests.Served("get_analysis_findings").Served + McpToolGuideTests.Served("get_analysis_findings").Tail;
        Assert.DoesNotContain("#3538", full, StringComparison.Ordinal);
        Assert.DoesNotContain("#3653", full, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3898 head pins for <c>get_daily_summary</c> and <c>get_daily_summary_range</c>, joining the sqlCore family.
/// Lite's twin is <c>Lite.Tests/McpToolGuideHeadsSqlCoreDailySummaryTests</c>. Both tools' heads are
/// byte-identical on both SKUs (the generic cross-SKU pin in <see cref="McpToolGuideTests"/> also covers this):
/// neither tool's guardrail facts differ by product on the head. Darling's own tail carries one extra paragraph
/// (the rollup-tier <c>unique_queries=null</c>/<c>days_missing</c> disclosure) that Lite's original prose never
/// had, because Lite's daily-summary payload has no rollup tier to disclose (<c>Lite/Mcp/McpHealthTools.cs</c>'s
/// <c>GetDailySummary</c>/<c>GetDailySummaryRange</c> bodies carry no <c>days_missing</c> key at all).
/// </summary>
public sealed class McpToolGuideHeadsSqlCoreDailySummaryTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_daily_summary",
        "get_daily_summary_range",
    ];

    [Fact]
    public void BothHeads_CarryTheStatusVocabulary_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        Assert.Contains("status empty: no row for that day.", McpToolGuideTests.Served("get_daily_summary").Served, StringComparison.Ordinal);
        Assert.Contains("data_state purged, zeros are absences not measurements, no verdict.", McpToolGuideTests.Served("get_daily_summary").Served, StringComparison.Ordinal);

        Assert.Contains("status empty: no collected day in range but the server has history elsewhere; status unavailable: nothing was ever collected.", McpToolGuideTests.Served("get_daily_summary_range").Served, StringComparison.Ordinal);
        Assert.Contains("zeros are absences, never Healthy.", McpToolGuideTests.Served("get_daily_summary_range").Served, StringComparison.Ordinal);
    }

    /// <summary>D9: <c>get_daily_summary_range</c>'s top-level "unavailable" means no collection has EVER been
    /// recorded for the server; a purged day still returns as an ordinary row, with data_state purged, inside
    /// the days array rather than as the top-level status. The head keeps the two apart so a reader does not
    /// conflate "this one row is purged" with "the whole range call failed".</summary>
    [Fact]
    public void RangeHead_TiesUnavailableToNeverCollected_NotToAPurgedRow()
    {
        var head = McpToolGuideTests.Served("get_daily_summary_range").Served;
        Assert.Contains("status unavailable: nothing was ever collected.", head, StringComparison.Ordinal);
        Assert.Contains("a row is data_state purged or past_horizon", head, StringComparison.Ordinal);
    }

    /// <summary>D6: Darling's own tail keeps the rollup-tier disclosure (<c>unique_queries=null</c>,
    /// <c>days_missing</c>) that only Darling's payload carries.</summary>
    [Fact]
    public void DarlingsTail_CarriesTheRollupTierDisclosure()
    {
        Assert.Contains("unique_queries is null (NOT 0)", McpToolGuideTests.Served("get_daily_summary").Tail!, StringComparison.Ordinal);
        Assert.Contains("unique_queries=null (NOT 0", McpToolGuideTests.Served("get_daily_summary_range").Tail!, StringComparison.Ordinal);
    }
}
