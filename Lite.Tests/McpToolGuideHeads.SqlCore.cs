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
/// #3898 D3 head pins for the "sqlCore" family's Lite twins: <c>analyze_server</c> and <c>compare_analysis</c>,
/// joined by <c>get_analysis_facts</c> and <c>get_analysis_findings</c> in a later lane, and by
/// <c>mute_analysis_finding</c> and <c>audit_config</c> in a later one still. Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsSqlCoreTests</c>, which also holds the cross-SKU lockstep pin. Both apps'
/// tool bodies mirror each other field-for-field, so the original prose (and now the head and tail) is
/// byte-identical on both SKUs, except <c>mute_analysis_finding</c>'s and <c>audit_config</c>'s tails, which are
/// per-product because the underlying facts differ (see <c>MuteHeadStatesTheSharedErrorStatus_AuditHeadNamesDarlingInOneClause</c>).
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
}
