/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A15/A16 — three sentences Lite's MCP surface said that its code did not do, pinned to the truth.
///
/// <para><c>audit_config</c> claimed to account for edition (Standard vs Enterprise) with zero edition branches:
/// the MAXDOP rule is topology-based (cores per socket, capped at 8), the others resource-based, and the edition
/// is REPORTED in the payload, never consulted. <c>get_cpu_utilization</c> called its source "15-second ring
/// buffer samples": the ring buffer (<c>RING_BUFFER_SCHEDULER_MONITOR</c>) writes one record per minute; the
/// 15-second cadence is <c>sys.dm_db_resource_stats</c>, the Azure SQL DB source, which is not a ring buffer. The
/// plan tools' "top operators" was a silent <c>Take(10)</c> with a ranking basis that switched between a
/// measurement and an estimate. The Darling twins of the first and third are pinned in <c>Darling.Tests</c>
/// (<c>DarlingMcpToolsTests</c>, <c>McpPlanAnalysisEnvelopeTests</c>); the CPU note's Darling twin is pinned
/// HERE, beside Lite's, because the two notes are one sentence on two SKUs and the pin that matters is that
/// they stay byte-identical (#3653 — Darling's copy was ported one lane after Lite's).</para>
/// </summary>
public sealed class McpDescriptionTruthPinTests
{
    [Fact]
    public void AuditConfig_DoesNotClaimEditionAwareness_AndTheBodyHasNoEditionBranch()
    {
        var description = Description(typeof(McpAnalysisTools), "audit_config");

        Assert.DoesNotContain("accounting for edition", description, StringComparison.Ordinal);
        Assert.Contains("NO check branches on it", description, StringComparison.Ordinal);

        var source = File.ReadAllText(RepoPath("Lite", "Mcp", "McpAnalysisTools.cs"));
        var start = source.IndexOf("Name = \"audit_config\"", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = source[start..source.IndexOf("FormatError(\"audit_config\"", StringComparison.Ordinal)];
        /* The only edition reads: the fact lookup, the name switch, and the payload echo. */
        Assert.Contains("edition = editionName", body, StringComparison.Ordinal);
        Assert.DoesNotContain("if (edition", body, StringComparison.Ordinal);
        Assert.DoesNotContain("edition ==", body, StringComparison.Ordinal);

        var instructions = McpInstructions.Text;
        Assert.DoesNotContain("Edition-aware", instructions, StringComparison.Ordinal);
        Assert.DoesNotContain("edition-aware", instructions, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both SKUs' <c>get_cpu_utilization</c> notes, one theory. Since #3960 bucketed the read, the note is no
    /// longer built inline per SKU (where two independent copies could drift, the #3696 lie told twice) — both
    /// tool bodies build their envelope through the shared <c>TrendPayloads.CpuUtilization</c>, whose
    /// <c>CpuCadenceNote</c> constant is now the ONE place the sentence is spelled. This pins that constant's text,
    /// and that each SKU's tool body actually reaches it rather than composing its own. (#3898 Phase 2 briefly
    /// rewrote this to expect a per-file inline literal; #3960's shared-constant refactor was never undone, so
    /// that rewrite failed against both SKUs — reverted back to reading the one shared constant.)
    /// </summary>
    [Theory]
    [InlineData("Lite/Mcp/McpCpuTools.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs")]
    public void GetCpuUtilization_NamesBothSourceCadences_AndNotAFifteenSecondRingBuffer(string file)
    {
        var source = File.ReadAllText(RepoPath(file.Split('/')));
        Assert.Contains("Name = \"get_cpu_utilization\"", source, StringComparison.Ordinal);
        Assert.Contains("TrendPayloads.CpuUtilization(", source, StringComparison.Ordinal);

        var note = CpuCadenceNote();
        Assert.DoesNotContain("15-second ring buffer", note, StringComparison.Ordinal);
        Assert.Contains("one RING_BUFFER_SCHEDULER_MONITOR record per minute", note, StringComparison.Ordinal);
        Assert.Contains("sys.dm_db_resource_stats row per 15 seconds on Azure SQL DB", note, StringComparison.Ordinal);
        /* The note defers to the measured count the payload already carries per bucket
           (CpuBucketPoint.Samples, projected as samples_in_bucket by TrendPayloads.CpuUtilization). */
        Assert.Contains("samples_in_bucket is the measured count", note, StringComparison.Ordinal);
    }

    /// <summary>The wire string of <see cref="PerformanceMonitor.Common"/>'s shared <c>TrendPayloads.CpuCadenceNote</c>
    /// (#3960), read from source since the type is internal to that assembly: one place, so there is nothing left
    /// for a per-SKU byte-identity check to prove.</summary>
    private static string CpuCadenceNote()
    {
        var source = File.ReadAllText(RepoPath("PerformanceMonitor.Common", "Mcp", "TrendPayloads.cs"));
        var start = source.IndexOf("CpuCadenceNote =", StringComparison.Ordinal);
        Assert.True(start > 0, "TrendPayloads.cs no longer declares CpuCadenceNote");
        var noteStart = source.IndexOf('"', start);
        return source[noteStart..source.IndexOf("\";", noteStart, StringComparison.Ordinal)];
    }

    /// <summary>
    /// The three Lite plan tools whose descriptions enumerate the payload (<c>analyze_query_store_plan</c>'s never
    /// did — it says what it fetches and when to use it, so there is nothing there to pin). #3696 made each of these
    /// say "no CREATE INDEX text — the suggestion is a hint, not a design" and pinned the phrase; #3805 restored
    /// <c>create_statement</c> because the rule #3696 cited was never made (DDL recommendations are legitimate
    /// product output — the engine's RCSI remediation, and the MISSING_INDEX finding's own <c>remediation_command</c>
    /// carrying this same statement), and the maintainer's spec for the restore reframed the field from suppression
    /// to honesty: "corroboration, with caveats". So the pin flips: the description names the field, calls it
    /// corroboration for a statement already measured slow and never a diagnosis, names the fixed per-row
    /// <c>caveat</c> and its regression-risk clause, and keeps <c>impact_basis</c> labelled. The suppression sentence
    /// must not come back. The Darling four are pinned in <c>Darling.Tests</c> (<c>DarlingMcpPlanToolsTests</c>)
    /// with the same fragments. #3898 split <c>analyze_query_plan</c> into a served head plus a
    /// <c>get_tool_guide</c> tail: the fragments below now split the same way, checked against whichever half
    /// the split actually put them in.
    /// </summary>
    [Fact]
    public void PlanTools_DescribeTheStatedOperatorCut_AndTheCreateStatement()
    {
        /* analyze_procedure_plan and analyze_plan_xml are still unconverted (#3898 lanes e2/e3): Head is the
           whole description for them (McpToolGuide.Split finds no marker), so every fragment below is still
           found there directly. */
        foreach (var tool in new[] { "analyze_procedure_plan", "analyze_plan_xml" })
        {
            var description = Description(typeof(McpPlanTools), tool);
            Assert.Contains("operators_returned / total_operators / truncated", description, StringComparison.Ordinal);
            Assert.Contains("operators_ranked_by", description, StringComparison.Ordinal);
            Assert.Contains("labelled impact_basis", description, StringComparison.Ordinal);
            Assert.Contains("create_statement — the optimizer's suggested CREATE INDEX for this statement", description, StringComparison.Ordinal);
            Assert.Contains("corroboration for a statement already measured slow, never a diagnosis", description, StringComparison.Ordinal);
            Assert.Contains("every row carries the fixed caveat", description, StringComparison.Ordinal);
            Assert.Contains("regression risk for other plans", description, StringComparison.Ordinal);
            Assert.DoesNotContain("no CREATE INDEX text", description, StringComparison.Ordinal);
            Assert.DoesNotContain("a hint, not a design", description, StringComparison.Ordinal);
        }

        /* analyze_query_plan (#3898 lane e1, converted): the safety-critical half of this guardrail --
           create_statement is corroboration, never a diagnosis, and costs writes / can regress other plans --
           stays in the served HEAD, in the head's own words under D2's 620-char cap. The exact original
           phrasing this loop used to check, including every fragment below, is unchanged in get_tool_guide's
           tail (dropcheck, and McpToolGuideHeadsAnalyzeQueryPlanTests.Tail_CarriesLitesOwnOriginalSentences_Verbatim,
           both pin that). */
        var queryPlanServed = global::Lite.Tests.McpToolGuideTests.Served("analyze_query_plan");
        Assert.Contains("create_statement corroborates, never a diagnosis", queryPlanServed.Served, StringComparison.Ordinal);
        Assert.Contains("it costs writes and can regress plans", queryPlanServed.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("no CREATE INDEX text", queryPlanServed.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("a hint, not a design", queryPlanServed.Served, StringComparison.Ordinal);
        var queryPlanTail = queryPlanServed.Tail!;
        Assert.Contains("operators_returned / total_operators / truncated", queryPlanTail, StringComparison.Ordinal);
        Assert.Contains("operators_ranked_by", queryPlanTail, StringComparison.Ordinal);
        Assert.Contains("labelled impact_basis", queryPlanTail, StringComparison.Ordinal);
        Assert.Contains("create_statement — the optimizer's suggested CREATE INDEX for this statement", queryPlanTail, StringComparison.Ordinal);
        Assert.Contains("corroboration for a statement already measured slow, never a diagnosis", queryPlanTail, StringComparison.Ordinal);
        Assert.Contains("every row carries the fixed caveat", queryPlanTail, StringComparison.Ordinal);
        Assert.Contains("regression risk for other plans", queryPlanTail, StringComparison.Ordinal);

        /* #3898 Phase 2 (D5, D6): the instructions' anti-pattern bullet that used to restate this is gone on
           both SKUs — the descriptions checked above are now the only surface. */
    }

    /* ---------------- plumbing ---------------- */

    /// <summary>The SERVED head (#3898 D3, re-pointed deliberately from the whole description): each fragment
    /// pinned here is a guardrail a caller needs from tools/list itself (the edition is not consulted; the
    /// operator cut and its basis; create_statement is corroboration, with its caveat), so it may not move into
    /// get_tool_guide's tail. Unconverted, the head is the whole description.</summary>
    private static string Description(Type toolType, string toolName) => PerformanceMonitor.Common.McpToolGuide.Split(toolType
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName)
        .GetCustomAttribute<DescriptionAttribute>()!.Description).Head;

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
