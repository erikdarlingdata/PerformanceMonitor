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

    /// <summary>Both SKUs' <c>get_cpu_utilization</c> notes, one theory: the same tokens on each, and the two
    /// wire strings byte-identical — the one-record-per-minute ring buffer is the same source on both, and a
    /// note that drifted on one SKU would be the #3696 lie told once more, to half the callers.</summary>
    [Theory]
    [InlineData("Lite/Mcp/McpCpuTools.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs")]
    public void GetCpuUtilization_NamesBothSourceCadences_AndNotAFifteenSecondRingBuffer(string file)
    {
        var note = CpuUtilizationNote(file, out var body);

        Assert.DoesNotContain("15-second ring buffer", note, StringComparison.Ordinal);
        Assert.Contains("one RING_BUFFER_SCHEDULER_MONITOR record per minute", note, StringComparison.Ordinal);
        Assert.Contains("sys.dm_db_resource_stats row per 15 seconds on Azure SQL DB", note, StringComparison.Ordinal);
        /* The note defers to the measured count the payload already carries per bucket. */
        Assert.Contains("samples_in_bucket is the measured count", note, StringComparison.Ordinal);
        Assert.Contains("samples_in_bucket = g.Count()", body, StringComparison.Ordinal);

        /* The twin pin: the sentence is ONE sentence. Asserted from both rows of the theory so a drift on
           either file reds under that file's name. */
        Assert.Equal(CpuUtilizationNote("Lite/Mcp/McpCpuTools.cs", out _), CpuUtilizationNote("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", out _));
    }

    /// <summary>The wire string of <c>get_cpu_utilization</c>'s <c>note</c> in <paramref name="file"/> (the
    /// comment above it may quote the old wording as history, so the pin reads the literal, not the body).</summary>
    private static string CpuUtilizationNote(string file, out string body)
    {
        var source = File.ReadAllText(RepoPath(file.Split('/')));
        var start = source.IndexOf("Name = \"get_cpu_utilization\"", StringComparison.Ordinal);
        Assert.True(start > 0, $"{file}: get_cpu_utilization is not declared here");
        body = source[start..source.IndexOf("FormatError(\"get_cpu_utilization\"", StringComparison.Ordinal)];

        var noteStart = body.IndexOf("note = \"", StringComparison.Ordinal);
        Assert.True(noteStart > 0, $"{file}: the tool publishes no note");
        return body[noteStart..body.IndexOf("\",", noteStart, StringComparison.Ordinal)];
    }

    [Fact]
    public void PlanTools_DescribeTheStatedOperatorCut_AndNoCreateIndexText()
    {
        foreach (var tool in new[] { "analyze_query_plan", "analyze_procedure_plan", "analyze_plan_xml" })
        {
            var description = Description(typeof(McpPlanTools), tool);
            Assert.Contains("operators_returned / total_operators / truncated", description, StringComparison.Ordinal);
            Assert.Contains("operators_ranked_by", description, StringComparison.Ordinal);
            Assert.Contains("no CREATE INDEX text", description, StringComparison.Ordinal);
            Assert.Contains("a hint, not a design", description, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("Missing indexes with CREATE statements", McpInstructions.Text, StringComparison.Ordinal);
    }

    /* ---------------- plumbing ---------------- */

    private static string Description(Type toolType, string toolName) => toolType
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName)
        .GetCustomAttribute<DescriptionAttribute>()!.Description;

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
