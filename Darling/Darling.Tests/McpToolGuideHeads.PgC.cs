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
/// #3898 D3 head pins for the "pgC" family: <c>get_pg_column_stats</c> (in <c>DarlingMcpPgIndexTools</c>,
/// which also declares the already-converted <c>get_pg_index_bloat</c> — out of this lane's scope and
/// untouched here), <c>get_pg_replication_stats</c> (<c>DarlingMcpPgReplicationStatsTools</c>) and
/// <c>get_pg_predicate_stats</c> (<c>DarlingMcpPgPredicateTools</c>). Darling only — none of the three has a
/// Lite twin. Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsPgCTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pg_column_stats",
        "get_pg_replication_stats",
        "get_pg_predicate_stats",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pg_column_stats", "the LATEST capture inside the window, not a history"),
        ("get_pg_column_stats", "runs DAILY, so a window under a day can be empty on a healthy server"),
        ("get_pg_column_stats", "n_distinct is a RATIO of table rows when negative, an absolute count when positive"),
        ("get_pg_column_stats", "Gated: only columns on tables above a size floor, visible to the monitoring login, are collected"),
        ("get_pg_replication_stats", "Reports the WORST value seen in the window beside the latest"),
        ("get_pg_replication_stats", "lag is spiky, so read worst_*, not just the latest"),
        ("get_pg_replication_stats", "Rows are SAMPLED: a replica that connected and left between captures may not appear"),
        ("get_pg_replication_stats", "empty is not proof none ever attached"),
        ("get_pg_replication_stats", "a slot with nothing connected retains WAL indefinitely"),
        ("get_pg_predicate_stats", "filtered_pct is rows_filtered/rows_evaluated, NULL when nothing was evaluated, never a false 0%"),
        ("get_pg_predicate_stats", "worst_estimate_error_ratio large means the PLANNER is misjudging this predicate"),
        ("get_pg_predicate_stats", "SAMPLED: counts are raw at sample_rate (often 0.01) and are NEVER scaled up here"),
        ("get_pg_predicate_stats", "queryid is a STRING and joins get_pg_top_queries"),
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

    /// <summary>D8: no renames, no consolidation, and no parameter needed D2 trimming — every parameter on all
    /// three tools was already at or under the 200-character cap before this conversion (as_of 167,
    /// hours_back 40-78, limit 35, server_name 28), so only the tools' own descriptions split into a head and
    /// a tail here.</summary>
    [Fact]
    public void EveryConvertedTool_KeepsItsUntrimmedParameters()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }
    }

    /// <summary>D8: <c>get_pg_index_bloat</c> shares <c>DarlingMcpPgIndexTools</c> with
    /// <c>get_pg_column_stats</c> but was converted by an earlier lane and is out of this one's scope — its
    /// head must be untouched by this PR.</summary>
    [Fact]
    public void GetPgIndexBloat_IsUntouchedByThisLane()
    {
        var served = McpToolGuideTests.Served("get_pg_index_bloat");
        Assert.Contains("ESTIMATED", served.Served, StringComparison.Ordinal);
        Assert.Contains("skipped_reason present = NO answer, not healthy", served.Served, StringComparison.Ordinal);
    }
}
