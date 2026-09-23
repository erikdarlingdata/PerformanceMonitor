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
/// #3898 D3 head pins for the "pgB" family: <c>get_pg_extensions</c>, <c>get_pg_write_stats</c>,
/// <c>get_pg_server_config</c> and <c>get_pg_server_config_changes</c>, all declared on
/// <c>DarlingMcpPgServerStateTools</c> (Darling only - none of the four has a Lite twin). Follows the pattern in
/// <see cref="McpToolGuideHeadsPgATests"/>.
/// </summary>
public sealed class McpToolGuideHeadsPgBTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pg_extensions",
        "get_pg_write_stats",
        "get_pg_server_config",
        "get_pg_server_config_changes",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pg_extensions", "Per-DATABASE, not per-cluster"),
        ("get_pg_extensions", "truncated withholds state totals"),
        ("get_pg_extensions", "installed rows like plpgsql are cut first"),
        ("get_pg_extensions", "Read install_census for the complete picture"),
        ("get_pg_write_stats", "as ONE row, not a series"),
        ("get_pg_write_stats", "checkpoints_requested is the signal"),
        ("get_pg_write_stats", "buffers_backend/buffers_backend_fsync are NULL on PostgreSQL 17+"),
        ("get_pg_write_stats", "wal_* columns are NULL on Aurora"),
        ("get_pg_write_stats", "A restart inside the window nulls checkpoints_requested"),
        ("get_pg_server_config", "LATEST IS A TIME"),
        ("get_pg_server_config", "captured_at is when it was taken"),
        ("get_pg_server_config", "pending_restart=true means the file and server disagree"),
        ("get_pg_server_config", "non_default_count is the snapshot's, not the page's"),
        ("get_pg_server_config", "database_overrides (present only if any exist)"),
        ("get_pg_server_config_changes", "NOT reported as a change"),
        ("get_pg_server_config_changes", "Session-scoped rows are excluded"),
        ("get_pg_server_config_changes", "change_kind: changed, set (old_value null) or reset (new_value null)"),
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

    /// <summary>Nothing dropped: the parameter-overflow sentences D2 moved off <c>get_pg_server_config</c>'s
    /// <c>include_defaults</c> and <c>get_pg_extensions</c>' <c>database_name</c> land verbatim in their tool's
    /// tail rather than disappearing when the parameter itself was trimmed to a guardrail sentence.</summary>
    [Fact]
    public void D2ParameterOverflow_LandsInTheTail_NotJustTheGuardrailSentence()
    {
        var serverConfig = McpToolGuideTests.Served("get_pg_server_config");
        Assert.Contains("kept in the non-default view whatever its source", serverConfig.Tail!, StringComparison.Ordinal);

        var extensions = McpToolGuideTests.Served("get_pg_extensions");
        Assert.Contains("1,632 rows against the 1,000-row maximum", extensions.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D8: no renames, no consolidation - all four still resolve as the same tool names with the same
    /// parameters, just a shorter served head and the two over-cap parameters (D2) trimmed to a pointer.</summary>
    [Fact]
    public void OverCapParameters_StayAtOrUnder200_AndKeepThePointer()
    {
        foreach (var (tool, param) in new[] { ("get_pg_server_config", "include_defaults"), ("get_pg_extensions", "database_name") })
        {
            var served = McpToolGuideTests.Served(tool);
            var p = Assert.Single(served.ParameterDescriptionLengths, x => x.Parameter == param);
            Assert.True(p.Length <= 200, $"{tool}.{param}: {p.Length} > 200");
        }
    }
}
