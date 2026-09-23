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

/// <summary>
/// #3898 D3 head pins for the rest of the "pgB" family: <c>get_pg_logging_audit</c> (<c>DarlingMcpPgLoggingAuditTools</c>),
/// <c>get_pg_io_stats</c> (<c>DarlingMcpPgIoTools</c>) and <c>get_pg_wait_sampling</c> (<c>DarlingMcpPgWaitSamplingTools</c>).
/// Darling only - none of the three has a Lite twin. A separate class from <see cref="McpToolGuideHeadsPgBTests"/>
/// so two lanes converting different files in the same family never conflict on one class body.
/// </summary>
public sealed class McpToolGuideHeadsPgBAuditIoWaitTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pg_logging_audit",
        "get_pg_io_stats",
        "get_pg_wait_sampling",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pg_logging_audit", "STORED configuration snapshot"),
        ("get_pg_logging_audit", "never the live server"),
        ("get_pg_logging_audit", "LATEST IS A TIME"),
        ("get_pg_logging_audit", "unknown means the setting is missing from the snapshot, not the same as off"),
        ("get_pg_logging_audit", "partial means a THRESHOLD is filtering"),
        ("get_pg_logging_audit", "PostgreSQL-only"),
        ("get_pg_io_stats", "track_io_timing is OFF by default"),
        ("get_pg_io_stats", "never a false 0.000"),
        ("get_pg_io_stats", "Write counters are always null on Aurora"),
        ("get_pg_io_stats", "THE PAGE IS BOUNDED BY limit"),
        ("get_pg_io_stats", "SHARES ARE OF THE WINDOW, NOT OF THE PAGE"),
        ("get_pg_wait_sampling", "One of three instruments feeds each target"),
        ("get_pg_wait_sampling", "extension_sampled"),
        ("get_pg_wait_sampling", "service_sampled"),
        ("get_pg_wait_sampling", "a FLOOR that undercounts sub-second waits"),
        ("get_pg_wait_sampling", "SAMPLE COUNTS, not measured durations"),
        ("get_pg_wait_sampling", "event_type CPU means running, not waiting"),
        ("get_pg_wait_sampling", "THE PAGE IS BOUNDED BY limit"),
        ("get_pg_wait_sampling", "SHARES ARE OF THE WINDOW, NOT OF THE PAGE"),
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

    /// <summary>Nothing dropped: every original sentence rides in the tail, including the ones D2's head
    /// compression left out of the terse head text (the full 7-setting list, the RDS/Aurora remedy syntax,
    /// the PLANNED consumer note, and the get_pg_plan_capture_readiness hand-off).</summary>
    [Fact]
    public void D3HeadCompression_LandsEveryDroppedSentenceInTheTail()
    {
        var audit = McpToolGuideTests.Served("get_pg_logging_audit");
        Assert.Contains("log_min_duration_statement, log_lock_waits, log_temp_files, log_autovacuum_min_duration, log_checkpoints, log_connections, log_disconnections", audit.Tail!, StringComparison.Ordinal);
        Assert.Contains("a parameter group on RDS/Aurora", audit.Tail!, StringComparison.Ordinal);
        Assert.Contains("says PLANNED where that consumer does not ship yet", audit.Tail!, StringComparison.Ordinal);
        Assert.Contains("get_pg_plan_capture_readiness", audit.Tail!, StringComparison.Ordinal);
        Assert.EndsWith("PostgreSQL-only.", audit.Tail!, StringComparison.Ordinal);

        var io = McpToolGuideTests.Served("get_pg_io_stats");
        Assert.Contains("Richer than SQL Server's file-level dm_io_virtual_file_stats", io.Tail!, StringComparison.Ordinal);
        Assert.Contains("Requires PostgreSQL 16 or later; valid on a standby.", io.Tail!, StringComparison.Ordinal);

        var wait = McpToolGuideTests.Served("get_pg_wait_sampling");
        Assert.Contains("Aurora native > pg_wait_sampling extension > service sampler", wait.Tail!, StringComparison.Ordinal);
        Assert.Contains("queryid joins get_pg_top_queries; queryid 0 is work belonging to no statement", wait.Tail!, StringComparison.Ordinal);
        Assert.Contains("The profile is cluster-wide and carries no database attribution by design.", wait.Tail!, StringComparison.Ordinal);
    }

    /// <summary>D8: no renames, no consolidation. D2's parameter cap: <c>get_pg_io_stats</c>'s <c>limit</c>
    /// description was 235 chars, tightened to fit 200 without dropping either guardrail word the shared
    /// <c>EveryPercentTool_NamesItsDenominator...</c> pin checks for.</summary>
    [Fact]
    public void IoStatsLimitParameter_FitsD2Cap_AndKeepsItsGuardrailWords()
    {
        var served = McpToolGuideTests.Served("get_pg_io_stats");
        var limit = Assert.Single(served.ParameterDescriptionLengths, x => x.Parameter == "limit");
        Assert.True(limit.Length <= 200, $"get_pg_io_stats.limit: {limit.Length} > 200");
    }
}
