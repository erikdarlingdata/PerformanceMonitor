/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7548: an empty get_pg_column_stats reply carries <c>hints.reason = "pg_stats_unreadable"</c> when the privilege filter is the cause, so the web
/// panel can say one sentence and the grant without reading the message. Any other empty cause sends no such hint, so the panel keeps the tool's text.
/// </summary>
public sealed class PgColumnStatsEmptyHintTests
{
    [Fact]
    public void ThePrivilegeFilterArm_SendsThePgStatsUnreadableReason()
    {
        var hint = DarlingMcpPgIndexTools.EmptyColumnStatsHints(PgColumnStatsCoverageArm.StatisticsNotVisible);

        Assert.NotNull(hint);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(hint));
        Assert.Equal("pg_stats_unreadable", doc.RootElement.GetProperty("reason").GetString());
    }

    [Fact]
    public void EveryOtherEmptyCause_SendsNoHint()
    {
        var others = Enum.GetValues<PgColumnStatsCoverageArm>().Where(a => a != PgColumnStatsCoverageArm.StatisticsNotVisible).ToArray();

        Assert.NotEmpty(others);
        foreach (var arm in others)
        {
            Assert.Null(DarlingMcpPgIndexTools.EmptyColumnStatsHints(arm));
        }
    }

    [Fact]
    public void TheEmptyReply_CarriesThePgStatsUnreadableHint_ForThePrivilegeFilter_AndNoneForAnotherCause()
    {
        // The reply the tool sends, not the helper alone: dropping the hints argument from the envelope must fail here.
        var visible = new PgColumnStatsCoverageVerdict(PgColumnStatsCoverageArm.StatisticsNotVisible, "Census.", "Cause.");
        using var unreadable = JsonDocument.Parse(DarlingMcpPgIndexTools.EmptyColumnStatsReply("pg-a", 24, visible));
        Assert.Equal("empty", unreadable.RootElement.GetProperty("status").GetString());
        Assert.Equal("pg_stats_unreadable", unreadable.RootElement.GetProperty("hints").GetProperty("reason").GetString());
        Assert.Contains("No column statistics for pg-a in the last 24 hour(s).", unreadable.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);

        var other = Enum.GetValues<PgColumnStatsCoverageArm>().First(a => a != PgColumnStatsCoverageArm.StatisticsNotVisible);
        using var floor = JsonDocument.Parse(DarlingMcpPgIndexTools.EmptyColumnStatsReply("pg-a", 24, new PgColumnStatsCoverageVerdict(other, "Census.", "Cause.")));
        Assert.False(floor.RootElement.TryGetProperty("hints", out var hints) && hints.TryGetProperty("reason", out _));
    }
}
