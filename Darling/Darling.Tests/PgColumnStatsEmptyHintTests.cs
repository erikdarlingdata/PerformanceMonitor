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
        Assert.Contains("No column statistics for pg-a in the last 24 hours.", unreadable.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);

        var other = Enum.GetValues<PgColumnStatsCoverageArm>().First(a => a != PgColumnStatsCoverageArm.StatisticsNotVisible);
        using var floor = JsonDocument.Parse(DarlingMcpPgIndexTools.EmptyColumnStatsReply("pg-a", 24, new PgColumnStatsCoverageVerdict(other, "Census.", "Cause.")));
        Assert.False(floor.RootElement.TryGetProperty("hints", out var hints) && hints.TryGetProperty("reason", out _));
    }

    /// <summary>#8738: pg_stats keeps correlation, null_frac and the top-value share as 4-byte floats, so a reply figure carries six significant digits.</summary>
    [Theory]
    [InlineData(0.965292751789093, 0.965293)]
    [InlineData(0.9639183282852173, 0.963918)]
    [InlineData(0.15341947972774506, 0.153419)]
    [InlineData(0.43907058238983154, 0.439071)]
    [InlineData(-0.0123456789, -0.0123457)]
    [InlineData(1.0, 1.0)]
    public void ColumnStatsFigure_RoundsToSixSignificantDigits(double stored, double expected)
    {
        Assert.Equal(expected, DarlingMcpPgIndexTools.SixSignificantDigits(stored));
    }

    [Fact]
    public void ColumnStatsFigure_AbsentStaysAbsent_AndANonFiniteValuePassesThrough()
    {
        Assert.Null(DarlingMcpPgIndexTools.SixSignificantDigits(null));
        Assert.True(double.IsNaN(DarlingMcpPgIndexTools.SixSignificantDigits(double.NaN)!.Value));
    }
}
