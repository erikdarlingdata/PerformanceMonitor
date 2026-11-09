/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's get_table_index_sizes row judges its baselines the way Darling's does: a nominal-window figure stands only
/// when the span from its baseline snapshot to the latest snapshot is within half the window of the window itself
/// (3.5 to 10.5 days for 7, 15 to 45 for 30). A store whose snapshots are sparse resolves both cutoffs to one old
/// snapshot, and without the rule the "7-day" figure is the 30-day one under another name.
/// </summary>
public sealed class ObjectSizeGrowthBaselineReachTests
{
    private static readonly DateTime Latest = new(2026, 11, 9, 6, 0, 0);

    private static ObjectSizeGrowthBaselineRow Row(
        decimal? current, decimal? at7d, decimal? at30d, DateTime? snapshot7d, DateTime? snapshot30d, decimal? oldest = 100, int daysOfData = 84) =>
        new()
        {
            DatabaseName = "Hist", SchemaName = "dbo", TableName = "TableA",
            CurrentReservedMb = current, CurrentUsedMb = current - 1, TotalRows = 1000, IndexCount = 1,
            ReservedMb7dAgo = at7d, ReservedMb30dAgo = at30d, ReservedMbOldest = oldest,
            Snapshot7dTime = snapshot7d, Snapshot30dTime = snapshot30d,
            EarliestSnapshotTime = Latest.AddDays(-daysOfData), LatestSnapshotTime = Latest, DaysOfData = daysOfData
        };

    [Fact]
    public void AGapThatLeavesOneSnapshotForBothCutoffs_KeepsThe30DayFigure_AndRefusesThe7DayOne()
    {
        var gap = Latest.AddDays(-41);
        var row = Row(current: 352, at7d: 229, at30d: 229, snapshot7d: gap, snapshot30d: gap);

        Assert.Equal(123m, row.Growth30dMb);
        Assert.Null(row.Growth7dMb);
        Assert.Equal(252m, row.GrowthOverAvailableHistoryMb);

        var note = McpObjectStatsTools.GrowthNote(row);
        Assert.NotNull(note);
        Assert.Contains("growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.DoesNotContain("created since", note, StringComparison.Ordinal);
        Assert.DoesNotContain("no snapshot 7+ days old exists", note, StringComparison.Ordinal);
    }

    [Fact]
    public void DailySnapshots_BothFiguresStand_AndTheRowCarriesNoNote()
    {
        var row = Row(current: 352, at7d: 331, at30d: 262, snapshot7d: Latest.AddDays(-7), snapshot30d: Latest.AddDays(-30));

        Assert.Equal(21m, row.Growth7dMb);
        Assert.Equal(90m, row.Growth30dMb);
        Assert.Null(McpObjectStatsTools.GrowthNote(row));
    }

    [Theory]
    [InlineData(7, 3.5, true)]
    [InlineData(7, 10.5, true)]
    [InlineData(7, 10.5 + 1.0 / 1440, false)]
    [InlineData(7, 3.5 - 1.0 / 1440, false)]
    [InlineData(30, 15, true)]
    [InlineData(30, 45, true)]
    [InlineData(30, 45 + 1.0 / 1440, false)]
    [InlineData(30, 15 - 1.0 / 1440, false)]
    public void AFigureStands_OnlyWhenItsSpanIsWithinHalfTheWindowOfTheWindow(int window, double spanDays, bool stands)
    {
        var snapshot = Latest.AddDays(-spanDays);
        var row = window == 7
            ? Row(current: 352, at7d: 300, at30d: null, snapshot7d: snapshot, snapshot30d: null)
            : Row(current: 352, at7d: null, at30d: 300, snapshot7d: null, snapshot30d: snapshot);
        var figure = window == 7 ? row.Growth7dMb : row.Growth30dMb;

        if (stands)
            Assert.Equal(52m, figure);
        else
            Assert.Null(figure);
    }

    [Fact]
    public void ATableYoungerThanTheGap_IsNotToldItsWholeSizeIsNewerThan7Days()
    {
        var gap = Latest.AddDays(-41);
        var row = Row(current: 30, at7d: null, at30d: null, snapshot7d: gap, snapshot30d: gap, oldest: null);

        var note = McpObjectStatsTools.GrowthNote(row)!;
        Assert.DoesNotContain("newer than 7 days", note, StringComparison.Ordinal);
        Assert.Contains("growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.Contains($"this table was not in the {gap:o} snapshot (created since), so growth_30d_mb and growth_pct_30d are null", note, StringComparison.Ordinal);
    }

    [Fact]
    public void AStalledCollector_WhoseLatestSnapshotIsDaysOld_DoesNotLabelATwoDaySpanAs7Days()
    {
        var row = Row(current: 352, at7d: 346, at30d: 262, snapshot7d: Latest.AddDays(-2), snapshot30d: Latest.AddDays(-30));

        Assert.Null(row.Growth7dMb);
        Assert.Equal(90m, row.Growth30dMb);
    }
}
