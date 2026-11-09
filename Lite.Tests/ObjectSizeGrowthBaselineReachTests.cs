/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's get_table_index_sizes row judges its baselines the way Darling's does: a nominal-window figure stands only
/// when the span from its baseline snapshot to the latest snapshot is within a day of the window (6 to 8 days for 7,
/// 29 to 31 for 30), the one-day rule the storage-growth reader already applies to database baselines. A store whose
/// snapshots are sparse resolves both cutoffs to one old snapshot, and without the rule the "7-day" figure is the
/// 30-day one under another name. Two figures that agree because the table held the same size at two different
/// snapshots are two real measurements, and both stand.
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
    public void TheReachRule_IsOneDayEitherSideOfTheWindow()
    {
        Assert.Equal(TimeSpan.FromDays(6), GrowthBaselineReach.ShortestSpan(7));
        Assert.Equal(TimeSpan.FromDays(8), GrowthBaselineReach.LongestSpan(7));
        Assert.Equal(TimeSpan.FromDays(29), GrowthBaselineReach.ShortestSpan(30));
        Assert.Equal(TimeSpan.FromDays(31), GrowthBaselineReach.LongestSpan(30));
    }

    [Fact]
    public void TwoBaselinesThatHeldTheSameSize_AreTwoRealMeasurements_AndBothStand()
    {
        var row = Row(current: 352m, at7d: 315.61m, at30d: 315.61m, snapshot7d: Latest.AddDays(-7), snapshot30d: Latest.AddDays(-30));

        Assert.True(row.Covers7d);
        Assert.True(row.Covers30d);
        Assert.Equal(36.39m, row.Growth7dMb);
        Assert.Equal(36.39m, row.Growth30dMb);
        Assert.Null(McpObjectStatsTools.GrowthNote(row));
        Assert.Null(McpObjectStatsTools.HistoryNote(row));
    }

    [Fact]
    public void AGapThatLeavesOneSnapshotForBothCutoffs_KeepsThe30DayFigure_AndRefusesThe7DayOne()
    {
        var gap = Latest.AddDays(-30);
        var row = Row(current: 352, at7d: 262, at30d: 262, snapshot7d: gap, snapshot30d: gap);

        Assert.Equal(90m, row.Growth30dMb);
        Assert.Null(row.Growth7dMb);
        Assert.Equal(252m, row.GrowthOverAvailableHistoryMb);

        var note = McpObjectStatsTools.GrowthNote(row);
        Assert.NotNull(note);
        Assert.Contains("growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.DoesNotContain("created since", note, StringComparison.Ordinal);
        Assert.DoesNotContain("no snapshot 7+ days old exists", note, StringComparison.Ordinal);

        var history = McpObjectStatsTools.HistoryNote(row)!;
        Assert.Contains(gap.ToString("o"), history, StringComparison.Ordinal);
        Assert.Contains("outside the 6 to 8 days a 7-day figure may span", history, StringComparison.Ordinal);
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
    [InlineData(7, 6, true)]
    [InlineData(7, 8, true)]
    [InlineData(7, 8 + 1.0 / 1440, false)]
    [InlineData(7, 6 - 1.0 / 1440, false)]
    [InlineData(30, 29, true)]
    [InlineData(30, 31, true)]
    [InlineData(30, 31 + 1.0 / 1440, false)]
    [InlineData(30, 29 - 1.0 / 1440, false)]
    public void AFigureStands_OnlyWhenItsSpanIsWithinADayOfTheWindow(int window, double spanDays, bool stands)
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
        var gap = Latest.AddDays(-30);
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
