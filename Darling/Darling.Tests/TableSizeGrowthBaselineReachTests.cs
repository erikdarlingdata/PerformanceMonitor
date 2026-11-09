/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_table_index_sizes' nominal-window figures span about the window they are named for. Each baseline is
/// the newest snapshot at or before its cutoff, and a store whose snapshots are sparse (a collector that
/// stalled, a server that was out of scope for a while) can put that snapshot weeks past the cutoff: a
/// "7-day" figure measured from a 30-day-old snapshot is the 30-day figure under another name, and the two
/// read identical on every steadily growing table. A figure is kept only when the span from its baseline to
/// the latest snapshot is within a day of the window (6 to 8 days for 7, 29 to 31 for 30), the same one-day
/// rule get_storage_growth judges its database baselines by, so the two windows can never resolve to one
/// measurement, and the row's note says which baseline was out of reach instead of blaming the table. Two
/// figures that agree because the table held the same size at two different snapshots are two real
/// measurements, and both stand.
/// </summary>
public sealed class TableSizeGrowthBaselineReachTests
{
    private static readonly DateTime Latest = new(2026, 11, 9, 6, 0, 0);

    private static TableSizeGrowthRow Row(
        double? current, double? at7d, double? at30d, DateTime? snapshot7d, DateTime? snapshot30d, double? oldest = 100, int daysOfData = 84) =>
        new("Hist", "dbo", "TableA", CurrentReservedMb: current, CurrentUsedMb: current - 1, TotalRows: 1000, IndexCount: 1,
            ReservedMb7dAgo: at7d, ReservedMb30dAgo: at30d, ReservedMbOldest: oldest,
            Snapshot7dTime: snapshot7d, Snapshot30dTime: snapshot30d,
            EarliestSnapshotTime: Latest.AddDays(-daysOfData), LatestSnapshotTime: Latest, DaysOfData: daysOfData);

    [Fact]
    public void TheReachRule_IsTheStorageGrowthReadersOneDayRule()
    {
        /* One rule for both readers: a table's baseline may sit as far from its mark as a database's may. */
        Assert.Equal(TimeSpan.FromDays(7) - DarlingFinOpsStorageGrowthReader.BaselineTolerance, GrowthBaselineReach.ShortestSpan(7));
        Assert.Equal(TimeSpan.FromDays(7) + DarlingFinOpsStorageGrowthReader.BaselineTolerance, GrowthBaselineReach.LongestSpan(7));
        Assert.Equal(TimeSpan.FromDays(30) - DarlingFinOpsStorageGrowthReader.BaselineTolerance, GrowthBaselineReach.ShortestSpan(30));
        Assert.Equal(TimeSpan.FromDays(30) + DarlingFinOpsStorageGrowthReader.BaselineTolerance, GrowthBaselineReach.LongestSpan(30));
        Assert.Equal("6 to 8 days", GrowthBaselineReach.DescribeRange(7));
        Assert.Equal("29 to 31 days", GrowthBaselineReach.DescribeRange(30));
    }

    [Fact]
    public void TwoBaselinesThatHeldTheSameSize_AreTwoRealMeasurements_AndBothStand()
    {
        /* Daily snapshots, and the table held 315.61 MB at both the 7-day and the 30-day snapshot: it did not change between
           them and grew 36.39 MB since. The two figures agree because the measurements agree, not because one baseline stood
           in for the other, so neither is refused and the row carries no note. */
        var row = Row(current: 352, at7d: 315.61, at30d: 315.61, snapshot7d: Latest.AddDays(-7), snapshot30d: Latest.AddDays(-30));

        Assert.True(row.Covers7d);
        Assert.True(row.Covers30d);
        Assert.Equal(36.39, row.Growth7dMb!.Value, 9);
        Assert.Equal(36.39, row.Growth30dMb!.Value, 9);
        Assert.Null(DarlingMcpObjectStatsTools.GrowthNote(row));
        Assert.Null(DarlingMcpObjectStatsTools.HistoryNote(row));
    }

    [Fact]
    public void AGapThatLeavesOneSnapshotForBothCutoffs_KeepsThe30DayFigure_AndRefusesThe7DayOne()
    {
        /* Snapshots exist for the last five days and from 30 days back; nothing between. Both cutoffs resolve to the
           30-day-old snapshot, where the table held 262 MB; it holds 352 MB now. */
        var gap = Latest.AddDays(-30);
        var row = Row(current: 352, at7d: 262, at30d: 262, snapshot7d: gap, snapshot30d: gap);

        Assert.Equal(90, row.Growth30dMb);
        Assert.Null(row.Growth7dMb);
        Assert.Equal(252, row.GrowthOverAvailableHistoryMb);

        var note = DarlingMcpObjectStatsTools.GrowthNote(row);
        Assert.NotNull(note);
        Assert.Contains("growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.DoesNotContain("created since", note, StringComparison.Ordinal);
        Assert.DoesNotContain("no snapshot 7+ days old exists", note, StringComparison.Ordinal);

        var history = DarlingMcpObjectStatsTools.HistoryNote(row)!;
        Assert.Contains($"the snapshot nearest 7 days ago is from {gap:o}", history, StringComparison.Ordinal);
        Assert.Contains("30 days before the latest snapshot", history, StringComparison.Ordinal);
        Assert.Contains("outside the 6 to 8 days a 7-day figure may span", history, StringComparison.Ordinal);
    }

    [Fact]
    public void SparseSnapshots_A12DayOldBaselineIsNotA7DayOne_ButA31DayOldOneIsA30DayOne()
    {
        var row = Row(current: 352, at7d: 316, at30d: 253, snapshot7d: Latest.AddDays(-12), snapshot30d: Latest.AddDays(-31));

        Assert.Null(row.Growth7dMb);
        Assert.Equal(99, row.Growth30dMb);
        Assert.NotNull(row.GrowthPct30d);
    }

    [Fact]
    public void DailySnapshots_BothFiguresStand_AndTheRowCarriesNoNote()
    {
        var row = Row(current: 352, at7d: 331, at30d: 262, snapshot7d: Latest.AddDays(-7), snapshot30d: Latest.AddDays(-30));

        Assert.Equal(21, row.Growth7dMb);
        Assert.Equal(90, row.Growth30dMb);
        Assert.Equal(90 * 100.0 / 262, row.GrowthPct30d!.Value, 9);
        Assert.Null(DarlingMcpObjectStatsTools.GrowthNote(row));
    }

    /* A daily collector's snapshots sit a whole number of days apart, give or take the seconds its runs start early or late,
       so the span is judged to the nearest whole day: 6, 7 or 8 days for a 7-day figure, 29, 30 or 31 for a 30-day one. A
       minute of drift either side of the 6- and 8-day edges a missed run lands on cannot flip the figure. */
    [Theory]
    [InlineData(7, 6, true)]
    [InlineData(7, 6 - 1.0 / 1440, true)]
    [InlineData(7, 6 + 1.0 / 1440, true)]
    [InlineData(7, 8, true)]
    [InlineData(7, 8 - 1.0 / 1440, true)]
    [InlineData(7, 8 + 1.0 / 1440, true)]
    [InlineData(7, 5.5, true)]
    [InlineData(7, 5.5 - 1.0 / 1440, false)]
    [InlineData(7, 8.5 - 1.0 / 1440, true)]
    [InlineData(7, 8.5, false)]
    [InlineData(7, 2, false)]
    [InlineData(30, 29 - 1.0 / 1440, true)]
    [InlineData(30, 31 + 1.0 / 1440, true)]
    [InlineData(30, 28.5, true)]
    [InlineData(30, 28.5 - 1.0 / 1440, false)]
    [InlineData(30, 31.5 - 1.0 / 1440, true)]
    [InlineData(30, 31.5, false)]
    public void AFigureStands_OnlyWhenItsSpanIsWithinADayOfTheWindow_ToTheNearestDay(int window, double spanDays, bool stands)
    {
        var snapshot = Latest.AddDays(-spanDays);
        var row = window == 7
            ? Row(current: 352, at7d: 300, at30d: null, snapshot7d: snapshot, snapshot30d: null)
            : Row(current: 352, at7d: null, at30d: 300, snapshot7d: null, snapshot30d: snapshot);
        var figure = window == 7 ? row.Growth7dMb : row.Growth30dMb;

        if (stands)
            Assert.Equal(52, figure);
        else
            Assert.Null(figure);
    }

    [Fact]
    public void ATableYoungerThanTheGap_IsNotToldItsWholeSizeIsNewerThan7Days()
    {
        /* The table was created 20 days ago. The only snapshot at or before either cutoff is 30 days old, so it is in
           neither: the 30-day verdict (created since) is true of it, the 7-day one is not. */
        var gap = Latest.AddDays(-30);
        var row = Row(current: 30, at7d: null, at30d: null, snapshot7d: gap, snapshot30d: gap, oldest: null);

        Assert.Null(row.Growth7dMb);
        Assert.Null(row.Growth30dMb);

        var note = DarlingMcpObjectStatsTools.GrowthNote(row)!;
        Assert.DoesNotContain("newer than 7 days", note, StringComparison.Ordinal);
        Assert.Contains("growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.Contains($"this table was not in the {gap:o} snapshot (created since), so growth_30d_mb and growth_pct_30d are null", note, StringComparison.Ordinal);
    }

    [Fact]
    public void AStalledCollector_WhoseLatestSnapshotIsDaysOld_DoesNotLabelATwoDaySpanAs7Days()
    {
        /* The latest snapshot is five days old, so the newest snapshot at or before the 7-day cutoff is only two days
           before it. */
        var row = Row(current: 352, at7d: 346, at30d: 262, snapshot7d: Latest.AddDays(-2), snapshot30d: Latest.AddDays(-30));

        Assert.Null(row.Growth7dMb);
        Assert.Equal(90, row.Growth30dMb);
        Assert.Contains("growth_7d_mb is null", DarlingMcpObjectStatsTools.GrowthNote(row), StringComparison.Ordinal);
    }

    [Fact]
    public void NoSnapshotThatOld_KeepsItsOwnWording()
    {
        var row = Row(current: 160, at7d: null, at30d: null, snapshot7d: null, snapshot30d: null, daysOfData: 3);

        var note = DarlingMcpObjectStatsTools.GrowthNote(row)!;
        Assert.Contains("no snapshot 7+ days old exists, so growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.Contains("no snapshot 30+ days old exists, so growth_30d_mb and growth_pct_30d are null", note, StringComparison.Ordinal);
        Assert.Contains("3 days of index snapshots", DarlingMcpObjectStatsTools.HistoryNote(row), StringComparison.Ordinal);
    }

    [Fact]
    public void AGapWiderThanADayOnBothSidesOfTheCutoff_SaysNoSnapshotLiesWithinADay_NotThatNoneIsThatOld()
    {
        /* The store holds 40 days of snapshots, so snapshots older than 7 days exist; none lies within a day of the 7-day
           cutoff, so the 7-day baseline is null. The 30-day baseline is in reach. */
        var row = Row(current: 352, at7d: null, at30d: 262, snapshot7d: null, snapshot30d: Latest.AddDays(-30), daysOfData: 40);

        Assert.Null(row.Growth7dMb);
        Assert.Equal(90, row.Growth30dMb);

        var note = DarlingMcpObjectStatsTools.GrowthNote(row)!;
        Assert.Contains("no snapshot lies within a day of 7 days ago", note, StringComparison.Ordinal);
        Assert.Contains("so growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.DoesNotContain("7+ days old", note, StringComparison.Ordinal);

        var history = DarlingMcpObjectStatsTools.HistoryNote(row)!;
        Assert.Contains("40 days of index snapshots", history, StringComparison.Ordinal);
        Assert.Contains("no snapshot lies within a day of 7 days ago", history, StringComparison.Ordinal);
        Assert.Contains("growth_7d_mb is null", history, StringComparison.Ordinal);
        Assert.DoesNotContain("do not exist", history, StringComparison.Ordinal);
    }
}
