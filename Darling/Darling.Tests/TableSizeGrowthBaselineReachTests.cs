/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_table_index_sizes' nominal-window figures span about the window they are named for. Each baseline is
/// the newest snapshot at or before its cutoff, and a store whose snapshots are sparse (a collector that
/// stalled, a server that was out of scope for a while) can put that snapshot weeks past the cutoff: a
/// "7-day" figure measured from a 41-day-old snapshot is the 30-day figure under another name, and the two
/// read identical on every steadily growing table. A figure is kept only when the span from its baseline to
/// the latest snapshot is within half the window of the window itself (3.5 to 10.5 days for 7, 15 to 45 for
/// 30), so the two windows can never resolve to one measurement, and the row's note says which baseline was
/// out of reach instead of blaming the table.
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
    public void AGapThatLeavesOneSnapshotForBothCutoffs_KeepsThe30DayFigure_AndRefusesThe7DayOne()
    {
        /* Snapshots exist for the last five days and from 41 days back; nothing between. Both cutoffs resolve to the
           41-day-old snapshot, where the table held 229 MB; it holds 352 MB now. */
        var gap = Latest.AddDays(-41);
        var row = Row(current: 352, at7d: 229, at30d: 229, snapshot7d: gap, snapshot30d: gap);

        Assert.Equal(123, row.Growth30dMb);
        Assert.Null(row.Growth7dMb);
        Assert.Equal(252, row.GrowthOverAvailableHistoryMb);

        var note = DarlingMcpObjectStatsTools.GrowthNote(row);
        Assert.NotNull(note);
        Assert.Contains("growth_7d_mb is null", note, StringComparison.Ordinal);
        Assert.DoesNotContain("created since", note, StringComparison.Ordinal);
        Assert.DoesNotContain("no snapshot 7+ days old exists", note, StringComparison.Ordinal);
    }

    [Fact]
    public void SparseSnapshots_A12DayOldBaselineIsNotA7DayOne_ButA33DayOldOneIsA30DayOne()
    {
        var row = Row(current: 352, at7d: 316, at30d: 253, snapshot7d: Latest.AddDays(-12), snapshot30d: Latest.AddDays(-33));

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
            Assert.Equal(52, figure);
        else
            Assert.Null(figure);
    }

    [Fact]
    public void ATableYoungerThanTheGap_IsNotToldItsWholeSizeIsNewerThan7Days()
    {
        /* The table was created 20 days ago. The only snapshot at or before either cutoff is 41 days old, so it is in
           neither: the 30-day verdict (created since) is true of it, the 7-day one is not. */
        var gap = Latest.AddDays(-41);
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
    }
}
