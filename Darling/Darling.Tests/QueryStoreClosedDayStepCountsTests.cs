/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#9690: a failed hourly prune slice shows in the closed-day step's counts, not only in the log.</summary>
public sealed class QueryStoreClosedDayStepCountsTests
{
    [Fact]
    public void APruneFailure_CountsAsOneFailure_AndLeadsTheDetail_WhileTheBuildsStillCount()
    {
        var tick = new QueryStoreClosedDayRollup.TickResult(
            Built: 2, Failed: 1, DaysRemoved: 3, Deferred: 4, HoursBuilt: 5, HoursFailed: 1, HoursRemoved: 6, HoursDeferred: 2, ClutterDaysFolded: 1,
            PruneFailure: "pruning hours 2026-01-01 03 to 2026-01-01 04 failed: statement timeout");

        var counts = DarlingWorker.QueryStoreClosedDayStepCounts(tick);

        Assert.Equal(3, counts.Failed);
        Assert.Equal(8L, counts.Built);
        Assert.Equal(9L, counts.Removed);
        Assert.StartsWith("pruning hours 2026-01-01 03 to 2026-01-01 04 failed: statement timeout", counts.Detail);
        Assert.Contains("6 left for the next tick", counts.Detail);
    }

    [Fact]
    public void APruneFailureWithNothingDeferred_IsTheWholeDetail_AndOneFailure()
    {
        var counts = DarlingWorker.QueryStoreClosedDayStepCounts(
            new QueryStoreClosedDayRollup.TickResult(0, 0, 0, 0, PruneFailure: "pruning hours a to b failed: boom"));

        Assert.Equal(1, counts.Failed);
        Assert.Equal("pruning hours a to b failed: boom", counts.Detail);
    }

    [Fact]
    public void AClutterDayFoldFailure_CountsAsAFailure_SoTheStepReadsFailed()
    {
        var counts = DarlingWorker.QueryStoreClosedDayStepCounts(
            new QueryStoreClosedDayRollup.TickResult(2, 0, 3, 0, HoursBuilt: 5, ClutterDaysFolded: 1, ClutterDaysFailed: 2));

        Assert.Equal(2, counts.Failed);
        Assert.Equal(8L, counts.Built);
        Assert.Null(counts.Detail);
    }

    [Fact]
    public void ATickWithoutAPruneFailure_KeepsTheSameCountsAsBefore()
    {
        var counts = DarlingWorker.QueryStoreClosedDayStepCounts(
            new QueryStoreClosedDayRollup.TickResult(2, 1, 3, 4, HoursBuilt: 5, HoursFailed: 1, HoursRemoved: 6, HoursDeferred: 2, ClutterDaysFolded: 1));

        Assert.Equal(2, counts.Failed);
        Assert.Equal(8L, counts.Built);
        Assert.Equal(9L, counts.Removed);
        Assert.Equal("6 left for the next tick", counts.Detail);

        Assert.Null(DarlingWorker.QueryStoreClosedDayStepCounts(new QueryStoreClosedDayRollup.TickResult(0, 0, 0, 0)).Detail);
    }
}
