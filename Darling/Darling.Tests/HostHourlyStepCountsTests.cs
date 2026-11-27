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

/// <summary>
/// #9711: the host hourly rollup tick catches a failed prune slice so the builds go on, and the step it records still has to say the prune failed.
/// </summary>
public sealed class HostHourlyStepCountsTests
{
    [Fact]
    public void ATickWithAPruneFailure_CountsOneFailure_AndLeadsTheDetailWithIt()
    {
        var tick = new QueryStatsHostHourly.TickResult(HoursBuilt: 3, ServerHoursBuilt: 6, Failed: 1, Deferred: 2, RowsRemoved: 40, PruneFailure: "prune of hours 2026-08-01 01:00 to 2026-08-01 02:00 failed: prune blocked");

        var counts = DarlingWorker.HostHourlyStepCounts(tick);

        Assert.Equal(2, counts.Failed);
        Assert.Equal(3L, counts.Built);
        Assert.Equal(40L, counts.Removed);
        Assert.StartsWith("prune of hours 2026-08-01 01:00", counts.Detail, System.StringComparison.Ordinal);
        Assert.Contains("2 left for the next tick", counts.Detail, System.StringComparison.Ordinal);
    }

    [Fact]
    public void ATickWithOnlyAPruneFailure_FailsTheStepWithTheFailureAsItsDetail()
    {
        var tick = new QueryStatsHostHourly.TickResult(0, 0, 0, 0, 0, "prune of hours a to b failed: x");

        var counts = DarlingWorker.HostHourlyStepCounts(tick);

        Assert.Equal(1, counts.Failed);
        Assert.Equal("prune of hours a to b failed: x", counts.Detail);
    }

    [Fact]
    public void ATickWithoutAPruneFailure_MapsAsItDidBefore()
    {
        var tick = new QueryStatsHostHourly.TickResult(HoursBuilt: 3, ServerHoursBuilt: 6, Failed: 1, Deferred: 2, RowsRemoved: 40);

        var counts = DarlingWorker.HostHourlyStepCounts(tick);

        Assert.Equal(1, counts.Failed);
        Assert.Equal(3L, counts.Built);
        Assert.Equal(40L, counts.Removed);
        Assert.Equal("2 left for the next tick", counts.Detail);
        Assert.Null(DarlingWorker.HostHourlyStepCounts(new QueryStatsHostHourly.TickResult(1, 1, 0, 0, 0)).Detail);
    }
}
