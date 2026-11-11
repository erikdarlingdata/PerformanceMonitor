/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8567: the parts of the oversized-plan backlog's bound that need no store: what a pass's run record says about
/// the rows it dropped unfetched, the cap those rows are held to, and how one row's stamps read as a verdict.
/// </summary>
public sealed class OversizedPlanBacklogBoundTests
{
    [Fact]
    public void APassThatDroppedRows_SaysHowManyAndWhy_AndIsStillASuccess()
    {
        var tally = new OversizedPlanBacklogSweep.SweepTally
        {
            ServersSwept = 2,
            PlansClaimed = 10,
            PlansCaptured = 10,
            RowsSuperseded = 7,
            RowsOverCap = 2,
        };

        var (status, message) = OversizedPlanBacklogSweep.BuildRunRecordSummary(tally);

        Assert.Equal("SUCCESS", status);
        Assert.Contains("9 dropped unfetched", message, StringComparison.Ordinal);
        Assert.Contains("7 superseded", message, StringComparison.Ordinal);
        Assert.Contains("2 past the pending cap", message, StringComparison.Ordinal);
    }

    [Fact]
    public void APassThatDroppedNothing_SaysNothingAboutDrops()
    {
        var (status, message) = OversizedPlanBacklogSweep.BuildRunRecordSummary(
            new OversizedPlanBacklogSweep.SweepTally { ServersSwept = 1 });

        Assert.Equal("SUCCESS", status);
        Assert.DoesNotContain("dropped", message, StringComparison.Ordinal);
        Assert.DoesNotContain("trim", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ATrimThatDidNotComplete_MakesThePassAWarning_ThatSaysSo()
    {
        var (status, message) = OversizedPlanBacklogSweep.BuildRunRecordSummary(
            new OversizedPlanBacklogSweep.SweepTally { ServersSwept = 3, TrimFaults = 1 });

        Assert.Equal("WARNING", status);
        Assert.Contains("the trim did not complete on 1 server", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePendingCap_IsWhatTheCeilingDrainsInOneRetentionPeriod_AndOneStatementDropsFewerThanThat()
    {
        Assert.Equal(
            OversizedPlanBacklogSweep.RowsPerServerPerDayCeiling * DarlingRetention.OversizedPlanBacklogRetentionDays,
            OversizedPlanBacklogSweep.PendingRowsPerServerCap);

        /* The per-pass claim is what it was: the cap bounds what waits, not what one pass fetches. */
        Assert.Equal(10, OversizedPlanBacklogSweep.MaxPlansPerServerPerTick);

        /* A day of passes at the per-statement limit outruns the cap many times over, so a server far past it comes down. */
        Assert.True(OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick > 0);
        Assert.True(
            (long)OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick * (int)(TimeSpan.FromDays(1) / OversizedPlanBacklogSweep.SweepInterval)
            > OversizedPlanBacklogSweep.PendingRowsPerServerCap);
    }

    [Theory]
    [InlineData(false, false, false, "pending")]
    [InlineData(false, false, true, "pending")]
    [InlineData(false, true, true, "expired")]
    [InlineData(false, true, false, "dropped")]
    [InlineData(true, false, true, "captured")]
    [InlineData(true, true, true, "captured")]
    public void ARowsStamps_ReadAsOneVerdict_AndAStampWithNoAttemptIsADrop(bool captured, bool expired, bool attempted, string expected)
    {
        var at = new DateTime(2026, 9, 13, 14, 50, 0, DateTimeKind.Unspecified);

        Assert.Equal(
            expected,
            DarlingOversizedPlanBacklogReader.VerdictOf(captured ? at : null, expired ? at : null, attempted ? at : null));
    }
}
