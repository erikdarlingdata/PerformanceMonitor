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

/// <summary>The shared rule that moves the Blocking charts' data start to where the blocked process threshold went on (#5098).</summary>
public sealed class BlockingThresholdCoverageTests
{
    private static readonly DateTime End = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Start = End.AddDays(-7);

    [Fact]
    public void ThresholdOnAtTheStart_KeepsTheCollectorsCoverage()
    {
        Assert.Equal(Start, BlockingThresholdCoverage.Combine(Start, End.AddDays(-1), onAtWindowStart: true, End.AddDays(-4), sawZeroSnapshot: true));
    }

    [Fact]
    public void ThresholdOnAtTheStart_StillTakesAnEarlierReport()
    {
        var report = End.AddDays(-8);
        Assert.Equal(report, BlockingThresholdCoverage.Combine(Start, report, true, null, false));
    }

    [Fact]
    public void FirstNonzeroSnapshotInTheWindow_LaterThanCoverage_IsTheAnswer()
    {
        Assert.Equal(End.AddDays(-4), BlockingThresholdCoverage.Combine(Start, End.AddDays(-3), false, End.AddDays(-4), true));
    }

    [Fact]
    public void AReportBeforeTheFirstNonzeroSnapshot_IsTheAnswer()
    {
        var report = End.AddDays(-4).AddHours(-12);
        Assert.Equal(report, BlockingThresholdCoverage.Combine(Start, report, false, End.AddDays(-4), true));
    }

    [Fact]
    public void CoverageLaterThanTheSnapshot_StaysTheCoverage()
    {
        var coverage = End.AddDays(-2);
        Assert.Equal(coverage, BlockingThresholdCoverage.Combine(coverage, End.AddDays(-1), false, End.AddDays(-4), true));
    }

    [Fact]
    public void OnlyZeroSnapshots_StartAtTheEarliestReport()
    {
        var report = End.AddDays(-1);
        Assert.Equal(report, BlockingThresholdCoverage.Combine(Start, report, false, null, true));
    }

    [Fact]
    public void NoThresholdSnapshots_KeepTodaysAnswer()
    {
        Assert.Equal(Start, BlockingThresholdCoverage.Combine(Start, End.AddDays(-1), false, null, false));
        Assert.Equal(End.AddDays(-1), BlockingThresholdCoverage.Combine(null, End.AddDays(-1), false, null, false));
    }

    [Fact]
    public void NonzeroSnapshotsInTheWindow_WithNoZeroSeen_LeaveTodaysAnswer()
    {
        /* The snapshots before the window may have been purged, or never taken: no zero seen means the threshold's start is unknown, not late. */
        Assert.Equal(Start, BlockingThresholdCoverage.Combine(Start, End.AddDays(-1), false, End.AddDays(-4), sawZeroSnapshot: false));
        Assert.Equal(End.AddDays(-1), BlockingThresholdCoverage.Combine(null, End.AddDays(-1), false, End.AddDays(-4), false));
    }

    [Fact]
    public void NothingKnown_IsNull()
    {
        Assert.Null(BlockingThresholdCoverage.Combine(null, null, false, null, false));
        Assert.Null(BlockingThresholdCoverage.Combine(null, null, true, null, true));
    }

    [Fact]
    public void NoCollectorCoverage_TakesTheThresholdStart_ButNeverLaterThanTheReport()
    {
        Assert.Equal(End.AddDays(-4), BlockingThresholdCoverage.Combine(null, End.AddDays(-3), false, End.AddDays(-4), true));
        Assert.Equal(End.AddDays(-5), BlockingThresholdCoverage.Combine(null, End.AddDays(-5), false, End.AddDays(-4), true));
    }
}
