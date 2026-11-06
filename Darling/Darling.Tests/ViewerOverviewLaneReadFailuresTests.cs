/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the Overview says when one of its ten lane reads fails. Each lane is drawn from its own read, so one failure costs one
/// lane, and the tab's status line names the reads that failed instead of saying the tab refreshed. Pure logic over the finished
/// tasks, so no store and no WPF object is needed.
/// </summary>
public sealed class ViewerOverviewLaneReadFailuresTests
{
    private static Task Done() => Task.FromResult(0);

    private static Task Failed(string message) => Task.FromException(new TimeoutException(message));

    [Fact]
    public void EveryReadCompleted_IsNoFailure()
    {
        Assert.Null(OverviewLaneReadFailures.Summary([("CPU %", Done()), ("Wait ms/sec", Done()), ("CPU baseline", Done())]));
    }

    [Fact]
    public void OneFailedRead_IsNamed_WithItsMessage()
    {
        var summary = OverviewLaneReadFailures.Summary([("CPU %", Done()), ("Wait ms/sec", Failed("the read missed its deadline")), ("Buffer Pool MB", Done())]);

        Assert.Equal("Overview lanes did not read Wait ms/sec: the read missed its deadline", summary);
    }

    [Fact]
    public void SeveralFailedReads_AreNamedInLaneOrder_AndTheFirstMessageIsGiven()
    {
        var summary = OverviewLaneReadFailures.Summary(
            [("CPU %", Failed("first")), ("Wait ms/sec", Done()), ("I/O ms", Failed("second")), ("I/O baseline", Failed("third"))]);

        Assert.Equal("Overview lanes did not read CPU %, I/O ms, I/O baseline: first", summary);
    }

    [Fact]
    public void ACancelledRead_IsNamed_WithoutAMessage()
    {
        var summary = OverviewLaneReadFailures.Summary([("CPU %", Task.FromCanceled(new CancellationToken(canceled: true))), ("Wait ms/sec", Done())]);

        Assert.Equal("Overview lanes did not read CPU %", summary);
    }
}
