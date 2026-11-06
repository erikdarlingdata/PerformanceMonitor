/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// What Lite's Overview says when one of its ten lane reads fails, through the shared <see cref="OverviewLaneReadFailures"/>.
/// Each lane is drawn from its own read, so one failure costs one lane, and the tab's status line names the reads that failed
/// instead of saying "Last refresh".
/// </summary>
public sealed class OverviewLaneReadFailuresTests
{
    private static Task Done() => Task.FromResult(0);

    [Fact]
    public void EveryReadCompleted_IsNoFailure()
    {
        Assert.Null(OverviewLaneReadFailures.Summary([("CPU %", Done()), ("Wait ms/sec", Done())]));
    }

    [Fact]
    public void FailedReads_AreNamedInLaneOrder_WithTheFirstMessage()
    {
        var summary = OverviewLaneReadFailures.Summary(
            [("CPU %", Task.FromException(new TimeoutException("first"))), ("Wait ms/sec", Done()), ("I/O ms", Task.FromException(new TimeoutException("second")))]);

        Assert.Equal("Overview lanes did not read CPU %, I/O ms: first", summary);
    }
}
