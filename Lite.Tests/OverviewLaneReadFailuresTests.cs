/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// What Lite's Overview says when one of its ten lane reads fails, through the shared <see cref="OverviewLaneReadFailures"/>.
/// Each lane is drawn from its own read, so one failure costs one lane, and the tab's status line names the reads that failed
/// instead of saying "Last refresh". A failed baseline read alone is not a failed refresh: the lane drew and only its band is
/// missing. The next three tests pin the tab's refresh pass itself (<see cref="ServerTab.RefreshStagesAsync"/>): a failed lane
/// read still lets the alert and permission badges refresh, and the status line is written after them.
/// </summary>
public sealed class OverviewLaneReadFailuresTests
{
    private static Task Done() => Task.FromResult(0);

    private static Task Failed(string message) => Task.FromException(new TimeoutException(message));

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

    [Fact]
    public void AFailedBaselineReadAlone_IsNotAFailedRefresh()
    {
        var status = OverviewLaneReadFailures.Status(
            laneReads: [("CPU %", Done()), ("Wait ms/sec", Done())],
            baselineReads: [("CPU baseline", Failed("the baseline read missed its deadline")), ("Wait baseline", Done())]);

        Assert.Null(status);
    }

    [Fact]
    public void AFailedLaneRead_IsAFailedRefresh_AndEveryFailedReadIsNamed()
    {
        var status = OverviewLaneReadFailures.Status(
            laneReads: [("CPU %", Failed("first")), ("Wait ms/sec", Done())],
            baselineReads: [("CPU baseline", Failed("second")), ("Wait baseline", Done())]);

        Assert.Equal("Overview lanes did not read CPU %, CPU baseline: first", status);
    }

    [Fact]
    public async Task AFailedLaneRead_StillRefreshesTheBadges_AndTheStatusLineIsTheFailure()
    {
        var order = new List<string>();

        var status = await ServerTab.RefreshStagesAsync(
            refreshVisibleTab: () => { order.Add("tab"); return Task.FromResult<string?>("Overview lanes did not read CPU %: the read missed its deadline"); },
            refreshBadges: () => { order.Add("badges"); return Task.CompletedTask; },
            lastRefreshText: () => { order.Add("stamp"); return "Last refresh: 12:00:00 (UTC)"; },
            CancellationToken.None);

        Assert.Equal(new[] { "tab", "badges" }, order);
        Assert.Equal("Error: Overview lanes did not read CPU %: the read missed its deadline", status);
    }

    [Fact]
    public async Task EveryReadCompleted_StampsTheLastRefresh_AfterTheBadges()
    {
        var order = new List<string>();

        var status = await ServerTab.RefreshStagesAsync(
            refreshVisibleTab: () => { order.Add("tab"); return Task.FromResult<string?>(null); },
            refreshBadges: () => { order.Add("badges"); return Task.CompletedTask; },
            lastRefreshText: () => { order.Add("stamp"); return "Last refresh: 12:00:00 (UTC)"; },
            CancellationToken.None);

        Assert.Equal(new[] { "tab", "badges", "stamp" }, order);
        Assert.Equal("Last refresh: 12:00:00 (UTC)", status);
    }

    [Fact]
    public async Task ASupersededPass_SkipsTheBadges_AndWritesNoStatusLine()
    {
        using var superseded = new CancellationTokenSource();
        var order = new List<string>();

        var status = await ServerTab.RefreshStagesAsync(
            refreshVisibleTab: () => { order.Add("tab"); superseded.Cancel(); return Task.FromResult<string?>("Overview lanes did not read CPU %"); },
            refreshBadges: () => { order.Add("badges"); return Task.CompletedTask; },
            lastRefreshText: () => { order.Add("stamp"); return "Last refresh: 12:00:00 (UTC)"; },
            superseded.Token);

        Assert.Equal(new[] { "tab" }, order);
        Assert.Null(status);
    }

    [Fact]
    public async Task AnExceptionOutsideTheLaneReads_IsTheFailure_NotLastRefresh()
    {
        var logged = new List<Exception>();

        var failed = await ServerTab.ReadOverviewAsync(
            () => throw new InvalidOperationException("axis sync broke"), logged.Add);

        Assert.Equal("Overview lanes did not refresh: axis sync broke", failed);
        Assert.Single(logged);

        var status = await ServerTab.RefreshStagesAsync(
            refreshVisibleTab: () => Task.FromResult(failed),
            refreshBadges: () => Task.CompletedTask,
            lastRefreshText: () => "Last refresh: 12:00:00 (UTC)",
            CancellationToken.None);

        Assert.Equal("Error: Overview lanes did not refresh: axis sync broke", status);
    }

    [Fact]
    public async Task ACancelledRefresh_IsNotAFailure()
    {
        var failed = await ServerTab.ReadOverviewAsync(
            () => Task.FromException<string?>(new OperationCanceledException()), _ => { });

        Assert.Null(failed);
    }

    [Fact]
    public async Task LanesThatReadFine_PassThroughUnchanged()
    {
        Assert.Null(await ServerTab.ReadOverviewAsync(() => Task.FromResult<string?>(null), _ => { }));
        Assert.Equal("Overview lanes did not read CPU %",
            await ServerTab.ReadOverviewAsync(() => Task.FromResult<string?>("Overview lanes did not read CPU %"), _ => { }));
    }
}
