/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: <c>get_query_fleet_summary</c> runs its plan-cache and Query Store reads together. A cancelled read reaches the
/// caller as a cancellation even when the other read faulted, so a cut call is not reported as a tool error.
/// </summary>
public sealed class QueryFleetSummaryConcurrentReadTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static Task Cancelled() => Task.FromCanceled(new CancellationToken(canceled: true));

    private static Task Faulted() => Task.FromException(new InvalidOperationException("read failed"));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ACancelledRead_ReachesTheCallerAsCancellation_WhenTheOtherReadFaulted(bool planCancelled)
    {
        var planRead = planCancelled ? Cancelled() : Faulted();
        var qsRead = planCancelled ? Faulted() : Cancelled();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            DarlingMcpQueryFleetSummaryTools.AwaitBothReadsAsync(planRead, qsRead).WaitAsync(Bound));
    }

    [Fact]
    public async Task AFaultedRead_ReachesTheCallerAsTheFault_WhenNeitherReadWasCancelled()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DarlingMcpQueryFleetSummaryTools.AwaitBothReadsAsync(Task.CompletedTask, Faulted()).WaitAsync(Bound));
        Assert.Equal("read failed", error.Message);
    }

    [Fact]
    public async Task BothReadsCompleting_Completes()
    {
        await DarlingMcpQueryFleetSummaryTools.AwaitBothReadsAsync(Task.CompletedTask, Task.CompletedTask).WaitAsync(Bound);
    }
}
