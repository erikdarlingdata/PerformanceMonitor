/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet sweep's launch (#3466 lane 2, #4925): the per-target separately-monitored lookups are part of the
/// tracked sweep task, so the collection loop that fires the sweep does not wait on one store read per
/// disconnected target. Driven pure: the lookups and the engine run are delegates, so these tests hold the
/// launch's shape without a store or a worker.
/// </summary>
public sealed class FleetSweepLaunchTests
{
    /* Three registered targets, none connected: every one takes the stored lookup, which is a store read. */
    private static readonly IReadOnlyList<(int ServerId, ServerRuntime? Runtime)> ThreeDisconnected =
        [(1, null), (2, null), (3, null)];

    [Fact]
    public async Task TheFire_ReturnsBeforeAnyDisconnectedTargetsLookupCompletes()
    {
        var gate = new TaskCompletionSource<IReadOnlyList<string>?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var lookups = 0;
        IReadOnlyDictionary<int, IReadOnlyList<string>?>? handedToTheSweep = null;

        var sweep = DarlingWorker.RunFleetSweepAsync(
            ThreeDisconnected,
            storedLookup: (serverId, ct) => { Interlocked.Increment(ref lookups); return gate.Task; },
            runtimeLookup: _ => throw new InvalidOperationException("no target here has a runtime"),
            runSweep: (separate, ct) => { handedToTheSweep = separate; return Task.CompletedTask; },
            NullLogger.Instance,
            CancellationToken.None);

        /* The launch has returned while the first store read is still pending: the loop thread is free. */
        Assert.False(sweep.IsCompleted);
        Assert.Equal(1, lookups);
        Assert.Null(handedToTheSweep);

        gate.SetResult(new[] { "db_shared" });
        await sweep;

        /* Every target was looked up, and the sweep ran with all three lists. */
        Assert.Equal(3, lookups);
        Assert.NotNull(handedToTheSweep);
        Assert.Equal(new[] { 1, 2, 3 }, handedToTheSweep.Keys.OrderBy(k => k));
        Assert.All(handedToTheSweep.Values, list => Assert.Equal(new[] { "db_shared" }, list));
    }

    [Fact]
    public async Task AFailedLookup_LeavesThatTargetUnscoped_AndTheSweepStillRuns()
    {
        IReadOnlyDictionary<int, IReadOnlyList<string>?>? handedToTheSweep = null;

        await DarlingWorker.RunFleetSweepAsync(
            ThreeDisconnected,
            storedLookup: (serverId, ct) => serverId == 2
                ? throw new InvalidOperationException("store unreachable")
                : Task.FromResult<IReadOnlyList<string>?>(new[] { $"db_{serverId}" }),
            runtimeLookup: _ => null,
            runSweep: (separate, ct) => { handedToTheSweep = separate; return Task.CompletedTask; },
            NullLogger.Instance,
            CancellationToken.None);

        Assert.NotNull(handedToTheSweep);
        Assert.Equal(new[] { "db_1" }, handedToTheSweep[1]);
        Assert.Null(handedToTheSweep[2]);
        Assert.Equal(new[] { "db_3" }, handedToTheSweep[3]);
    }

    /// <summary>
    /// The task is fire-and-track and completes unobserved, so a stop during a lookup must end it quietly
    /// rather than fault it, and must not run the sweep.
    /// </summary>
    [Fact]
    public async Task AStopDuringALookup_EndsTheTaskQuietly_WithoutRunningTheSweep()
    {
        using var cts = new CancellationTokenSource();
        var sweepRan = false;

        await DarlingWorker.RunFleetSweepAsync(
            ThreeDisconnected,
            storedLookup: async (serverId, ct) =>
            {
                await cts.CancelAsync();
                ct.ThrowIfCancellationRequested();
                return null;
            },
            runtimeLookup: _ => null,
            runSweep: (separate, ct) => { sweepRan = true; return Task.CompletedTask; },
            NullLogger.Instance,
            cts.Token);

        Assert.False(sweepRan);
    }
}
