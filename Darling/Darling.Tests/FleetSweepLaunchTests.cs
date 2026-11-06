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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet sweep's launch (#3466 lane 2, #4925): the per-target separately-monitored lookups are part of the
/// tracked sweep task, so the collection loop that fires the sweep does not wait on one store read per
/// disconnected target. Driven pure: the stored lookup and the engine run are delegates, and the registry is the
/// snapshot handed in at the fire, so these tests hold the launch's shape without a store or a worker.
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
            registryServers: null,
            storedLookup: (serverId, ct) => { Interlocked.Increment(ref lookups); return gate.Task; },
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
            registryServers: null,
            storedLookup: (serverId, ct) => serverId == 2
                ? throw new InvalidOperationException("store unreachable")
                : Task.FromResult<IReadOnlyList<string>?>(new[] { $"db_{serverId}" }),
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
            registryServers: null,
            storedLookup: async (serverId, ct) =>
            {
                await cts.CancelAsync();
                ct.ThrowIfCancellationRequested();
                return null;
            },
            runSweep: (separate, ct) => { sweepRan = true; return Task.CompletedTask; },
            NullLogger.Instance,
            cts.Token);

        Assert.False(sweepRan);
    }

    /// <summary>
    /// Every lookup in one sweep reads the registry snapshot taken at the fire. The sweep is handed that snapshot and
    /// has no path to the live registry, so a registry published while a store read is still pending does not reach
    /// the connected master's lookup that runs after it: one sweep never mixes two registry snapshots.
    /// </summary>
    [Fact]
    public async Task EveryLookupInASweep_ReadsTheRegistrySnapshotTakenAtTheFire()
    {
        const string host = "sweep-a.database.windows.net";
        var master = new MonitoredServer { Name = "master", Host = host, Database = "master", StoredServerId = 11 };
        var sibling = new MonitoredServer { Name = "tenant_a", Host = host, Database = "tenant_a", StoredServerId = 12 };
        var registry = new MonitoredServerRegistryState();
        registry.Publish([master, sibling]);
        var atTheFire = registry.Read();

        var masterRuntime = new ServerRuntime
        {
            Config = master,
            ConnectionString = "Server=" + host,
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
            StorageName = master.StorageName,
            ServerId = master.ServerId,
        };
        var gate = new TaskCompletionSource<IReadOnlyList<string>?>(TaskCreationOptions.RunContinuationsAsynchronously);
        IReadOnlyDictionary<int, IReadOnlyList<string>?>? handedToTheSweep = null;

        /* A disconnected target first, so the connected master's lookup runs only after the store read below releases. */
        var sweep = DarlingWorker.RunFleetSweepAsync(
            [(1, null), (master.ServerId, masterRuntime)],
            atTheFire?.Servers,
            storedLookup: (serverId, ct) => gate.Task,
            runSweep: (separate, ct) => { handedToTheSweep = separate; return Task.CompletedTask; },
            NullLogger.Instance,
            CancellationToken.None);

        /* The registry reloads while that read is pending, and the sibling is gone from the live set. */
        registry.Publish([master]);
        gate.SetResult(null);
        await sweep;

        Assert.NotNull(handedToTheSweep);
        Assert.Null(handedToTheSweep[1]);
        Assert.Equal(new[] { "tenant_a" }, handedToTheSweep[master.ServerId]);
    }
}
