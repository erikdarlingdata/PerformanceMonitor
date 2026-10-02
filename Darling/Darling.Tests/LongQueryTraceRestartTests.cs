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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4961: a SQL Server restart stops the long-query trace's session, because the per-install session is created with
/// <c>STARTUP_STATE = OFF</c>, and a stopped session reads as a quiet one. When a collector run sees the instance's
/// identity move, the worker clears the long-query latch, so the next sweep's reconcile starts the session again
/// instead of waiting for the hourly create pass. Each test drives the worker's real reconcile on a server whose
/// session is the server's own, with the create replaced, so no server or store is needed.
/// </summary>
public sealed class LongQueryTraceRestartTests : IAsyncDisposable
{
    /* Never opened: these tests collect nothing. */
    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private const string InstallIdValue = "0a1b2c3d";
    private static readonly string OwnSession = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue);

    private sealed class Rig
    {
        public required DarlingCollectorRunner Runner { get; init; }
        public required DarlingWorker.ServerLoopState State { get; init; }
        public List<(string Database, bool Create, string Name)> Calls { get; } = new();
        public DateTime Clock { get; set; } = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);
        public DarlingSelfAlertTests.CapturingLogger Logger { get; } = new();

        public Task ReconcileAsync(bool enabled) =>
            DarlingWorker.ReconcileLongQueryTraceAsync(
                State, Runner, enabled, Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(), Clock, Logger, CancellationToken.None);
    }

    /// <summary>A server on an engine with no per-database sessions, an on-premises server: its session is the server's.</summary>
    private Rig BuildOnPremRig()
    {
        var config = new MonitoredServer { Name = "lqtrace-sql", Host = "lqtrace-sql" };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = "Server=lqtrace-sql;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = false },
            StorageName = "lqtrace-sql",
            ServerId = 4961,
        };

        var runner = new DarlingCollectorRunner(
            _store,
            new CollectorDeltaCalculator(),
            installId: () => InstallIdValue);

        var rig = new Rig
        {
            Runner = runner,
            State = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime },
        };

        runner.LongQueryTraceDatabaseOverrideForTests = (_, database, create, sessionName, _) =>
        {
            rig.Calls.Add((database, create, sessionName));
            return Task.CompletedTask;
        };

        return rig;
    }

    private static CollectorMeasurement[] IdentityMoved(long count = 1) =>
        new[] { new CollectorMeasurement(ServerEpoch.IdentityChangesMeasurement, count) };

    [Fact]
    public async Task AMovedStartTime_ClearsTheLatch_AndTheNextSweepEnsuresTheSession()
    {
        var rig = BuildOnPremRig();

        await rig.ReconcileAsync(enabled: true);
        Assert.True(rig.State.LongQueryTraceApplied);
        Assert.Single(rig.Calls);

        /* Latched: the next sweep opens no connection. */
        await rig.ReconcileAsync(enabled: true);
        Assert.Single(rig.Calls);

        /* A collector run saw the instance's identity move: the restart stopped the session. */
        var cleared = DarlingWorker.ForgetLongQueryTraceLatchOnRestart(rig.State, IdentityMoved());

        Assert.True(cleared);
        Assert.Null(rig.State.LongQueryTraceApplied);
        Assert.Null(rig.State.LongQueryTraceAppliedAtUtc);

        /* Within the hour of the create, so only the cleared latch can make this sweep ensure. */
        rig.Clock = rig.Clock.AddMinutes(1);
        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(2, rig.Calls.Count);
        Assert.All(rig.Calls, call => Assert.True(call.Create));
        Assert.All(rig.Calls, call => Assert.Equal(OwnSession, call.Name));
        Assert.True(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task AMovedStartTime_WithTheTraceOff_ChecksTheDropAgainOnTheNextSweep()
    {
        var rig = BuildOnPremRig();

        await rig.ReconcileAsync(enabled: false);
        Assert.False(rig.State.LongQueryTraceApplied);
        Assert.Single(rig.Calls);

        Assert.True(DarlingWorker.ForgetLongQueryTraceLatchOnRestart(rig.State, IdentityMoved()));

        await rig.ReconcileAsync(enabled: false);
        Assert.Equal(2, rig.Calls.Count);
        Assert.All(rig.Calls, call => Assert.False(call.Create));
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    public async Task ARunThatSawNoMove_LeavesTheLatchAlone(long count)
    {
        var rig = BuildOnPremRig();
        await rig.ReconcileAsync(enabled: true);

        var cleared = DarlingWorker.ForgetLongQueryTraceLatchOnRestart(rig.State, IdentityMoved(count));
        Assert.False(cleared);

        /* Nor does a run that measured something else, or nothing. */
        Assert.False(DarlingWorker.ForgetLongQueryTraceLatchOnRestart(
            rig.State, new[] { new CollectorMeasurement(ServerEpoch.StatementsChangesMeasurement, 1) }));
        Assert.False(DarlingWorker.ForgetLongQueryTraceLatchOnRestart(rig.State, CollectorContext.NoMeasurements));

        Assert.True(rig.State.LongQueryTraceApplied);
        Assert.NotNull(rig.State.LongQueryTraceAppliedAtUtc);

        rig.Calls.Clear();
        await rig.ReconcileAsync(enabled: true);
        Assert.Empty(rig.Calls);
    }

    [Fact]
    public void TheCollectorRun_ClearsTheLatch_FromItsOwnMeasurements()
    {
        var worker = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        var run = worker.IndexOf("private async Task<int> RunOneAsync(", StringComparison.Ordinal);
        Assert.True(run >= 0, "RunOneAsync must exist.");
        var end = worker.IndexOf("\n    }\n", run, StringComparison.Ordinal);
        var body = worker[run..end];

        Assert.Contains("ForgetLongQueryTraceLatchOnRestart(server, result.Measurements)", body);
    }
}
