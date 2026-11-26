/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9743: a server that refused the blocked process threshold bootstrap is asked once, and a fault of the moment is not remembered
/// as a refusal. Removing a server from the monitored set drops its refusal, so the same server added again is asked again, and the
/// servers that stay keep theirs. The SqlExceptions are real ones, read by Darling's own <see cref="DarlingXeSessions.ErrorNumbersOf"/>,
/// the reader Darling hands the bootstrap.
/// </summary>
public sealed class BlockedProcessThresholdForgetOnRemovalTests : IAsyncDisposable
{
    private const string Settings = "Server=tcp:lab.invalid,1433;Initial Catalog=master";

    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    /// <summary>What sp_configure answers a login that lacks ALTER SETTINGS, and what AWS RDS answers the batch with.</summary>
    private static SqlException Refusal() =>
        LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 14, "User does not have permission to perform this action.");

    private static SqlException Error(int number) =>
        LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(number, 14, $"SQL Server error {number}.");

    private static IEnumerable<int> Numbers(DbException ex) => DarlingXeSessions.ErrorNumbersOf(ex);

    /// <summary>A stand-in for the batch: counts how often it is sent and answers from the behavior.</summary>
    private sealed class Batch
    {
        private readonly Func<int, Task<int>> _behavior;

        public Batch(Func<int, Task<int>> behavior) => _behavior = behavior;

        public int Sent { get; private set; }

        public Task<int> SendAsync() => _behavior(++Sent);
    }

    private static Batch Refuses() => new(_ => Task.FromException<int>(Refusal()));

    private static DarlingWorker.ServerLoopState Server(string name) =>
        new() { Config = new MonitoredServer { Name = name, Host = name + ".invalid" } };

    [Theory]
    [InlineData(15247)]
    [InlineData(5812)]
    [InlineData(2812)]
    public async Task AServersOwnRefusalIsRecordedOnce_AndTheNextRunDoesNotAsk(int number)
    {
        var runner = new DarlingCollectorRunner(_store, new CollectorDeltaCalculator());
        var batch = new Batch(_ => Task.FromException<int>(Error(number)));

        var first = await runner.BlockedProcessThresholdBootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var next = await runner.BlockedProcessThresholdBootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, first.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, next.Outcome);
        Assert.Equal(1, batch.Sent);
    }

    [Theory]
    [InlineData(1205)]
    [InlineData(1222)]
    [InlineData(64)]
    [InlineData(121)]
    public async Task AFaultOfTheMomentIsNotRemembered_AndTheBatchIsSentAgainOnTheNextRun(int number)
    {
        var runner = new DarlingCollectorRunner(_store, new CollectorDeltaCalculator());
        var batch = new Batch(attempt => attempt == 1 ? Task.FromException<int>(Error(number)) : Task.FromResult(0));

        var faulted = await runner.BlockedProcessThresholdBootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var retried = await runner.BlockedProcessThresholdBootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Transient, faulted.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.Configured, retried.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task ARemovedServersRefusalIsDropped_AndTheServersThatStayKeepTheirs()
    {
        var runner = new DarlingCollectorRunner(_store, new CollectorDeltaCalculator());
        var worker = SelfAlertFleetPassTests.MakeWorker();
        worker.RunnerForTests = runner;
        var leaving = Server("leaving");
        var staying = Server("staying");
        var servers = new List<DarlingWorker.ServerLoopState> { leaving, staying };
        var leavingBatch = Refuses();
        var stayingBatch = Refuses();

        await runner.BlockedProcessThresholdBootstrap.RunAsync(leaving.Config.ServerId, Settings, leavingBatch.SendAsync, Numbers);
        await runner.BlockedProcessThresholdBootstrap.RunAsync(staying.Config.ServerId, Settings, stayingBatch.SendAsync, Numbers);

        worker.ReconcileServers(servers, new[] { staying.Config });

        var readded = await runner.BlockedProcessThresholdBootstrap.RunAsync(leaving.Config.ServerId, Settings, leavingBatch.SendAsync, Numbers);
        var kept = await runner.BlockedProcessThresholdBootstrap.RunAsync(staying.Config.ServerId, Settings, stayingBatch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, readded.Outcome);
        Assert.Equal(2, leavingBatch.Sent);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, kept.Outcome);
        Assert.Equal(1, stayingBatch.Sent);
    }
}
