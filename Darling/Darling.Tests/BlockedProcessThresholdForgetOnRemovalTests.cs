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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9743: a server that refused the blocked process threshold bootstrap is asked once. Removing it from the monitored set drops that
/// refusal, so the same server added again is asked again, and the servers that stay keep theirs.
/// </summary>
public sealed class BlockedProcessThresholdForgetOnRemovalTests : IAsyncDisposable
{
    private const string Settings = "Server=tcp:lab.invalid,1433;Initial Catalog=master";

    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private sealed class Refusal : DbException
    {
        public Refusal() : base("the login lacks permission")
        {
        }
    }

    private sealed class Batch
    {
        public int Sent { get; private set; }

        public Task<int> SendAsync()
        {
            Sent++;
            return Task.FromException<int>(new Refusal());
        }
    }

    private static DarlingWorker.ServerLoopState Server(string name) =>
        new() { Config = new MonitoredServer { Name = name, Host = name + ".invalid" } };

    [Fact]
    public async Task ARemovedServersRefusalIsDropped_AndTheServersThatStayKeepTheirs()
    {
        var runner = new DarlingCollectorRunner(_store, new CollectorDeltaCalculator());
        var worker = SelfAlertFleetPassTests.MakeWorker();
        worker.RunnerForTests = runner;
        var leaving = Server("leaving");
        var staying = Server("staying");
        var servers = new List<DarlingWorker.ServerLoopState> { leaving, staying };
        var leavingBatch = new Batch();
        var stayingBatch = new Batch();

        await runner.BlockedProcessThresholdBootstrap.RunAsync(leaving.Config.ServerId, Settings, leavingBatch.SendAsync, _ => false);
        await runner.BlockedProcessThresholdBootstrap.RunAsync(staying.Config.ServerId, Settings, stayingBatch.SendAsync, _ => false);

        worker.ReconcileServers(servers, new[] { staying.Config });

        var readded = await runner.BlockedProcessThresholdBootstrap.RunAsync(leaving.Config.ServerId, Settings, leavingBatch.SendAsync, _ => false);
        var kept = await runner.BlockedProcessThresholdBootstrap.RunAsync(staying.Config.ServerId, Settings, stayingBatch.SendAsync, _ => false);

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, readded.Outcome);
        Assert.Equal(2, leavingBatch.Sent);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, kept.Outcome);
        Assert.Equal(1, stayingBatch.Sent);
    }
}
