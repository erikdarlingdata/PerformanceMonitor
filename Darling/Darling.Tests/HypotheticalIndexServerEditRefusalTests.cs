/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6170: <c>test_hypothetical_index</c> reads the server's definition together with its runtime, in one hold of the servers lock. A
/// server whose stored definition no longer equals the one its connection was made from has been edited and is reconnecting, so the
/// check is refused and no sign-in is attempted: the login pair on the edited definition must not be paired with an address the
/// older definition chose.
///
/// <para>Runs without a store or a monitored server. The runtime's connection string points at an unroutable host, and the command
/// answers before it would use it.</para>
/// </summary>
public sealed class HypotheticalIndexServerEditRefusalTests
{
    private static DarlingWorker MakeWorker() => new(
        NullLogger<DarlingWorker>.Instance,
        NullLoggerFactory.Instance,
        new McpRuntimeState(),
        new WebRuntimeState(),
        new MonitoredServerRegistryState(),
        new CollectorRuntimeState(),
        new WebTlsCertificateState(),
        new BaselineCache(),
        new ReadLatencyAccumulator());

    private static MonitoredServer PgServer(string host) => new()
    {
        Name = "pg-edit-refusal",
        Host = host,
        Engine = "postgresql",
        HypotheticalIndexUsername = "hypo_login",
        HypotheticalIndexEncryptedPassword = "env:DARLING_TEST_NEVER_SET_6170",
    };

    private static async Task<CommandOutcome> RunAsync(DarlingWorker worker, DarlingWorker.ServerLoopState state)
    {
        var method = typeof(DarlingWorker).GetMethod("RunTestHypotheticalIndexAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var request = new HypotheticalIndexRequest("1", "public", "t", ["a"], "db");
        var task = (Task<CommandOutcome>)method.Invoke(
            worker, [new List<DarlingWorker.ServerLoopState> { state }, state.Config.ServerId, request, CancellationToken.None])!;
        return await task;
    }

    [Fact]
    public async Task AServerEditedSinceItConnected_IsRefused_AndNoSignInIsAttempted()
    {
        var connectedWith = PgServer("old-host.invalid");
        var edited = PgServer("old-host.invalid");
        edited.Host = "new-host.invalid";

        var state = new DarlingWorker.ServerLoopState
        {
            Config = edited,
            Runtime = new ServerRuntime
            {
                Config = connectedWith,
                ConnectionString = "Host=old-host.invalid;Port=1;Username=monitor;Password=unused;Timeout=1",
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                StorageName = connectedWith.StorageName,
                ServerId = connectedWith.ServerId,
            },
        };

        var outcome = await RunAsync(MakeWorker(), state);

        Assert.False(outcome.Success);
        Assert.Equal("server definition changed", outcome.ResultStatus);
        using var doc = JsonDocument.Parse(outcome.ResultJson!);
        Assert.Contains("reconnect", doc.RootElement.GetProperty("error").GetString());
    }
}
