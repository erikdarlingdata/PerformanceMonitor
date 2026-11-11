/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's MCP gate, held to the matrix it shares with Darling (<see cref="McpGateParityMatrix"/>). Each configuration starts
/// the real <see cref="McpHostService"/> on a free localhost port with that token and those settings; each row then asks
/// that listener over HTTP, as one caller, for <c>tools/list</c> and for a call to every tool in the matrix, and every
/// answer must be the one the shared rule gives. Lite has no server-admin setting, so the two values of that switch start
/// the same host settings. No call reaches a tool body, so nothing reads or writes a store.
/// </summary>
public sealed class McpGateParityLiteTests(McpGateParityLiteTests.Hosts hosts) : IClassFixture<McpGateParityLiteTests.Hosts>
{
    /// <summary>One start: the service when the host accepted the configuration, and the client aimed at it.</summary>
    internal sealed record Start(McpHostService? Service, HttpClient Client) : IGateHost;

    /// <summary>One started host per configuration, shared by that configuration's rows and stopped with the class.</summary>
    public sealed class Hosts : IAsyncLifetime
    {
        private readonly string _folder = Path.Combine(Path.GetTempPath(), "lite-mcp-gate-parity-" + Guid.NewGuid().ToString("N"));
        private readonly Dictionary<GateConfig, Start> _starts = [];
        private readonly List<DuckDbInitializer> _ducks = [];

        public ValueTask InitializeAsync()
        {
            Directory.CreateDirectory(_folder);
            return ValueTask.CompletedTask;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var start in _starts.Values)
            {
                start.Client.Dispose();
                if (start.Service is not null)
                {
                    try { await start.Service.StopAsync(CancellationToken.None); } catch { /* best effort */ }
                }
            }

            foreach (var duck in _ducks)
            {
                duck.Dispose();
            }

            try { Directory.Delete(_folder, recursive: true); } catch { /* best effort */ }
        }

        internal async Task<Start> ForAsync(GateConfig config)
        {
            if (_starts.TryGetValue(config, out var existing))
            {
                return existing;
            }

            var port = FreePort();
            var client = new HttpClient { BaseAddress = new Uri($"http://localhost:{port}/"), Timeout = TimeSpan.FromSeconds(60) };
            var duck = new DuckDbInitializer(Path.Combine(_folder, $"m{port}.duckdb"));
            _ducks.Add(duck);

            McpHostService? service;
            try
            {
                service = new McpHostService(
                    new LocalDataService(duck),
                    new ServerManager(_folder),
                    new MuteRuleService(new AlertWebhookCancelTests.NoMuteRules(), new AppLoggerAdapter<MuteRuleService>()),
                    duck,
                    port,
                    DispatchProxy.Create<IInvestigationRequestStore, McpHostGateLiveTests.NoStore>(),
                    McpGateParityMatrix.StoredTokenValue(config),
                    config.TokenlessLoopback,
                    allowWrites: config.Writes,
                    allowInvestigations: config.Investigations);
            }
            catch (ArgumentException)
            {
                /* The host refuses a configuration with no token and the tokenless setting off. */
                service = null;
            }

            if (service is not null)
            {
                await service.StartAsync(CancellationToken.None);
                await WaitUntilListeningAsync(port);
            }

            var start = new Start(service, client);
            _starts[config] = start;
            return start;
        }

        /// <summary>The listener comes up after <c>StartAsync</c> returns; this returns once the port accepts a connection.</summary>
        private static async Task WaitUntilListeningAsync(int port)
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            while (true)
            {
                try
                {
                    using var probe = new TcpClient();
                    await probe.ConnectAsync(IPAddress.Loopback, port, deadline.Token);
                    return;
                }
                catch (SocketException) when (!deadline.IsCancellationRequested)
                {
                    await Task.Delay(50);
                }
                catch (Exception e) when (e is SocketException or OperationCanceledException)
                {
                    throw new TimeoutException($"Lite's MCP host did not listen on port {port} within 60 seconds.", e);
                }
            }
        }

        private static int FreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
            finally { listener.Stop(); }
        }
    }

    [Theory]
    [MemberData(nameof(McpGateParityMatrix.SharedRows), MemberType = typeof(McpGateParityMatrix))]
    public async Task Lite_AnswersTheSharedRule(
        bool writes, bool serverAdmin, bool investigations, GateToken token, bool tokenlessLoopback, GateCaller caller)
    {
        var config = new GateConfig(writes, serverAdmin, investigations, token, tokenlessLoopback);
        var start = await hosts.ForAsync(config);
        if (!McpGateParityMatrix.Starts(config))
        {
            Assert.True(start.Service is null, $"{config} must not start: it has no token and the tokenless setting is off");
            return;
        }

        Assert.True(start.Service is not null, $"{config} must start");
        McpGateParityMatrix.AssertNoDifferences(
            GateApp.Lite, config, caller, await McpGateParityMatrix.CheckAsync(GateApp.Lite, config, caller, start));
    }

    /// <summary>Lite's investigations arm adds 2 tools to the list, the ones the shared rule names, and no other.</summary>
    [Fact]
    public async Task Lite_InvestigationsArm_AddsExactlyTheToolsTheSharedRuleNames() =>
        await McpGateParityMatrix.AssertInvestigationsArmAddsWhatTheRuleNamesAsync(
            GateApp.Lite,
            await hosts.ForAsync(McpGateParityMatrix.ReadsOnly),
            await hosts.ForAsync(McpGateParityMatrix.InvestigationsArm));
}
