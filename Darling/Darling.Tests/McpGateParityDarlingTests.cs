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
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Darling's MCP gate, held to the matrix it shares with Lite (<see cref="McpGateParityMatrix"/>). Each configuration is
/// written as a <c>darling.json</c> <c>mcp</c> block and started through the service's real start path on a free loopback
/// port; each row then asks that listener over HTTP, as one caller, for <c>tools/list</c> and for a call to every tool in
/// the matrix, and every answer must be the one the shared rule gives. The store string points at a closed port: no call
/// reaches a tool body, so nothing reads or writes a store.
/// <para>A real start sets process-wide state (the declared peers, the file-level alert settings and the trigram index
/// option), each to the value an empty configuration gives. The class is in <c>darling-config-env</c>, which runs with
/// nothing in parallel, so no start lands in the middle of a class that set one of them for its own assertions.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class McpGateParityDarlingTests(McpGateParityDarlingTests.Hosts hosts) : IClassFixture<McpGateParityDarlingTests.Hosts>
{
    /// <summary>The host's typed logger over a capturing one, so a start that fails can say why.</summary>
    private sealed class HostLogger(CapturingTestLogger inner) : ILogger<DarlingMcpHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>One start: the service, the port it was given, whether it started, and the client aimed at it.</summary>
    internal sealed record Start(DarlingMcpHostService Service, int Port, bool Started, HttpClient Client, string Log) : IGateHost;

    /// <summary>One started service per configuration, shared by that configuration's rows and stopped with the class.</summary>
    public sealed class Hosts : IAsyncLifetime
    {
        private readonly Dictionary<GateConfig, Start> _starts = [];

        public ValueTask InitializeAsync() => ValueTask.CompletedTask;

        public async ValueTask DisposeAsync()
        {
            foreach (var start in _starts.Values)
            {
                start.Client.Dispose();
                await start.Service.DisposeFailedStartAsync();
            }
        }

        internal async Task<Start> ForAsync(GateConfig config)
        {
            if (_starts.TryGetValue(config, out var existing))
            {
                return existing;
            }

            var port = FreePort();
            var log = new CapturingTestLogger();
            var service = new DarlingMcpHostService(new HostLogger(log), new McpRuntimeState(), new MonitoredServerRegistryState());
            var tryStart = typeof(DarlingMcpHostService).GetMethod("TryStartServerAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var toggle = new DarlingHostBinding.EndpointToggle(true, port, DarlingHostBinding.EndpointToggleOrigin.File, false, false);
            var started = await (Task<bool>)tryStart.Invoke(service, [Config(config, port), toggle, CancellationToken.None])!;
            var client = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false, UseCookies = false })
            {
                BaseAddress = new Uri($"http://localhost:{port}"),
                Timeout = TimeSpan.FromSeconds(60),
            };
            var start = new Start(service, port, started, client, log.Joined);
            _starts[config] = start;
            return start;
        }
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    /// <summary>The configuration as darling.json: the three switches, the tokenless setting, the stored token as
    /// <c>mcp.network.token</c> with no listen address (a loopback-only start), and the three named tokens when asked for.</summary>
    private static DarlingConfig Config(GateConfig config, int port)
    {
        var store = $"Host=127.0.0.1;Port={FreePort()};Username=darling;Password=x;Database=darlingtest;Timeout=1";
        var mcp = "'enabled': true, 'port': " + port
            + ", 'allowWrites': " + Json(config.Writes)
            + ", 'allowServerAdmin': " + Json(config.ServerAdmin)
            + ", 'allowInvestigations': " + Json(config.Investigations)
            + ", 'allowTokenlessLoopback': " + Json(config.TokenlessLoopback);
        if (McpGateParityMatrix.StoredTokenValue(config) is { } stored)
        {
            mcp += ", 'network': { 'token': '" + stored + "' }";
        }

        if (config.NamedTokens)
        {
            mcp += ", 'tokens': [ "
                + "{ 'name': 'parity-operator', 'token': '" + McpGateParityMatrix.OperatorToken + "', 'role': 'operator' }, "
                + "{ 'name': 'parity-agent', 'token': '" + McpGateParityMatrix.AgentToken + "', 'role': 'agent' }, "
                + "{ 'name': 'parity-reader', 'token': '" + McpGateParityMatrix.ReaderToken + "', 'role': 'reader' } ]";
        }

        return DarlingConfig.Parse(
            ("{ 'postgres': { 'managed': false, 'connectionString': '" + store + "', 'webConnectionString': '" + store
            + "', 'mcpConnectionString': '" + store + "' }, 'servers': [ { 'host': 'SQL2022' } ], 'mcp': { " + mcp + " } }")
            .Replace('\'', '"'));
    }

    private static string Json(bool value) => value ? "true" : "false";

    private static bool Listens(int port) =>
        IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners().Any(endpoint => endpoint.Port == port);

    private async Task RunRowAsync(GateConfig config, GateCaller caller)
    {
        var start = await hosts.ForAsync(config);
        if (!McpGateParityMatrix.Starts(config))
        {
            Assert.False(start.Started, $"{config} must not start: it has no token and the tokenless setting is off");
            Assert.False(Listens(start.Port), $"{config} did not start, so nothing may listen on its port");
            return;
        }

        Assert.True(start.Started, $"{config} must start: {start.Log}");
        McpGateParityMatrix.AssertNoDifferences(
            GateApp.Darling, config, caller, await McpGateParityMatrix.CheckAsync(GateApp.Darling, config, caller, start));
    }

    /// <summary>The shared rows: the configurations Lite is asked too.</summary>
    [Theory]
    [MemberData(nameof(McpGateParityMatrix.SharedRows), MemberType = typeof(McpGateParityMatrix))]
    public Task Darling_AnswersTheSharedRule(
        bool writes, bool serverAdmin, bool investigations, GateToken token, bool tokenlessLoopback, GateCaller caller) =>
        RunRowAsync(new GateConfig(writes, serverAdmin, investigations, token, tokenlessLoopback), caller);

    /// <summary>The named-token rows: each role, seen through the matrix's tools, is one of the shared rule's arms, and no
    /// switch widens or narrows it.</summary>
    [Theory]
    [MemberData(nameof(McpGateParityMatrix.NamedTokenRows), MemberType = typeof(McpGateParityMatrix))]
    public Task Darling_NamedTokenRoles_AnswerTheSharedRule(
        bool writes, bool serverAdmin, bool investigations, GateToken token, bool tokenlessLoopback, GateCaller caller) =>
        RunRowAsync(new GateConfig(writes, serverAdmin, investigations, token, tokenlessLoopback, NamedTokens: true), caller);

    /// <summary>Darling's investigations arm adds 19 tools to the list, the ones the shared rule names, and no other.</summary>
    [Fact]
    public async Task Darling_InvestigationsArm_AddsExactlyTheToolsTheSharedRuleNames() =>
        await McpGateParityMatrix.AssertInvestigationsArmAddsWhatTheRuleNamesAsync(
            GateApp.Darling,
            await hosts.ForAsync(McpGateParityMatrix.ReadsOnly),
            await hosts.ForAsync(McpGateParityMatrix.InvestigationsArm));
}
