/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9577: the one call-tool filter that holds every MCP tool call until the store's migrations are done. The live facts build the
/// SAME server production serves (<see cref="DarlingMcpHostService.ConfigureMcpServices"/> and
/// <see cref="DarlingMcpHostService.ConfigurePipeline"/>, the pattern <see cref="McpToolLatencyRecordingTests"/> uses) with a probe tool
/// that counts its runs, and drive readiness by hand through a flag the test sets: no clock, no delay. The resolver facts read the
/// decision for each kind of process directly.
/// </summary>
public sealed class McpStoreReadyFilterTests
{
    /// <summary>A test-only read tool, registered on these tests' hosts only: it counts its runs, so a held call is seen to have run nothing.</summary>
    [McpServerToolType]
    private sealed class StoreReadyProbeTools
    {
        internal static int Runs;

        [McpServerTool(Name = "store_ready_probe", ReadOnly = true), Description("Test-only: answers probe-ran and counts the run.")]
        public static string Probe()
        {
            Interlocked.Increment(ref Runs);
            return "probe-ran";
        }
    }

    private static async Task<TestServer> BuildServer(Func<bool>? storeReady)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        var sharedBaselines = new BaselineCache();
        builder.Services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(
            postgres, planFetcher: null, logger: NullLogger.Instance, baselineCache: sharedBaselines));
        builder.Services.AddSingleton<ILogger>(NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, readLatency: null, readLatencyLogger: NullLogger.Instance,
            storeReady: storeReady);
        builder.Services.AddMcpServer().WithTools<StoreReadyProbeTools>();

        var app = builder.Build();
        var host = new DarlingMcpHostService(
            NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState());
        host.ConfigurePipeline(
            app, networkMode: false, networkListenIp: null, allowedCidr: IPNetwork.Parse("127.0.0.1/32"), bearerToken: "store-ready-token");

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<string> CallAsync(TestServer server, string path, string tool, string arguments)
    {
        var body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + tool + "\",\"arguments\":" + arguments + "}}";
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = path;
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(body);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        return await reader.ReadToEndAsync();
    }

    /// <summary>The refusal's JSON, read out of the answer's one text block.</summary>
    private static JsonDocument RefusalOf(string answer)
    {
        var data = Array.Find(answer.Split('\n'), l => l.StartsWith("data:", StringComparison.Ordinal));
        using var envelope = JsonDocument.Parse(data is null ? answer : data["data:".Length..].Trim());
        var result = envelope.RootElement.GetProperty("result");
        Assert.True(result.GetProperty("isError").GetBoolean());
        return JsonDocument.Parse(result.GetProperty("content")[0].GetProperty("text").GetString()!);
    }

    [Fact]
    public async Task WhileTheStoreIsNotReady_AToolCallIsRefused_AndTheToolDoesNotRun()
    {
        var ready = false;
        using var server = await BuildServer(() => ready);
        var before = Volatile.Read(ref StoreReadyProbeTools.Runs);

        var held = await CallAsync(server, "/", "store_ready_probe", "{}");

        Assert.Equal(before, Volatile.Read(ref StoreReadyProbeTools.Runs));
        Assert.DoesNotContain("probe-ran", held, StringComparison.Ordinal);
        using var refusal = RefusalOf(held);
        Assert.Equal("refused", refusal.RootElement.GetProperty("status").GetString());
        Assert.Equal("store_not_ready", refusal.RootElement.GetProperty("reason").GetString());
        Assert.Equal("store_ready_probe", refusal.RootElement.GetProperty("tool").GetString());
        Assert.False(refusal.RootElement.GetProperty("changed").GetBoolean());
        Assert.Contains("this call was not run", refusal.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("Retry in a few seconds", refusal.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnceTheStoreIsReady_TheSameCallRuns_WithNoRestart()
    {
        var ready = false;
        using var server = await BuildServer(() => ready);
        var before = Volatile.Read(ref StoreReadyProbeTools.Runs);
        _ = RefusalOf(await CallAsync(server, "/", "store_ready_probe", "{}"));

        ready = true;
        var served = await CallAsync(server, "/", "store_ready_probe", "{}");

        Assert.Contains("probe-ran", served, StringComparison.Ordinal);
        Assert.DoesNotContain("store_not_ready", served, StringComparison.Ordinal);
        Assert.Equal(before + 1, Volatile.Read(ref StoreReadyProbeTools.Runs));
    }

    [Theory]
    [InlineData("/core", "get_fleet_overview", "{}")]
    [InlineData("/", "get_fleet_overview", "{}")]
    public async Task EveryPath_HoldsARealTool_BeforeItReachesTheStore(string path, string tool, string arguments)
    {
        using var server = await BuildServer(() => false);

        using var refusal = RefusalOf(await CallAsync(server, path, tool, arguments));

        Assert.Equal("store_not_ready", refusal.RootElement.GetProperty("reason").GetString());
        Assert.Equal(tool, refusal.RootElement.GetProperty("tool").GetString());
    }

    [Fact]
    public async Task OnTheLeanPath_CallTool_IsHeldByTheInnerToolsName()
    {
        var ready = false;
        using var server = await BuildServer(() => ready);
        var before = Volatile.Read(ref StoreReadyProbeTools.Runs);

        using var refusal = RefusalOf(await CallAsync(server, "/lean", "call_tool", "{\"name\":\"store_ready_probe\",\"arguments\":{}}"));

        Assert.Equal("store_not_ready", refusal.RootElement.GetProperty("reason").GetString());
        Assert.Equal("store_ready_probe", refusal.RootElement.GetProperty("tool").GetString());
        Assert.Equal(before, Volatile.Read(ref StoreReadyProbeTools.Runs));

        ready = true;
        var served = await CallAsync(server, "/lean", "call_tool", "{\"name\":\"store_ready_probe\",\"arguments\":{}}");
        Assert.Contains("probe-ran", served, StringComparison.Ordinal);
        Assert.Equal(before + 1, Volatile.Read(ref StoreReadyProbeTools.Runs));
    }

    [Theory]
    [InlineData("find_tools", "{\"query\":\"store ready probe\"}")]
    [InlineData("get_tool_guide", "{}")]
    public async Task TheToolsThatReadNoStore_AnswerWhileTheStoreIsNotReady(string tool, string arguments)
    {
        using var server = await BuildServer(() => false);

        var answer = await CallAsync(server, "/", tool, arguments);

        Assert.DoesNotContain("store_not_ready", answer, StringComparison.Ordinal);
        Assert.Contains("\"result\"", answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHostWithNoStoreReadySignal_IsNotHeld()
    {
        using var server = await BuildServer(null);
        var before = Volatile.Read(ref StoreReadyProbeTools.Runs);

        var served = await CallAsync(server, "/", "store_ready_probe", "{}");

        Assert.Contains("probe-ran", served, StringComparison.Ordinal);
        Assert.Equal(before + 1, Volatile.Read(ref StoreReadyProbeTools.Runs));
    }

    [Fact]
    public void AProcessThatMigrates_IsNotReady_UntilTheWorkerMarksTheStoreReady()
    {
        var gate = new StoreHealthWarmGate();
        var collector = new CollectorRuntimeState();
        var ready = McpStoreReadyFilter.StoreReadyFor(isFrontDoor: false, collector, gate);

        Assert.False(ready());

        collector.PublishRetrying(CollectorRuntimeState.StartupStep.Store, attempt: 1, attempts: 25);
        Assert.False(ready());

        gate.MarkStoreReady();
        Assert.True(ready());
    }

    [Fact]
    public void AFrontDoor_AHostWithNoWorker_AndAWorkerThatStoodDown_AreReadyFromTheStart()
    {
        var neverMarked = new StoreHealthWarmGate();

        Assert.True(McpStoreReadyFilter.StoreReadyFor(isFrontDoor: true, new CollectorRuntimeState(), neverMarked)());
        Assert.True(McpStoreReadyFilter.StoreReadyFor(isFrontDoor: false, collector: null, neverMarked)());

        var stopped = new CollectorRuntimeState();
        var ready = McpStoreReadyFilter.StoreReadyFor(isFrontDoor: false, stopped, neverMarked);
        Assert.False(ready());
        stopped.PublishStopped(CollectorRuntimeState.StartupStep.Store);
        Assert.True(ready());
        Assert.False(neverMarked.IsStoreReady);
    }
}
