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
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4442 scope 2: a live MCP <c>tools/call</c> pin, through the ACTUAL
/// <see cref="DarlingMcpHostService.ConfigureMcpServices"/> + <see cref="DarlingMcpHostService.ConfigurePipeline"/>
/// pair every production request runs (the same <see cref="DarlingMcpHostGateLiveTests.BuildServer"/> pattern,
/// with a caller-owned <see cref="ReadLatencyAccumulator"/> passed through the optional parameter those methods
/// now take) — not the filter's <c>ClassifyResult</c> helper called by hand, and not the accumulator's own unit
/// tests. Every fact here builds its own server and its own accumulator, so it does not race any other class's
/// use of the process-wide singleton.
/// </summary>
public sealed class McpToolLatencyRecordingTests
{
    private const string Token = "mcp-latency-pin-token";

    /// <summary>A test-only tool, registered ONLY on this test's host (never in production's
    /// <c>ConfigureMcpServices</c>): one call that returns cleanly, and one shaped like a tool's own caught
    /// 57014 statement-timeout envelope, so the filter's error-envelope branch is exercised end to end rather
    /// than by a thrown exception.</summary>
    [McpServerToolType]
    private sealed class LatencyProbeTools
    {
        [McpServerTool(Name = "latency_probe_ok"), Description("Test-only: always answers Ok.")]
        public static string ProbeOk() => "probe-ok";

        [McpServerTool(Name = "latency_probe_timeout"), Description("Test-only: answers the 57014 statement-timeout envelope.")]
        public static string ProbeTimeout() =>
            PerformanceMonitor.Common.McpHelpers.FormatError(
                "latency_probe_timeout",
                new NpgsqlException("57014: canceling statement due to statement timeout"));
    }

    /// <summary>Builds a <see cref="TestServer"/> over the REAL <see cref="DarlingMcpHostService.ConfigureMcpServices"/>
    /// and <see cref="DarlingMcpHostService.ConfigurePipeline"/>, wired to <paramref name="readLatency"/> — the
    /// same construction <see cref="DarlingMcpHostGateLiveTests.BuildServer"/> uses, plus the test-only probe
    /// tool registered directly on <c>builder.Services</c> (never through production's tool registrations).</summary>
    private static async Task<TestServer> BuildServer(ReadLatencyAccumulator readLatency)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        /* #4726: per call, like the production host - one analysis service per tools/call, one shared BaselineCache. */
        var sharedBaselines = new BaselineCache();
        builder.Services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: sharedBaselines));
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, readLatency,
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        /* The probe tool is added AFTER ConfigureMcpServices's own AddMcpServer() call, chaining onto the
           SAME builder — WithGeminiCompatibleTools appends to the already-registered tool set rather than
           replacing it, so every real Darling tool from ConfigureMcpServices stays registered too. */
        builder.Services.AddMcpServer().WithTools<LatencyProbeTools>();

        var app = builder.Build();

        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());

        host.ConfigurePipeline(
            app,
            networkMode: false,
            networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"),
            bearerToken: Token);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static Task<(int StatusCode, string Body)> ToolsCallAsync(TestServer server, string path, string toolName)
    {
        var requestBody = $"{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{{\"name\":\"{toolName}\",\"arguments\":{{}}}}}}";
        return SendJsonRpcAsync(server, path, requestBody);
    }

    private static async Task<(int StatusCode, string Body)> SendJsonRpcAsync(TestServer server, string path, string requestBody)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = path;
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(requestBody);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        return (ctx.Response.StatusCode, body);
    }

    [Fact]
    public async Task ACheapToolCall_RecordsExactlyOneMcpSample_WithOutcomeOk()
    {
        var readLatency = new ReadLatencyAccumulator();
        using var server = await BuildServer(readLatency);

        var (statusCode, _) = await ToolsCallAsync(server, "/", "latency_probe_ok");
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        var drained = readLatency.Drain();
        var sample = Assert.Single(drained, d => d.Surface == ReadSurface.Mcp && d.Route == "latency_probe_ok");
        Assert.Equal(ReadOutcome.Ok, sample.Outcome);
        Assert.Equal(1, sample.Count);
    }

    [Fact]
    public async Task AToolThatReturnsTheTimeoutEnvelope_RecordsOneMcpSample_WithOutcomeTimeout()
    {
        var readLatency = new ReadLatencyAccumulator();
        using var server = await BuildServer(readLatency);

        var (statusCode, _) = await ToolsCallAsync(server, "/", "latency_probe_timeout");
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        var drained = readLatency.Drain();
        var sample = Assert.Single(drained, d => d.Surface == ReadSurface.Mcp && d.Route == "latency_probe_timeout");
        Assert.Equal(ReadOutcome.Timeout, sample.Outcome);
        Assert.Equal(1, sample.Count);
    }

    [Fact]
    public async Task ACoreToolCall_IsAlsoRecorded()
    {
        var readLatency = new ReadLatencyAccumulator();
        using var server = await BuildServer(readLatency);

        /* list_servers is in DarlingCoreToolProfile's closure, so it dispatches on /core. */
        var (statusCode, _) = await ToolsCallAsync(server, "/core", "list_servers");
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        var drained = readLatency.Drain();
        var sample = Assert.Single(drained, d => d.Surface == ReadSurface.Mcp && d.Route == "list_servers");
        Assert.Equal(1, sample.Count);
    }

    [Fact]
    public async Task RunCustomViewPanel_RecordsNoMcpSample()
    {
        var readLatency = new ReadLatencyAccumulator();
        using var server = await BuildServer(readLatency);

        /* #4782: the argument is 'spec', the tool's one model-supplied parameter. This request used to send a
           'view_id' argument no parameter declares, which the unknown-argument guard (the first call-tool
           filter) refuses before the tool or the latency filter runs -- so the assertion below held without
           the filter's skip for this tool ever being exercised. */
        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"run_custom_view_panel\",\"arguments\":{\"spec\":\"{}\"}}}";
        var (statusCode, _) = await SendJsonRpcAsync(server, "/", requestBody);
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        var drained = readLatency.Drain();
        Assert.DoesNotContain(drained, d => d.Surface == ReadSurface.Mcp && d.Route == "run_custom_view_panel");
    }

    /// <summary>#4782: the tool's composed-panel run is recorded ONCE, as a <c>Compose</c> sample, into the
    /// accumulator the MCP host was given -- the filter skips this tool because the shared runner already
    /// records it. A <c>spec</c> with no <c>panel</c> is refused by the runner's validation before it reaches
    /// the store (no database), and the runner records the run either way.</summary>
    [Fact]
    public async Task RunCustomViewPanel_RecordsOneComposeSample_IntoTheAccumulatorTheHostWasGiven()
    {
        var readLatency = new ReadLatencyAccumulator();
        using var server = await BuildServer(readLatency);

        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"run_custom_view_panel\",\"arguments\":{\"spec\":\"{}\"}}}";
        var (statusCode, _) = await SendJsonRpcAsync(server, "/", requestBody);
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        var drained = readLatency.Drain();
        var sample = Assert.Single(drained, d => d.Surface == ReadSurface.Compose);
        Assert.Equal(ReadOutcome.Error, sample.Outcome);
        Assert.Equal(1, sample.Count);
        Assert.DoesNotContain(drained, d => d.Surface == ReadSurface.Mcp && d.Route == "run_custom_view_panel");
    }
}
