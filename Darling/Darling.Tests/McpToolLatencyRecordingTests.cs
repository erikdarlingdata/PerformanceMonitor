/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
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
using PerformanceMonitor.Darling.Storage;

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

        [McpServerTool(Name = "latency_probe_identity"), Description("Test-only: answers who the write log would name for this call.")]
        public static string ProbeIdentity() => ReadCaller.WriteIdentity();

        [McpServerTool(Name = "latency_probe_gate_failed"), Description("Test-only: answers Ok after noting a gate failure.")]
        public static string ProbeGateFailed()
        {
            ReadScope.NoteFallback(ReadFallback.GateFailed, "probe", null);
            return "probe-ok";
        }

        [McpServerTool(Name = "latency_probe_gate_failed_then_error"), Description("Test-only: notes a gate failure, then answers an error envelope.")]
        public static string ProbeGateFailedThenError()
        {
            ReadScope.NoteFallback(ReadFallback.GateFailed, "probe", null);
            return PerformanceMonitor.Common.McpHelpers.FormatError("latency_probe_gate_failed_then_error", new InvalidOperationException("probe failed"));
        }

        [McpServerTool(Name = "latency_probe_servers_json"), Description("Test-only: takes a JSON-bearing string like add_servers does and fails.")]
        public static string ProbeServersJson([Description("A JSON batch.")] string servers_json) =>
            PerformanceMonitor.Common.McpHelpers.FormatError("latency_probe_servers_json", new InvalidOperationException("probe failed"));

        /// <summary>#5506 part 3: held until the test releases it, so admission's slots stay taken.</summary>
        internal static TaskCompletionSource Hold = new(TaskCreationOptions.RunContinuationsAsynchronously);

        [McpServerTool(Name = "latency_probe_hold"), Description("Test-only: answers Ok once the test releases it.")]
        public static async Task<string> ProbeHold()
        {
            await Hold.Task;
            return "probe-ok";
        }

        /// <summary>#5506 part 3: counts its runs and waits for <see cref="JsonHold"/>, so the read cache tests see whether
        /// the tool ran.</summary>
        internal static int JsonRuns;

        internal static TaskCompletionSource JsonHold = CompletedHold();

        internal static TaskCompletionSource CompletedHold()
        {
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            done.SetResult();
            return done;
        }

        [McpServerTool(Name = "latency_probe_json"), Description("Test-only: answers a JSON object that names its run.")]
        public static async Task<string> ProbeJson([Description("Hours.")] int hours_back = 24)
        {
            var run = System.Threading.Interlocked.Increment(ref JsonRuns);
            await JsonHold.Task;
            return "{\"rows\":[" + hours_back + "],\"run\":" + run + "}";
        }

        /// <summary>#5506 part 3 (round 2, M1): counts its runs and waits for <see cref="TimeoutHold"/>, then answers the
        /// 57014 statement-timeout envelope.</summary>
        internal static int HeldTimeoutRuns;

        internal static TaskCompletionSource TimeoutHold = CompletedHold();

        [McpServerTool(Name = "latency_probe_held_timeout"), Description("Test-only: answers the 57014 envelope once the test releases it.")]
        public static async Task<string> ProbeHeldTimeout()
        {
            System.Threading.Interlocked.Increment(ref HeldTimeoutRuns);
            await TimeoutHold.Task;
            return PerformanceMonitor.Common.McpHelpers.FormatError(
                "latency_probe_held_timeout",
                new NpgsqlException("57014: canceling statement due to statement timeout"));
        }

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
    private static async Task<TestServer> BuildServer(ReadLatencyAccumulator readLatency, SlowReadLog? slowReads = null, ReadAdmission? admission = null,
        bool gcf = false)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        if (gcf)
        {
            builder.Services.AddSingleton<IGcfOutputFormat>(new FixedGcfOutputFormat(true));
        }

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
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, slowReads, readAdmission: admission);

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

    private static Task<(int StatusCode, string Body)> ToolsCallAsync(TestServer server, string path, string toolName, string arguments)
    {
        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + toolName + "\",\"arguments\":" + arguments + "}}";
        return SendJsonRpcAsync(server, path, requestBody);
    }

    /// <summary>The real clock, except that "now" is one fixed instant. The read cache keys an answer by the TTL bucket of now
    /// (60 s for the 7-day band), so two identical calls a few milliseconds apart landed in different buckets, and ran two
    /// reads, whenever a test started within milliseconds of a bucket boundary (a CI run failed at 23:39:00 UTC). A fixed
    /// "now" keeps both calls in one bucket; timers and elapsed time stay real.</summary>
    private sealed class FixedNowTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now = new(2026, 1, 1, 0, 0, 30, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;
    }

    /// <summary>#5506 part 3: an admission, cache on, that classifies <c>latency_probe_json</c> as heavy in every band.</summary>
    private static ReadAdmission CachingAdmission()
    {
        var admission = new ReadAdmission(new FixedNowTimeProvider(), ReadAdmission.DefaultWaitCap, null, 2);
        var table = new System.Collections.Generic.Dictionary<(ReadSurface, string, string), long>();
        foreach (var band in new[] { ReadWindowBand.None, ReadWindowBand.OneHour, ReadWindowBand.OneDay, ReadWindowBand.SevenDays, ReadWindowBand.Longer })
        {
            table[(ReadSurface.Mcp, "latency_probe_json", band)] = 5_000;
            table[(ReadSurface.Mcp, "latency_probe_held_timeout", band)] = 5_000;
        }

        admission.SetTable(table);
        return admission;
    }

    [Fact]
    public async Task ReadCache_AHeavyRead_IsAnsweredFromMemory_TheSecondTime_WithNoToolRun()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = CachingAdmission();
        using var server = await BuildServer(readLatency, admission: admission);
        LatencyProbeTools.JsonHold = LatencyProbeTools.CompletedHold();
        var before = LatencyProbeTools.JsonRuns;

        /* The number forms and the omitted default are pinned in ReadCacheKeyTests (the guard refuses 24.0 for an int, and a probe tool is not in the host's
           recorded parameter defaults). */
        var first = await ToolsCallAsync(server, "/", "latency_probe_json", "{\"hours_back\":24}");
        var second = await ToolsCallAsync(server, "/", "latency_probe_json", "{\"hours_back\":24}");
        var third = await ToolsCallAsync(server, "/", "latency_probe_json", "{\"hours_back\":24}");

        Assert.Equal(before + 1, LatencyProbeTools.JsonRuns);
        Assert.Contains("run", first.Body, StringComparison.Ordinal);
        Assert.Equal(first.Body, second.Body);
        Assert.Equal(first.Body, third.Body);

        var drained = readLatency.Drain().Where(d => d.Route == "latency_probe_json").ToList();
        Assert.Equal(1, Assert.Single(drained, d => d.Outcome == ReadOutcome.Ok).Count);
        Assert.Equal(2, Assert.Single(drained, d => d.Outcome == ReadOutcome.CacheHit).Count);
        var stats = Assert.Single(admission.Stats().Routes, r => r.Route == "latency_probe_json");
        Assert.Equal((2L, 1L), (stats.CacheHits, stats.CacheMisses));
    }

    [Fact]
    public async Task ReadCache_TwoIdenticalMisses_RunOneRead_AndTheJoinedCallRecordsCacheHit()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = CachingAdmission();
        using var server = await BuildServer(readLatency, admission: admission);
        LatencyProbeTools.JsonHold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var before = LatencyProbeTools.JsonRuns;

        var first = ToolsCallAsync(server, "/", "latency_probe_json", "{\"hours_back\":168}");
        await WaitUntil(() => LatencyProbeTools.JsonRuns == before + 1);
        var second = ToolsCallAsync(server, "/", "latency_probe_json", "{\"hours_back\":168}");
        try
        {
            /* Round 2, L9: the second call has joined when the flight counts two waiters (no fixed delay). */
            await WaitUntil(() => admission.Cache.WaitingCallers == 2);
        }
        finally
        {
            LatencyProbeTools.JsonHold.TrySetResult();
        }

        var answers = await Task.WhenAll(first, second);

        Assert.Equal(before + 1, LatencyProbeTools.JsonRuns);
        Assert.Equal(answers[0].Body, answers[1].Body);
        var drained = readLatency.Drain().Where(d => d.Route == "latency_probe_json").ToList();
        Assert.Equal(1, Assert.Single(drained, d => d.Outcome == ReadOutcome.Ok).Count);
        Assert.Equal(1, Assert.Single(drained, d => d.Outcome == ReadOutcome.CacheHit).Count);
    }

    /// <summary>#5506 part 3 (round 2, M1): one shared read that times out, with two calls joined to it, records exactly one
    /// timeout, the first call's; each joined call records cache_hit for the shared answer it got.</summary>
    [Fact]
    public async Task ReadCache_ASharedReadThatTimesOut_WithTwoJoinedCalls_RecordsExactlyOneTimeout()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = CachingAdmission();
        using var server = await BuildServer(readLatency, admission: admission);
        LatencyProbeTools.TimeoutHold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var before = LatencyProbeTools.HeldTimeoutRuns;

        Task<(int StatusCode, string Body)>[] calls;
        try
        {
            var first = ToolsCallAsync(server, "/", "latency_probe_held_timeout", "{}");
            await WaitUntil(() => LatencyProbeTools.HeldTimeoutRuns == before + 1);
            calls =
            [
                first,
                ToolsCallAsync(server, "/", "latency_probe_held_timeout", "{}"),
                ToolsCallAsync(server, "/", "latency_probe_held_timeout", "{}"),
            ];
            await WaitUntil(() => admission.Cache.WaitingCallers == 3);
        }
        finally
        {
            LatencyProbeTools.TimeoutHold.TrySetResult();
        }

        var answers = await Task.WhenAll(calls);
        Assert.Equal(before + 1, LatencyProbeTools.HeldTimeoutRuns);
        Assert.All(answers, a => Assert.Contains("57014", a.Body, StringComparison.Ordinal));
        var drained = readLatency.Drain().Where(d => d.Route == "latency_probe_held_timeout").ToList();
        Assert.Equal(1, drained.Where(d => d.Outcome == ReadOutcome.Timeout).Sum(d => d.Count));
        Assert.Equal(2, drained.Where(d => d.Outcome == ReadOutcome.CacheHit).Sum(d => d.Count));
        Assert.All(drained, d => Assert.Contains(d.Outcome, new[] { ReadOutcome.Timeout, ReadOutcome.CacheHit }));
    }

    [Fact]
    public async Task ReadCache_ALightRead_IsNeverCached()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 2);
        using var server = await BuildServer(readLatency, admission: admission);
        LatencyProbeTools.JsonHold = LatencyProbeTools.CompletedHold();
        var before = LatencyProbeTools.JsonRuns;

        await ToolsCallAsync(server, "/", "latency_probe_json", "{\"hours_back\":1}");
        await ToolsCallAsync(server, "/", "latency_probe_json", "{\"hours_back\":1}");

        Assert.Equal(before + 2, LatencyProbeTools.JsonRuns);
        Assert.DoesNotContain(admission.Stats().Routes, r => r.Route == "latency_probe_json" && (r.CacheHits > 0 || r.CacheMisses > 0));
    }

    private static Task<(int StatusCode, string Body)> ToolsCallAsync(TestServer server, string path, string toolName)
    {
        var requestBody = $"{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{{\"name\":\"{toolName}\",\"arguments\":{{}}}}}}";
        return SendJsonRpcAsync(server, path, requestBody);
    }

    private static async Task<(int StatusCode, string Body)> SendJsonRpcAsync(TestServer server, string path, string requestBody, string? userAgent = null, string? authorization = null)
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
            if (userAgent is not null)
            {
                c.Request.Headers.UserAgent = userAgent;
            }

            if (authorization is not null)
            {
                c.Request.Headers.Authorization = authorization;
            }
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        return (ctx.Response.StatusCode, body);
    }

    [Fact]
    public async Task AFailedWriteToolCall_CarryingAServerBatchWithAPassword_IsRecordedWithoutThePassword()
    {
        var slowReads = new SlowReadLog(0);
        using var server = await BuildServer(new ReadLatencyAccumulator(), slowReads);
        var batch = "[{\\\"server_name\\\":\\\"x\\\",\\\"password\\\":\\\"Hunter2!\\\"}]";
        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"latency_probe_servers_json\",\"arguments\":{\"servers_json\":\"" + batch + "\"}}}";

        await SendJsonRpcAsync(server, "/", requestBody);

        Assert.True(slowReads.TryRead(out var record));
        Assert.Equal("latency_probe_servers_json", record!.Route);
        Assert.DoesNotContain("Hunter2!", record.ArgumentsJson, StringComparison.Ordinal);
        Assert.DoesNotContain("password\":\"", record.ArgumentsJson, StringComparison.Ordinal);
        Assert.Contains(SlowReadLog.OmittedMarker, record.ArgumentsJson, StringComparison.Ordinal);
    }

    [Fact]
    public void ASlowOrFailedWriteCall_StoresTheArgumentNamesOnly_NeverTheValues()
    {
        var supplied = new Dictionary<string, System.Text.Json.JsonElement>
        {
            ["notes"] = System.Text.Json.JsonDocument.Parse("\"call me at the on-call number 555-0100\"").RootElement.Clone(),
            ["rule_id"] = System.Text.Json.JsonDocument.Parse("42").RootElement.Clone(),
        };
        var toolName = DarlingMcpWritePolicy.WriteTools.First();

        var arguments = McpToolLatencyFilter.SlowReadArguments(toolName, supplied);
        var record = SlowReadLog.Build(ReadScope.Open(null).Scope, ReadSurface.Mcp, toolName, ReadOutcome.Error, 10, arguments, null, DateTime.UtcNow);

        Assert.Contains("\"notes\"", record.ArgumentsJson, StringComparison.Ordinal);
        Assert.Contains("\"rule_id\"", record.ArgumentsJson, StringComparison.Ordinal);
        Assert.DoesNotContain("555-0100", record.ArgumentsJson, StringComparison.Ordinal);
        Assert.DoesNotContain("42", record.ArgumentsJson, StringComparison.Ordinal);
    }

    [Fact]
    public void ASlowReadCall_KeepsItsValues_AndRedactsAUrlArgument()
    {
        var supplied = new Dictionary<string, System.Text.Json.JsonElement>
        {
            ["hours_back"] = System.Text.Json.JsonDocument.Parse("6").RootElement.Clone(),
            ["target_url"] = System.Text.Json.JsonDocument.Parse("\"https://hooks.example.test/abc123\"").RootElement.Clone(),
        };

        var arguments = McpToolLatencyFilter.SlowReadArguments("get_wait_stats", supplied);
        var record = SlowReadLog.Build(ReadScope.Open(null).Scope, ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Error, 10, arguments, null, DateTime.UtcNow);

        Assert.DoesNotContain("abc123", record.ArgumentsJson, StringComparison.Ordinal);
        Assert.Contains("\"hours_back\":6", record.ArgumentsJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AToolThatNotesAFallback_RecordsGateFailed_AndAFailedToolStillRecordsError()
    {
        var readLatency = new ReadLatencyAccumulator();
        using var server = await BuildServer(readLatency);

        await ToolsCallAsync(server, "/", "latency_probe_gate_failed");
        await ToolsCallAsync(server, "/", "latency_probe_gate_failed_then_error");

        var drained = readLatency.Drain();
        Assert.Equal(ReadOutcome.GateFailed, Assert.Single(drained, d => d.Route == "latency_probe_gate_failed").Outcome);
        Assert.Equal(ReadOutcome.Error, Assert.Single(drained, d => d.Route == "latency_probe_gate_failed_then_error").Outcome);
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

    /// <summary>#5793: the stateless host has no client info on a tool call from a client on the initialize handshake, so the
    /// caller is the first product token of its User-Agent, on both the hourly key and the slow-read row.</summary>
    [Fact]
    public async Task AToolCallWithNoClientInfo_IsNamedByItsUserAgentProduct_OnTheHourAndTheSlowRead()
    {
        var readLatency = new ReadLatencyAccumulator();
        var slowReads = new SlowReadLog(0);
        using var server = await BuildServer(readLatency, slowReads);
        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"latency_probe_ok\",\"arguments\":{}}}";

        await SendJsonRpcAsync(server, "/", requestBody, "node/22.3.0 (win32; x64) undici/6.19");
        await SendJsonRpcAsync(server, "/", requestBody, "python-httpx/0.27.0");

        var sample = Assert.Single(readLatency.Drain(), d => d.Route == "latency_probe_ok");
        Assert.Equal(2, sample.DistinctCallers);
        Assert.True(slowReads.TryRead(out var first));
        Assert.True(slowReads.TryRead(out var second));
        Assert.Equal(new[] { "ua:node/22.3.0", "ua:python-httpx/0.27.0" }, new[] { first!.Caller, second!.Caller });
    }

    /// <summary>#6231: what a write log line names for a call is the identity the service itself knows (a fingerprint of the bearer token the
    /// request carried, never the token) followed by the name the client gave, labelled as the client's own claim.</summary>
    [Fact]
    public async Task TheWriteIdentity_IsTheTokenFingerprint_ThenTheClientReportedName()
    {
        using var server = await BuildServer(new ReadLatencyAccumulator(), new SlowReadLog(0));
        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"latency_probe_identity\",\"arguments\":{}}}";
        const string Token = "probe-token-6231-not-a-real-one";

        var (_, withToken) = await SendJsonRpcAsync(server, "/", requestBody, "node/22.3.0 (win32; x64)", "Bearer " + Token);
        Assert.Contains(ReadCaller.OfMcpToken(Token) + ", client says: ua:node/22.3.0", withToken, StringComparison.Ordinal);
        Assert.StartsWith("token ", ReadCaller.OfMcpToken(Token), StringComparison.Ordinal);
        Assert.DoesNotContain(Token, withToken, StringComparison.Ordinal);

        /* Another token is another identity, whatever name the client gives. */
        Assert.NotEqual(ReadCaller.OfMcpToken(Token), ReadCaller.OfMcpToken(Token + "x"));
        var (_, withoutToken) = await SendJsonRpcAsync(server, "/", requestBody, "node/22.3.0 (win32; x64)");
        Assert.Contains("no token, client says: ua:node/22.3.0", withoutToken, StringComparison.Ordinal);
    }

    /// <summary>#5793: a tool call that carries neither client info nor a User-Agent records <c>unknown</c>.</summary>
    [Fact]
    public async Task AToolCallWithNeitherClientInfoNorUserAgent_IsUnknown()
    {
        var slowReads = new SlowReadLog(0);
        using var server = await BuildServer(new ReadLatencyAccumulator(), slowReads);

        await ToolsCallAsync(server, "/", "latency_probe_ok");

        Assert.True(slowReads.TryRead(out var record));
        Assert.Equal("unknown", record!.Caller);
    }

    /// <summary>#5793: run_custom_view_panel records through the shared composed-panel runner, which takes the caller the tool
    /// filter worked out for the call.</summary>
    [Fact]
    public async Task ACustomViewPanelCall_RecordsTheMcpCallerOnItsComposeSample()
    {
        var readLatency = new ReadLatencyAccumulator();
        var slowReads = new SlowReadLog(0);
        using var server = await BuildServer(readLatency, slowReads);
        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"run_custom_view_panel\",\"arguments\":{\"spec\":\"{}\"}}}";

        await SendJsonRpcAsync(server, "/", requestBody, "Claude-User/1.0");

        Assert.Equal(1, Assert.Single(readLatency.Drain(), d => d.Surface == ReadSurface.Compose).DistinctCallers);
        Assert.True(slowReads.TryRead(out var record));
        Assert.Equal("compose", record!.Surface);
        Assert.Equal("ua:Claude-User/1.0", record.Caller);
    }

    /// <summary>#5506 part 3: an admission whose table makes <paramref name="surface"/>/<paramref name="route"/> heavy.</summary>
    private static ReadAdmission HeavyAdmission(ReadSurface surface, string route, string band)
    {
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 2);
        /* #5506 part 3: these tests measure admission; with the read cache on, identical calls would share one read. */
        admission.Cache.Enabled = false;
        /* A pair is heavy only when its p90 is strictly above ReadAdmission.HeavyThresholdMs (2,000), so 2,000 would be light and
           nothing would queue: plant the next histogram bucket's bound, 3,000. */
        admission.SetTable(new System.Collections.Generic.Dictionary<(ReadSurface, string, string), long> { [(surface, route, band)] = 3_000 });
        return admission;
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        for (var i = 0; i < 500 && !condition(); i++)
        {
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.True(condition(), "Admission did not reach the expected state in 5 seconds.");
    }

    /// <summary>#5506 part 3: fills both slots and the whole queue from flows of their own; the returned tickets free them.</summary>
    private static async Task<System.Collections.Generic.List<Task<ReadAdmissionTicket>>> FillAsync(ReadAdmission admission, ReadSurface surface, string route, string band)
    {
        var tickets = new System.Collections.Generic.List<Task<ReadAdmissionTicket>>();
        for (var i = 0; i < 2 + ReadAdmission.QueueLimit; i++)
        {
            tickets.Add(Task.Run(() => admission.AdmitAsync(surface, route, band, System.Threading.CancellationToken.None)));
            var queued = Math.Max(0, i - 1);
            await WaitUntil(() => admission.Stats().Queued == queued && admission.Stats().Running == Math.Min(2, i + 1));
        }

        return tickets;
    }

    private static async Task ReleaseAsync(System.Collections.Generic.List<Task<ReadAdmissionTicket>> tickets)
    {
        foreach (var ticket in tickets)
        {
            (await ticket).Dispose();
        }
    }

    [Fact]
    public async Task AtLimitTwo_AThirdHeavyToolCall_Waits_ThenRuns_AndLeavesOneQueueWaitSample()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = HeavyAdmission(ReadSurface.Mcp, "latency_probe_hold", "none");
        using var server = await BuildServer(readLatency, admission: admission);
        LatencyProbeTools.Hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var calls = new[]
        {
            ToolsCallAsync(server, "/", "latency_probe_hold"),
            ToolsCallAsync(server, "/", "latency_probe_hold"),
            ToolsCallAsync(server, "/", "latency_probe_hold"),
        };
        await WaitUntil(() => admission.Stats().Running == 2 && admission.Stats().Queued == 1);

        LatencyProbeTools.Hold.SetResult();
        var answers = await Task.WhenAll(calls);
        Assert.All(answers, a => Assert.Contains("probe-ok", a.Body, StringComparison.Ordinal));

        var drained = readLatency.Drain().Where(d => d.Route == "latency_probe_hold").ToList();
        Assert.Equal(3, Assert.Single(drained, d => d.Outcome == ReadOutcome.Ok).Count);
        Assert.Equal(1, Assert.Single(drained, d => d.Outcome == ReadOutcome.QueueWait).Count);
        Assert.Equal(0, admission.Stats().Running);
    }

    [Fact]
    public async Task AFullQueue_AnswersBusy_WithTheErrorFlag_AndRecordsBusy()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = HeavyAdmission(ReadSurface.Mcp, "latency_probe_ok", "none");
        using var server = await BuildServer(readLatency, admission: admission);
        var held = await FillAsync(admission, ReadSurface.Mcp, "latency_probe_ok", "none");

        var (statusCode, body) = await ToolsCallAsync(server, "/", "latency_probe_ok");
        Assert.Equal(StatusCodes.Status200OK, statusCode);
        Assert.Contains("\"isError\":true", body, StringComparison.Ordinal);
        Assert.Contains("busy", body, StringComparison.Ordinal);
        Assert.Contains("retry_after_seconds", body, StringComparison.Ordinal);
        Assert.DoesNotContain("probe-ok", body, StringComparison.Ordinal);

        var sample = Assert.Single(readLatency.Drain(), d => d.Route == "latency_probe_ok");
        Assert.Equal(ReadOutcome.Busy, sample.Outcome);
        await ReleaseAsync(held);
    }

    [Fact]
    public async Task AHealthRead_IsNotAdmitted_EvenWithTheQueueFull()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = HeavyAdmission(ReadSurface.Mcp, "get_read_latency", "24h");
        using var server = await BuildServer(readLatency, admission: admission);
        var held = await FillAsync(admission, ReadSurface.Mcp, "get_read_latency", "24h");

        await ToolsCallAsync(server, "/", "get_read_latency");
        Assert.DoesNotContain(readLatency.Drain(), d => d.Route == "get_read_latency" && d.Outcome == ReadOutcome.Busy);
        await ReleaseAsync(held);
    }

    [Fact]
    public async Task TheInvestigationWait_IsNeitherTimedNorAdmitted_WhileAnotherRead_IsBoth()
    {
        /* #5856 review round 1: the long poll waits up to 25 seconds by design. With both slots and the whole queue taken,
           and both tools planted as heavy, an admitted call answers busy: the wait answers its own refusal instead (the
           tool ran, no store needed) and leaves no read-latency sample, while the probe read answers busy and records. */
        var readLatency = new ReadLatencyAccumulator();
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 2);
        admission.Cache.Enabled = false;
        admission.SetTable(new System.Collections.Generic.Dictionary<(ReadSurface, string, string), long>
        {
            [(ReadSurface.Mcp, DarlingMcpInvestigationTools.WaitToolName, ReadWindowBand.None)] = 3_000,
            [(ReadSurface.Mcp, "latency_probe_ok", ReadWindowBand.None)] = 3_000,
        });
        using var server = await BuildServer(readLatency, admission: admission);
        var held = await FillAsync(admission, ReadSurface.Mcp, "latency_probe_ok", ReadWindowBand.None);

        var wait = await ToolsCallAsync(server, "/", DarlingMcpInvestigationTools.WaitToolName, "{\"timeout_seconds\":26}");
        var probe = await ToolsCallAsync(server, "/", "latency_probe_ok");
        await ReleaseAsync(held);

        Assert.Contains("Invalid timeout_seconds value", wait.Body, StringComparison.Ordinal);
        Assert.DoesNotContain("retry_after_seconds", wait.Body, StringComparison.Ordinal);
        Assert.Contains("retry_after_seconds", probe.Body, StringComparison.Ordinal);

        var drained = readLatency.Drain();
        Assert.DoesNotContain(drained, d => d.Route == DarlingMcpInvestigationTools.WaitToolName);
        Assert.Equal(ReadOutcome.Busy, Assert.Single(drained, d => d.Route == "latency_probe_ok").Outcome);
    }

    [Fact]
    public async Task RunCustomViewPanel_RefusedByTheSharedRunner_AnswersBusy_WithTheErrorFlag()
    {
        var readLatency = new ReadLatencyAccumulator();

        /* A spec of "{}" records under the catch-all label and the runner's default 24-hour window. */
        var admission = HeavyAdmission(ReadSurface.Compose, "compose:unknown", "24h");
        using var server = await BuildServer(readLatency, admission: admission);
        var held = await FillAsync(admission, ReadSurface.Compose, "compose:unknown", "24h");

        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"run_custom_view_panel\",\"arguments\":{\"spec\":\"{}\"}}}";
        var (_, body) = await SendJsonRpcAsync(server, "/", requestBody);
        Assert.Contains("\"isError\":true", body, StringComparison.Ordinal);
        Assert.Contains("busy", body, StringComparison.Ordinal);

        var sample = Assert.Single(readLatency.Drain(), d => d.Surface == ReadSurface.Compose);
        Assert.Equal(ReadOutcome.Busy, sample.Outcome);
        await ReleaseAsync(held);
    }

    /// <summary>#5506 part 3 (round 1, L6): with GCF on, the busy text a refused panel returns can reach the filter as the
    /// re-encoded wire, not the JSON envelope. The tool flags the refusal, so the error flag is set either way, and the
    /// envelope checks read a GCF wire through its JSON form.</summary>
    [Fact]
    public async Task RunCustomViewPanel_RefusedWithGcfOn_StillCarriesTheErrorFlag()
    {
        var readLatency = new ReadLatencyAccumulator();
        var admission = HeavyAdmission(ReadSurface.Compose, "compose:unknown", "24h");
        admission.SetTable(new System.Collections.Generic.Dictionary<(ReadSurface, string, string), long> { [(ReadSurface.Compose, "compose:unknown", "24h")] = 5_000 });
        using var server = await BuildServer(readLatency, admission: admission, gcf: true);
        var held = await FillAsync(admission, ReadSurface.Compose, "compose:unknown", "24h");

        var requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"run_custom_view_panel\",\"arguments\":{\"spec\":\"{}\"}}}";
        var (_, body) = await SendJsonRpcAsync(server, "/", requestBody);
        Assert.Contains("\"isError\":true", body, StringComparison.Ordinal);
        Assert.Contains("busy", body, StringComparison.Ordinal);
        await ReleaseAsync(held);
    }

    [Fact]
    public void AGcfWireOfABusyOrErrorEnvelope_IsReadAsTheEnvelope()
    {
        var busy = "{\"status\":\"busy\",\"message\":\"The service is running as many long reads as it allows (2), and 16 more are waiting.\",\"retry_after_seconds\":18}";
        var error = PerformanceMonitor.Common.McpHelpers.FormatError("get_wait_stats", new InvalidOperationException("probe failed"));
        foreach (var json in new[] { error })
        {
            /* Whatever GCF makes of the envelope (its wire, or the JSON when the wire is not smaller), it reads the same. */
            var text = GcfOutput.TryEncode(json) ?? json;
            var envelope = McpToolLatencyFilter.EnvelopeText(text);
            Assert.StartsWith("{", envelope, StringComparison.Ordinal);
            var result = new CallToolResult { Content = [new TextContentBlock { Text = text }] };
            Assert.NotEqual(ReadOutcome.Ok, McpToolLatencyFilter.ClassifyResult(result, System.Threading.CancellationToken.None));
        }

        Assert.True(McpToolLatencyFilter.IsBusyEnvelope(new CallToolResult { Content = [new TextContentBlock { Text = GcfOutput.TryEncode(busy) ?? busy }] }));
        Assert.Equal("not json", McpToolLatencyFilter.EnvelopeText("not json"));
    }
}
