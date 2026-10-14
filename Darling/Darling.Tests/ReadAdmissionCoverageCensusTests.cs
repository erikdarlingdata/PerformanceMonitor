/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5506 part 3 (D8): every read that opens a <see cref="ReadScope"/> goes through read admission, or is a health read on
/// the skip roster with its reason. Source-text census over the service: a new read site that opens a scope without
/// admitting fails here, and so does a change to the MCP filter order or the filter's one skipped tool.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ReadAdmissionCoverageCensusTests
{
    private static readonly string ServiceDir = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service");

    /// <summary>The files that open a read scope today, and how many each opens. A new site is added here only with its
    /// admission call beside it.</summary>
    private static readonly Dictionary<string, int> ScopeSites = new(StringComparer.Ordinal)
    {
        ["DarlingWebEndpoints.cs"] = 4, // the /api/read/{name} loop, RunComposedPanelAsync, /api/fleet and /api/alerts/test
        ["DarlingTriageEndpoint.cs"] = 1, // /api/triage (the three dedicated routes record their own samples, #5506 part 3)
        ["McpToolLatencyFilter.cs"] = 1, // every MCP tool call but run_custom_view_panel
    };

    [Fact]
    public void EveryReadScopeSite_IsFollowedByAnAdmissionCall()
    {
        var found = new Dictionary<string, int>(StringComparer.Ordinal);
        var offenders = new List<string>();
        foreach (var file in Directory.EnumerateFiles(ServiceDir, "*.cs", SearchOption.AllDirectories)
                     .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                                 && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)))
        {
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                if (!lines[i].Contains("ReadScope.Open(", StringComparison.Ordinal) || lines[i].TrimStart().StartsWith("///", StringComparison.Ordinal))
                {
                    continue;
                }

                var name = Path.GetFileName(file);
                found[name] = found.GetValueOrDefault(name) + 1;

                /* The admission call comes after the scope and the band, within the next few statements, or (#5506 part 3)
                   the read cache's dispatch does, whose miss path runs the method that admits
                   (CacheMissPath_RunsTheMethodThatAdmits). */
                var window = string.Join("\n", lines.Skip(i + 1).Take(25));
                if (!window.Contains("AdmitAsync(", StringComparison.Ordinal) && !window.Contains("AdmitWebRouteAsync(", StringComparison.Ordinal)
                    && !window.Contains(".IsCached(", StringComparison.Ordinal))
                {
                    offenders.Add($"{name}:{i + 1}");
                }
            }
        }

        Assert.Empty(offenders);
        Assert.Equal(ScopeSites.OrderBy(p => p.Key), found.OrderBy(p => p.Key));
    }

    /// <summary>#5506 part 3: a read the cache cannot answer runs through the same method as an uncached one, and that
    /// method admits it, so only a hit (or a caller that joins a read already admitted) skips admission.</summary>
    [Theory]
    [InlineData("Mcp/McpToolLatencyFilter.cs", "private async Task<CallToolResult> RunReadAsync(", "cache.Join(key, ttl, token => RunReadAsync(")]
    [InlineData("DarlingWebEndpoints.cs", "async Task<WebReadAnswer> RunWebReadAsync()", "return RunWebReadAsync();")]
    public void CacheMissPath_RunsTheMethodThatAdmits(string file, string method, string joinCall)
    {
        var source = File.ReadAllText(Path.Combine(ServiceDir, file));
        var at = source.IndexOf(method, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{file} no longer declares {method}");
        var body = source.Substring(at, Math.Min(2_000, source.Length - at));
        Assert.Contains("AdmitAsync(", body, StringComparison.Ordinal);
        Assert.Contains(joinCall, source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSkipRoster_IsTheHealthReads_EachWithAReason_AndEachAToolThatExists()
    {
        Assert.Equal(
            new[] { "get_collection_health", "get_collection_log", "get_read_latency", "get_slow_reads", "get_store_log", "get_store_metrics", "list_servers" },
            ReadAdmission.SkippedRoutes.Keys.OrderBy(k => k, StringComparer.Ordinal));

        var toolSource = string.Join("\n", Directory.EnumerateFiles(Path.Combine(ServiceDir, "Mcp"), "*.cs").Select(File.ReadAllText));
        foreach (var (name, reason) in ReadAdmission.SkippedRoutes)
        {
            Assert.StartsWith("health read: ", reason, StringComparison.Ordinal);
            Assert.Contains($"Name = \"{name}\"", toolSource, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheFilterOrder_IsUnchanged_AndTheLatencyFilterAdmits()
    {
        var host = File.ReadAllText(Path.Combine(ServiceDir, "Mcp", "DarlingMcpHostService.cs"));
        var guard = host.IndexOf(".AddCallToolFilter(McpUnknownArgumentGuard.Instance)", StringComparison.Ordinal);
        var latency = host.IndexOf(".AddCallToolFilter(toolLatency.AsFilter())", StringComparison.Ordinal);
        var gcf = host.IndexOf(".AddCallToolFilter(GcfCallToolFilter.Instance)", StringComparison.Ordinal);
        var sweep = host.IndexOf(".AddCallToolFilter(SensitiveStatementOutputFilter.Instance)", StringComparison.Ordinal);
        Assert.True(guard >= 0 && guard < latency && latency < gcf && gcf < sweep, "The call-tool filter order changed.");
        Assert.Contains("new McpToolLatencyFilter(hostReadLatency, readLatencyLogger, slowReads, readAdmission)", host, StringComparison.Ordinal);

        var filter = File.ReadAllText(Path.Combine(ServiceDir, "Mcp", "McpToolLatencyFilter.cs"));
        Assert.Contains("_admission.AdmitAsync(ReadSurface.Mcp, toolName, windowBand, cancellationToken)", filter, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFilterSkipsOnlyTheComposedPanelTool_AndTheSharedRunnerAdmitsIt()
    {
        Assert.Equal("run_custom_view_panel", McpToolLatencyFilter.SkippedToolName);
        var filter = File.ReadAllText(Path.Combine(ServiceDir, "Mcp", "McpToolLatencyFilter.cs"));
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(filter, @"string\.Equals\(toolName, SkippedToolName"));

        var web = File.ReadAllText(Path.Combine(ServiceDir, "DarlingWebEndpoints.cs"));
        var runner = web.IndexOf("internal static async Task<ComposeRunOutcome> RunComposedPanelAsync(", StringComparison.Ordinal);
        var core = web.IndexOf("RunComposedPanelCoreAsync(postgres, body,", runner, StringComparison.Ordinal);
        Assert.True(runner >= 0 && core > runner);
        Assert.Contains("admission.AdmitAsync(ReadSurface.Compose, composeRoute, composeBand, cancellationToken)", web[runner..core], StringComparison.Ordinal);

        /* D4: the MCP tool passes the request's token, so a client that leaves the queue leaves it. */
        var tool = File.ReadAllText(Path.Combine(ServiceDir, "Mcp", "DarlingMcpCustomViewTools.cs"));
        Assert.Contains("RunComposedPanelAsync(postgres, body, cancellationToken, readLatency", tool, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDedicatedWebReadRoutes_Admit()
    {
        var web = File.ReadAllText(Path.Combine(ServiceDir, "DarlingWebEndpoints.cs"));
        Assert.Contains("AdmitWebRouteAsync(readAdmission, \"get_fleet_overview\"", web, StringComparison.Ordinal);
        Assert.Contains("AdmitWebRouteAsync(readAdmission, \"test_custom_alert_rule\"", web, StringComparison.Ordinal);
        var triage = File.ReadAllText(Path.Combine(ServiceDir, "DarlingTriageEndpoint.cs"));
        Assert.Contains("AdmitWebRouteAsync(readAdmission, TriageRoute", triage, StringComparison.Ordinal);
    }
}

/// <summary>#5506 part 3: a refused web read answers 429 with a Retry-After header and the sentence as <c>error</c>, and
/// records one <c>busy</c> sample. Refused before the handler runs, so no store is needed.</summary>
public sealed class ReadAdmissionWebTests
{
    private sealed class NullLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => false;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    }

    [Fact]
    public async Task AFullQueue_Answers429_WithRetryAfter_AndRecordsBusy()
    {
        const string route = "get_notification_routes";
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 2);
        /* #5506 part 3: these tests measure admission; with the read cache on, identical calls would share one read. */
        admission.Cache.Enabled = false;
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long> { [(ReadSurface.Web, route, band)] = 3_000 });

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();

        var readLatency = new ReadLatencyAccumulator();
        var host = new DarlingWebHostService(
            new NullLogger<DarlingWebHostService>(), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(),
            baselineCache: null, readLatency: readLatency, readAdmission: admission);
        host.ConfigurePipeline(app, postgres, networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), accessToken: "unused-in-loopback-mode", oidcClient: null);
        await app.StartAsync(TestContext.Current.CancellationToken);

        var held = new List<Task<ReadAdmissionTicket>>();
        for (var i = 0; i < 2 + ReadAdmission.QueueLimit; i++)
        {
            held.Add(Task.Run(() => admission.AdmitAsync(ReadSurface.Web, route, band, CancellationToken.None)));
            var queued = Math.Max(0, i - 1);
            for (var spin = 0; spin < 500 && admission.Stats().Queued != queued; spin++)
            {
                await Task.Delay(10, TestContext.Current.CancellationToken);
            }
        }

        using var client = app.GetTestServer().CreateClient();
        var response = await client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken);
        Assert.Equal((HttpStatusCode)429, response.StatusCode);
        Assert.Equal("18", response.Headers.GetValues("Retry-After").Single());
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Contains("\"error\":\"The service is running as many long reads as it allows (2), and 16 more are waiting.", body, StringComparison.Ordinal);

        var sample = Assert.Single(readLatency.Drain(), d => d.Route == route);
        Assert.Equal(ReadOutcome.Busy, sample.Outcome);

        foreach (var ticket in held)
        {
            (await ticket).Dispose();
        }

        await app.StopAsync(TestContext.Current.CancellationToken);
    }
}
