/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6795: the slow-read capture keeps the slowest statements of a read, not the first ones, and a web read the browser
/// cancelled after the slow-read threshold is captured as cancelled instead of vanishing at the rethrow. No database: the
/// scope and the log are plain objects, and the web reads run a test-only dispatch entry through the real pipeline.
/// </summary>
public sealed class SlowestStatementCaptureTests
{
    /// <summary>The shape of the measured read: dozens of small probes first, the big aggregations after statement 64.</summary>
    private static ReadScope ReadThatStartsSmallAndEndsBig(int small, int big, ReadScope scope)
    {
        for (var i = 1; i <= small; i++)
        {
            scope.AddStatement("SELECT probe_" + i, 5 + (i % 7), null);
        }

        for (var i = 1; i <= big; i++)
        {
            scope.AddStatement("SELECT aggregate_" + i, 40_000 + i, null);
        }

        return scope;
    }

    [Fact]
    public void AScopeOfOneHundredStatements_KeepsTheSlowestOnes_AndReportsTheTotal()
    {
        using var opened = ReadScope.Open(null);
        var scope = ReadThatStartsSmallAndEndsBig(small: 80, big: 20, opened.Scope);

        var kept = scope.Snapshot();
        Assert.Equal(ReadScope.MaxStatements, kept.Count);
        Assert.Equal(100, scope.StatementTotal);
        Assert.Equal(100 - ReadScope.MaxStatements, scope.StatementsDropped);

        /* All twenty aggregations ran after statement 80, past anything the first-64 rule kept. */
        var aggregates = kept.Where(t => t.Label.StartsWith("SELECT aggregate_", StringComparison.Ordinal)).ToList();
        Assert.Equal(20, aggregates.Count);
        Assert.All(aggregates, t => Assert.True(t.Ordinal > 80));

        /* Kept in arrival order, each with the ordinal it arrived at. */
        Assert.Equal(kept.OrderBy(t => t.Ordinal).Select(t => t.Ordinal), kept.Select(t => t.Ordinal));
        Assert.Equal(kept.Count, kept.Select(t => t.Ordinal).Distinct().Count());
    }

    [Fact]
    public void AStatementFasterThanEveryKeptOne_IsCountedAndNotKept()
    {
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;
        for (var i = 1; i <= ReadScope.MaxStatements; i++)
        {
            scope.AddStatement("SELECT kept_" + i, 10, null);
        }

        for (var i = 1; i <= 100; i++)
        {
            scope.AddStatement("SELECT faster_" + i, 5, null);
        }

        var kept = scope.Snapshot();
        Assert.Equal(ReadScope.MaxStatements + 100, scope.StatementTotal);
        Assert.Equal(100, scope.StatementsDropped);
        Assert.Equal(Enumerable.Range(1, ReadScope.MaxStatements), kept.Select(t => t.Ordinal));
        Assert.DoesNotContain(kept, t => t.Label.StartsWith("SELECT faster_", StringComparison.Ordinal));

        /* The dropped ones did not disturb the set: a slower statement still evicts the latest of the tied ones. */
        scope.AddStatement("SELECT slower", 11, null);
        Assert.Equal(Enumerable.Range(1, ReadScope.MaxStatements - 1).Append(ReadScope.MaxStatements + 101), scope.Snapshot().Select(t => t.Ordinal));
    }

    [Fact]
    public void ATieWithTheFastestKeptOne_KeepsTheEarlierStatement()
    {
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;
        for (var i = 1; i <= 200; i++)
        {
            scope.AddStatement("SELECT same_" + i, 3, null);
        }

        var kept = scope.Snapshot();
        Assert.Equal(200, scope.StatementTotal);
        Assert.Equal(Enumerable.Range(1, ReadScope.MaxStatements), kept.Select(t => t.Ordinal));
    }

    [Fact]
    public void ASlowerStatement_EvictsTheLatestOfTheTiedFastestOnes_NotTheEarliest()
    {
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;
        for (var i = 1; i <= ReadScope.MaxStatements; i++)
        {
            scope.AddStatement("SELECT tied_" + i, 10, null);
        }

        scope.AddStatement("SELECT slower", 11, null);

        /* Fifty at 10 ms, then one at 11 ms: the one that goes is the 50th, so the earlier tied statements stay. */
        var kept = scope.Snapshot();
        Assert.Equal(Enumerable.Range(1, ReadScope.MaxStatements - 1).Append(ReadScope.MaxStatements + 1), kept.Select(t => t.Ordinal));
        Assert.Equal(ReadScope.MaxStatements + 1, scope.StatementTotal);
    }

    [Fact]
    public void ASlowReadRow_StoresTheSlowestFiftyOfAHundred_WithTheirOrdinals_AndTheTotalCount()
    {
        var log = new SlowReadLog();
        using var opened = ReadScope.Open(null);
        var scope = ReadThatStartsSmallAndEndsBig(small: 85, big: 15, opened.Scope);

        log.Offer(scope, ReadSurface.Web, "get_resource_use_by_tag", ReadOutcome.Ok, 140_000, null, null, null);

        Assert.True(log.TryRead(out var record));
        var stored = JsonNode.Parse(record!.StatementsJson)!.AsArray().Select(s => s!.AsObject()).ToList();
        Assert.Equal(SlowReadLog.MaxStatementsStored, stored.Count);
        Assert.Equal(100, record.StatementCount);
        Assert.True(record.StatementsTruncated);

        var bigOnes = stored.Where(s => ((string)s["label"]!).StartsWith("SELECT aggregate_", StringComparison.Ordinal)).ToList();
        Assert.Equal(15, bigOnes.Count);
        Assert.All(bigOnes, s => Assert.True((int)s["ordinal"]! > 85));
        Assert.Equal(stored.OrderBy(s => (int)s["ordinal"]!).Select(s => (int)s["ordinal"]!), stored.Select(s => (int)s["ordinal"]!));

        /* The summary get_slow_reads shows names the slowest statements, which the old capture never held. */
        var summary = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpSlowReadTools.StatementSummary(JsonNode.Parse(record.StatementsJson)!.AsArray());
        Assert.Contains("aggregate_15", summary, StringComparison.Ordinal);
    }

    private const string AbortedRoute = "__test_aborted_slow_read";

    private static async Task<TestServer> BuildServer(SlowReadLog slowReads, ReadLatencyAccumulator accumulator)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();
        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            baselineCache: null,
            readLatency: accumulator,
            slowReads: slowReads);

        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: false,
            networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"),
            accessToken: "unused-in-loopback-mode",
            oidcClient: null);

        await app.StartAsync();
        return app.GetTestServer();
    }

    /// <summary>One web read that runs <paramref name="runMs"/>, runs one statement, and is then abandoned by the browser. The
    /// read ends either with the cancellation the token raises or, as a store connect that gives up can, with another
    /// exception while the token is already cancelled.</summary>
    private static async Task<SlowReadRecord?> RunAbortedRead(int thresholdMs, int runMs, bool endsWithOtherException, ReadLatencyAccumulator accumulator)
    {
        var slowReads = new SlowReadLog(thresholdMs);
        using var aborted = new CancellationTokenSource();
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((AbortedRoute, async (_, _, _) =>
        {
            ReadScope.Current!.AddStatement("SELECT 1 FROM slow_thing", runMs, null);
            await Task.Delay(runMs);
            aborted.Cancel();
            if (endsWithOtherException)
            {
                throw new IOException("The client reset the request stream.");
            }

            aborted.Token.ThrowIfCancellationRequested();
            return string.Empty;
        }));

        var server = await BuildServer(slowReads, accumulator);
        await server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = "/api/read/" + AbortedRoute;
            ctx.Request.QueryString = new QueryString("?hours=6");
            ctx.Request.Headers.Host = "localhost";
            ctx.Connection.RemoteIpAddress = IPAddress.Loopback;
            ctx.RequestAborted = aborted.Token;
        });

        return slowReads.TryRead(out var record) ? record : null;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWebReadTheBrowserCancelledAfterTheThreshold_IsCaptured_AsCancelled_WithRouteTimeAndStatements(bool endsWithOtherException)
    {
        var accumulator = new ReadLatencyAccumulator();
        var record = await RunAbortedRead(thresholdMs: 150, runMs: 250, endsWithOtherException, accumulator);

        Assert.NotNull(record);
        Assert.Equal("web", record!.Surface);
        Assert.Equal(AbortedRoute, record.Route);
        Assert.Equal("cancelled", record.Outcome);
        Assert.InRange(record.TotalMs, 150, 60_000);
        Assert.Equal(1, record.StatementCount);
        Assert.Contains("slow_thing", record.StatementsJson, StringComparison.Ordinal);
        Assert.Contains("hours", record.ArgumentsJson, StringComparison.Ordinal);

        /* The same read is one cancelled read_latency sample, so get_read_latency and get_slow_reads agree about it. */
        var sample = Assert.Single(accumulator.Drain(), d => d.Surface == ReadSurface.Web && d.Route == AbortedRoute && d.Outcome == ReadOutcome.Cancelled);
        Assert.Equal(1, sample.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWebReadTheBrowserCancelledUnderTheThreshold_IsNotCaptured(bool endsWithOtherException)
    {
        var accumulator = new ReadLatencyAccumulator();
        var record = await RunAbortedRead(thresholdMs: 600_000, runMs: 250, endsWithOtherException, accumulator);

        Assert.Null(record);

        /* Under the threshold the slow-read log declines it, but the cancelled read still counts as one sample. */
        var sample = Assert.Single(accumulator.Drain(), d => d.Surface == ReadSurface.Web && d.Route == AbortedRoute && d.Outcome == ReadOutcome.Cancelled);
        Assert.Equal(1, sample.Count);
    }
}
