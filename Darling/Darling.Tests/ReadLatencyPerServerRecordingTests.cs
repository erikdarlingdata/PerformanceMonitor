/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Net;
using System.Text;
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
/// #4782: a web server records a read's latency into the accumulator ITS OWN
/// <see cref="DarlingWebEndpoints.MapAll"/> call was given, so a second server set up in the same process
/// (six test classes call <c>MapAll</c>, and xUnit runs classes in parallel) cannot take its samples.
///
/// <para>Each fact builds server A with accumulator A, then server B with accumulator B (the second
/// <c>MapAll</c> call), THEN sends the request to A. When the accumulator was held process-wide, B's setup
/// replaced A's, so A's sample landed in B's accumulator and A's own was empty. No database: the routes
/// exercised here answer before any read (the dispatch entry throws, and the composed-panel body fails its
/// validation), so the pool over a dummy connection string is never opened.</para>
/// </summary>
public sealed class ReadLatencyPerServerRecordingTests
{
    private static async Task<TestServer> BuildServer(ReadLatencyAccumulator readLatency)
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
            readLatency: readLatency);

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

    private static Task<HttpContext> Send(TestServer server, string method, string path, string? jsonBody = null)
    {
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = method;
            ctx.Request.Path = path;
            ctx.Request.Headers.Host = "localhost";
            ctx.Connection.RemoteIpAddress = IPAddress.Loopback;
            if (jsonBody is not null)
            {
                var bytes = Encoding.UTF8.GetBytes(jsonBody);
                ctx.Request.ContentType = "application/json";
                ctx.Request.Body = new MemoryStream(bytes);
                ctx.Request.ContentLength = bytes.Length;
            }
        });
    }

    /// <summary>The <c>/api/read/*</c> record site: a real 57014 <see cref="PostgresException"/> travels the
    /// dispatch loop of server A while server B has been mapped since.</summary>
    [Fact]
    public async Task AReadOnServerA_RecordsIntoAccumulatorA_EvenAfterServerBWasMapped()
    {
        const string routeName = "__test_statement_timeout";
        using var seam = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope(
            (routeName, (_, _, _) =>
                throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014")));

        var accumulatorA = new ReadLatencyAccumulator();
        var accumulatorB = new ReadLatencyAccumulator();
        var serverA = await BuildServer(accumulatorA);
        _ = await BuildServer(accumulatorB);

        var response = await Send(serverA, "GET", "/api/read/" + routeName);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, response.Response.StatusCode);

        var sample = Assert.Single(accumulatorA.Drain(), d => d.Surface == ReadSurface.Web && d.Route == routeName);
        Assert.Equal(ReadOutcome.Timeout, sample.Outcome);
        Assert.Equal(1, sample.Count);
        Assert.Empty(accumulatorB.Drain());
    }

    /// <summary>The composed-panel record site (<c>RunComposedPanelAsync</c>): a body with no <c>panel</c> is
    /// refused by the runner's validation before it reaches the store, and the runner records the run either
    /// way. Same shape as above: server A answers after server B was mapped.</summary>
    [Fact]
    public async Task AComposedPanelRunOnServerA_RecordsIntoAccumulatorA_EvenAfterServerBWasMapped()
    {
        var accumulatorA = new ReadLatencyAccumulator();
        var accumulatorB = new ReadLatencyAccumulator();
        var serverA = await BuildServer(accumulatorA);
        _ = await BuildServer(accumulatorB);

        var response = await Send(serverA, "POST", "/api/compose/run", "{}");
        Assert.Equal(StatusCodes.Status400BadRequest, response.Response.StatusCode);

        var sample = Assert.Single(accumulatorA.Drain(), d => d.Surface == ReadSurface.Compose);
        Assert.Equal(ReadOutcome.Error, sample.Outcome);
        Assert.Equal(1, sample.Count);
        Assert.Empty(accumulatorB.Drain());
    }
}
