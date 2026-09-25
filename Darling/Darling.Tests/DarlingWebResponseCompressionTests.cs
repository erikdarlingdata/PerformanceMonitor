/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4188 live-HTTP proof: response compression and Cache-Control, through the SAME <c>ConfigureResponseCompression</c>
/// + <c>ConfigurePipeline</c> methods production calls, over a real <see cref="TestServer"/> — the #4128 pattern
/// <see cref="DarlingWebHostGateLiveTests"/> established, so this proves the WIRED pipeline compresses and
/// caches the way the source claims, not a hand-copied second pipeline that could drift from it.
///
/// <para>Loopback mode throughout (no token/CIDR gate to thread), and only routes that need no store hit
/// (<c>/api/ping</c>, the static JS the SPA ships) — this class asserts response SHAPE, not store content, so
/// it stays ungated (no live Postgres needed, unlike <see cref="AvailabilityGroupCountReadTests"/>).</para>
/// </summary>
public sealed class DarlingWebResponseCompressionTests
{
    /// <summary>
    /// Builds a <see cref="TestServer"/> running the REAL <c>ConfigureResponseCompression</c> (on the builder)
    /// and <c>ConfigurePipeline</c> (on the built app) — the same two calls <c>TryStartServerAsync</c> makes in
    /// production, in the same order. <c>ContentRootPath</c>/<c>WebRootPath</c> point at the actual service
    /// project's <c>wwwroot</c> on disk (mirroring the production FOOTGUN comment on that same wiring) so
    /// <c>UseStaticFiles</c> serves the real <c>js/app.js</c> rather than 404ing against the test binary's own
    /// output directory.
    /// </summary>
    private static async Task<TestServer> BuildServer()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        // A pool none of these routes open (/api/ping reads only CollectorRuntimeState; static files read disk).
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);

        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();

        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache());

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

    private static Task<HttpContext> Send(TestServer server, string path, string? acceptEncoding)
    {
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = path;
            ctx.Request.Headers.Host = "localhost";
            ctx.Connection.RemoteIpAddress = IPAddress.Loopback;
            if (acceptEncoding is not null)
            {
                ctx.Request.Headers.AcceptEncoding = acceptEncoding;
            }
        });
    }

    /// <summary>#4202 removed <c>application/json</c> from the compression MIME list (BREACH: a fixed secret
    /// in a response body alongside attacker-controlled input can be recovered via size observation). JSON API
    /// responses are NOT compressed, but still carry <c>Cache-Control: no-store</c> from the pipeline
    /// middleware.</summary>
    [Fact]
    public async Task JsonApi_AcceptEncodingGzip_IsNotCompressed_ButNoStore()
    {
        using var server = await BuildServer();
        var ctx = await Send(server, "/api/ping", "gzip");

        Assert.Equal(string.Empty, ctx.Response.Headers.ContentEncoding.ToString());
        Assert.Equal("no-store", ctx.Response.Headers.CacheControl.ToString());
    }

    [Fact]
    public async Task JsonApi_AcceptEncodingBrotli_IsNotCompressed()
    {
        using var server = await BuildServer();
        var ctx = await Send(server, "/api/ping", "br");

        Assert.Equal(string.Empty, ctx.Response.Headers.ContentEncoding.ToString());
    }

    [Fact]
    public async Task JsonApi_NoAcceptEncoding_IsNotCompressed()
    {
        using var server = await BuildServer();
        var ctx = await Send(server, "/api/ping", acceptEncoding: null);

        Assert.Equal(string.Empty, ctx.Response.Headers.ContentEncoding.ToString());
    }

    [Fact]
    public async Task StaticJsAsset_AcceptEncodingGzip_RespondsCompressed_WithNoCache()
    {
        using var server = await BuildServer();
        var ctx = await Send(server, "/js/app.js", "gzip");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal("gzip", ctx.Response.Headers.ContentEncoding.ToString());
        Assert.Equal("no-cache", ctx.Response.Headers.CacheControl.ToString());
    }
}
