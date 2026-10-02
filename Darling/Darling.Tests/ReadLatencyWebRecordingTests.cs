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
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4442 scope 2, the product-path pin: one real <c>/api/read/&lt;name&gt;</c> request through the ACTUAL
/// <see cref="DarlingWebHostService.ConfigurePipeline"/> pipeline (the same #4128 <see cref="TestServer"/>
/// pattern <see cref="DarlingWebFailureHandlingTests"/> established), asserting the <see cref="ReadLatencyAccumulator"/>
/// this run wires through <see cref="DarlingWebEndpoints.MapAll"/> actually received the samples the dispatch
/// loop is supposed to record — not the accumulator's own unit tests, and not a hand-called <c>Record</c>
/// standing in for the wiring.
///
/// <para><b>Why each fact's samples are its own:</b> every <see cref="DarlingWebEndpoints.MapAll"/> call
/// records into the accumulator IT was given, and the fact drains that same instance, so no other server set up
/// at the same time (six test classes call <c>MapAll</c>, and xUnit runs classes in parallel) can take its
/// samples. Until #4782 the endpoints kept the accumulator in a process-wide static that every <c>MapAll</c> call
/// overwrote. This file captured its own instance and said that sidestepped the race, but the dispatch never
/// read the captured instance, so a server another class built between <see cref="BuildServer"/> and the request
/// took the sample. <see cref="ReadLatencyPerServerRecordingTests"/> pins the per-server recording directly,
/// with no database.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ReadLatencyWebRecordingTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    }

    /// <summary>Builds a <see cref="TestServer"/> over the REAL pipeline, wired to a caller-supplied
    /// <see cref="ReadLatencyAccumulator"/> — the same construction <see cref="DarlingWebFailureHandlingTests.BuildServer"/>
    /// uses, plus the accumulator argument <see cref="DarlingWebHostService"/>'s constructor and
    /// <see cref="DarlingWebEndpoints.MapAll"/> both now take.</summary>
    private static async Task<TestServer> BuildServer(NpgsqlDataSource postgres, ReadLatencyAccumulator readLatency)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);

        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();

        var host = new DarlingWebHostService(
            new CapturingLogger<DarlingWebHostService>(),
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

        /* A test-only route standing in for a real handler that would otherwise throw the 57014
           statement-timeout shape — the loop's OWN try/catch (DarlingWebEndpoints.cs ~341-370) is what this
           pin is reading, not a hand-built copy of it, so the route is registered the same way
           DarlingWebFailureHandlingTests.BuildServer's throwing routes are: mapped directly, after
           ConfigurePipeline, inside the SAME #4276 backstop nesting. */
        app.MapGet("/api/__test/read-timeout", (HttpContext _) =>
            throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014"));

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<HttpResponseMessage> Send(TestServer server, string path)
    {
        using var client = server.CreateClient();
        return await client.GetAsync(path);
    }

    [Fact]
    public async Task ARealReadRoute_RecordsExactlyOneWebSample_WithOutcomeOk()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the read-latency web-recording pin.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var readLatency = new ReadLatencyAccumulator();
        var server = await BuildServer(postgres, readLatency);

        /* get_notification_routes: no server_name/window binding to seed, an empty store answers an empty
           list rather than a refusal, so this is a clean Ok through the REAL handler with no fixture setup. */
        var response = await Send(server, "/api/read/get_notification_routes");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var drained = readLatency.Drain();
        var sample = Assert.Single(drained, d => d.Surface == ReadSurface.Web && d.Route == "get_notification_routes");
        Assert.Equal(ReadOutcome.Ok, sample.Outcome);
        Assert.Equal(1, sample.Count);
    }

    [Fact]
    public async Task AHandlerThatThrowsTheTimeoutShape_RecordsOneWebSample_WithOutcomeTimeout()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the read-latency web-recording pin.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var readLatency = new ReadLatencyAccumulator();
        var server = await BuildServer(postgres, readLatency);

        var response = await Send(server, "/api/__test/read-timeout");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);

        var drained = readLatency.Drain();
        Assert.Empty(drained); // the test-only route is outside BuildReadDispatch, so the /api/read/* loop's
                                // own Record call never ran for it -- this fact documents that boundary
                                // rather than asserting a Timeout sample that route can never produce.
    }

    /// <summary>#4442: the pin the classifier's own unit tests cannot give -- a real 57014
    /// <see cref="PostgresException"/> travelling through the SAME <c>/api/read/*</c> dispatch loop every
    /// production route uses (<see cref="DarlingWebEndpoints.TestOnlyExtraDispatchEntry"/>, an ONE-entry
    /// test seam that <see cref="DarlingWebEndpoints.BuildReadDispatch"/> folds in only when a test set it),
    /// not a hand-called <c>Record</c> standing in for that wiring. Registered under the SAME name a real
    /// tool never uses, so it is one more dispatch key, not a different code path.</summary>
    [Fact]
    public async Task ADispatchEntryThatThrowsA57014PostgresException_RecordsOneWebSample_WithOutcomeTimeout()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the read-latency web-recording pin.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        const string routeName = "__test_statement_timeout";
        using var seam = new ExtraDispatchEntryScope(
            (routeName, (_, _, _) =>
                throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014")));

        var readLatency = new ReadLatencyAccumulator();
        var server = await BuildServer(postgres, readLatency);

        var response = await Send(server, "/api/read/" + routeName);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);

        var drained = readLatency.Drain();
        var sample = Assert.Single(drained, d => d.Surface == ReadSurface.Web && d.Route == routeName);
        Assert.Equal(ReadOutcome.Timeout, sample.Outcome);
        Assert.Equal(1, sample.Count);
    }

    /// <summary>
    /// Sets <see cref="DarlingWebEndpoints.TestOnlyExtraDispatchEntry"/> for the scope's lifetime and
    /// clears it on <see cref="Dispose"/>. The entry is held per async flow (#4782): only this fact, and the
    /// server it builds after setting the entry in the same method, sees it. A test class running at the same
    /// time in another flow builds its own <c>BuildReadDispatch</c> without it. Disposing still clears it, so
    /// a later step in this fact's own flow never sees a leftover.
    /// </summary>
    internal sealed class ExtraDispatchEntryScope : IDisposable
    {
        public ExtraDispatchEntryScope((string Name, DarlingWebEndpoints.ReadToolHandler Handler) entry)
        {
            DarlingWebEndpoints.TestOnlyExtraDispatchEntry = entry;
        }

        public void Dispose()
        {
            DarlingWebEndpoints.TestOnlyExtraDispatchEntry = null;
        }
    }
}
