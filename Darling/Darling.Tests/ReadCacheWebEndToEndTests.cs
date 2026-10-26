/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
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
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #5506 part 3 (fix round 1, C2, C3, L5, C1): the read cache and admission through the real web pipeline. Each test
/// adds its own read with <see cref="DarlingWebEndpoints.TestOnlyExtraDispatchEntry"/>, classified heavy, so no store is
/// needed: the handler stands in for the store read and counts its calls.
/// </summary>
public sealed class ReadCacheWebEndToEndTests
{
    private sealed class NullLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    }

    private static ReadAdmission HeavyAdmission(int limit, ReadSurface surface, string route, string band, TimeProvider? time = null)
    {
        var admission = new ReadAdmission(time ?? new TimerTimeProvider(), ReadAdmission.DefaultWaitCap, null, limit);
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long> { [(surface, route, band)] = 5_000 });
        return admission;
    }

    /// <summary>The production web pipeline over an unreachable store, plus a test-only write route (any 2xx non-GET
    /// <c>/api/*</c> answer moves the cache's write generation) and an optional middleware ahead of everything.</summary>
    internal static async Task<(WebApplication App, NpgsqlDataSource Postgres)> BuildServer(ReadAdmission admission, ReadLatencyAccumulator readLatency,
        Func<RequestDelegate, RequestDelegate>? first = null, Task? heldWriteEnd = null)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();

        /* The writes below need an editing seat: a loopback dashboard with no token serves reads only. Every request comes
           from loopback holding a shared-token session cookie signed with the key the pipeline is given. */
        var signingKey = System.Security.Cryptography.RandomNumberGenerator.GetBytes(32);
        var sessionCookie = DarlingWebHostService.SessionCookieName + "="
            + DarlingWebHostService.BuildSessionCookieValue(signingKey, DateTimeOffset.UtcNow.AddHours(1), null);
        app.Use(next => context =>
        {
            context.Connection.RemoteIpAddress ??= IPAddress.Loopback;
            context.Request.Headers.Cookie = sessionCookie;
            return next(context);
        });
        if (first is not null)
        {
            app.Use(first);
        }

        var host = new DarlingWebHostService(
            new NullLogger<DarlingWebHostService>(), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(),
            baselineCache: null, readLatency: readLatency, readAdmission: admission);
        host.ConfigurePipeline(app, postgres, networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), accessToken: "read-cache-end-to-end-token-0123456789", oidcClient: null,
            requireTokenWhenLoopbackOnly: true, sessionSigningKeyForTests: signingKey);
        app.MapPost("/api/__test/write", () => Results.Text("{}", "application/json"));

        /* Round 2, L2: a write whose 2xx and body leave at once, while its handler stays until heldWriteEnd ends. */
        app.MapPost("/api/__test/held-write", async (HttpContext context) =>
        {
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsync("{}", context.RequestAborted);
            await context.Response.Body.FlushAsync(context.RequestAborted);
            await (heldWriteEnd ?? Task.CompletedTask);
        });
        await app.StartAsync(TestContext.Current.CancellationToken);
        return (app, postgres);
    }

    private static async Task WaitUntil(Func<bool> condition, string what)
    {
        for (var i = 0; i < 2_000 && !condition(); i++)
        {
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.True(condition(), what);
    }

    [Fact(Timeout = 60_000)]
    public async Task TwoIdenticalHeavyReads_MakeOneStoreRead_TheSecondIsACacheHitWithNoAdmission_AndAWriteInvalidates()
    {
        const string route = "get_test_cached_read";
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        var admission = HeavyAdmission(2, ReadSurface.Web, route, band);
        var readLatency = new ReadLatencyAccumulator();
        var calls = 0;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((route, (_, _, _) =>
            Task.FromResult("{\"rows\":[" + Interlocked.Increment(ref calls).ToString(System.Globalization.CultureInfo.InvariantCulture) + "]}")));
        var (app, postgres) = await BuildServer(admission, readLatency);
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();

        var first = await client.GetStringAsync("/api/read/" + route, TestContext.Current.CancellationToken);
        var second = await client.GetStringAsync("/api/read/" + route, TestContext.Current.CancellationToken);
        Assert.Equal(1, calls);
        Assert.Equal(first, second);

        var stats = Assert.Single(admission.Stats().Routes, r => r.Route == route);
        Assert.Equal(1, stats.Admitted);
        Assert.Equal(1, stats.CacheMisses);
        Assert.Equal(1, stats.CacheHits);
        var samples = readLatency.Drain().Where(d => d.Route == route).Select(d => d.Outcome).OrderBy(o => o).ToList();
        Assert.Equal(new[] { ReadOutcome.Ok, ReadOutcome.CacheHit }.OrderBy(o => o), samples);

        using var write = await client.PostAsync("/api/__test/write", new StringContent("{}", Encoding.UTF8, "application/json"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, write.StatusCode);
        var third = await client.GetStringAsync("/api/read/" + route, TestContext.Current.CancellationToken);
        Assert.Equal(2, calls);
        Assert.NotEqual(first, third);
    }

    /// <summary>C3 (D6): the caller that starts a shared read leaves; the read keeps running for the caller that joined it,
    /// which gets the answer, and the first caller's own request still sees its own abort (its RequestAborted is never
    /// replaced by the read's token). The first caller that leaves is the flight's lead, and a lead's request stays open
    /// until the read ends (<c>ReadFlight.WaitAsync</c>: the read records through the lead's scope), so its client call
    /// completes only after the read does: the read's gate is released before that call is awaited. (Awaiting the first
    /// call before releasing the gate waited for a read that only the test could end, and hung the run.)</summary>
    [Fact(Timeout = 60_000)]
    public async Task TheFirstCallerLeaves_TheJoinedCallerStillGetsTheAnswer_AndTheFirstKeepsItsOwnAbort()
    {
        const string route = "get_test_shared_read";
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        var admission = HeavyAdmission(2, ReadSurface.Web, route, band);
        var readLatency = new ReadLatencyAccumulator();
        var calls = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        HttpContext? readContext = null;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((route, async (c, _, _) =>
        {
            Interlocked.Increment(ref calls);
            readContext = c;
            entered.TrySetResult();
            await gate.Task;
            return "{\"rows\":[42]}";
        }));

        var firstDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (app, postgres) = await BuildServer(admission, readLatency, next => async context =>
        {
            var isFirst = context.Request.Headers.ContainsKey("X-Test-First");
            try
            {
                await next(context);
            }
            finally
            {
                if (isFirst)
                {
                    firstDone.TrySetResult(context.RequestAborted.IsCancellationRequested);
                }
            }
        });
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();
        var timeout = TimeSpan.FromSeconds(20);

        using var firstLeaves = new CancellationTokenSource();
        using var firstRequest = new HttpRequestMessage(HttpMethod.Get, "/api/read/" + route);
        firstRequest.Headers.Add("X-Test-First", "1");
        var firstCall = client.SendAsync(firstRequest, firstLeaves.Token);
        await entered.Task.WaitAsync(timeout, TestContext.Current.CancellationToken);

        var joinedCall = client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken);

        try
        {
            /* Round 2, L9: the second caller has joined when the flight counts two waiters (no fixed delay). */
            await WaitUntil(() => admission.Cache.WaitingCallers == 2, "the second caller never joined the shared read");
            await firstLeaves.CancelAsync();

            /* The first caller has left (one waiter) and the joined caller still waits: the shared read must not be cancelled. */
            await WaitUntil(() => admission.Cache.WaitingCallers == 1, "the first caller never left the shared read");
            Assert.NotNull(readContext);
            Assert.False(readContext!.RequestAborted.IsCancellationRequested, "the shared read was cancelled while a caller still waited for it");
            Assert.False(joinedCall.IsCompleted, "the joined caller was answered before the shared read ended");
        }
        finally
        {
            /* On any path the read ends, so neither request can outlive the test. */
            gate.TrySetResult();
        }

        using var joined = await joinedCall.WaitAsync(timeout, TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, joined.StatusCode);
        Assert.Contains("42", await joined.Content.ReadAsStringAsync(TestContext.Current.CancellationToken), StringComparison.Ordinal);
        Assert.Equal(1, calls);

        /* The lead's request ends once the read has, and its client call reports the cancel. */
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => firstCall.WaitAsync(timeout, TestContext.Current.CancellationToken));
        Assert.True(await firstDone.Task.WaitAsync(timeout, TestContext.Current.CancellationToken),
            "the first caller's request no longer saw its own abort: its RequestAborted was replaced");
    }

    /// <summary>Round 2, M1: one shared read that times out, with two callers joined to it, records exactly one timeout: the
    /// lead's. Each joined caller records only cache_hit for the shared answer it got, so one store read counts once in
    /// count, timeouts and the percentiles.</summary>
    [Fact(Timeout = 60_000)]
    public async Task ASharedReadThatTimesOut_WithTwoJoinedCallers_RecordsExactlyOneTimeout()
    {
        const string route = "get_test_shared_timeout";
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        var admission = HeavyAdmission(2, ReadSurface.Web, route, band);
        var readLatency = new ReadLatencyAccumulator();
        var calls = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((route, async (_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            entered.TrySetResult();
            await gate.Task;
            throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
        }));
        var (app, postgres) = await BuildServer(admission, readLatency);
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();
        var timeout = TimeSpan.FromSeconds(20);

        var lead = client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken);
        Task<HttpResponseMessage>[] joined;
        try
        {
            await entered.Task.WaitAsync(timeout, TestContext.Current.CancellationToken);
            joined =
            [
                client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken),
                client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken),
            ];
            await WaitUntil(() => admission.Cache.WaitingCallers == 3, "the two callers never joined the shared read");
        }
        finally
        {
            gate.TrySetResult();
        }

        var responses = await Task.WhenAll(joined.Prepend(lead)).WaitAsync(timeout, TestContext.Current.CancellationToken);
        foreach (var response in responses)
        {
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            response.Dispose();
        }

        Assert.Equal(1, calls);
        var samples = readLatency.Drain().Where(d => d.Route == route).ToList();
        Assert.Equal(1, samples.Where(d => d.Outcome == ReadOutcome.Timeout).Sum(d => d.Count));
        Assert.Equal(2, samples.Where(d => d.Outcome == ReadOutcome.CacheHit).Sum(d => d.Count));
        Assert.All(samples, d => Assert.Contains(d.Outcome, new[] { ReadOutcome.Timeout, ReadOutcome.CacheHit }));
    }

    /// <summary>Round 2, L9 (C3's claim): a caller joined to a read that admission refuses gets the busy answer with its own
    /// Retry-After header, and records its own busy.</summary>
    [Fact(Timeout = 60_000)]
    public async Task AJoinedCaller_OfARefusedRead_GetsTheBusyAnswerWithRetryAfter()
    {
        const string route = "get_test_shared_busy";
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        var clock = new TimerTimeProvider();
        var admission = HeavyAdmission(1, ReadSurface.Web, route, band, clock);
        var readLatency = new ReadLatencyAccumulator();
        var calls = 0;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((route, (_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult("{\"rows\":[1]}");
        }));
        var (app, postgres) = await BuildServer(admission, readLatency);
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();
        var timeout = TimeSpan.FromSeconds(20);

        /* The only slot is taken in a flow of its own, so the first caller queues (5 s expected, under the 45 s cap). */
        var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, route, band, TestContext.Current.CancellationToken));
        HttpResponseMessage[] responses;
        try
        {
            var lead = client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken);
            await WaitUntil(() => admission.Stats().Queued == 1, "the first caller never queued");
            var joined = client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken);
            await WaitUntil(() => admission.Cache.WaitingCallers == 2, "the second caller never joined the queued read");

            /* The cap passes while the first caller waits: it is refused, and the joined caller gets the same answer. */
            clock.Advance(ReadAdmission.DefaultWaitCap);
            responses = await Task.WhenAll(lead, joined).WaitAsync(timeout, TestContext.Current.CancellationToken);
        }
        finally
        {
            held.Dispose();
        }

        foreach (var response in responses)
        {
            Assert.Equal((HttpStatusCode)429, response.StatusCode);
            Assert.NotNull(response.Headers.RetryAfter);
            Assert.True(response.Headers.RetryAfter!.Delta >= TimeSpan.FromSeconds(ReadAdmission.MinRetryAfterSeconds));
            response.Dispose();
        }

        Assert.Equal(0, calls);
        var samples = readLatency.Drain().Where(d => d.Route == route).ToList();
        Assert.Equal(2, samples.Where(d => d.Outcome == ReadOutcome.Busy).Sum(d => d.Count));
    }

    /// <summary>Round 2, L2: the write generation moves before a write's 2xx leaves, so a read sent the moment the 2xx
    /// arrives, while the write's handler has not yet returned, misses the entry stored before the write.</summary>
    [Fact(Timeout = 60_000)]
    public async Task ARefreshTheMomentAWriteAnswers_MissesThePreWriteEntry()
    {
        const string route = "get_test_refresh_after_write";
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        var admission = HeavyAdmission(2, ReadSurface.Web, route, band);
        var readLatency = new ReadLatencyAccumulator();
        var calls = 0;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((route, (_, _, _) =>
            Task.FromResult("{\"rows\":[" + Interlocked.Increment(ref calls).ToString(System.Globalization.CultureInfo.InvariantCulture) + "]}")));
        var writeEnd = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (app, postgres) = await BuildServer(admission, readLatency, heldWriteEnd: writeEnd.Task);
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();
        var timeout = TimeSpan.FromSeconds(20);

        var before = await client.GetStringAsync("/api/read/" + route, TestContext.Current.CancellationToken);
        Assert.Equal(1, calls);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, "/api/__test/held-write")
            {
                Content = new StringContent("{}", Encoding.UTF8, "application/json"),
            };
            using var write = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, TestContext.Current.CancellationToken)
                .WaitAsync(timeout, TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.OK, write.StatusCode);

            var after = await client.GetStringAsync("/api/read/" + route, TestContext.Current.CancellationToken);
            Assert.Equal(2, calls);
            Assert.NotEqual(before, after);
        }
        finally
        {
            writeEnd.TrySetResult();
        }
    }

    /// <summary>Round 2, M1 (compose): two callers joined to a composed-panel run that timed out record no timeout of their
    /// own, only cache_hit. The run here is the test's own flight on the panel's key: the shared runner's run would need a
    /// store to time out, and it records its one outcome itself, unchanged by this fix.</summary>
    [Fact(Timeout = 60_000)]
    public async Task AComposedRunThatTimesOut_WithTwoJoinedCallers_RecordsNoTimeoutForThem()
    {
        const string route = "compose:unknown";
        var body = (JsonObject)JsonNode.Parse("{\"panel\":{}}")!;
        const string band = "24h";
        var admission = HeavyAdmission(2, ReadSurface.Compose, route, band);
        var readLatency = new ReadLatencyAccumulator();
        var recorder = new ReadLatencyRecorder(readLatency, null, null, admission);
        var cache = admission.Cache;
        var ttl = ReadResultCache.TtlOf(band);
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");
        var timedOut = ComposeRunOutcome.AuthorQueryError("canceling statement due to statement timeout", "57014");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lead = cache.Join(ReadCacheKey.ForCompose(ReadSurface.Mcp, route, body.ToJsonString(), cache.Generation, cache, ttl), ttl,
            async _ =>
            {
                await gate.Task;
                return timedOut;
            },
            _ => null);
        Assert.True(lead.IsLead);

        Task<ComposeRunOutcome>[] joined;
        try
        {
            joined =
            [
                DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, TestContext.Current.CancellationToken, recorder,
                    remapClientTimeout: true, cacheHost: ReadSurface.Mcp),
                DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, TestContext.Current.CancellationToken, recorder,
                    remapClientTimeout: true, cacheHost: ReadSurface.Mcp),
            ];
            await WaitUntil(() => cache.WaitingCallers == 3, "the two callers never joined the composed run");
        }
        finally
        {
            gate.TrySetResult();
        }

        var outcomes = await Task.WhenAll(joined).WaitAsync(TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken);
        Assert.All(outcomes, o => Assert.Equal("57014", o.AuthorSqlState));
        var samples = readLatency.Drain().Where(d => d.Surface == ReadSurface.Compose).ToList();
        Assert.Equal(0, samples.Where(d => d.Outcome == ReadOutcome.Timeout).Sum(d => d.Count));
        Assert.Equal(2, samples.Where(d => d.Outcome == ReadOutcome.CacheHit).Sum(d => d.Count));
        Assert.All(samples, d => Assert.Equal(ReadOutcome.CacheHit, d.Outcome));
    }

    /// <summary>L5: a malformed integer knob is refused at once, before the cache and admission, even when the route is
    /// heavy and every slot is taken.</summary>
    [Fact(Timeout = 60_000)]
    public async Task ANonIntegerWindow_OnAHeavyRouteWithNoFreeSlot_Answers400AtOnce()
    {
        const string route = "get_test_refused_read";
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        var admission = HeavyAdmission(1, ReadSurface.Web, route, band, TimeProvider.System);

        var calls = 0;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((route, (_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult("{\"rows\":[]}");
        }));
        var (app, postgres) = await BuildServer(admission, new ReadLatencyAccumulator());
        await using var _app = app;
        await using var _postgres = postgres;

        /* The slot is taken in another flow, so this test's own flow holds no admission marker. */
        using var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, route, band, CancellationToken.None), TestContext.Current.CancellationToken);
        Assert.Equal(1, admission.Stats().Running);

        using var client = app.GetTestServer().CreateClient();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        using var response = await client.GetAsync("/api/read/" + route + "?hours=abc", deadline.Token);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, calls);
        Assert.Equal(0, admission.Stats().Queued);
    }

    /// <summary>C1: a composed panel answered from the cache is the stored text itself, on the host that stored it; the
    /// other host's entry is separate (D4). The web route writes the stored bytes unchanged.</summary>
    [Fact(Timeout = 60_000)]
    public async Task AComposedPanel_IsAnsweredFromItsOwnHostsEntry_ByteForByte()
    {
        const string route = "compose:unknown";
        var body = (JsonObject)JsonNode.Parse("{\"panel\":{}}")!;
        var band = "24h";
        var admission = HeavyAdmission(2, ReadSurface.Compose, route, band);
        var readLatency = new ReadLatencyAccumulator();
        var recorder = new ReadLatencyRecorder(readLatency, null, null, admission);
        var cache = admission.Cache;
        var ttl = ReadResultCache.TtlOf(band);
        const string mcpText = "{\"sql\":\"select 1\",\"rows\":[{\"n\":1}],\"annotations\":[]}";
        const string webText = "{\"sql\":\"select 1\",\"rows\":[{\"n\":1}],\"annotations\":[],\"web\":true}";
        Assert.True(cache.TryStore(ReadCacheKey.ForCompose(ReadSurface.Mcp, route, body.ToJsonString(), cache.Generation, cache, ttl), mcpText, ttl));

        var (app, postgres) = await BuildServer(admission, readLatency);
        await using var _app = app;
        await using var _postgres = postgres;

        var mcp = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, TestContext.Current.CancellationToken, recorder,
            remapClientTimeout: true, cacheHost: ReadSurface.Mcp);
        Assert.Same(mcpText, mcp.PayloadText);
        Assert.NotNull(mcp.Payload);
        var hit = Assert.Single(readLatency.Drain(), d => d.Surface == ReadSurface.Compose);
        Assert.Equal(ReadOutcome.CacheHit, hit.Outcome);

        /* The web host's entry is its own: stored under the web key, the route answers exactly those bytes. */
        Assert.True(cache.TryStore(ReadCacheKey.ForCompose(ReadSurface.Web, route, body.ToJsonString(), cache.Generation, cache, ttl), webText, ttl));
        using var client = app.GetTestServer().CreateClient();
        using var response = await client.PostAsync("/api/compose/run", new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json"),
            TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(webText, await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
    }
}

/// <summary>#5506 part 3 (fix round 1, L4, L7): admission through the web route wrapper and the scope's in-flight count.</summary>
public sealed class ReadAdmissionWrapperTests
{
    /// <summary>L4: the dedicated web routes admit through <see cref="DarlingWebEndpoints.AdmitWebRouteAsync"/>; a read nested
    /// under one passes at once at a limit of 1, because the wrapper leaves the admission marker in the caller's flow.</summary>
    [Fact(Timeout = 60_000)]
    public async Task AReadNestedUnderAnAdmittedWebRoute_PassesAtOnce_AtALimitOfOne()
    {
        var admission = new ReadAdmission(new TimerTimeProvider(), ReadAdmission.DefaultWaitCap, null, 1);
        admission.Cache.Enabled = false;
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long>
        {
            [(ReadSurface.Web, "get_fleet_overview", ReadWindowBand.None)] = 5_000,
            [(ReadSurface.Web, "get_wait_stats", ReadWindowBand.None)] = 5_000,
        });

        await Task.Run(async () =>
        {
            using var outer = await DarlingWebEndpoints.AdmitWebRouteAsync(admission, "get_fleet_overview", ReadWindowBand.None, new DefaultHttpContext());
            Assert.True(outer.MayRun);
            Assert.Equal(1, admission.Stats().Running);

            var inner = admission.AdmitAsync(ReadSurface.Web, "get_wait_stats", ReadWindowBand.None, CancellationToken.None);
            Assert.True(inner.IsCompleted, "a read nested under an admitted web route queued for a second slot");
            using var innerTicket = await inner;
            Assert.Equal(1, admission.Stats().Running);
        }, TestContext.Current.CancellationToken);
    }

    /// <summary>L7: a read waiting in the admission queue is not counted as running, so <c>max_in_flight</c> counts reads
    /// running; it counts again once the slot is granted.</summary>
    [Fact(Timeout = 60_000)]
    public async Task AQueuedRead_IsNotCountedInFlight_UntilItsSlotIsGranted()
    {
        const string route = "get_wait_stats";
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 1);
        admission.Cache.Enabled = false;
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long> { [(ReadSurface.Web, route, ReadWindowBand.None)] = 5_000 });
        var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, route, ReadWindowBand.None, CancellationToken.None), TestContext.Current.CancellationToken);

        await Task.Run(async () =>
        {
            using var opened = ReadScope.Open(null);
            Assert.True(opened.Scope.IsCounted);
            var admitting = ReadScope.AdmitAsync(admission, ReadSurface.Web, route, ReadWindowBand.None, CancellationToken.None);
            Assert.False(admitting.IsCompleted);
            Assert.False(opened.Scope.IsCounted);

            held.Dispose();
            using var ticket = await admitting.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.True(ticket.MayRun);
            Assert.True(opened.Scope.IsCounted);
        }, TestContext.Current.CancellationToken);
    }
}
