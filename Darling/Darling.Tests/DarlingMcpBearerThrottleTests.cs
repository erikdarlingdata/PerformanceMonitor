/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The MCP bearer gate's per-source wait, and the token floor at start.
///
/// <para>The gate installs in network mode and, since the write switch, on the loopback bind whenever the write
/// tools are served. Before this, a wrong token cost nothing there, so a token could be tried at line rate by
/// anything that could reach the port (a local process, for the loopback bind), and a token typed by hand could
/// be one word. The wait is <see cref="DarlingSignInThrottle"/>, the same policy the web dashboard's sign-in
/// uses; the floor is <see cref="DarlingHostBinding.DescribeTokenProblem"/>.</para>
///
/// <para>The wait is driven through the real <see cref="DarlingMcpHostService.ConfigurePipeline"/> on a
/// <see cref="TestServer"/> with a settable clock, so no case sleeps and none depends on how fast the test
/// host runs. The start path itself needs a store and a port, so the two places it resolves a token are pinned
/// by source, the same way the web host's are.</para>
/// </summary>
public sealed class DarlingMcpBearerThrottleTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string Token = "correct-mcp-token-value";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");
    private static readonly IPAddress OtherInCidrRemote = IPAddress.Parse("192.168.1.51");

    /// <summary>The two places the gate installs: network mode, and the loopback bind with writes served.</summary>
    private static string HostFor(bool networkMode) => networkMode ? ListenIp : "localhost";

    private static IPAddress SourceFor(bool networkMode) => networkMode ? InCidrRemote : IPAddress.Loopback;

    /* Every loopback address is ONE source, so "another source" on the loopback bind is a non-loopback one. */
    private static IPAddress OtherSourceFor(bool networkMode) => networkMode ? OtherInCidrRemote : IPAddress.Parse("203.0.113.7");

    /// <summary>The i-th source that spends part of the total budget.</summary>
    private static IPAddress SpenderFor(bool networkMode, int i) =>
        IPAddress.Parse((networkMode ? "192.168.1." : "203.0.113.") + (100 + i).ToString(CultureInfo.InvariantCulture));

    private sealed class Clock
    {
        public DateTime Now = new(2026, 9, 29, 6, 0, 0, DateTimeKind.Utc);

        public DateTime Read() => Now;
    }

    private readonly record struct Reply(int Status, string? RetryAfter, string? WwwAuthenticate, string Body = "");

    /// <summary>The REAL <c>ConfigureMcpServices</c> and <c>ConfigurePipeline</c>, the two calls the start path
    /// makes, with a data source no case opens (every request stops at a gate or at tools/list).</summary>
    private static async Task<TestServer> BuildServer(bool networkMode, bool requireToken, Clock clock)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        builder.Services.AddSingleton(new PerformanceMonitor.Darling.Analysis.DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: new PerformanceMonitor.Darling.Analysis.BaselineCache()));
        builder.Services.AddSingleton<ILogger>(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(builder.Services, DarlingPeerDirectory.Snapshot.Empty);

        var app = builder.Build();

        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());

        host.ConfigurePipeline(
            app,
            networkMode: networkMode,
            networkListenIp: networkMode ? IPAddress.Parse(ListenIp) : null,
            allowedCidr: IPNetwork.Parse(AllowedCidr),
            bearerToken: Token,
            requireTokenWhenLoopbackOnly: requireToken,
            utcNow: clock.Read);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<Reply> ToolsListAsync(
        TestServer server, string path, string host, IPAddress remote, string? authorization)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = path;
            c.Request.Headers.Host = host;
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = remote;
            if (authorization is not null)
            {
                c.Request.Headers.Authorization = authorization;
            }
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();

        return new Reply(
            ctx.Response.StatusCode,
            ctx.Response.Headers.RetryAfter.Count == 0 ? null : ctx.Response.Headers.RetryAfter.ToString(),
            ctx.Response.Headers.WWWAuthenticate.Count == 0 ? null : ctx.Response.Headers.WWWAuthenticate.ToString(),
            body);
    }

    /// <summary>Spends the whole total budget from other sources: fifteen of them, each using its free misses
    /// and the one that starts its wait.</summary>
    private static async Task SpendTheTotalBudget(TestServer server, string host, bool networkMode)
    {
        var sources = DarlingSignInThrottle.GlobalMaxAttempts / (DarlingSignInThrottle.FreeFailures + 1);
        for (var k = 0; k < sources; k++)
        {
            for (var j = 0; j <= DarlingSignInThrottle.FreeFailures; j++)
            {
                var refused = await ToolsListAsync(server, "/", host, SpenderFor(networkMode, k), "Bearer wrong-" + k + "-" + j);
                Assert.Equal(StatusCodes.Status401Unauthorized, refused.Status);
            }
        }
    }

    private static string Seconds(TimeSpan span) =>
        ((int)Math.Ceiling(span.TotalSeconds)).ToString(CultureInfo.InvariantCulture);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RepeatedWrongTokens_FromOneSource_AreHeldWithRetryAfter_AndTheRightTokenIsHeldToo(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);
        var host = HostFor(networkMode);
        var source = SourceFor(networkMode);

        /* The free failures cost no wait: each is compared and answered 401, with no Retry-After. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            var refused = await ToolsListAsync(server, "/", host, source, "Bearer wrong-" + i);
            Assert.Equal(StatusCodes.Status401Unauthorized, refused.Status);
            Assert.Equal("Bearer", refused.WwwAuthenticate);
            Assert.Null(refused.RetryAfter);
        }

        /* One more failure starts the wait. It was itself compared, so it is still a 401... */
        var last = await ToolsListAsync(server, "/", host, source, "Bearer wrong-again");
        Assert.Equal(StatusCodes.Status401Unauthorized, last.Status);

        /* ...and inside the wait even the RIGHT token is held: 429 with Retry-After, never compared. */
        var held = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, held.Status);
        Assert.Equal(Seconds(DarlingSignInThrottle.BaseDelay), held.RetryAfter);

        /* The hold follows the source, not the path: /core is behind the same gate. */
        var heldCore = await ToolsListAsync(server, "/core", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, heldCore.Status);

        /* Another source is not held by the first one's failures. */
        var other = await ToolsListAsync(server, "/", host, OtherSourceFor(networkMode), "Bearer " + Token);
        Assert.Equal(StatusCodes.Status200OK, other.Status);

        /* Once the wait has passed the source may try again, and a right token is served. */
        clock.Now += DarlingSignInThrottle.BaseDelay;
        var after = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status200OK, after.Status);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EachFailureAfterAWait_DoublesTheNextOne(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);
        var host = HostFor(networkMode);
        var source = SourceFor(networkMode);

        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            await ToolsListAsync(server, "/", host, source, "Bearer wrong-" + i);
        }

        var first = await ToolsListAsync(server, "/", host, source, "Bearer held-probe");
        Assert.Equal(StatusCodes.Status429TooManyRequests, first.Status);
        Assert.Equal(Seconds(DarlingSignInThrottle.BaseDelay), first.RetryAfter);

        /* The wait ends, the source fails again, and the wait it earns is twice as long. */
        clock.Now += DarlingSignInThrottle.BaseDelay;
        var again = await ToolsListAsync(server, "/", host, source, "Bearer wrong-after-wait");
        Assert.Equal(StatusCodes.Status401Unauthorized, again.Status);

        var second = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, second.Status);
        Assert.Equal(Seconds(DarlingSignInThrottle.BaseDelay * 2), second.RetryAfter);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RequestsThatPresentNoBearerToken_AreRefusedButNeverCountAgainstTheSource(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);
        var host = HostFor(networkMode);
        var source = SourceFor(networkMode);

        /* A client that was never given the token, or sends the wrong scheme, compared nothing against it.
           If those counted, one such client would hold every other client on its address. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures + 6; i++)
        {
            var none = await ToolsListAsync(server, "/", host, source, null);
            Assert.Equal(StatusCodes.Status401Unauthorized, none.Status);

            var basic = await ToolsListAsync(server, "/", host, source, "Basic " + Token);
            Assert.Equal(StatusCodes.Status401Unauthorized, basic.Status);

            var empty = await ToolsListAsync(server, "/", host, source, "Bearer ");
            Assert.Equal(StatusCodes.Status401Unauthorized, empty.Status);
        }

        var right = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status200OK, right.Status);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ARightToken_GivesBackOnlyItsOwnAttempt_NeverTheCountOfOtherClientsOnTheSource(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);
        var host = HostFor(networkMode);
        var source = SourceFor(networkMode);

        /* Another client on this address has missed the free number of times. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            var refused = await ToolsListAsync(server, "/", host, source, "Bearer wrong-" + i);
            Assert.Equal(StatusCodes.Status401Unauthorized, refused.Status);
        }

        /* The operator presents the right token: served, and that one attempt is given back. */
        var right = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status200OK, right.Status);

        /* The other client's count survived: its next miss is the first one past the free number and starts
           the wait. Had the success cleared the source, this miss would be the first of a fresh count and the
           next request would still be served. */
        var next = await ToolsListAsync(server, "/", host, source, "Bearer wrong-next");
        Assert.Equal(StatusCodes.Status401Unauthorized, next.Status);
        var held = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, held.Status);
        Assert.Equal(Seconds(DarlingSignInThrottle.BaseDelay), held.RetryAfter);
    }

    [Theory]
    [InlineData(true, 64)]
    [InlineData(false, 64)]
    [InlineData(true, 16)]
    [InlineData(false, 16)]
    public async Task ManyConcurrentWrongTokens_FromOneSource_CompareExactlyTheAllowance_AndTheRestAreHeld(bool networkMode, int burst)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);
        var host = HostFor(networkMode);
        var source = SourceFor(networkMode);

        /* The free number of misses plus the one that starts the wait: the most one source gets compared in
           a window. Requests released together must not compare more than that. */
        var gate = new System.Threading.ManualResetEventSlim(false);
        var tasks = new Task<Reply>[burst];
        for (var i = 0; i < burst; i++)
        {
            var guess = "Bearer burst-" + i;
            tasks[i] = Task.Run(async () =>
            {
                gate.Wait();
                return await ToolsListAsync(server, "/", host, source, guess);
            });
        }

        gate.Set();
        var replies = await Task.WhenAll(tasks);

        var compared = replies.Count(r => r.Status == StatusCodes.Status401Unauthorized);
        var held = replies.Where(r => r.Status == StatusCodes.Status429TooManyRequests).ToList();
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, compared);
        Assert.Equal(burst - compared, held.Count);
        Assert.All(held, r => Assert.True(int.Parse(r.RetryAfter!, CultureInfo.InvariantCulture) >= 1));

        /* And the right token is held with the rest while the wait lasts. */
        var right = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, right.Status);
    }

    /// <summary>Releases one request per entry together and returns each reply with whether its token was the right one.</summary>
    private static async Task<(Reply Reply, bool Right)[]> Release(
        TestServer server, string host, IPAddress source, IReadOnlyList<bool> rightTokens)
    {
        var gate = new System.Threading.ManualResetEventSlim(false);
        var tasks = new Task<(Reply, bool)>[rightTokens.Count];
        for (var i = 0; i < rightTokens.Count; i++)
        {
            var right = rightTokens[i];
            var authorization = "Bearer " + (right ? Token : "wrong-" + i);
            tasks[i] = Task.Run(async () =>
            {
                gate.Wait();
                return (await ToolsListAsync(server, "/", host, source, authorization), right);
            });
        }

        gate.Set();
        return await Task.WhenAll(tasks);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SixteenConcurrentRightTokens_AreAllServed(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);

        /* The reservations of requests still in flight count against the free allowance; a request that only
           meets those waits for them instead of being answered 429. */
        for (var round = 0; round < 5; round++)
        {
            var replies = await Release(server, HostFor(networkMode), SourceFor(networkMode), Enumerable.Repeat(true, 16).ToList());
            Assert.All(replies, r => Assert.Equal(StatusCodes.Status200OK, r.Reply.Status));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConcurrentRightTokens_BesideAFewWrongOnes_AreAllServed_AndTheWrongOnesAreCompared(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);

        /* Two wrong tokens are inside the free number, so no wait starts whatever the order. */
        var mix = Enumerable.Repeat(true, 14).Concat(Enumerable.Repeat(false, 2)).ToList();
        var replies = await Release(server, HostFor(networkMode), SourceFor(networkMode), mix);

        Assert.All(replies.Where(r => r.Right), r => Assert.Equal(StatusCodes.Status200OK, r.Reply.Status));
        Assert.All(replies.Where(r => !r.Right), r => Assert.Equal(StatusCodes.Status401Unauthorized, r.Reply.Status));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConcurrentRightAndWrongTokens_CompareNoMoreWrongTokensThanTheAllowance(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);

        var mix = Enumerable.Repeat(true, 8).Concat(Enumerable.Repeat(false, 8)).ToList();
        var replies = await Release(server, HostFor(networkMode), SourceFor(networkMode), mix);

        /* The wait starts with the fourth wrong token, so exactly that many were compared; a right token is
           served only if it came before the wait, and held (never compared) after it. */
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, replies.Count(r => r.Reply.Status == StatusCodes.Status401Unauthorized));
        Assert.All(replies.Where(r => r.Reply.Status == StatusCodes.Status401Unauthorized), r => Assert.False(r.Right));
        Assert.All(replies.Where(r => r.Reply.Status == StatusCodes.Status200OK), r => Assert.True(r.Right));
        Assert.All(replies.Where(r => r.Reply.Status == StatusCodes.Status429TooManyRequests),
            r => Assert.True(int.Parse(r.Reply.RetryAfter!, CultureInfo.InvariantCulture) >= 1));
        Assert.Equal(replies.Length, replies.Count(r => r.Reply.Status is StatusCodes.Status401Unauthorized
            or StatusCodes.Status200OK or StatusCodes.Status429TooManyRequests));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OtherSourcesSpendingTheTotalBudget_DoNotStopASourceThatPresentedTheRightToken(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);
        var host = HostFor(networkMode);
        var worker = SourceFor(networkMode);

        /* A client that works: its right token is served. */
        Assert.Equal(StatusCodes.Status200OK, (await ToolsListAsync(server, "/", host, worker, "Bearer " + Token)).Status);

        await SpendTheTotalBudget(server, host, networkMode);

        /* A source that has not presented the right token is held by the total, and the answer says so. */
        var stranger = await ToolsListAsync(server, "/", host, OtherSourceFor(networkMode), "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, stranger.Status);
        Assert.Contains("not necessarily this one", stranger.Body, StringComparison.Ordinal);
        Assert.Contains("restarting the service clears it", stranger.Body, StringComparison.Ordinal);
        Assert.Contains("No token is compared", stranger.Body, StringComparison.Ordinal);
        Assert.DoesNotContain("from this address", stranger.Body, StringComparison.Ordinal);
        Assert.True(int.Parse(stranger.RetryAfter!, CultureInfo.InvariantCulture) > (int)DarlingSignInThrottle.MaxDelay.TotalSeconds);

        /* The working client keeps being served, request after request, while the total is spent. */
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(StatusCodes.Status200OK, (await ToolsListAsync(server, "/", host, worker, "Bearer " + Token)).Status);
        }

        /* Its own wait is not lifted: wrong tokens from that source still start it, and then even the right
           token waits, with the per-source answer. */
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            Assert.Equal(StatusCodes.Status401Unauthorized, (await ToolsListAsync(server, "/", host, worker, "Bearer wrong-own-" + i)).Status);
        }

        var held = await ToolsListAsync(server, "/", host, worker, "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, held.Status);
        Assert.Contains("from this address", held.Body, StringComparison.Ordinal);
        Assert.Equal(Seconds(DarlingSignInThrottle.BaseDelay), held.RetryAfter);
    }

    [Fact]
    public async Task EveryLoopbackAddress_ShareOneWait_OnTheLoopbackBind()
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode: false, requireToken: true, clock);

        /* Four wrong tokens from four loopback spellings are one source's four. */
        var spellings = new[] { "127.0.0.1", "127.0.0.2", "::1", "::ffff:127.0.0.3" };
        foreach (var spelling in spellings)
        {
            var refused = await ToolsListAsync(server, "/", "localhost", IPAddress.Parse(spelling), "Bearer wrong-" + spelling);
            Assert.Equal(StatusCodes.Status401Unauthorized, refused.Status);
        }

        foreach (var spelling in new[] { "127.0.0.1", "127.9.9.9", "::1" })
        {
            var held = await ToolsListAsync(server, "/", "localhost", IPAddress.Parse(spelling), "Bearer " + Token);
            Assert.Equal(StatusCodes.Status429TooManyRequests, held.Status);
        }

        /* A source that is not loopback is its own. */
        var other = await ToolsListAsync(server, "/", "localhost", OtherSourceFor(networkMode: false), "Bearer " + Token);
        Assert.Equal(StatusCodes.Status200OK, other.Status);
    }

    [Fact]
    public async Task Ipv6Addresses_InOneSlash64_ShareTheWait_AndAnotherSlash64DoesNot()
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode: false, requireToken: true, clock);

        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            /* Each miss comes from a different address in the same /64. */
            var refused = await ToolsListAsync(
                server, "/", "localhost", IPAddress.Parse("2001:db8:1:2::" + (i + 1).ToString("x", CultureInfo.InvariantCulture)), "Bearer wrong-" + i);
            Assert.Equal(StatusCodes.Status401Unauthorized, refused.Status);
        }

        var sameBlock = await ToolsListAsync(server, "/", "localhost", IPAddress.Parse("2001:db8:1:2:aaaa:bbbb:cccc:dddd"), "Bearer " + Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, sameBlock.Status);

        var otherBlock = await ToolsListAsync(server, "/", "localhost", IPAddress.Parse("2001:db8:1:3::1"), "Bearer " + Token);
        Assert.Equal(StatusCodes.Status200OK, otherBlock.Status);
    }

    [Fact]
    public async Task TheHeldAnswer_SaysNoTokenIsComparedAndThatARestartClearsTheWait()
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode: false, requireToken: true, clock);

        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            await ToolsListAsync(server, "/", "localhost", IPAddress.Loopback, "Bearer wrong-" + i);
        }

        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Authorization = "Bearer " + Token;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });
        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();

        Assert.Equal(StatusCodes.Status429TooManyRequests, ctx.Response.StatusCode);
        Assert.Contains("No token is compared during the wait", body, StringComparison.Ordinal);
        Assert.Contains("restarting the service clears it", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LoopbackWithWritesOff_HasNoTokenGate_SoNothingIsRefusedOrHeld()
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode: false, requireToken: false, clock);

        /* The tokenless local bind, byte for byte what it was: a stream of wrong or absent credentials is
           never a 401 and never a 429. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures * 4; i++)
        {
            var reply = await ToolsListAsync(
                server, i % 2 == 0 ? "/" : "/core", "localhost", IPAddress.Loopback, i % 3 == 0 ? null : "Bearer wrong-" + i);
            Assert.Equal(StatusCodes.Status200OK, reply.Status);
            Assert.Null(reply.RetryAfter);
        }
    }

    /* ------------------------ the token floor at start, by behavior ------------------------ */

    [Theory]
    [InlineData("hunter2")]
    [InlineData("aaaaaaaaaaaaaaa")]
    public void ANetworkTokenUnderTheFloor_TurnsTheNetworkBindOff_ButStillGatesTheLoopbackListener(string token)
    {
        /* The decision the start path applies to the token it resolved (ApplyTokenFloor): the bind drops to loopback-only. */
        var floor = DarlingHostBinding.ApplyTokenFloor(token, networkMode: true);
        Assert.False(floor.NetworkMode);
        Assert.NotNull(floor.Problem);

        /* The token is kept as the bearer token, so the loopback listener the start falls back to requires it. */
        Assert.True(DarlingMcpHostService.RequireTokenWhenLoopbackOnly(floor.NetworkMode, token));
    }

    [Fact]
    public void ANetworkTokenAtTheFloor_KeepsTheNetworkBind()
    {
        var floor = DarlingHostBinding.ApplyTokenFloor("abcdefghijklmnopqrstuvwxyz012345"[..DarlingHostBinding.MinimumTokenLength], networkMode: true);

        Assert.True(floor.NetworkMode);
        Assert.Null(floor.Problem);
    }

    [Fact]
    public void ALoopbackOnlyStart_IsNeverMovedToTheNetworkByTheFloor()
    {
        Assert.False(DarlingHostBinding.ApplyTokenFloor("abcdefghijklmnopqrstuvwxyz012345", networkMode: false).NetworkMode);
    }

    [Fact]
    public void TheTokenProblemText_NamesTheFloor_NotTheLengthOrTheValue()
    {
        var problem = DarlingHostBinding.DescribeTokenProblem("hunter2");

        Assert.NotNull(problem);
        Assert.Contains("shorter than " + DarlingHostBinding.MinimumTokenLength, problem, StringComparison.Ordinal);
        Assert.DoesNotContain("7", problem, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", problem, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("", false, null)]
    [InlineData("short", true, "shorter")]
    [InlineData("0123456789abcdef", true, null)]
    public void WriteToolsFollowTheToken_AndOnlyWhenWritesAreRequested(string token, bool expectToken, string? expectProblem)
    {
        var on = new PerformanceMonitor.Darling.Service.McpConfig { AllowWrites = true };
        var off = new PerformanceMonitor.Darling.Service.McpConfig();

        var requested = DarlingMcpWritePolicy.ResolveForStart(on, token, out var problem);
        Assert.Equal(expectToken && expectProblem is null, requested.Writes);
        if (expectProblem is null)
        {
            Assert.Null(problem);
        }
        else
        {
            Assert.Contains(expectProblem, problem, StringComparison.Ordinal);
        }

        /* Writes not requested: the token is not looked at, so a short one raises no problem. */
        var notRequested = DarlingMcpWritePolicy.ResolveForStart(off, token, out var quiet);
        Assert.False(notRequested.Writes);
        Assert.Null(quiet);
    }

    [Theory]
    [InlineData(false, "tok", true)]
    [InlineData(false, "", false)]
    [InlineData(true, "tok", false)]
    public void TheLoopbackGate_IsRequiredExactlyWhenLoopbackOnlyWithAToken(bool network, string token, bool expected)
    {
        Assert.Equal(expected, DarlingMcpHostService.RequireTokenWhenLoopbackOnly(network, token));
    }
}
