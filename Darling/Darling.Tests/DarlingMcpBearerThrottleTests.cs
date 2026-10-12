/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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

    private static IPAddress OtherSourceFor(bool networkMode) => networkMode ? OtherInCidrRemote : IPAddress.IPv6Loopback;

    private sealed class Clock
    {
        public DateTime Now = new(2026, 9, 29, 6, 0, 0, DateTimeKind.Utc);

        public DateTime Read() => Now;
    }

    private readonly record struct Reply(int Status, string? RetryAfter, string? WwwAuthenticate);

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
        await reader.ReadToEndAsync();

        return new Reply(
            ctx.Response.StatusCode,
            ctx.Response.Headers.RetryAfter.Count == 0 ? null : ctx.Response.Headers.RetryAfter.ToString(),
            ctx.Response.Headers.WWWAuthenticate.Count == 0 ? null : ctx.Response.Headers.WWWAuthenticate.ToString());
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

        var first = await ToolsListAsync(server, "/", host, source, null);
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
    public async Task ARightToken_ClearsTheSourcesFailures(bool networkMode)
    {
        var clock = new Clock();
        using var server = await BuildServer(networkMode, requireToken: !networkMode, clock);
        var host = HostFor(networkMode);
        var source = SourceFor(networkMode);

        /* Two batches of the free failures with a success between: had the success not cleared the first
           batch, the second batch would reach the wait and its later attempts would be 429. */
        for (var batch = 0; batch < 2; batch++)
        {
            for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
            {
                var refused = await ToolsListAsync(server, "/", host, source, "Bearer wrong-" + batch + "-" + i);
                Assert.Equal(StatusCodes.Status401Unauthorized, refused.Status);
            }

            var right = await ToolsListAsync(server, "/", host, source, "Bearer " + Token);
            Assert.Equal(StatusCodes.Status200OK, right.Status);
        }
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

    /* ------------------------ the token floor at start, by source ------------------------ */

    private static string StartPathBody(out string code)
    {
        code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");
        var start = code.IndexOf("private async Task<bool> TryStartServerAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0, "TryStartServerAsync is gone, so this pin proves nothing");

        var end = code.IndexOf("internal static void ConfigureMcpServices(", start, StringComparison.Ordinal);
        Assert.True(end > start, "ConfigureMcpServices no longer follows TryStartServerAsync, so the slice is wrong");
        return code[start..end];
    }

    [Fact]
    public void TheStartPath_RoutesEveryResolvedToken_ThroughTheFloor_BeforeItGatesAnything()
    {
        var body = StartPathBody(out _);

        /* The network bind resolves the MCP token here; the loopback bind resolves its own through
           ResolveLoopbackOnlyToken, and the write tools check that one against the floor separately (see the next
           test). A third resolution would need its own floor, so this fails until it is added on purpose. */
        var resolutions = Regex.Matches(body, @"\.ResolveToken\(out var usedPlaintext\)");
        Assert.Single(resolutions);

        foreach (Match resolution in resolutions)
        {
            var tail = body[resolution.Index..];

            /* A token under the floor turns the network bind off and keeps the token for the loopback gate; a token
               that clears it is adopted for the network bind. */
            Assert.Matches(
                new Regex(
                    @"DarlingHostBinding\.DescribeTokenProblem\(token\);\s*if \(tokenProblem is not null\)\s*\{.*?networkMode = false;\s*bearerToken = token!?;\s*\}\s*else\s*\{\s*bearerToken = token!?;",
                    RegexOptions.Singleline),
                tail.Length > 2500 ? tail[..2500] : tail);
        }
    }

    [Fact]
    public void ANetworkTokenUnderTheFloor_FailsClosedToLoopbackOnly_NamingTheSetting()
    {
        var body = StartPathBody(out _);

        var floor = body.IndexOf("DarlingHostBinding.DescribeTokenProblem(token);", StringComparison.Ordinal);
        Assert.True(floor >= 0, "the network bind no longer routes its token through the floor");

        var block = body.Substring(floor, 700);
        Assert.Contains("_logger.LogCritical(", block, StringComparison.Ordinal);
        Assert.Contains("mcp.network.token / mcp.network.encryptedToken", block, StringComparison.Ordinal);
        Assert.Contains("binding loopback-only", block, StringComparison.Ordinal);
        Assert.Contains("networkMode = false;", block, StringComparison.Ordinal);
    }

    [Fact]
    public void AWriteToolTokenUnderTheFloor_KeepsTheWriteToolsOff_NamingTheSetting_AndOnlyWhenWritesAreRequested()
    {
        var body = StartPathBody(out _);

        var writesRequested = body.IndexOf("if (config.Mcp.AllowWrites)", StringComparison.Ordinal);
        Assert.True(writesRequested >= 0, "the write-tool token path is gone, so this pin proves nothing");

        /* Inside the writes-requested branch, so a short token with the write tools off is never looked at:
           loopback with writes off stays the tokenless bind it was. */
        var floor = body.IndexOf("DarlingHostBinding.DescribeTokenProblem(bearerToken)", writesRequested, StringComparison.Ordinal);
        Assert.True(floor > writesRequested, "the floor must sit inside the writes-requested branch");

        var block = body.Substring(floor, 700);
        Assert.Contains("_logger.LogCritical(", block, StringComparison.Ordinal);
        Assert.Contains("mcp.network.token / mcp.network.encryptedToken", block, StringComparison.Ordinal);
        Assert.Contains("the MCP write tools stay off", block, StringComparison.Ordinal);
        Assert.DoesNotContain("networkMode = false;", block, StringComparison.Ordinal);

        /* No token adopted means tokenPresent is false, and the policy serves no write. */
        Assert.Contains("DarlingMcpWritePolicy.Resolve(config.Mcp, tokenPresent: bearerToken.Length > 0 && writeTokenProblem is null)", body, StringComparison.Ordinal);
    }
}
