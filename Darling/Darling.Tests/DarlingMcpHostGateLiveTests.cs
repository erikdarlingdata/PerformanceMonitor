/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4128 part (b): a live HTTP proof of the MCP host's gates, INCLUDING <c>/core</c>, through the SAME
/// <see cref="DarlingMcpHostService.ConfigurePipeline"/> and <see cref="DarlingMcpHostService.ConfigureMcpServices"/>
/// methods production calls (extracted from <c>TryStartServerAsync</c> for exactly this purpose), driven by a
/// real Kestrel pipeline over <see cref="TestServer"/> rather than by reading source text or calling the pure
/// decision functions directly. <see cref="HostHeaderGuardTests"/> already proves the WIRING ORDER by parsing
/// the source; this class proves the wired pipeline actually REFUSES and ADMITS requests the way the source
/// claims it does, end to end — untrusted Host headers, missing/wrong/right bearer tokens, and (the part
/// unique to this host) that <c>/core</c>'s <c>tools/list</c> is narrowed to the pinned set while a
/// <c>tools/call</c> outside it gets the SDK's own unknown-tool refusal.
///
/// <para>The gates read <c>context.Connection.RemoteIpAddress</c>, so requests go through
/// <see cref="TestServer.SendAsync"/> where that is needed (the Host-header cases); the MCP conversation
/// itself (initialize / tools/list / tools/call) is spoken as plain JSON-RPC HTTP POSTs over
/// <see cref="TestServer.CreateClient"/>, since the SDK's transport reads the body, not the connection.</para>
/// </summary>
public sealed class DarlingMcpHostGateLiveTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string Token = "correct-mcp-token-value";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");

    /// <summary>
    /// Builds a <see cref="TestServer"/> running the REAL <see cref="DarlingMcpHostService.ConfigureMcpServices"/>
    /// and <see cref="DarlingMcpHostService.ConfigurePipeline"/> — the exact methods the production
    /// <c>TryStartServerAsync</c> calls, in the same order, with untrusted-field-content-safe test doubles for
    /// the singletons the tool classes require (a data source the gates never open, since none of them touch
    /// Postgres — a request is refused or reaches tools/list before any tool body runs).
    /// </summary>
    private static async Task<TestServer> BuildServer(bool networkMode)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        // A pool the gates never open — every case here is refused, or reaches tools/list, before any tool body runs.
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        builder.Services.AddSingleton(new PerformanceMonitor.Darling.Analysis.DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: new PerformanceMonitor.Darling.Analysis.BaselineCache()));
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(builder.Services, PerformanceMonitor.Darling.Service.Mcp.DarlingPeerDirectory.Snapshot.Empty);

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
            bearerToken: Token);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static Task<HttpContext> SendRaw(TestServer server, string path, string host, IPAddress? remote, string? bearer = null)
    {
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = path;
            ctx.Request.Headers.Host = host;
            ctx.Connection.RemoteIpAddress = remote;
            if (bearer is not null)
            {
                ctx.Request.Headers.Authorization = $"Bearer {bearer}";
            }
        });
    }

    /// <summary>
    /// A JSON-RPC POST over stateless HTTP — no session id round-trip needed (see the <c>Stateless = true</c>
    /// transport option). Built on <see cref="TestServer.SendAsync"/>, not <c>CreateClient</c>: the CIDR
    /// gate reads <c>context.Connection.RemoteIpAddress</c>, which <c>CreateClient</c>'s in-memory context
    /// leaves null — a null remote fails the CIDR check closed by design, so every case here must set a
    /// remote address explicitly, including the loopback-exempt ones. The Accept header carries both media
    /// types the SDK's streamable-HTTP transport requires.
    /// </summary>
    private static Task<(int StatusCode, string Body)> SendJsonRpcAsync(
        TestServer server, string path, string host, IPAddress remote, string method, string paramsJson, string? bearer)
    {
        var requestBody = $"{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"{method}\",\"params\":{paramsJson}}}";
        return SendJsonRpcCoreAsync(server, path, host, remote, requestBody, bearer);
    }

    private static async Task<(int StatusCode, string Body)> SendJsonRpcCoreAsync(
        TestServer server, string path, string host, IPAddress remote, string requestBody, string? bearer)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = path;
            c.Request.Headers.Host = host;
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(requestBody);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = remote;
            if (bearer is not null)
            {
                c.Request.Headers.Authorization = $"Bearer {bearer}";
            }

            c.Response.Body = new System.IO.MemoryStream();
        });

        ctx.Response.Body.Seek(0, System.IO.SeekOrigin.Begin);
        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        return (ctx.Response.StatusCode, body);
    }

    private static Task<(int StatusCode, string Body)> ToolsListAsync(TestServer server, string path, string host, IPAddress remote, string? bearer = null)
        => SendJsonRpcAsync(server, path, host, remote, "tools/list", "{}", bearer);

    private static Task<(int StatusCode, string Body)> ToolsCallAsync(TestServer server, string path, string host, IPAddress remote, string toolName, string? bearer = null)
        => SendJsonRpcAsync(server, path, host, remote, "tools/call", $"{{\"name\":\"{toolName}\",\"arguments\":{{}}}}", bearer);

    /// <summary>Network mode, no token at all: refused on BOTH <c>/</c> and <c>/core</c> before either
    /// endpoint's handler, or the SDK transport, ever runs.</summary>
    [Theory]
    [InlineData("/")]
    [InlineData("/core")]
    public async Task NetworkMode_NoToken_IsRefusedOnBothPaths(string path)
    {
        using var server = await BuildServer(networkMode: true);

        var (statusCode, _) = await ToolsListAsync(server, path, ListenIp, InCidrRemote);

        Assert.Equal(StatusCodes.Status401Unauthorized, statusCode);
    }

    /// <summary>A foreign Host header is refused on both paths, in both modes — the DNS-rebinding guard runs
    /// FIRST, before the bearer/CIDR checks even see the request.</summary>
    [Theory]
    [InlineData(true, "/")]
    [InlineData(true, "/core")]
    [InlineData(false, "/")]
    [InlineData(false, "/core")]
    public async Task ForeignHost_IsRefusedOnBothPaths_InBothModes(bool networkMode, string path)
    {
        using var server = await BuildServer(networkMode);
        var ctx = await SendRaw(server, path, "evil.com", networkMode ? InCidrRemote : IPAddress.Loopback, bearer: networkMode ? Token : null);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>Network mode, the right token, an in-CIDR remote: a tools/list on <c>/</c> succeeds — proof
    /// the request got PAST every gate.</summary>
    [Fact]
    public async Task NetworkMode_RightTokenInCidr_ToolsListSucceeds()
    {
        using var server = await BuildServer(networkMode: true);

        var (statusCode, _) = await ToolsListAsync(server, "/", ListenIp, InCidrRemote, bearer: Token);

        Assert.Equal(StatusCodes.Status200OK, statusCode);
    }

    /// <summary>Loopback mode, no token: no token middleware is installed at all in this mode, so a
    /// tools/list on <c>/</c> passes.</summary>
    [Fact]
    public async Task LoopbackMode_NoToken_ToolsListSucceeds()
    {
        using var server = await BuildServer(networkMode: false);

        var (statusCode, _) = await ToolsListAsync(server, "/", "localhost", IPAddress.Loopback);

        Assert.Equal(StatusCodes.Status200OK, statusCode);
    }

    /// <summary>
    /// #3898 D7's own claim, proven live: a <c>/core</c> tools/list returns exactly
    /// <see cref="DarlingCoreToolProfile"/>'s pinned set, and its instructions lead with the /core note —
    /// not the full-surface instructions <c>/</c> serves.
    /// </summary>
    [Fact]
    public async Task Core_ToolsList_IsExactlyThePinnedSet_WithTheCoreInstructionsNote()
    {
        using var server = await BuildServer(networkMode: false);

        var (statusCode, body) = await ToolsListAsync(server, "/core", "localhost", IPAddress.Loopback);
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        var payload = ReadJsonRpcResult(body);
        var toolNames = payload.GetProperty("tools").EnumerateArray()
            .Select(t => t.GetProperty("name").GetString() ?? "")
            .ToHashSet();

        Assert.Equal(DarlingCoreToolProfile.Closure.ToHashSet(), toolNames);

        var instructions = payload.TryGetProperty("instructions", out var instructionsElement)
            ? instructionsElement.GetString() ?? ""
            : "";
        Assert.Contains("/core", instructions, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>A <c>tools/call</c> on <c>/core</c> for a tool OUTSIDE its pinned set gets the SDK's own
    /// unknown-tool error — Darling registers no fallback <c>CallToolHandler</c>, so this is a real subset,
    /// not a listing filter (#3898 D7).</summary>
    [Fact]
    public async Task Core_ToolsCall_OutsideThePinnedSet_GetsTheSdkUnknownToolError()
    {
        using var server = await BuildServer(networkMode: false);

        const string outsideTool = "delete_custom_view"; // a WRITE tool never in the /core read-only profile
        Assert.DoesNotContain(outsideTool, DarlingCoreToolProfile.Closure);

        var (statusCode, body) = await ToolsCallAsync(server, "/core", "localhost", IPAddress.Loopback, outsideTool);
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        using var document = JsonDocument.Parse(ExtractJsonPayload(body));
        Assert.True(document.RootElement.TryGetProperty("error", out var error), $"expected a JSON-RPC error envelope, got: {body}");
        Assert.Contains("Unknown tool", error.GetProperty("message").GetString(), StringComparison.OrdinalIgnoreCase);
    }

    private static JsonElement ReadJsonRpcResult(string body)
    {
        using var document = JsonDocument.Parse(ExtractJsonPayload(body));
        return document.RootElement.GetProperty("result").Clone();
    }

    /// <summary>The Streamable-HTTP transport may answer as <c>text/event-stream</c> (an SSE frame carrying
    /// one <c>data: {json}</c> line) or as a bare JSON body, depending on negotiation; strip the SSE framing
    /// when present so both shapes parse the same way.</summary>
    private static string ExtractJsonPayload(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        return dataLine is null ? body : dataLine["data:".Length..].Trim();
    }
}
