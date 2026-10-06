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
/// <c>TestServer.CreateClient</c>, since the SDK's transport reads the body, not the connection.</para>
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
    private static async Task<TestServer> BuildServer(
        bool networkMode, string? hostName = null, ILogger? logger = null, string? rawAllowedHostName = null,
        bool requireTokenWhenLoopbackOnly = false)
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
            bearerToken: Token,
            // #5288: the configured name is decided exactly as TryStartServerAsync decides it, through
            // ResolveAllowedHostName from the FINAL mode, so loopback mode never admits it. Passing the name here
            // unconditionally would hide the very rule that HostName_AdmittedInNetworkMode_RefusedInLoopbackMode_OtherNamesStill400 pins.
            // rawAllowedHostName is the one exception, for the test that hands ConfigurePipeline a name the resolver
            // would never pass it (a malformed punycode label): the pipeline itself must survive that.
            allowedHostName: rawAllowedHostName ?? DarlingMcpHostService.ResolveAllowedHostName(
                hostName, networkMode, logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance),
            // #5288: true only for a start that network mode degraded out of (TLS refused, token resolved); the
            // never-network loopback-only server in the tests below leaves it false, exactly as the host does.
            requireTokenWhenLoopbackOnly: requireTokenWhenLoopbackOnly);

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
        });

        /* TestServer hands back its own response stream (a reader over what the pipeline wrote), and it
           can't seek: read it from where it stands. Replacing Response.Body in the configure callback has no
           effect, because TestServer installs its own response feature. */
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

    /// <summary>
    /// #5288: network mode configured, then degraded to loopback-only because the TLS certificate was refused, token
    /// resolved. The loopback-only server keeps the bearer-token gate (the host passes
    /// <c>requireTokenWhenLoopbackOnly</c> for exactly that start), on both paths and both loopback families: no token
    /// and a wrong token get 401, the right token gets <c>tools/list</c>. Every client presented the token before the
    /// certificate lapsed, so nothing that worked stops working.
    /// </summary>
    [Theory]
    [InlineData("/", "127.0.0.1")]
    [InlineData("/core", "127.0.0.1")]
    [InlineData("/", "::1")]
    [InlineData("/core", "::1")]
    public async Task TlsRefusal_DegradedLoopbackServer_StillRequiresTheToken(string path, string loopback)
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);
        var remote = IPAddress.Parse(loopback);

        var (noToken, _) = await ToolsListAsync(server, path, "localhost", remote);
        Assert.Equal(StatusCodes.Status401Unauthorized, noToken);

        var (wrongToken, _) = await ToolsListAsync(server, path, "localhost", remote, "not-the-token");
        Assert.Equal(StatusCodes.Status401Unauthorized, wrongToken);

        var (withToken, body) = await ToolsListAsync(server, path, "localhost", remote, Token);
        Assert.True(withToken == StatusCodes.Status200OK, $"the right token must reach tools/list, got {withToken}: {body}");
    }

    /// <summary>The Host guard still runs first on the token-keeping loopback-only server, so a foreign Host is 400
    /// whatever token it carries.</summary>
    [Fact]
    public async Task TlsRefusal_DegradedLoopbackServer_ForeignHostIsStill400()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);
        var ctx = await SendRaw(server, "/", "evil.com", IPAddress.Loopback, bearer: Token);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>
    /// #5288: the CIDR check runs BEFORE the token check, so an address outside <c>allowFrom</c> is answered 403
    /// whatever it sends: no token, a wrong token and the right token all get the same status, and a client that
    /// cannot route in learns nothing about the token from the port. An in-list client with the same wrong token
    /// still gets 401, so the two gates stay distinguishable to the operator.
    /// </summary>
    [Theory]
    [InlineData("/", null)]
    [InlineData("/", "not-the-token")]
    [InlineData("/", Token)]
    [InlineData("/core", null)]
    [InlineData("/core", "not-the-token")]
    [InlineData("/core", Token)]
    public async Task OffListRemote_RightOrWrongToken_Both403(string path, string? bearer)
    {
        using var server = await BuildServer(networkMode: true);

        var (offList, _) = await ToolsListAsync(server, path, ListenIp, IPAddress.Parse("203.0.113.50"), bearer);
        Assert.Equal(StatusCodes.Status403Forbidden, offList);

        if (bearer == "not-the-token")
        {
            var (inList, _) = await ToolsListAsync(server, path, ListenIp, InCidrRemote, bearer);
            Assert.Equal(StatusCodes.Status401Unauthorized, inList);
        }
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

    /// <summary>
    /// #5288, review F2: <c>mcp.network.hostName</c> is admitted by the Host guard in NETWORK mode only. With the
    /// name configured, network mode admits it (as written, or in any case) on both paths and still demands the
    /// token, loopback mode given the SAME configured name refuses it 400 while its loopback names keep working,
    /// and every OTHER name is still refused 400 in both. The name reaches the pipeline through
    /// <see cref="DarlingMcpHostService.ResolveAllowedHostName"/>, the method production calls with the final
    /// mode, so this fails the moment the name is passed in loopback mode too.
    /// </summary>
    [Theory]
    [InlineData("/")]
    [InlineData("/core")]
    public async Task HostName_AdmittedInNetworkMode_RefusedInLoopbackMode_OtherNamesStill400(string path)
    {
        const string hostName = "mcp.corp.example";

        using var network = await BuildServer(networkMode: true, hostName);

        // Admitted, as written and in any case: with the token the request gets past every gate to a handler.
        foreach (var host in new[] { hostName, "MCP.Corp.Example" })
        {
            var (status, _) = await ToolsListAsync(network, path, host, InCidrRemote, bearer: Token);
            Assert.True(status == StatusCodes.Status200OK, $"network mode, Host '{host}' on {path}: expected 200, got {status}");
        }

        // A Host is not a credential: the guard admits the name, and the token gate still runs after it.
        var (noToken, _) = await ToolsListAsync(network, path, hostName, InCidrRemote);
        Assert.Equal(StatusCodes.Status401Unauthorized, noToken);

        // The names the guard always admitted are untouched by the extra one.
        foreach (var host in new[] { ListenIp, "localhost" })
        {
            var (status, _) = await ToolsListAsync(network, path, host, InCidrRemote, bearer: Token);
            Assert.True(status == StatusCodes.Status200OK, $"network mode, Host '{host}' on {path}: expected 200, got {status}");
        }

        // Every other name is still 400, token and all: another site, a longer name that only ENDS with it, a
        // longer one that only STARTS with it, its parent domain, a child of it, and an IP that is not the listen IP.
        foreach (var other in new[] { "evil.com", "evil-mcp.corp.example", "mcp.corp.example.evil.com", "corp.example", "sub.mcp.corp.example", "10.9.9.9" })
        {
            var ctx = await SendRaw(network, path, other, InCidrRemote, bearer: Token);
            Assert.True(
                ctx.Response.StatusCode == StatusCodes.Status400BadRequest,
                $"network mode, Host '{other}' on {path}: expected 400, got {ctx.Response.StatusCode}");
        }

        // Loopback mode with the SAME name configured: refused, because that surface is tokenless and a name the
        // operator wrote for the network listener's clients buys nothing there.
        using var loopback = await BuildServer(networkMode: false, hostName);

        foreach (var host in new[] { hostName, "MCP.Corp.Example" })
        {
            var ctx = await SendRaw(loopback, path, host, IPAddress.Loopback);
            Assert.True(
                ctx.Response.StatusCode == StatusCodes.Status400BadRequest,
                $"loopback mode, Host '{host}' on {path}: expected 400, got {ctx.Response.StatusCode}");
        }

        // ...while the loopback names keep working, and a foreign name is still refused.
        var (loopbackStatus, _) = await ToolsListAsync(loopback, path, "localhost", IPAddress.Loopback);
        Assert.Equal(StatusCodes.Status200OK, loopbackStatus);

        var foreign = await SendRaw(loopback, path, "evil.com", IPAddress.Loopback);
        Assert.Equal(StatusCodes.Status400BadRequest, foreign.Response.StatusCode);
    }

    /// <summary>
    /// #5288 item 3: a host name written in Unicode is admitted as the ASCII (punycode) name a client sends in its
    /// Host header. ASP.NET Core decodes that header to Unicode before the guard sees it, so the pipeline compares
    /// the configured name in the same decoded form; a different internationalized name is still refused.
    /// </summary>
    [Fact]
    public async Task HostName_WrittenInUnicode_IsAdmittedAsItsPunycodeHost()
    {
        using var server = await BuildServer(networkMode: true, hostName: "b\u00FCcher.example");

        var (punycode, body) = await ToolsListAsync(server, "/", "xn--bcher-kva.example", InCidrRemote, bearer: Token);
        Assert.True(punycode == StatusCodes.Status200OK, $"expected 200, got {punycode}: {body}");

        var other = await SendRaw(server, "/", "xn--e1afmkfd.example", InCidrRemote, bearer: Token);
        Assert.Equal(StatusCodes.Status400BadRequest, other.Response.StatusCode);
    }

    /// <summary>
    /// #5288: IDNA names are case-insensitive, so a punycode host name written in upper case is admitted exactly like
    /// the lower-case spelling. The framework decodes a Host header's <c>xn--</c> labels only when the prefix is lower
    /// case (the form a client sends), so the configured name is held in lower case and decodes the same way; a
    /// different internationalized name is still refused.
    /// </summary>
    [Theory]
    [InlineData("XN--BCHER-KVA.EXAMPLE")]
    [InlineData("xn--bcher-kva.example")]
    public async Task HostName_UpperCasePunycode_IsAdmitted(string configured)
    {
        using var server = await BuildServer(networkMode: true, hostName: configured);

        var (punycode, body) = await ToolsListAsync(server, "/", "xn--bcher-kva.example", InCidrRemote, bearer: Token);
        Assert.True(punycode == StatusCodes.Status200OK, $"expected 200, got {punycode}: {body}");

        var other = await SendRaw(server, "/", "xn--e1afmkfd.example", InCidrRemote, bearer: Token);
        Assert.Equal(StatusCodes.Status400BadRequest, other.Response.StatusCode);
    }

    /// <summary>
    /// #5288 item 2: a host name that is SET but is not a bare DNS name admits NOTHING, not even the part a lenient
    /// parser would keep (the host before a port, a name a wildcard would match), and logs exactly one Warning at
    /// start that names the key and the value as written. The listener is otherwise unaffected: the listen IP and
    /// the token still work, because a typo in an optional name must not take MCP down.
    /// </summary>
    [Theory]
    [InlineData("mcp.corp.example:5152", "mcp.corp.example")]
    [InlineData("https://mcp.corp.example/", "mcp.corp.example")]
    [InlineData("mcp.corp.example/mcp", "mcp.corp.example")]
    [InlineData("*.corp.example", "mcp.corp.example")]
    [InlineData("10.9.9.9", "10.9.9.9")]
    [InlineData("mcp.corp.example..", "mcp.corp.example")]
    [InlineData("b\u00FCcher..example", "xn--bcher-kva.example")]
    [InlineData("xn--a", "evil.com")]
    [InlineData("mcp.xn--a.example", "evil.com")]
    public async Task HostName_SetButRefused_IsNotAdmitted_AndLogsOneWarning(string refusedValue, string hostAClientWouldSend)
    {
        var logger = new CapturingTestLogger();
        using var server = await BuildServer(networkMode: true, refusedValue, logger);

        var line = Assert.Single(logger.Lines);
        Assert.StartsWith("Warning: ", line);
        Assert.Contains("mcp.network.hostName", line);
        Assert.Contains(refusedValue, line);

        var refused = await SendRaw(server, "/", hostAClientWouldSend, InCidrRemote, bearer: Token);
        Assert.Equal(StatusCodes.Status400BadRequest, refused.Response.StatusCode);

        var (listenIp, _) = await ToolsListAsync(server, "/", ListenIp, InCidrRemote, bearer: Token);
        Assert.Equal(StatusCodes.Status200OK, listenIp);
    }

    /// <summary>
    /// #5288: <c>ConfigurePipeline</c> admits whatever name it is given, so it must not fail start-up for a malformed
    /// punycode label (<c>xn--a</c> decodes to nothing and makes <c>HostString.FromUriComponent</c> throw
    /// <c>ArgumentException</c>), even though <c>McpNetworkConfig.NormalizeHostName</c> already refuses such a name
    /// before production reaches this method. Called directly, with the raw name, so this fails the moment the
    /// pipeline's own conversion loses its catch. The conversion falls back to the raw value: the pipeline still
    /// builds, still refuses a foreign Host, and still admits the names it always admits.
    /// </summary>
    [Theory]
    [InlineData("xn--a")]
    [InlineData("mcp.xn--a.example")]
    public async Task MalformedPunycodeAllowedHostName_PassedStraightToConfigurePipeline_DoesNotFailStartUp(string rawAllowedHostName)
    {
        using var server = await BuildServer(networkMode: true, rawAllowedHostName: rawAllowedHostName);

        var foreign = await SendRaw(server, "/", "evil.com", InCidrRemote, bearer: Token);
        Assert.Equal(StatusCodes.Status400BadRequest, foreign.Response.StatusCode);

        foreach (var host in new[] { ListenIp, "localhost" })
        {
            var (status, body) = await ToolsListAsync(server, "/", host, InCidrRemote, bearer: Token);
            Assert.True(status == StatusCodes.Status200OK, $"Host '{host}': expected 200, got {status}: {body}");
        }
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

        /* The instructions travel in the initialize result, not tools/list. A /core session's lead with
           DarlingCoreToolProfile.CoreNote; the full set's do not. */
        var (coreInitStatus, coreInitBody) = await SendJsonRpcAsync(server, "/core", "localhost", IPAddress.Loopback,
            "initialize", InitializeParams, bearer: null);
        Assert.Equal(StatusCodes.Status200OK, coreInitStatus);
        var coreInstructions = ReadJsonRpcResult(coreInitBody).GetProperty("instructions").GetString() ?? "";
        Assert.StartsWith(DarlingCoreToolProfile.CoreNote, coreInstructions, StringComparison.Ordinal);

        var (fullInitStatus, fullInitBody) = await SendJsonRpcAsync(server, "/", "localhost", IPAddress.Loopback,
            "initialize", InitializeParams, bearer: null);
        Assert.Equal(StatusCodes.Status200OK, fullInitStatus);
        var fullInstructions = ReadJsonRpcResult(fullInitBody).GetProperty("instructions").GetString() ?? "";
        Assert.False(fullInstructions.StartsWith(DarlingCoreToolProfile.CoreNote, StringComparison.Ordinal));
    }

    private const string InitializeParams =
        "{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"gate-test\",\"version\":\"1\"}}";

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

    /// <summary>A <c>tools/call</c> that sends <paramref name="argumentsJson"/> as the call's arguments object.</summary>
    private static Task<(int StatusCode, string Body)> ToolsCallWithArgumentsAsync(
        TestServer server, string path, string host, IPAddress remote, string toolName, string argumentsJson)
        => SendJsonRpcAsync(
            server, path, host, remote, "tools/call", $"{{\"name\":\"{toolName}\",\"arguments\":{argumentsJson}}}", bearer: null);

    /// <summary>
    /// The argument guard's typed refusal reaches a caller over the stateless HTTP pipeline the product ships. The guard
    /// takes its record of each tool's parameter types from the request's services, and the in-process server the other
    /// pins run on hands it those, so only a call made like this one shows that a real client's request resolves it too.
    /// A null for <c>hours_back</c> is the proof: the schema says "integer" and does not say which parameters take a
    /// null, so only the typed path refuses it. With the record missing the null would reach the SDK's binder, which
    /// answers "An error occurred invoking ..." and names neither the argument nor the reason.
    /// </summary>
    [Fact]
    public async Task ToolsCall_ANullForAWholeNumber_IsRefusedByName_OverTheShippedHttpPipeline()
    {
        using var server = await BuildServer(networkMode: false);

        var (statusCode, body) = await ToolsCallWithArgumentsAsync(
            server, "/", "localhost", IPAddress.Loopback, "get_alert_history", "{\"hours_back\":null}");
        Assert.Equal(StatusCodes.Status200OK, statusCode);

        var result = ReadJsonRpcResult(body);
        Assert.True(result.GetProperty("isError").GetBoolean(), $"expected an error result, got: {body}");
        var text = result.GetProperty("content").EnumerateArray().Single().GetProperty("text").GetString() ?? "";
        Assert.True(
            PerformanceMonitor.Common.McpHelpers.IsRefusalEnvelope(text), $"expected the argument guard's refusal, got: {text}");

        using var refusal = JsonDocument.Parse(text);
        Assert.Equal("hours_back", refusal.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains(
            "Argument 'hours_back' for tool 'get_alert_history' takes a whole number of hours, and cannot be null, and the call sent null.",
            refusal.RootElement.GetProperty("message").GetString(),
            StringComparison.Ordinal);
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
