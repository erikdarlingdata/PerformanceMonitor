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
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4128 part (a): a live HTTP proof of the web host's gates, through the SAME <c>ConfigurePipeline</c>
/// method production calls (extracted from <c>TryStartServerAsync</c> for exactly this purpose), driven by a
/// real Kestrel pipeline over <see cref="TestServer"/> rather than by reading source text or calling the pure
/// decision functions directly. <see cref="HostHeaderGuardTests"/> already proves the WIRING ORDER by
/// parsing the source; this class proves the wired pipeline actually REFUSES and ADMITS requests the way the
/// source claims it does end to end (Host header spoof, missing/valid/wrong token, in/out-of-CIDR remote).
///
/// <para>The gates read <c>context.Connection.RemoteIpAddress</c>, so every request goes through
/// <see cref="TestServer.SendAsync"/> (which allows setting it) rather than through
/// <c>TestServer.CreateClient</c> (which does not expose it).</para>
/// </summary>
public sealed class DarlingWebHostGateLiveTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string Token = "correct-token-value";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");
    private static readonly IPAddress OutOfCidrRemote = IPAddress.Parse("10.0.0.9");

    /// <summary>
    /// Builds a <see cref="TestServer"/> running the REAL <c>DarlingWebHostService.ConfigurePipeline</c> —
    /// the exact method the production <c>TryStartServerAsync</c> calls right after <c>builder.Build()</c>,
    /// in the same order, with the same values (see that call site). The only difference from production is
    /// the transport (TestServer instead of Kestrel sockets) and the store pool (a data source that is never
    /// opened, because none of these gates touch Postgres).
    /// </summary>
    private static async Task<TestServer> BuildServer(bool networkMode, string? publicBaseUrlHost = null, DarlingWebOidcClient? oidcClient = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        // A pool the gates never open (they run entirely ahead of DarlingWebEndpoints.MapAll's routes).
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);

        // #4188: ConfigurePipeline now calls app.UseResponseCompression(), which resolves its options from DI —
        // registered here exactly as the production builder in TryStartServerAsync registers them, or every
        // request below throws resolving a service nothing added.
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
            networkMode: networkMode,
            networkListenIp: networkMode ? IPAddress.Parse(ListenIp) : null,
            allowedCidr: IPNetwork.Parse(AllowedCidr),
            accessToken: Token,
            oidcClient: oidcClient,
            publicBaseUrlHost: publicBaseUrlHost);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static Task<HttpContext> Send(TestServer server, string path, string host, IPAddress? remote, string? token = null, string? cookie = null)
    {
        var target = token is null ? path : $"{path}?token={Uri.EscapeDataString(token)}";
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = target.Contains('?') ? target[..target.IndexOf('?')] : target;
            ctx.Request.QueryString = target.Contains('?') ? new QueryString(target[target.IndexOf('?')..]) : QueryString.Empty;
            ctx.Request.Headers.Host = host;
            ctx.Connection.RemoteIpAddress = remote;
            if (cookie is not null) ctx.Request.Headers["Cookie"] = cookie;
        });
    }

    /// <summary>Like <see cref="Send"/>, but also captures the response body — needed for the #4187 JSON-body
    /// pins below, which none of the status/header-only tests above needed. A MemoryStream stands in for the
    /// real response body so it can be read back after the pipeline finishes writing to it.</summary>
    private static async Task<(HttpContext Context, string Body)> SendWithBody(
        TestServer server, string path, string host, IPAddress? remote, string? token = null, string? cookie = null)
    {
        var target = token is null ? path : $"{path}?token={Uri.EscapeDataString(token)}";
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = target.Contains('?') ? target[..target.IndexOf('?')] : target;
            c.Request.QueryString = target.Contains('?') ? new QueryString(target[target.IndexOf('?')..]) : QueryString.Empty;
            c.Request.Headers.Host = host;
            c.Connection.RemoteIpAddress = remote;
            if (cookie is not null) c.Request.Headers["Cookie"] = cookie;
        });

        /* TestServer hands back its own ResponseBodyReaderStream — it supports Read but not Seek, and (unlike
           a MemoryStream this method used to substitute) it is already positioned at the start for a body
           nothing has read yet, so no rewind is needed or possible. */
        var body = await new StreamReader(ctx.Response.Body).ReadToEndAsync();
        return (ctx, body);
    }

    /// <summary>Network mode, no credential at all: the Host header is legitimate but no token/cookie is
    /// presented, so the auth gate serves the login page (200 — see <c>WebRequestAction.ShowLogin</c>,
    /// deliberately not a 401; <c>HostHeaderGuardTests</c> and <c>DarlingWebAuthTests</c> pin why).</summary>
    [Fact]
    public async Task NetworkMode_NoToken_ShowsLoginRatherThanServingTheApp()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Send(server, "/", ListenIp, InCidrRemote);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        // The login page, not a static-file/app response: its body is generated inline by WriteLoginPageAsync.
        Assert.Equal("text/html; charset=utf-8", ctx.Response.ContentType);
    }

    /// <summary>Network mode, the right token, an in-CIDR remote: the gate exchanges the token for a
    /// session cookie and 302-redirects (<c>WebRequestAction.SetCookieAndRedirect</c>) — proof the request
    /// got PAST every gate, not a login page and not a refusal.</summary>
    [Fact]
    public async Task NetworkMode_RightTokenInCidr_PassesTheGates()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Send(server, "/", ListenIp, InCidrRemote, token: Token);

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        Assert.True(ctx.Response.Headers.SetCookie.Count > 0, "a valid token must be exchanged for a session cookie");
    }

    /// <summary>
    /// #4221: an alert link is a hash route, and the token form's hidden <c>return</c> field (populated by
    /// <c>pathname + search + hash</c>) rides along as an ordinary query parameter on the token-strip 302 —
    /// <c>BuildPathWithoutToken</c> strips only <c>token</c> and preserves everything else untouched. This
    /// proves the LIVE pipeline (real ASP.NET Core query parsing/redirect, not just the pure
    /// <c>SanitizeRedirectPath</c>/<c>BuildPathWithoutToken</c> functions in isolation) round-trips a
    /// fragment-bearing <c>return</c> value byte for byte, with no token left lingering.
    /// </summary>
    [Fact]
    public async Task NetworkMode_RightTokenWithHashReturn_KeepsFragmentInReturnValue()
    {
        using var server = await BuildServer(networkMode: true);
        const string returnValue = "/#/triage?server=a&metric=b";
        var target = $"/?token={Uri.EscapeDataString(Token)}&return={Uri.EscapeDataString(returnValue)}";

        var ctx = await server.SendAsync(req =>
        {
            req.Request.Method = "GET";
            req.Request.Path = "/";
            req.Request.QueryString = new QueryString(target[target.IndexOf('?')..]);
            req.Request.Headers.Host = ListenIp;
            req.Connection.RemoteIpAddress = InCidrRemote;
        });

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        var location = ctx.Response.Headers.Location.ToString();
        Assert.DoesNotContain("token=", location, StringComparison.Ordinal);

        var parsed = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(location[location.IndexOf('?')..]);
        Assert.Equal(returnValue, parsed["return"].ToString());
    }

    /// <summary>Network mode, the right token, but the remote is OUTSIDE <c>allowFrom</c>: the CIDR gate is
    /// outermost (#2550) and forbids before the token is even considered.</summary>
    [Fact]
    public async Task NetworkMode_RightTokenOutsideCidr_IsForbidden()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Send(server, "/", ListenIp, OutOfCidrRemote, token: Token);

        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
    }

    /// <summary>A foreign Host header is refused by the DNS-rebinding guard in NETWORK mode — before the
    /// CIDR/token gate ever runs (#1576/#1648).</summary>
    [Fact]
    public async Task NetworkMode_ForeignHost_IsRefused()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Send(server, "/", "evil.com", InCidrRemote, token: Token);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>#4220: web.publicBaseUrl's host is admitted as one extra allowed Host value — a DNS name
    /// darling.sample.json suggests but that, before this, the guard refused unconditionally (it only ever
    /// compared against networkListenIp or the loopback names).</summary>
    [Fact]
    public async Task NetworkMode_PublicBaseUrlHost_IsAdmitted()
    {
        using var server = await BuildServer(networkMode: true, publicBaseUrlHost: "monitor.example.com");
        var ctx = await Send(server, "/", "monitor.example.com", InCidrRemote, token: Token);

        Assert.NotEqual(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>The admission names exactly ONE host — every other hostname, including one that merely looks
    /// similar, still gets 400.</summary>
    [Fact]
    public async Task NetworkMode_AnyOtherHost_StillRefused_EvenWithAPublicBaseUrlHostConfigured()
    {
        using var server = await BuildServer(networkMode: true, publicBaseUrlHost: "monitor.example.com");
        var ctx = await Send(server, "/", "evil.com", InCidrRemote, token: Token);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>The same rebinding guard runs in LOOPBACK mode too (#1576 fixed a loopback-only gap): a
    /// foreign Host is refused even though loopback registers no CIDR/token middleware at all.</summary>
    [Fact]
    public async Task LoopbackMode_ForeignHost_IsRefused()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Send(server, "/", "evil.com", IPAddress.Loopback);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>Loopback mode's own posture: no token middleware is registered at all, so a loopback Host
    /// with no token reaches past the gates (a 404 from the endpoint mapping / static files proves it got
    /// through — no gate short-circuited with a 400/403, and there is no login page in this mode).</summary>
    [Fact]
    public async Task LoopbackMode_NoToken_ReachesPastTheGates()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Send(server, "/some/unmapped/path", "localhost", IPAddress.Loopback);

        Assert.NotEqual(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.NotEqual(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
    }

    /* ---------------------------------------------------------------------------------------------------
       #4187: an /api/* call with no valid session gets 401 + a small JSON body, never the 200 HTML login
       form the SAME unauthenticated request gets on a page route. Before this fix the SPA's fetch layer
       (util.js classifyResponse) parsed the login form as a failed JSON.parse and rendered an expired or
       rotated session as empty data rather than "sign in again".
       --------------------------------------------------------------------------------------------------- */

    /// <summary>The core pin: no credential at all on an /api/* path answers 401 JSON, not the 200 login form
    /// <see cref="NetworkMode_NoToken_ShowsLoginRatherThanServingTheApp"/> proves for a page route.</summary>
    [Fact]
    public async Task NetworkMode_NoToken_ApiPath_Returns401Json()
    {
        using var server = await BuildServer(networkMode: true);
        var (ctx, body) = await SendWithBody(server, "/api/fleet", ListenIp, InCidrRemote);

        Assert.Equal(StatusCodes.Status401Unauthorized, ctx.Response.StatusCode);
        Assert.Equal("application/json; charset=utf-8", ctx.Response.ContentType);
        Assert.Contains("\"error\"", body, StringComparison.Ordinal);
        Assert.DoesNotContain("<html", body, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Proves the split is per-request, not per-mode: on the exact same server, the exact same
    /// unauthenticated caller gets 401 for the API path and the 200 form for the page path.</summary>
    [Fact]
    public async Task NetworkMode_NoToken_ApiPath_DiffersFromPageRoute()
    {
        using var server = await BuildServer(networkMode: true);
        var apiCtx = await Send(server, "/api/fleet", ListenIp, InCidrRemote);
        var pageCtx = await Send(server, "/", ListenIp, InCidrRemote);

        Assert.Equal(StatusCodes.Status401Unauthorized, apiCtx.Response.StatusCode);
        Assert.Equal(StatusCodes.Status200OK, pageCtx.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", pageCtx.Response.ContentType);
    }

    /// <summary>The cookie signing key is a per-process random value (#4187's own root cause): a restart
    /// rotates it and every previously-issued cookie fails <c>TryValidateSessionCookie</c> from then on. A
    /// syntactically-present but unverifiable cookie stands in for that without actually restarting the
    /// process, and must be refused exactly like no cookie at all — 401 JSON on an /api/* path.</summary>
    [Fact]
    public async Task NetworkMode_StaleSessionCookie_ApiPath_Returns401Json()
    {
        using var server = await BuildServer(networkMode: true);
        var (ctx, body) = await SendWithBody(
            server, "/api/fleet", ListenIp, InCidrRemote, cookie: "darling_web_session=not-a-validly-signed-value");

        Assert.Equal(StatusCodes.Status401Unauthorized, ctx.Response.StatusCode);
        Assert.Equal("application/json; charset=utf-8", ctx.Response.ContentType);
        Assert.Contains("\"error\"", body, StringComparison.Ordinal);
    }

    /// <summary>OIDC (#2550) is the third auth mode the gate supports. <c>DecideWebRequest</c> only diverts the
    /// exact <c>/auth/oidc/*</c> flow paths; every other unauthenticated request — including every /api/* call
    /// — reaches the SAME ShowLogin arm the token/cookie modes do, so enabling SSO must not regress the split.
    /// The client is real but talks to no server in this test (discovery is fetched lazily, on first use of
    /// the flow paths, which this request never reaches).</summary>
    [Fact]
    public async Task NetworkMode_OidcConfigured_NoCredential_ApiPath_Returns401Json()
    {
        var oidcClient = new DarlingWebOidcClient(new DarlingWebOidcClient.ResolvedOptions(
            "https://idp.example.test", "client-id", null, "openid", null, null,
            Array.Empty<string>(), Array.Empty<string>()));

        using var server = await BuildServer(networkMode: true, oidcClient: oidcClient);
        var (ctx, body) = await SendWithBody(server, "/api/fleet", ListenIp, InCidrRemote);

        Assert.Equal(StatusCodes.Status401Unauthorized, ctx.Response.StatusCode);
        Assert.Contains("\"error\"", body, StringComparison.Ordinal);
    }
}
