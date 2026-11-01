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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
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

    /// <summary>Adapts <see cref="CapturingTestLogger"/> (a plain <see cref="ILogger"/>) to the generic
    /// <see cref="ILogger{TCategoryName}"/> <see cref="DarlingWebHostService"/>'s constructor requires.</summary>
    private sealed class CapturingHostLogger(CapturingTestLogger inner) : ILogger<DarlingWebHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>
    /// Builds a <see cref="TestServer"/> running the REAL <c>DarlingWebHostService.ConfigurePipeline</c> —
    /// the exact method the production <c>TryStartServerAsync</c> calls right after <c>builder.Build()</c>,
    /// in the same order, with the same values (see that call site). The only difference from production is
    /// the transport (TestServer instead of Kestrel sockets) and the store pool (a data source that is never
    /// opened, because none of these gates touch Postgres).
    /// </summary>
    private static async Task<TestServer> BuildServer(
        bool networkMode, string? publicBaseUrlHost = null, DarlingWebOidcClient? oidcClient = null,
        bool requireTokenWhenLoopbackOnly = false, CapturingTestLogger? hostLogger = null, string accessToken = Token)
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

        // hostLogger is the host's own logger, the one the gates write their refusal lines to; the Host-refusal line test reads it.
        var host = new DarlingWebHostService(
            hostLogger is null ? NullLogger<DarlingWebHostService>.Instance : new CapturingHostLogger(hostLogger),
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache());

        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: networkMode,
            // #5288: the Host guard's listen address is decided exactly as TryStartServerAsync decides it, through
            // ResolveHostGuardListenIp from the FINAL mode. The host parses the configured address before any degrade,
            // so a start that degraded out of network mode (requireTokenWhenLoopbackOnly) still has it in hand and the
            // resolver is what drops it; a start that never had a network block has none to hand over.
            networkListenIp: DarlingMcpHostService.ResolveHostGuardListenIp(
                networkMode, networkMode || requireTokenWhenLoopbackOnly ? IPAddress.Parse(ListenIp) : null),
            allowedCidr: IPNetwork.Parse(AllowedCidr),
            accessToken: accessToken,
            oidcClient: oidcClient,
            publicBaseUrlHost: publicBaseUrlHost,
            // #5288: true only for a start that network mode degraded out of (TLS refused, token resolved); the
            // never-network loopback-only server in the tests below leaves it false, exactly as the host does.
            requireTokenWhenLoopbackOnly: requireTokenWhenLoopbackOnly);

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

    /// <summary>The login form's own request: a form-encoded POST of the token (and the page to return to)
    /// to <see cref="DarlingWebHostService.LoginPath"/>, which is the only place the token is accepted.</summary>
    internal static Task<HttpContext> PostSignIn(
        TestServer server, string host, IPAddress? remote, string token, string? returnPath = null, Action<HttpContext>? configure = null)
    {
        var body = $"token={Uri.EscapeDataString(token)}"
            + (returnPath is null ? string.Empty : $"&return={Uri.EscapeDataString(returnPath)}");
        var bytes = System.Text.Encoding.UTF8.GetBytes(body);
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = "POST";
            ctx.Request.Path = DarlingWebHostService.LoginPath;
            ctx.Request.Headers.Host = host;
            ctx.Request.ContentType = "application/x-www-form-urlencoded";
            ctx.Request.ContentLength = bytes.Length;
            ctx.Request.Body = new MemoryStream(bytes);
            ctx.Connection.RemoteIpAddress = remote;
            configure?.Invoke(ctx);
        });
    }

    /// <summary>Network mode, the right token posted by the form, an in-CIDR remote: the sign-in leg
    /// exchanges the token for a session cookie and 302-redirects — proof the request got PAST every gate,
    /// not a login page and not a refusal.</summary>
    [Fact]
    public async Task NetworkMode_RightTokenInCidr_PassesTheGates()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await PostSignIn(server, ListenIp, InCidrRemote, Token);

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        Assert.True(ctx.Response.Headers.SetCookie.Count > 0, "a valid token must be exchanged for a session cookie");
    }

    /// <summary>The token is accepted from the form's POST body only. The same token in a GET's query string
    /// is not exchanged, whatever its value: no cookie, no redirect, the login page instead. A URL is the
    /// one place a credential is copied without anyone meaning to (history, bookmarks, Referer headers).</summary>
    [Fact]
    public async Task NetworkMode_RightTokenInUrl_IsNotExchanged()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Send(server, "/", ListenIp, InCidrRemote, token: Token);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", ctx.Response.ContentType);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
    }

    /// <summary>
    /// #4221: an alert link is a hash route, and the login form's hidden <c>return</c> field (populated by
    /// <c>pathname + search + hash</c>) rides the sign-in POST; the 302 that follows lands on it byte for
    /// byte, through the same open-redirect guard as before, with no token anywhere in the URL. This proves
    /// the LIVE pipeline (real ASP.NET Core form parsing/redirect, not the pure <c>SanitizeRedirectPath</c>
    /// in isolation) round-trips a fragment-bearing <c>return</c> value.
    /// </summary>
    [Fact]
    public async Task NetworkMode_RightTokenWithHashReturn_KeepsFragmentInReturnValue()
    {
        using var server = await BuildServer(networkMode: true);
        const string returnValue = "/#/triage?server=a&metric=b";

        var ctx = await PostSignIn(server, ListenIp, InCidrRemote, Token, returnPath: returnValue);

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        var location = ctx.Response.Headers.Location.ToString();
        Assert.DoesNotContain("token=", location, StringComparison.Ordinal);
        Assert.Equal(returnValue, location);
    }

    /// <summary>Network mode, the right token, but the remote is OUTSIDE <c>allowFrom</c>: the CIDR gate is
    /// outermost (#2550) and forbids before the token is even considered — the sign-in leg included.</summary>
    [Fact]
    public async Task NetworkMode_RightTokenOutsideCidr_IsForbidden()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await PostSignIn(server, ListenIp, OutOfCidrRemote, Token);

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

    /// <summary>#5288: the line the Host guard logs for a refused name tells the operator which settings decide the
    /// names it admits: the loopback names, the listen address (<c>web.network.listen</c>) and <c>web.publicBaseUrl</c>'s
    /// host. The MCP listener's line names its own settings in the same shape
    /// (<see cref="DarlingMcpHostGateLiveTests"/>). Read from the host's own logger through the real pipeline.</summary>
    [Fact]
    public async Task NetworkMode_ForeignHost_TheRefusalLogLine_NamesTheSettingsThatDecideTheAdmittedNames()
    {
        var hostLogger = new CapturingTestLogger();
        using var server = await BuildServer(networkMode: true, publicBaseUrlHost: "monitor.example.com", hostLogger: hostLogger);
        var ctx = await Send(server, "/", "evil.com", InCidrRemote, token: Token);
        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);

        var line = Assert.Single(hostLogger.Lines);
        Assert.StartsWith("Warning: Web dashboard ", line, StringComparison.Ordinal);
        Assert.Contains("the Host header 'evil.com' is not an address this endpoint binds", line, StringComparison.Ordinal);
        Assert.Contains("web.network.listen when LAN-exposed", line, StringComparison.Ordinal);
        Assert.Contains("web.publicBaseUrl's host", line, StringComparison.Ordinal);
        Assert.DoesNotContain("monitor.example.com", line, StringComparison.Ordinal);
    }

    /// <summary>The token form's same-site refusal exists with or without OIDC, so its log line names its own gate
    /// (the same-site check, with web.publicBaseUrl as the setting that fixes a proxied start), never the OIDC flow's.</summary>
    [Fact]
    public async Task NetworkMode_ACrossSiteSignInPost_TheRefusalLogLine_NamesTheSameSiteGate_NotTheOidcFlow()
    {
        var hostLogger = new CapturingTestLogger();
        using var server = await BuildServer(networkMode: true, hostLogger: hostLogger);
        var ctx = await PostSignIn(server, ListenIp, InCidrRemote, Token, configure: c => c.Request.Headers.Origin = "https://other.example");
        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);

        var line = Assert.Single(hostLogger.Lines);
        Assert.Contains(DarlingHttpRefusalLog.Describe(DarlingRefusalGate.TokenSignInSite), line, StringComparison.Ordinal);
        Assert.Contains("web.publicBaseUrl", line, StringComparison.Ordinal);
        Assert.DoesNotContain("OIDC", line, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingHttpRefusalLog.Describe(DarlingRefusalGate.SignIn), line, StringComparison.Ordinal);
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

    /// <summary>#5288 (#4220): an internationalized <c>web.publicBaseUrl</c> host is admitted for the Host a
    /// browser sends. A browser always sends the punycode (<c>xn--</c>) form of the name, ASP.NET Core hands the
    /// guard the DECODED form of that header (<c>b&#252;cher.example</c>), and <c>TriageLink.TryGetHost</c> returns
    /// <c>Uri.Host</c>, which keeps whichever spelling the operator wrote in the URL. Both spellings of the URL
    /// must therefore admit the one Host the browser sends, with or without a port. The URL goes through
    /// <c>TriageLink.TryGetHost</c> here exactly as <c>TryStartServerAsync</c> does before it hands the host to
    /// <c>ConfigurePipeline</c>.</summary>
    [Theory]
    [InlineData("https://xn--bcher-kva.example/", "xn--bcher-kva.example")]
    [InlineData("https://b\u00FCcher.example/", "xn--bcher-kva.example")]
    [InlineData("https://xn--bcher-kva.example:5153/", "xn--bcher-kva.example:5153")]
    [InlineData("https://b\u00FCcher.example:5153/", "xn--bcher-kva.example:5153")]
    public async Task NetworkMode_IdnPublicBaseUrlHost_IsAdmitted_ForThePunycodeHostABrowserSends(string publicBaseUrl, string hostHeader)
    {
        var publicBaseUrlHost = TriageLink.TryGetHost(publicBaseUrl);
        Assert.NotNull(publicBaseUrlHost);

        using var server = await BuildServer(networkMode: true, publicBaseUrlHost: publicBaseUrlHost);
        var ctx = await Send(server, "/", hostHeader, InCidrRemote, token: Token);

        Assert.True(
            ctx.Response.StatusCode != StatusCodes.Status400BadRequest,
            $"web.publicBaseUrl '{publicBaseUrl}' (host '{publicBaseUrlHost}') refused its own Host '{hostHeader}' with {ctx.Response.StatusCode}");
    }

    /// <summary>The IDN admission names exactly ONE host: another punycode name (<c>m&#252;nchen.example</c>), a
    /// plain foreign name and the name with its non-ASCII letter dropped all still get 400.</summary>
    [Theory]
    [InlineData("xn--mnchen-3ya.example")]
    [InlineData("evil.com")]
    [InlineData("bcher.example")]
    public async Task NetworkMode_IdnPublicBaseUrlHost_StillRefusesEveryOtherHost(string hostHeader)
    {
        var publicBaseUrlHost = TriageLink.TryGetHost("https://xn--bcher-kva.example/");
        using var server = await BuildServer(networkMode: true, publicBaseUrlHost: publicBaseUrlHost);
        var ctx = await Send(server, "/", hostHeader, InCidrRemote, token: Token);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>#5288: a malformed punycode label in <c>web.publicBaseUrl</c> (<c>xn--a</c> decodes to nothing)
    /// must not fail start-up. The conversion that puts the name in the form the guard compares falls back to
    /// the raw value, so the pipeline still builds, still refuses a foreign Host and still admits a loopback
    /// one.</summary>
    [Theory]
    [InlineData("https://xn--a/")]
    [InlineData("https://xn--bcher-kva-.example/")]
    public async Task NetworkMode_MalformedPunycodePublicBaseUrlHost_DoesNotFailStartUp_AndAdmitsNothingExtra(string publicBaseUrl)
    {
        var publicBaseUrlHost = TriageLink.TryGetHost(publicBaseUrl);
        Assert.NotNull(publicBaseUrlHost);

        using var server = await BuildServer(networkMode: true, publicBaseUrlHost: publicBaseUrlHost);

        var foreign = await Send(server, "/", "evil.com", InCidrRemote, token: Token);
        Assert.Equal(StatusCodes.Status400BadRequest, foreign.Response.StatusCode);

        var loopback = await Send(server, "/", "localhost", InCidrRemote, token: Token);
        Assert.NotEqual(StatusCodes.Status400BadRequest, loopback.Response.StatusCode);
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

    /// <summary>
    /// #6286: a page link followed from another site (a Teams card, mail, a work item) is a cross-site top-level
    /// navigation, so the SameSite=Strict session cookie is not sent and a live session looks signed out. A GET page
    /// request with no valid cookie that the browser marks <c>Sec-Fetch-Site: cross-site</c> is answered with the
    /// re-navigation page (a constant script that reloads the page's own address from this site, which carries the
    /// cookie) instead of the sign-in form. The page takes nothing from the request: not the path, not the query.
    /// </summary>
    [Fact]
    public async Task ACrossSitePageRequest_WithNoCookie_GetsTheReNavigationPage_NotTheSignInForm()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (ctx, body) = await SendPage(server, "/triage", "?next=https%3A%2F%2Felsewhere.example%2F&q=1", "localhost", IPAddress.Loopback, "cross-site");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", ctx.Response.ContentType);
        Assert.Contains(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
        Assert.Contains("location.reload()", body, StringComparison.Ordinal);
        Assert.Equal(DarlingWebHostService.ReNavigationPagePolicy, ctx.Response.Headers["Content-Security-Policy"].ToString());
        Assert.Equal("no-store", ctx.Response.Headers.CacheControl.ToString());
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
        // Nothing the request carried is in the page, so it cannot send anyone elsewhere.
        Assert.DoesNotContain("elsewhere.example", body, StringComparison.Ordinal);
        Assert.DoesNotContain("triage", body, StringComparison.Ordinal);
        Assert.DoesNotContain("<form", body, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The loop guard: the re-navigation is same-origin, so with still no session it gets the sign-in
    /// form, exactly the page a request with no fetch metadata at all gets.</summary>
    [Theory]
    [InlineData("same-origin")]
    [InlineData("same-site")]
    [InlineData("none")]
    [InlineData(null)]
    public async Task ARequestThatIsNotCrossSite_WithNoCookie_GetsTheSignInForm(string? fetchSite)
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (ctx, body) = await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, fetchSite);
        var (_, plain) = await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, fetchSite: null, fetchMode: null, fetchDest: null);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
        Assert.Equal(plain, body);
        Assert.Contains("<form", body, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Only a top-level GET page request is answered with the page: an /api/* call keeps its 401 JSON, a
    /// cross-site POST and a cross-site image or script request get no re-navigation page.</summary>
    [Fact]
    public async Task ACrossSiteRequest_ThatIsNotAPageNavigation_NeverGetsTheReNavigationPage()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (api, apiBody) = await SendPage(server, "/api/fleet", string.Empty, "localhost", IPAddress.Loopback, "cross-site");
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Response.StatusCode);
        Assert.Contains("\"error\"", apiBody, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, apiBody, StringComparison.Ordinal);

        var (_, imageBody) = await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, "cross-site", fetchMode: "no-cors", fetchDest: "image");
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, imageBody, StringComparison.Ordinal);

        var post = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/triage";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers["Sec-Fetch-Site"] = "cross-site";
            c.Request.Headers["Sec-Fetch-Mode"] = "navigate";
            c.Request.Headers["Sec-Fetch-Dest"] = "document";
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });
        var postBody = await new StreamReader(post.Response.Body).ReadToEndAsync();
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, postBody, StringComparison.Ordinal);
    }

    /// <summary>A valid session cookie that does arrive on a cross-site request (a browser that does not enforce Strict)
    /// is served as before: the gate lets it through and no re-navigation page is written.</summary>
    [Fact]
    public async Task ACrossSitePageRequest_WithAValidCookie_IsServedAsBefore()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);
        var exchange = await PostSignIn(server, "localhost", IPAddress.Loopback, Token);
        var setCookie = Assert.Single(exchange.Response.Headers.SetCookie) ?? string.Empty;
        var cookie = setCookie[..setCookie.IndexOf(';')];

        // A path nothing maps answers 404 once the gate lets the request through (the sign-in form or the re-navigation page is 200).
        var (ctx, body) = await SendPage(server, "/some/unmapped/path", string.Empty, "localhost", IPAddress.Loopback, "cross-site", cookie: cookie);

        Assert.Equal(StatusCodes.Status404NotFound, ctx.Response.StatusCode);
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
    }

    /// <summary>
    /// A navigation the person started (a click or a typed address) carries <c>Sec-Fetch-User: ?1</c>; a script
    /// redirect, a meta refresh or a popup nobody clicked does not. The re-navigation page is for the first kind only.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("?0")]
    [InlineData("1")]
    [InlineData("true")]
    public async Task ACrossSitePageRequest_NotStartedByAPerson_GetsTheSignInForm_AsBefore(string? fetchUser)
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (ctx, body) = await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, "cross-site", fetchUser: fetchUser);
        var (_, plain) = await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, fetchSite: null, fetchMode: null, fetchDest: null, fetchUser: null);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
        Assert.Equal(plain, body);
        Assert.Contains("<form", body, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The same click-started cross-site request does get the re-navigation page, and only a GET does.</summary>
    [Theory]
    [InlineData("HEAD")]
    [InlineData("OPTIONS")]
    public async Task ACrossSiteHeadOrOptionsRequest_NeverGetsTheReNavigationPage(string method)
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (ctx, body) = await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, "cross-site", method: method);
        var (_, page) = await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, "cross-site");

        Assert.Contains(DarlingWebHostService.ReNavigationScriptBlock, page, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
        Assert.DoesNotContain("Go to the sign-in page", body, StringComparison.Ordinal);
        // The page's own policy (the hash of its script) is what a written page carries; neither method gets it.
        Assert.NotEqual(DarlingWebHostService.ReNavigationPagePolicy, ctx.Response.Headers["Content-Security-Policy"].ToString());
        Assert.Equal(DarlingWebHostService.ReNavigationPagePolicy, (await SendPage(server, "/triage", string.Empty, "localhost", IPAddress.Loopback, "cross-site")).Context.Response.Headers["Content-Security-Policy"].ToString());
    }

    /// <summary>The API is matched the way its no-store stamp is: any letter case, and the bare /api path.</summary>
    [Theory]
    [InlineData("/API/fleet")]
    [InlineData("/Api/fleet")]
    [InlineData("/api")]
    [InlineData("/API")]
    public async Task ACrossSiteRequestToTheApi_InAnyCase_OrTheBareApiPath_IsTheApi_NotThePage(string path)
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (ctx, body) = await SendPage(server, path, string.Empty, "localhost", IPAddress.Loopback, "cross-site");

        Assert.Equal(StatusCodes.Status401Unauthorized, ctx.Response.StatusCode);
        Assert.StartsWith("application/json", ctx.Response.ContentType, StringComparison.Ordinal);
        Assert.Contains("\"error\"", body, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
    }

    /// <summary>The sign-in flow's own callback arrives from the identity provider, a cross-site navigation: it is
    /// handled as the sign-in flow (here the stale-attempt answer), never answered with the re-navigation page.</summary>
    [Fact]
    public async Task ACrossSiteOidcCallback_ReachesTheSignInFlow_NotTheReNavigationPage()
    {
        using var server = await BuildServer(networkMode: true, oidcClient: FakeOidcClient());

        var (ctx, body) = await SendPage(server, "/auth/oidc/callback", "?code=a&state=s", ListenIp, InCidrRemote, "cross-site");

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Contains("stale or was not started", body, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
    }

    /// <summary>The page itself: the fallback message and a static link to the sign-in page, and the page-wide
    /// headers (no framing, no referrer, its own browsing-context group).</summary>
    [Fact]
    public async Task TheReNavigationPage_CarriesTheLinkAndTheHeaders_OfEveryOtherPage()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (ctx, body) = await SendPage(server, "/triage", "?q=1", "localhost", IPAddress.Loopback, "cross-site");

        Assert.Contains(DarlingWebHostService.ReNavigationScriptBlock, body, StringComparison.Ordinal);
        Assert.Contains("<a href='/'", body, StringComparison.Ordinal);
        Assert.Contains("Go to the sign-in page", body, StringComparison.Ordinal);
        Assert.Contains("Open the address again to continue.", body, StringComparison.Ordinal);
        Assert.Equal("DENY", ctx.Response.Headers.XFrameOptions.ToString());
        Assert.Contains("frame-ancestors 'none'", ctx.Response.Headers["Content-Security-Policy"].ToString(), StringComparison.Ordinal);
        Assert.Equal("no-referrer", ctx.Response.Headers["Referrer-Policy"].ToString());
        Assert.Equal("same-origin", ctx.Response.Headers["Cross-Origin-Opener-Policy"].ToString());
    }

    /// <summary>Every HTML page this host writes opens in its own browsing-context group; a JSON answer needs none.</summary>
    [Fact]
    public async Task TheSignInPage_CarriesTheOpenerPolicy_AndAnApiAnswerDoesNot()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (signIn, _) = await SendPage(server, "/", string.Empty, "localhost", IPAddress.Loopback, fetchSite: null, fetchMode: null, fetchDest: null, fetchUser: null);
        Assert.Equal("same-origin", signIn.Response.Headers["Cross-Origin-Opener-Policy"].ToString());

        var (api, _) = await SendPage(server, "/api/fleet", string.Empty, "localhost", IPAddress.Loopback, fetchSite: null, fetchMode: null, fetchDest: null, fetchUser: null);
        Assert.False(api.Response.Headers.ContainsKey("Cross-Origin-Opener-Policy"));
    }

    private static async Task<(HttpContext Context, string Body)> SendPage(
        TestServer server, string path, string query, string host, IPAddress? remote, string? fetchSite,
        string? fetchMode = "navigate", string? fetchDest = "document", string? cookie = null,
        string? fetchUser = "?1", string method = "GET")
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = method;
            if (fetchUser is not null) c.Request.Headers["Sec-Fetch-User"] = fetchUser;
            c.Request.Path = path;
            c.Request.QueryString = new QueryString(query);
            c.Request.Headers.Host = host;
            c.Connection.RemoteIpAddress = remote;
            if (fetchSite is not null) c.Request.Headers["Sec-Fetch-Site"] = fetchSite;
            if (fetchMode is not null) c.Request.Headers["Sec-Fetch-Mode"] = fetchMode;
            if (fetchDest is not null) c.Request.Headers["Sec-Fetch-Dest"] = fetchDest;
            if (cookie is not null) c.Request.Headers["Cookie"] = cookie;
        });
        return (ctx, await new StreamReader(ctx.Response.Body).ReadToEndAsync());
    }

    /// <summary>
    /// #5288: network mode configured, then degraded to loopback-only because the TLS certificate was refused, token
    /// resolved. The loopback-only server keeps the token-to-cookie gate (the host passes
    /// <c>requireTokenWhenLoopbackOnly</c> for exactly that start): no credential on an <c>/api/*</c> path gets 401
    /// and on a page route gets the login form, the right token is exchanged for a session cookie, and that cookie
    /// then gets past the gate. Every client presented the token before the certificate lapsed, so nothing that
    /// worked stops working.
    /// </summary>
    [Fact]
    public async Task TlsRefusal_DegradedLoopbackServer_StillRequiresTheToken()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var (api, apiBody) = await SendWithBody(server, "/api/fleet", "localhost", IPAddress.Loopback);
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Response.StatusCode);
        Assert.Contains("\"error\"", apiBody, StringComparison.Ordinal);

        var page = await Send(server, "/", "localhost", IPAddress.Loopback);
        Assert.Equal(StatusCodes.Status200OK, page.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", page.Response.ContentType);

        var wrong = await Send(server, "/api/fleet", "localhost", IPAddress.Loopback, token: "not-the-token");
        Assert.Equal(StatusCodes.Status401Unauthorized, wrong.Response.StatusCode);

        var exchange = await PostSignIn(server, "localhost", IPAddress.Loopback, Token);
        Assert.Equal(StatusCodes.Status302Found, exchange.Response.StatusCode);
        var setCookie = Assert.Single(exchange.Response.Headers.SetCookie) ?? string.Empty;
        var cookie = setCookie[..setCookie.IndexOf(';')];

        // A path nothing maps answers 404 once the gate lets the request through; without the cookie it is the login form.
        var withCookie = await Send(server, "/some/unmapped/path", "localhost", IPAddress.Loopback, cookie: cookie);
        Assert.Equal(StatusCodes.Status404NotFound, withCookie.Response.StatusCode);
    }

    /// <summary>
    /// A token the operator configured gates a loopback-only dashboard too. The production start resolves it with
    /// <see cref="DarlingWebHostService.ResolveLoopbackOnlyToken"/> and hands the answer to the pipeline as the access
    /// token with the loopback-only token gate on; this builds the pipeline from the same answer: an API call with no
    /// token is 401, a wrong token is 401, the right token is exchanged for a session cookie and the cookie passes.
    /// </summary>
    [Fact]
    public async Task LoopbackOnly_ConfiguredToken_IsRequired_RightTokenPasses()
    {
        var resolved = DarlingWebHostService.ResolveLoopbackOnlyToken(
            new WebNetworkConfig { Token = Token }, allowTokenlessLoopback: false, NullLogger.Instance);
        Assert.False(resolved.Refuse);
        Assert.Equal(Token, resolved.Token);

        using var server = await BuildServer(
            networkMode: false, requireTokenWhenLoopbackOnly: resolved.Token.Length > 0, accessToken: resolved.Token);

        var (api, apiBody) = await SendWithBody(server, "/api/fleet", "localhost", IPAddress.Loopback);
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Response.StatusCode);
        Assert.Contains("\"error\"", apiBody, StringComparison.Ordinal);

        var wrong = await Send(server, "/api/fleet", "localhost", IPAddress.Loopback, token: "not-the-token");
        Assert.Equal(StatusCodes.Status401Unauthorized, wrong.Response.StatusCode);

        var exchange = await PostSignIn(server, "localhost", IPAddress.Loopback, Token);
        Assert.Equal(StatusCodes.Status302Found, exchange.Response.StatusCode);
        var setCookie = Assert.Single(exchange.Response.Headers.SetCookie) ?? string.Empty;
        var cookie = setCookie[..setCookie.IndexOf(';')];

        var withCookie = await Send(server, "/some/unmapped/path", "localhost", IPAddress.Loopback, cookie: cookie);
        Assert.Equal(StatusCodes.Status404NotFound, withCookie.Response.StatusCode);
    }

    /// <summary>A pipeline configured with a token under the length floor and no explicit reads-only reason works the reason out
    /// itself: the sign-in works and reads pass, and a write is refused 403 with <see cref="DarlingWebHostService.ShortTokenWriteRefusedMessage"/>.
    /// Before, a caller that left the argument out got writes with a short token.</summary>
    [Fact]
    public async Task LoopbackOnly_ShortToken_WithNoExplicitReason_ServesReadsAndRefusesWrites()
    {
        const string shortToken = "tiny";
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true, accessToken: shortToken);
        var exchange = await PostSignIn(server, "localhost", IPAddress.Loopback, shortToken);
        Assert.Equal(StatusCodes.Status302Found, exchange.Response.StatusCode);
        var setCookie = Assert.Single(exchange.Response.Headers.SetCookie) ?? string.Empty;
        var cookie = setCookie[..setCookie.IndexOf(';')];

        var read = await Send(server, "/some/unmapped/path", "localhost", IPAddress.Loopback, cookie: cookie);
        Assert.Equal(StatusCodes.Status404NotFound, read.Response.StatusCode);

        var write = await server.SendAsync(ctx =>
        {
            ctx.Request.Method = "POST";
            ctx.Request.Path = "/api/some-write";
            ctx.Request.Headers.Host = "localhost";
            ctx.Request.Headers["Cookie"] = cookie;
            ctx.Connection.RemoteIpAddress = IPAddress.Loopback;
        });
        Assert.Equal(StatusCodes.Status403Forbidden, write.Response.StatusCode);
        var body = await new StreamReader(write.Response.Body).ReadToEndAsync();
        Assert.Contains("reads only", body, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>No token configured, loopback-only: with <c>web.allowTokenlessLoopback</c> on the dashboard serves
    /// local browsers with no token, for reads only; with it off the start is refused, blank token or no block alike.</summary>
    [Fact]
    public async Task LoopbackOnly_NoTokenConfigured_StaysOpenOnlyWithTheOptOut()
    {
        Assert.True(DarlingWebHostService.ResolveLoopbackOnlyToken(
            new WebNetworkConfig { Token = "   " }, allowTokenlessLoopback: false, NullLogger.Instance).MissingToken);
        Assert.True(DarlingWebHostService.ResolveLoopbackOnlyToken(null, allowTokenlessLoopback: false, NullLogger.Instance).Refuse);

        var resolved = DarlingWebHostService.ResolveLoopbackOnlyToken(
            new WebNetworkConfig { Token = "   " }, allowTokenlessLoopback: true, NullLogger.Instance);
        Assert.False(resolved.Refuse);
        Assert.Equal("", resolved.Token);
        Assert.Equal("", DarlingWebHostService.ResolveLoopbackOnlyToken(null, allowTokenlessLoopback: true, NullLogger.Instance).Token);

        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: resolved.Token.Length > 0);
        var ctx = await Send(server, "/some/unmapped/path", "localhost", IPAddress.Loopback);

        Assert.Equal(StatusCodes.Status404NotFound, ctx.Response.StatusCode);
    }

    /// <summary>A configured token that cannot be decrypted stops the start (Critical line, no token in it) instead of
    /// serving the loopback dashboard open.</summary>
    [Fact]
    public void LoopbackOnly_ConfiguredTokenThatCannotBeUsed_RefusesTheStart_WithOneCriticalLine()
    {
        var log = new CapturingTestLogger();
        var resolved = DarlingWebHostService.ResolveLoopbackOnlyToken(
            new WebNetworkConfig { EncryptedToken = "not-a-protected-blob" }, allowTokenlessLoopback: true, new CapturingHostLogger(log));

        Assert.True(resolved.Refuse);
        Assert.Equal("", resolved.Token);
        Assert.Contains(log.Lines, l => l.Contains("Web dashboard token", StringComparison.OrdinalIgnoreCase)
            && l.Contains("web dashboard is not started", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(log.Lines, l => l.Contains("not-a-protected-blob", StringComparison.Ordinal));
    }

    private static int FreeTcpPort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    /// <summary>
    /// Network mode with a token that is configured but cannot be used (a blob this host cannot decrypt, a variable
    /// that is not set) does not start the dashboard at all, exactly as a loopback-only start does: the real start
    /// returns false, logs one Critical line, and nothing answers on the loopback port.
    /// </summary>
    [Theory]
    [InlineData("\"encryptedToken\": \"not-a-protected-blob\"")]
    [InlineData("\"token\": \"env:DARLING_TEST_WEB_TOKEN_THAT_IS_NOT_SET\"")]
    public async Task NetworkMode_ConfiguredTokenThatCannotBeUsed_DoesNotStart_AndNothingAnswersOnTheLoopbackPort(string tokenJson)
    {
        var port = FreeTcpPort();
        var config = DarlingConfig.Parse(
            ("{ 'postgres': { 'managed': true }, 'servers': [ { 'host': 'SQL2022' } ], 'web': { 'enabled': true, 'port': "
            + port + ", 'network': { 'listen': '" + ListenIp + "', 'allowFrom': '" + AllowedCidr + "', " + tokenJson + " } } }")
            .Replace('\'', '"'));
        var log = new CapturingTestLogger();
        var host = new DarlingWebHostService(
            new CapturingHostLogger(log), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
        var toggle = new DarlingHostBinding.EndpointToggle(true, port, DarlingHostBinding.EndpointToggleOrigin.File, false, false);
        var tryStart = typeof(DarlingWebHostService).GetMethod(
            "TryStartServerAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var started = true;
        var answered = false;
        try
        {
            started = await (Task<bool>)tryStart.Invoke(host, [config, toggle, System.Threading.CancellationToken.None])!;
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(5));
                await client.ConnectAsync(IPAddress.Loopback, port, cts.Token);
                answered = true;
            }
            catch (System.Net.Sockets.SocketException)
            {
            }
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }

        Assert.False(started, "a configured token that cannot be used must stop the start in network mode");
        Assert.False(answered, "nothing may answer on the loopback port");
        Assert.Contains(log.Lines, l => l.Contains("Web dashboard token", StringComparison.OrdinalIgnoreCase)
            && l.Contains("web dashboard is not started", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(log.Lines, l => l.Contains("not-a-protected-blob", StringComparison.Ordinal));
    }

    /// <summary>The Host guard still runs first on the token-keeping loopback-only server, so a foreign Host is 400
    /// even with the right token.</summary>
    [Fact]
    public async Task TlsRefusal_DegradedLoopbackServer_ForeignHostIsStill400()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);
        var ctx = await Send(server, "/", "evil.com", IPAddress.Loopback, token: Token);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
    }

    /// <summary>
    /// #5288: the degraded loopback-only server listens on loopback only, so its Host guard admits loopback names
    /// only. A request that names the configured listen address is answered 400 even with the right token, because
    /// nothing is listening there any more, while a loopback name with the same token is exchanged for a session
    /// cookie (past every gate). The server is built through <c>ResolveHostGuardListenIp</c> with the final mode, the
    /// way the host builds it.
    /// </summary>
    [Fact]
    public async Task TlsRefusal_DegradedLoopbackServer_AdmitsLoopbackNamesOnly_NotTheListenAddress()
    {
        using var server = await BuildServer(networkMode: false, requireTokenWhenLoopbackOnly: true);

        var named = await Send(server, "/", ListenIp, IPAddress.Loopback, token: Token);
        Assert.Equal(StatusCodes.Status400BadRequest, named.Response.StatusCode);

        var loopbackName = await PostSignIn(server, "localhost", IPAddress.Loopback, Token);
        Assert.Equal(StatusCodes.Status302Found, loopbackName.Response.StatusCode);
    }

    /// <summary>
    /// #5288: the web checks the CIDR before it looks at any credential, and keeps doing so: an address outside
    /// <c>allowFrom</c> is answered 403 whatever it sends (no token, a wrong token, the right token), on a page
    /// route and on an <c>/api/*</c> path alike. An in-list client with the wrong token is not refused with a 403.
    /// </summary>
    [Theory]
    [InlineData("/", null)]
    [InlineData("/", "not-the-token")]
    [InlineData("/", Token)]
    [InlineData("/api/fleet", null)]
    [InlineData("/api/fleet", "not-the-token")]
    [InlineData("/api/fleet", Token)]
    public async Task OffListRemote_RightOrWrongToken_Both403(string path, string? token)
    {
        using var server = await BuildServer(networkMode: true);

        var offList = await Send(server, path, ListenIp, IPAddress.Parse("203.0.113.50"), token: token);
        Assert.Equal(StatusCodes.Status403Forbidden, offList.Response.StatusCode);

        if (token == "not-the-token")
        {
            var inList = await Send(server, path, ListenIp, InCidrRemote, token: token);
            Assert.NotEqual(StatusCodes.Status403Forbidden, inList.Response.StatusCode);
        }
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

    /// <summary>#5239 L2: the Manage Servers route is refused for an anonymous network-mode caller through the real
    /// pipeline, the same as <c>/api/fleet</c>.</summary>
    [Fact]
    public async Task NetworkMode_NoToken_AdminServersPath_Returns401Json()
    {
        using var server = await BuildServer(networkMode: true);
        var (ctx, body) = await SendWithBody(server, "/api/admin/servers", ListenIp, InCidrRemote);

        Assert.Equal(StatusCodes.Status401Unauthorized, ctx.Response.StatusCode);
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

    /* ---- #5245: a single-value query key sent more than once is refused, never read as the joined "a,b" ---- */

    private static DarlingWebOidcClient FakeOidcClient() => new(new DarlingWebOidcClient.ResolvedOptions(
        "https://idp.example.test", "client-id", null, "openid", null, null,
        Array.Empty<string>(), Array.Empty<string>()));

    /// <summary>A request with the query string exactly as given (the helpers above build theirs from one token).</summary>
    private static async Task<(HttpContext Context, string Body)> Fetch(TestServer server, string path, string rawQuery)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = path;
            c.Request.QueryString = new QueryString(rawQuery);
            c.Request.Headers.Host = ListenIp;
            c.Connection.RemoteIpAddress = InCidrRemote;
        });

        return (ctx, await new StreamReader(ctx.Response.Body).ReadToEndAsync());
    }

    /// <summary>A token in the URL is never compared, so neither a repeated key that joins to the configured value
    /// (<c>?token=left&amp;token=right</c> against <c>left,right</c>) nor the single-value form signs a client in.</summary>
    [Fact]
    public async Task NetworkMode_TokenInTheUrl_IsNeverAToken_RepeatedOrSingle()
    {
        using var server = await BuildServer(networkMode: true, accessToken: "left,right");

        var (repeated, _) = await Fetch(server, "/", "?token=left&token=right");
        Assert.Equal(StatusCodes.Status200OK, repeated.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", repeated.Response.ContentType);
        Assert.Equal(0, repeated.Response.Headers.SetCookie.Count);

        var (single, _) = await Fetch(server, "/", "?token=left%2Cright");
        Assert.Equal(StatusCodes.Status200OK, single.Response.StatusCode);
        Assert.Equal(0, single.Response.Headers.SetCookie.Count);
    }

    /// <summary>A repeated <c>?token=</c> is refused and still leaves one Token refusal line, as a wrong single token does:
    /// the line says a token in the URL is not accepted and holds neither token value (#5245).</summary>
    [Theory]
    [InlineData("/", StatusCodes.Status200OK)]
    [InlineData("/api/fleet", StatusCodes.Status401Unauthorized)]
    public async Task NetworkMode_TokenInTheUrl_LeavesOneRefusalLine_WithNoTokenValue(string path, int expectedStatus)
    {
        var hostLogger = new CapturingTestLogger();
        using var server = await BuildServer(networkMode: true, hostLogger: hostLogger);

        var (ctx, _) = await Fetch(server, path, "?token=first-value&token=second-value");
        Assert.Equal(expectedStatus, ctx.Response.StatusCode);

        var line = Assert.Single(hostLogger.Lines);
        Assert.StartsWith("Warning: Web dashboard ", line, StringComparison.Ordinal);
        Assert.Contains("a token was presented in the URL", line, StringComparison.Ordinal);
        Assert.DoesNotContain("first-value", line, StringComparison.Ordinal);
        Assert.DoesNotContain("second-value", line, StringComparison.Ordinal);
        Assert.DoesNotContain(Token, line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OidcLogin_RepeatedReturnKey_IsRefusedBeforeTheProviderIsAsked()
    {
        using var server = await BuildServer(networkMode: true, oidcClient: FakeOidcClient());

        var (repeated, body) = await Fetch(server, "/auth/oidc/login", "?return=%2Fa&return=%2Fb");
        Assert.Equal(StatusCodes.Status400BadRequest, repeated.Response.StatusCode);
        Assert.Contains("repeated a parameter", body, StringComparison.Ordinal);

        /* One return key takes the old path: the discovery fetch (this IdP does not exist), not the refusal. */
        var (_, singleBody) = await Fetch(server, "/auth/oidc/login", "?return=%2Fa");
        Assert.DoesNotContain("repeated a parameter", singleBody, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("?error=a&error=b")]
    [InlineData("?error=access_denied&error_description=a&error_description=b")]
    [InlineData("?code=a&code=b&state=s")]
    [InlineData("?code=a&state=s&state=t")]
    public async Task OidcCallback_RepeatedKey_IsRefused(string rawQuery)
    {
        using var server = await BuildServer(networkMode: true, oidcClient: FakeOidcClient());

        var (ctx, body) = await Fetch(server, "/auth/oidc/callback", rawQuery);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Contains("repeated a parameter", body, StringComparison.Ordinal);
    }

    /// <summary>Each key once behaves as it always did: a provider error is the provider's 403, and a code with no
    /// transaction behind it is the stale-attempt 400.</summary>
    [Theory]
    [InlineData("?error=access_denied&error_description=nope", StatusCodes.Status403Forbidden, "refused the sign-in")]
    [InlineData("?code=a&state=s", StatusCodes.Status400BadRequest, "stale or was not started")]
    public async Task OidcCallback_EachKeyOnce_BehavesAsBefore(string rawQuery, int expectedStatus, string expectedText)
    {
        using var server = await BuildServer(networkMode: true, oidcClient: FakeOidcClient());

        var (ctx, body) = await Fetch(server, "/auth/oidc/callback", rawQuery);

        Assert.Equal(expectedStatus, ctx.Response.StatusCode);
        Assert.Contains(expectedText, body, StringComparison.Ordinal);
        Assert.DoesNotContain("repeated a parameter", body, StringComparison.Ordinal);
    }
}
