/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web host's response hardening and sign-in behavior, through the SAME <c>ConfigurePipeline</c> the
/// production start calls (see <see cref="DarlingWebHostGateLiveTests"/> for why): the framing and sniffing
/// headers on every answer with HSTS only over HTTPS, the token accepted from the form's POST and never from
/// a URL, the growing per-source wait after failed attempts, and the forwarded headers honored from a
/// configured proxy only — which is what gives a proxied HTTPS request a Secure cookie and an https OIDC
/// redirect URI.
/// </summary>
public sealed class DarlingWebSignInHardeningTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string Token = "correct-token-value";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");
    private static readonly IPAddress ProxyRemote = IPAddress.Parse("192.168.1.10");
    private static readonly IPAddress NotAProxyRemote = IPAddress.Parse("192.168.1.11");

    private static async Task<TestServer> BuildServer(
        bool networkMode, IReadOnlyList<IPNetwork>? trustedProxies = null, DarlingWebOidcClient? oidcClient = null)
    {
        var builder = WebApplication.CreateBuilder();
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
            new BaselineCache());

        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: networkMode,
            networkListenIp: networkMode ? IPAddress.Parse(ListenIp) : null,
            allowedCidr: IPNetwork.Parse(AllowedCidr),
            accessToken: Token,
            oidcClient: oidcClient,
            publicBaseUrlHost: "monitor.example.com",
            trustedProxies: trustedProxies);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static Task<HttpContext> Get(TestServer server, string path, string host, IPAddress remote, Action<HttpContext>? configure = null)
        => server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = path;
            ctx.Request.Headers.Host = host;
            ctx.Connection.RemoteIpAddress = remote;
            configure?.Invoke(ctx);
        });

    private static IReadOnlyList<IPNetwork> OneProxy()
    {
        Assert.True(DarlingHostBinding.TryParseTrustedProxies(new[] { ProxyRemote.ToString() }, out var proxies, out _));
        return proxies;
    }

    /* ---------------- response hardening headers ---------------- */

    [Fact]
    public async Task EveryResponse_CarriesFramingAndSniffingHeaders_InLoopbackMode()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Get(server, "/some/unmapped/path", "localhost", IPAddress.Loopback);

        Assert.Equal(DarlingWebSecurityHeaders.ContentSecurityPolicy, ctx.Response.Headers.ContentSecurityPolicy.ToString());
        Assert.Equal("DENY", ctx.Response.Headers.XFrameOptions.ToString());
        Assert.Equal("nosniff", ctx.Response.Headers.XContentTypeOptions.ToString());
    }

    [Fact]
    public async Task TheLoginPage_CarriesTheHeadersToo()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Get(server, "/", ListenIp, InCidrRemote);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        /* The sign-in page sets its own policy (it carries inline blocks); it still refuses framing. */
        Assert.Contains("frame-ancestors 'none'", ctx.Response.Headers.ContentSecurityPolicy.ToString(), StringComparison.Ordinal);
        Assert.Equal("DENY", ctx.Response.Headers.XFrameOptions.ToString());
        Assert.Equal("nosniff", ctx.Response.Headers.XContentTypeOptions.ToString());
    }

    [Fact]
    public async Task Hsts_IsSentOverHttpsOnly()
    {
        using var server = await BuildServer(networkMode: false);

        var plain = await Get(server, "/x", "localhost", IPAddress.Loopback);
        Assert.Equal(0, plain.Response.Headers.StrictTransportSecurity.Count);

        var secure = await Get(server, "/x", "localhost", IPAddress.Loopback, ctx => ctx.Request.Scheme = "https");
        Assert.Equal(DarlingWebSecurityHeaders.StrictTransportSecurityValue, secure.Response.Headers.StrictTransportSecurity.ToString());
    }

    /* ---------------- the sign-in leg ---------------- */

    [Fact]
    public async Task GetOnTheSignInPath_RendersTheForm_AndAUrlTokenThereIsNotExchanged()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Get(server, DarlingWebHostService.LoginPath, ListenIp, InCidrRemote,
            c => c.Request.QueryString = new QueryString("?token=" + Uri.EscapeDataString(Token)));

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", ctx.Response.ContentType);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
    }

    [Fact]
    public async Task ASignInPostWithoutAFormBody_IsRefused()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = DarlingWebHostService.LoginPath;
            c.Request.Headers.Host = ListenIp;
            c.Request.ContentType = "application/json";
            c.Connection.RemoteIpAddress = InCidrRemote;
        });

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
    }

    [Fact]
    public async Task RepeatedWrongTokens_FromOneSource_AreHeldWithRetryAfter_AndTheRightTokenIsHeldToo()
    {
        using var server = await BuildServer(networkMode: true);

        /* The free failures cost nothing: each answers the login page with no wait. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            var refused = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "wrong-" + i);
            Assert.Equal(StatusCodes.Status200OK, refused.Response.StatusCode);
            Assert.Equal(0, refused.Response.Headers.SetCookie.Count);
        }

        /* One more failure starts the wait... */
        var last = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "wrong-again");
        Assert.Equal(StatusCodes.Status200OK, last.Response.StatusCode);

        /* ...and inside it even the RIGHT token is refused with 429 + Retry-After, never compared. */
        var held = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token);
        Assert.Equal(StatusCodes.Status429TooManyRequests, held.Response.StatusCode);
        Assert.True(int.Parse(held.Response.Headers.RetryAfter.ToString()) >= 1);
        Assert.Equal(0, held.Response.Headers.SetCookie.Count);

        /* Another source is not held by the first one's failures. */
        var other = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, IPAddress.Parse("192.168.1.51"), Token);
        Assert.Equal(StatusCodes.Status302Found, other.Response.StatusCode);
    }

    /* ---------------- forwarded headers from a configured proxy only ---------------- */

    [Fact]
    public async Task BehindAConfiguredProxy_AForwardedHttpsSignIn_MintsASecureCookie()
    {
        using var server = await BuildServer(networkMode: true, trustedProxies: OneProxy());
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, ProxyRemote, Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-Proto"] = "https";
            c.Request.Headers["X-Forwarded-For"] = InCidrRemote.ToString();
        });

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        var cookie = ctx.Response.Headers.SetCookie.ToString();
        Assert.Contains("secure", cookie, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(DarlingWebSecurityHeaders.StrictTransportSecurityValue, ctx.Response.Headers.StrictTransportSecurity.ToString());
    }

    [Fact]
    public async Task FromASourceThatIsNotAConfiguredProxy_TheForwardedHeadersAreIgnored()
    {
        using var server = await BuildServer(networkMode: true, trustedProxies: OneProxy());
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, NotAProxyRemote, Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-Proto"] = "https";
            c.Request.Headers["X-Forwarded-For"] = "10.0.0.9";
        });

        /* Not forbidden: the out-of-CIDR X-Forwarded-For was not believed, the in-CIDR connection was. And
           not Secure: the forwarded scheme was not believed either. */
        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        Assert.DoesNotContain("secure", ctx.Response.Headers.SetCookie.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, ctx.Response.Headers.StrictTransportSecurity.Count);
    }

    [Fact]
    public async Task WithNoProxiesConfigured_TheForwardedHeadersAreIgnoredFromEveryone()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, ProxyRemote, Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-Proto"] = "https";
        });

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        Assert.DoesNotContain("secure", ctx.Response.Headers.SetCookie.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BehindAConfiguredProxy_TheCidrGateJudgesTheForwardedClient()
    {
        using var server = await BuildServer(networkMode: true, trustedProxies: OneProxy());
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, ProxyRemote, Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-For"] = "10.0.0.9";
        });

        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
    }

    [Fact]
    public async Task BehindAConfiguredProxy_TheOidcRedirectUri_FollowsTheForwardedScheme()
    {
        var oidcClient = new DarlingWebOidcClient(
            new DarlingWebOidcClient.ResolvedOptions(
                "https://idp.example.test", "client-id", null, "openid", null, null,
                Array.Empty<string>(), Array.Empty<string>()),
            new StubDiscoveryHandler());

        using var server = await BuildServer(networkMode: true, trustedProxies: OneProxy(), oidcClient: oidcClient);
        var ctx = await Get(server, DarlingWebHostService.OidcLoginPath, "monitor.example.com", ProxyRemote, c =>
        {
            c.Request.Headers["X-Forwarded-Proto"] = "https";
            c.Request.Headers["X-Forwarded-For"] = InCidrRemote.ToString();
        });

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        var location = ctx.Response.Headers.Location.ToString();
        Assert.Contains(Uri.EscapeDataString("https://monitor.example.com" + DarlingWebHostService.OidcCallbackPath), location, StringComparison.Ordinal);
        Assert.Contains("secure", ctx.Response.Headers.SetCookie.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Answers the discovery document the OIDC login leg fetches, with nothing else.</summary>
    private sealed class StubDiscoveryHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            const string document = "{\"issuer\":\"https://idp.example.test\","
                + "\"authorization_endpoint\":\"https://idp.example.test/authorize\","
                + "\"token_endpoint\":\"https://idp.example.test/token\"}";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(document, System.Text.Encoding.UTF8, "application/json"),
            });
        }
    }
}
