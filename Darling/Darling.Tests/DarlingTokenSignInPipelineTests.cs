/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
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
/// #8735: <c>web.network.oidc.tokenSignIn</c> through the SAME <c>ConfigurePipeline</c> the production start calls.
/// A refused POST to <c>/auth/login</c> answers 403 with the fixed code, whatever token was posted, before the form is
/// read or the throttle is touched; the login page's token field follows the mode.
/// </summary>
public sealed class DarlingTokenSignInPipelineTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string Token = "correct-token-value";
    private const string NamedToken = "named-token-value-123456789012";
    private static readonly IPAddress Lan = IPAddress.Parse("192.168.1.50");
    private static readonly byte[] SigningKey = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();

    private static async Task<TestServer> Build(
        string mode, IReadOnlyList<IPNetwork>? trustedProxies = null, bool oidc = false, string allowFrom = AllowedCidr)
    {
        Assert.True(DarlingTokenSignIn.TryParse(mode, out var parsed));

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();
        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(), new CollectorRuntimeState(),
            new WebTlsCertificateState(), new BaselineCache());

        DarlingWebOidcClient? client = oidc
            ? new DarlingWebOidcClient(
                new DarlingWebOidcClient.ResolvedOptions(
                    "https://idp.example.test", "client-id", null, "openid", null, null,
                    Array.Empty<string>(), Array.Empty<string>()),
                new StubDiscoveryHandler())
            : null;

        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        host.ConfigurePipeline(
            app, postgres,
            networkMode: true,
            networkListenIp: IPAddress.Parse(ListenIp),
            allowedCidr: IPNetwork.Parse(allowFrom),
            accessToken: Token,
            oidcClient: client,
            publicBaseUrlHost: "monitor.example.com",
            sessionSigningKeyForTests: SigningKey,
            trustedProxies: trustedProxies,
            utcNow: () => now,
            webTokens: DarlingWebNamedTokenTests.SetFor(DarlingWebNamedTokenTests.Entry("operator", NamedToken, "operator")),
            tokenSignIn: parsed);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<(HttpContext Ctx, string Body)> Post(
        TestServer server, IPAddress remote, string? token, string? returnPath = "/dash", string host = ListenIp,
        Action<HttpContext>? configure = null)
    {
        var bytes = token is null
            ? Array.Empty<byte>()
            : Encoding.UTF8.GetBytes($"token={Uri.EscapeDataString(token)}"
                + (returnPath is null ? string.Empty : $"&return={Uri.EscapeDataString(returnPath)}"));
        using var sink = new MemoryStream();
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = DarlingWebHostService.LoginPath;
            c.Request.Headers.Host = host;
            c.Request.Headers.Origin = "http://" + host;
            c.Request.Headers["Sec-Fetch-Site"] = "same-origin";
            if (token is not null)
            {
                c.Request.ContentType = "application/x-www-form-urlencoded";
                c.Request.ContentLength = bytes.Length;
                c.Request.Body = new MemoryStream(bytes);
            }

            c.Connection.RemoteIpAddress = remote;
            c.Response.Body = sink;
            configure?.Invoke(c);
        });
        return (ctx, Encoding.UTF8.GetString(sink.ToArray()));
    }

    private static async Task<(HttpContext Ctx, string Body)> Get(
        TestServer server, IPAddress remote, string path, string? cookie = null, string host = ListenIp)
    {
        using var sink = new MemoryStream();
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = path;
            c.Request.Headers.Host = host;
            c.Request.Headers["Sec-Fetch-Site"] = "same-origin";
            if (cookie is not null)
            {
                c.Request.Headers["Cookie"] = cookie;
            }

            c.Connection.RemoteIpAddress = remote;
            c.Response.Body = sink;
        });
        return (ctx, Encoding.UTF8.GetString(sink.ToArray()));
    }

    private static void AssertRefused((HttpContext Ctx, string Body) answer)
    {
        Assert.Equal(StatusCodes.Status403Forbidden, answer.Ctx.Response.StatusCode);
        Assert.Contains(DarlingTokenSignIn.RefusalMarkerHtml, answer.Body, StringComparison.Ordinal);
        Assert.Equal(0, answer.Ctx.Response.Headers.SetCookie.Count);
    }

    [Theory]
    [InlineData(Token)]
    [InlineData(NamedToken)]
    public async Task Loopback_LanPost_WithValidToken_Is403WithCode(string token)
    {
        using var server = await Build("loopback");
        AssertRefused(await Post(server, Lan, token));
    }

    [Fact]
    public async Task Refusal_IsIdentical_ForValidSharedNamedAndGarbage()
    {
        using var server = await Build("loopback");
        var answers = new[]
        {
            await Post(server, Lan, Token),
            await Post(server, Lan, NamedToken),
            await Post(server, Lan, "garbage"),
            await Post(server, Lan, null),
        };

        foreach (var answer in answers)
        {
            AssertRefused(answer);
        }

        Assert.Single(answers.Select(a => a.Ctx.Response.StatusCode).Distinct());
        Assert.Single(answers.Select(a => a.Body).Distinct());
        var headerSets = answers
            .Select(a => string.Join("|", a.Ctx.Response.Headers
                .Where(h => !string.Equals(h.Key, "Date", StringComparison.OrdinalIgnoreCase))
                .OrderBy(h => h.Key, StringComparer.OrdinalIgnoreCase)
                .Select(h => h.Key + "=" + h.Value)))
            .Distinct()
            .ToList();
        Assert.Single(headerSets);
    }

    [Fact]
    public async Task Refusal_IsNotCounted_ByTheThrottle()
    {
        using var server = await Build("loopback");
        for (var i = 0; i < 12; i++)
        {
            var answer = await Post(server, Lan, "garbage");
            Assert.Equal(StatusCodes.Status403Forbidden, answer.Ctx.Response.StatusCode);
            Assert.NotEqual(StatusCodes.Status429TooManyRequests, answer.Ctx.Response.StatusCode);
        }
    }

    [Fact]
    public async Task Refusal_KeepsAnExistingOidcSession()
    {
        using var server = await Build("loopback", oidc: true);
        var value = DarlingWebHostService.BuildSessionCookieValue(
            SigningKey, DateTimeOffset.UtcNow.AddHours(1),
            DarlingWebSeat.EncodeCookieSubject("alice@example.com", WebOidcRole.Admin, new string('k', DarlingWebSeat.OwnerKeyLength)));
        var cookie = DarlingWebHostService.SessionCookieName + "=" + value;

        // A path nothing maps answers 404 once the gate lets the session through; the sign-in page is 200.
        var before = await Get(server, Lan, "/some/unmapped/path", cookie);
        Assert.Equal(StatusCodes.Status404NotFound, before.Ctx.Response.StatusCode);

        var refused = await Post(server, Lan, Token, configure: c => c.Request.Headers["Cookie"] = cookie);
        AssertRefused(refused);

        var after = await Get(server, Lan, "/some/unmapped/path", cookie);
        Assert.Equal(StatusCodes.Status404NotFound, after.Ctx.Response.StatusCode);
    }

    [Theory]
    [InlineData(Token)]
    [InlineData(NamedToken)]
    public async Task Loopback_LoopbackPost_SignsIn_SharedAndNamed(string token)
    {
        using var server = await Build("loopback");
        var answer = await Post(server, IPAddress.Loopback, token);

        Assert.Equal(StatusCodes.Status302Found, answer.Ctx.Response.StatusCode);
        Assert.True(answer.Ctx.Response.Headers.SetCookie.Count > 0);
    }

    [Fact]
    public async Task Loopback_LoopbackWrongToken_RerendersWithTokenFieldAndReturn()
    {
        using var server = await Build("loopback", oidc: true);
        var answer = await Post(server, IPAddress.Loopback, "wrong-token-value", "/dash/page");

        Assert.Equal(StatusCodes.Status200OK, answer.Ctx.Response.StatusCode);
        Assert.Contains("name='token'", answer.Body, StringComparison.Ordinal);
        Assert.Contains("id='return'", answer.Body, StringComparison.Ordinal);
        Assert.Contains("/dash/page", answer.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Loopback_LocalTrustedProxyForwardingLan_Is403()
    {
        Assert.True(DarlingHostBinding.TryParseTrustedProxies(new[] { "127.0.0.1" }, out var proxies, out _));
        using var server = await Build("loopback", trustedProxies: proxies);

        var answer = await Post(server, IPAddress.Loopback, Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-For"] = Lan.ToString();
        });

        AssertRefused(answer);
    }

    [Fact]
    public async Task Loopback_RemoteProxyForwardingLoopbackAddress_Is403()
    {
        /* The forwarded address rewrites the remote to 127.0.0.1 (admitted here by allowFrom), but the connection itself
           came from the proxy, so the stashed connection address (not the rewritten one) decides and the sign-in is
           refused. A handler that read no connection address would accept this one. */
        Assert.True(DarlingHostBinding.TryParseTrustedProxies(new[] { "192.168.1.10" }, out var proxies, out _));
        using var server = await Build("loopback", trustedProxies: proxies, allowFrom: "0.0.0.0/0");

        var answer = await Post(server, IPAddress.Parse("192.168.1.10"), Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-For"] = "127.0.0.1";
        });

        AssertRefused(answer);
    }

    [Fact]
    public async Task Off_LoopbackPost_Is403()
    {
        using var server = await Build("off");
        AssertRefused(await Post(server, IPAddress.Loopback, Token));
    }

    [Fact]
    public async Task LoginGet_TokenFieldFollowsTheMode()
    {
        using (var loopback = await Build("loopback", oidc: true))
        {
            Assert.DoesNotContain("name='token'", (await Get(loopback, Lan, DarlingWebHostService.LoginPath)).Body, StringComparison.Ordinal);
            Assert.Contains("name='token'", (await Get(loopback, IPAddress.Loopback, DarlingWebHostService.LoginPath)).Body, StringComparison.Ordinal);
        }

        using (var off = await Build("off", oidc: true))
        {
            Assert.DoesNotContain("name='token'", (await Get(off, Lan, DarlingWebHostService.LoginPath)).Body, StringComparison.Ordinal);
            Assert.DoesNotContain("name='token'", (await Get(off, IPAddress.Loopback, DarlingWebHostService.LoginPath)).Body, StringComparison.Ordinal);
        }

        using (var on = await Build("on", oidc: true))
        {
            Assert.Contains("name='token'", (await Get(on, Lan, DarlingWebHostService.LoginPath)).Body, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task On_IsUnchanged()
    {
        using var server = await Build("on");
        var answer = await Post(server, Lan, Token);

        Assert.Equal(StatusCodes.Status302Found, answer.Ctx.Response.StatusCode);
        Assert.True(answer.Ctx.Response.Headers.SetCookie.Count > 0);
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
                Content = new StringContent(document, Encoding.UTF8, "application/json"),
            });
        }
    }
}
