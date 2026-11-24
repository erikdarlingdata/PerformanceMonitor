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
        string mode, IReadOnlyList<IPNetwork>? trustedProxies = null, bool oidc = false, string allowFrom = AllowedCidr,
        IReadOnlyList<string>? mcpTokens = null, ILogger<DarlingWebHostService>? logger = null,
        DarlingWebGroupTokens? managedGroups = null)
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
            logger ?? NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(), new CollectorRuntimeState(),
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
            mcpTokens: mcpTokens,
            managedGroups: managedGroups,
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
        TestServer server, IPAddress remote, string path, string? cookie = null, string host = ListenIp,
        Action<HttpContext>? configure = null)
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
            configure?.Invoke(c);
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
    public async Task Loopback_RemoteProxyForwardingLoopbackAddress_SharedCookie_IsSignedOut()
    {
        /* The twin of the test above for the per-request gate: the forwarded 127.0.0.1 passes allowFrom, but the connection came from
           the proxy, so a token session is not accepted. A gate that judged the forwarded address would serve this cookie. */
        Assert.True(DarlingHostBinding.TryParseTrustedProxies(new[] { "192.168.1.10" }, out var proxies, out _));
        using var server = await Build("loopback", oidc: true, trustedProxies: proxies, allowFrom: "0.0.0.0/0");

        AssertSignedOut(await Get(server, IPAddress.Parse("192.168.1.10"), UnmappedPath, SharedCookie(), configure: c =>
        {
            c.Request.Headers["X-Forwarded-For"] = "127.0.0.1";
        }));
    }

    /// <summary>Forwards each line to a <see cref="CapturingTestLogger"/>.</summary>
    private sealed class CapturedHostLogger(CapturingTestLogger inner) : ILogger<DarlingWebHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => inner.Log(logLevel, eventId, state, exception, formatter);
    }

    [Theory]
    [InlineData(UnmappedPath, "HTTP 200")]
    [InlineData("/api/session", "HTTP 401")]
    public async Task Gate_RefusedTokenSession_LogsTheModeAndTheStatusTheClientGets(string path, string expectedStatus)
    {
        var log = new CapturingTestLogger();
        using var server = await Build("loopback", oidc: true, logger: new CapturedHostLogger(log));

        await Get(server, Lan, path, SharedCookie());

        var line = Assert.Single(log.Lines, l => l.Contains("tokenSignIn", StringComparison.Ordinal));
        Assert.Contains("'loopback'", line, StringComparison.Ordinal);
        Assert.Contains("not accepted from this connection", line, StringComparison.Ordinal);
        Assert.Contains(expectedStatus, line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Handler_RefusedSignIn_LogsTheModeAndNeverTheToken()
    {
        var log = new CapturingTestLogger();
        using var server = await Build("off", logger: new CapturedHostLogger(log));

        await Post(server, Lan, Token);

        var line = Assert.Single(log.Lines, l => l.Contains("tokenSignIn", StringComparison.Ordinal));
        Assert.Contains("'off'", line, StringComparison.Ordinal);
        Assert.Contains("HTTP 403", line, StringComparison.Ordinal);
        Assert.DoesNotContain(log.Lines, l => l.Contains(Token, StringComparison.Ordinal));
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

    private static string SharedCookie()
        => DarlingWebHostService.SessionCookieName + "=" + DarlingWebHostService.BuildSessionCookieValue(
            SigningKey, DateTimeOffset.UtcNow.AddHours(1));

    private static string NamedCookie(string name)
        => DarlingWebHostService.SessionCookieName + "=" + DarlingWebHostService.BuildSessionCookieValue(
            SigningKey, DateTimeOffset.UtcNow.AddHours(1), DarlingWebSeat.EncodeTokenCookieSubject(name));

    private static string OidcCookie()
        => DarlingWebHostService.SessionCookieName + "=" + DarlingWebHostService.BuildSessionCookieValue(
            SigningKey, DateTimeOffset.UtcNow.AddHours(1),
            DarlingWebSeat.EncodeCookieSubject("alice@example.com", WebOidcRole.Admin, new string('k', DarlingWebSeat.OwnerKeyLength)));

    /// <summary>A path nothing maps answers 404 once the gate lets the session through; the sign-in page is 200.</summary>
    private const string UnmappedPath = "/some/unmapped/path";

    private static void AssertSignedOut((HttpContext Ctx, string Body) answer)
    {
        Assert.Equal(StatusCodes.Status200OK, answer.Ctx.Response.StatusCode);
        Assert.DoesNotContain("name='token'", answer.Body, StringComparison.Ordinal);
        Assert.Contains("id='return'", answer.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Loopback_SharedCookieFromLan_IsSignedOut()
    {
        using var server = await Build("loopback", oidc: true);
        AssertSignedOut(await Get(server, Lan, UnmappedPath, SharedCookie()));
    }

    [Fact]
    public async Task Loopback_NamedCookieFromLan_IsSignedOut()
    {
        using var server = await Build("loopback", oidc: true);
        var cookie = NamedCookie("operator");

        Assert.Equal(StatusCodes.Status404NotFound, (await Get(server, IPAddress.Loopback, UnmappedPath, cookie)).Ctx.Response.StatusCode);
        AssertSignedOut(await Get(server, Lan, UnmappedPath, cookie));
    }

    private static string GroupCookie(DarlingWebGroupTokens.Match match)
        => DarlingWebHostService.SessionCookieName + "=" + DarlingWebHostService.BuildSessionCookieValue(
            SigningKey, DateTimeOffset.UtcNow.AddHours(1), DarlingWebSeat.EncodeGroupCookieSubject(match.GroupId, match.Generation));

    [Fact]
    public async Task Loopback_GroupCookieFromLan_IsSignedOut_WithNoGroupLookup_WhileLoopbackReachesTheLookup()
    {
        /* The group lookup is observed through the metadata fake the group tokens read from: a refused cookie must not cost a store trip,
           and the loopback control proves the counter does see a lookup (a null managedGroups would also sign the cookie out, and would
           prove nothing). */
        using var rig = new ManagedWebGroupGuardTests.Rig();
        var cookie = GroupCookie(await rig.RefreshedMatchAsync());
        using var server = await Build("loopback", oidc: true, managedGroups: rig.Groups);
        var baseline = rig.Metadata.Reads;

        AssertSignedOut(await Get(server, Lan, UnmappedPath, cookie));
        var api = await Get(server, Lan, "/api/session", cookie);
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Ctx.Response.StatusCode);
        Assert.StartsWith("application/json", api.Ctx.Response.ContentType, StringComparison.Ordinal);
        Assert.Equal(baseline, rig.Metadata.Reads);

        var control = await Get(server, IPAddress.Loopback, UnmappedPath, cookie);
        Assert.Equal(StatusCodes.Status404NotFound, control.Ctx.Response.StatusCode);
        Assert.Equal(baseline + 1, rig.Metadata.Reads);
    }

    [Fact]
    public async Task Loopback_SharedCookieFromLan_OnApi_Gets401Json()
    {
        using var server = await Build("loopback", oidc: true);
        var answer = await Get(server, Lan, "/api/session", SharedCookie());

        Assert.Equal(StatusCodes.Status401Unauthorized, answer.Ctx.Response.StatusCode);
        Assert.StartsWith("application/json", answer.Ctx.Response.ContentType, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Loopback_SharedCookieFromLoopback_IsServed()
    {
        using var server = await Build("loopback", oidc: true);
        var answer = await Get(server, IPAddress.Loopback, UnmappedPath, SharedCookie());

        Assert.Equal(StatusCodes.Status404NotFound, answer.Ctx.Response.StatusCode);
    }

    [Fact]
    public async Task Loopback_OidcCookieFromLan_IsServed()
    {
        using var server = await Build("loopback", oidc: true);
        var answer = await Get(server, Lan, UnmappedPath, OidcCookie());

        Assert.Equal(StatusCodes.Status404NotFound, answer.Ctx.Response.StatusCode);
    }

    [Fact]
    public async Task Off_SharedCookieFromLoopback_IsSignedOut()
    {
        using var server = await Build("off", oidc: true);
        AssertSignedOut(await Get(server, IPAddress.Loopback, UnmappedPath, SharedCookie()));
    }

    [Fact]
    public void Off_InLoopbackOnlyModeWithoutToken_PipelineRefusesToBuild()
    {
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

        Assert.Throws<InvalidOperationException>(() => host.ConfigurePipeline(
            app, postgres,
            networkMode: false,
            networkListenIp: null,
            allowedCidr: IPNetwork.Parse(AllowedCidr),
            accessToken: string.Empty,
            oidcClient: null,
            requireTokenWhenLoopbackOnly: false,
            tokenSignIn: TokenSignInMode.Off));
    }

    [Fact]
    public async Task Loopback_RefusedPost_ModeCheckRunsBeforeSameSiteAndMcpSameValueChecks()
    {
        using var server = await Build("loopback", mcpTokens: new[] { Token });
        var answer = await Post(server, Lan, Token, configure: c =>
        {
            c.Request.Headers.Origin = "http://evil.example.com";
            c.Request.Headers["Sec-Fetch-Site"] = "cross-site";
        });

        AssertRefused(answer);
    }

    [Fact]
    public async Task Loopback_LanGetOfGate_WithOidc_ServesNoTokenField()
    {
        using var server = await Build("loopback", oidc: true);
        var answer = await Get(server, Lan, "/");

        Assert.Equal(StatusCodes.Status200OK, answer.Ctx.Response.StatusCode);
        Assert.DoesNotContain("name='token'", answer.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Loopback_WithoutAnOidcClient_LanGetsTheLocalOnlyPage_AndTheNoLinkRefusal_WhileLoopbackKeepsTheTokenField()
    {
        using var server = await Build("loopback", oidc: false);

        foreach (var path in new[] { DarlingWebHostService.LoginPath, "/" })
        {
            var lanGet = await Get(server, Lan, path);
            Assert.Equal(StatusCodes.Status200OK, lanGet.Ctx.Response.StatusCode);
            Assert.DoesNotContain("name='token'", lanGet.Body, StringComparison.Ordinal);
            Assert.DoesNotContain("/auth/oidc/login", lanGet.Body, StringComparison.Ordinal);
            Assert.Contains("only from the machine that runs this service", lanGet.Body, StringComparison.Ordinal);
            Assert.Contains("form-action 'none'", lanGet.Ctx.Response.Headers.ContentSecurityPolicy.ToString(), StringComparison.Ordinal);
        }

        var lanPost = await Post(server, Lan, Token);
        AssertRefused(lanPost);
        Assert.DoesNotContain("/auth/oidc/login", lanPost.Body, StringComparison.Ordinal);

        var loopbackGet = await Get(server, IPAddress.Loopback, DarlingWebHostService.LoginPath);
        Assert.Contains("name='token'", loopbackGet.Body, StringComparison.Ordinal);
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
