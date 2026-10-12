/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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

    /* ---------------- the sign-in POST: its origin, its body in bytes, and one atomic count per attempt ---------------- */

    private static async Task<string> BodyOf(HttpContext ctx)
    {
        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        return await reader.ReadToEndAsync();
    }

    /// <summary>A body that counts the bytes the host asked of it, optionally after waiting at a gate.</summary>
    private sealed class MeteredBody : System.IO.Stream
    {
        private readonly System.IO.Stream _inner;
        private readonly Task? _gate;
        private readonly Action? _entered;
        private int _signalled;

        public MeteredBody(byte[] content, Task? gate = null, Action? entered = null)
        {
            _inner = new System.IO.MemoryStream(content);
            _gate = gate;
            _entered = entered;
        }

        public long BytesRead { get; private set; }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, System.IO.SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_gate is not null)
            {
                if (Interlocked.Exchange(ref _signalled, 1) == 0)
                {
                    _entered?.Invoke();
                }

                await _gate;
            }

            var read = await _inner.ReadAsync(buffer, cancellationToken);
            BytesRead += read;
            return read;
        }
    }

    [Theory]
    [InlineData("https://evil.example")]
    [InlineData("null")]
    [InlineData("http://192.168.1.205:8080")]
    [InlineData("not a url")]
    public async Task ACrossSiteSignInPost_IsRefused_BeforeItCountsAsAnAttempt(string origin)
    {
        using var server = await BuildServer(networkMode: true);

        for (var i = 0; i < DarlingSignInThrottle.FreeFailures * 3; i++)
        {
            var refused = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "wrong-" + i,
                configure: c => c.Request.Headers.Origin = origin);
            Assert.Equal(StatusCodes.Status403Forbidden, refused.Response.StatusCode);
            Assert.Equal(0, refused.Response.Headers.SetCookie.Count);
        }

        /* None of them counted: the source still has its whole free number, and its own page signs in. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            var miss = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "typo-" + i);
            Assert.Equal(StatusCodes.Status200OK, miss.Response.StatusCode);
        }

        var right = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token);
        Assert.Equal(StatusCodes.Status302Found, right.Response.StatusCode);
    }

    [Fact]
    public async Task ACrossSiteSignInPost_WithARightToken_IsRefusedToo_AndSecFetchSiteCrossSiteIsRefused()
    {
        using var server = await BuildServer(networkMode: true);

        var wrongSite = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token,
            configure: c => c.Request.Headers.Origin = "https://evil.example");
        Assert.Equal(StatusCodes.Status403Forbidden, wrongSite.Response.StatusCode);
        Assert.Equal(0, wrongSite.Response.Headers.SetCookie.Count);

        var fetchSite = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token,
            configure: c => c.Request.Headers["Sec-Fetch-Site"] = "cross-site");
        Assert.Equal(StatusCodes.Status403Forbidden, fetchSite.Response.StatusCode);

        /* With no Origin, the Referer decides. */
        var refererWrong = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token,
            configure: c => c.Request.Headers.Referer = "https://evil.example/page");
        Assert.Equal(StatusCodes.Status403Forbidden, refererWrong.Response.StatusCode);
    }

    [Theory]
    [InlineData("http://192.168.1.205", null)]
    [InlineData("HTTP://192.168.1.205", null)]
    [InlineData(null, "http://192.168.1.205/auth/login")]
    [InlineData("https://monitor.example.com", null)]
    [InlineData(null, null)]
    public async Task ASameSiteSignInPost_IsAccepted(string? origin, string? referer)
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token, configure: c =>
        {
            if (origin is not null)
            {
                c.Request.Headers.Origin = origin;
            }

            if (referer is not null)
            {
                c.Request.Headers.Referer = referer;
            }
        });

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        Assert.True(ctx.Response.Headers.SetCookie.Count > 0);
    }

    [Fact]
    public async Task TheLoginPage_KeepsTheRealOriginOnItsOwnPost()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Get(server, "/", ListenIp, InCidrRemote);

        /* The response header says no-referrer, and under that policy a browser would send "Origin: null"
           with the form's post, which the origin check cannot tell from a post made by another site. */
        Assert.Contains("<meta name='referrer' content='same-origin'>", await BodyOf(ctx), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABodyOverTheCap_IsCutOffOnBytesRead_WhateverTheFraming()
    {
        using var server = await BuildServer(networkMode: true);
        var huge = new MeteredBody(new byte[1024 * 1024]);

        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token, configure: c =>
        {
            /* Chunked: no Content-Length to test. */
            c.Request.ContentLength = null;
            c.Request.Headers.TransferEncoding = "chunked";
            c.Request.Body = huge;
        });

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
        Assert.True(huge.BytesRead <= DarlingWebHostService.MaxLoginFormBytes + 1,
            $"read {huge.BytesRead} bytes of a body capped at {DarlingWebHostService.MaxLoginFormBytes}");
    }

    [Fact]
    public async Task AMultipartBody_IsNotReadAtAll()
    {
        using var server = await BuildServer(networkMode: true);
        var multipart = new MeteredBody(System.Text.Encoding.UTF8.GetBytes(
            "--b\r\nContent-Disposition: form-data; name=\"token\"\r\n\r\n" + Token + "\r\n--b--\r\n"));

        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token, configure: c =>
        {
            c.Request.ContentType = "multipart/form-data; boundary=b";
            c.Request.ContentLength = null;
            c.Request.Body = multipart;
        });

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
        Assert.Equal(0, multipart.BytesRead);
    }

    [Fact]
    public async Task ASmallChunkedForm_StillSignsIn()
    {
        using var server = await BuildServer(networkMode: true);
        var form = new MeteredBody(System.Text.Encoding.UTF8.GetBytes("token=" + Uri.EscapeDataString(Token)));

        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token, configure: c =>
        {
            c.Request.ContentLength = null;
            c.Request.Body = form;
        });

        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ManyConcurrentWrongTokens_FromOneSource_CompareExactlyTheAllowance_AndTheRestAreHeld(bool staged)
    {
        using var server = await BuildServer(networkMode: true);
        const int Burst = 40;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;

        /* Staged: every request has sent its headers and is waiting in its body read. Releasing them together
           must still count them one by one, because the count happens after the body is in. */
        var tasks = new Task<HttpContext>[Burst];
        for (var i = 0; i < Burst; i++)
        {
            var guess = "burst-" + i;
            tasks[i] = Task.Run(() => DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, guess, configure: c =>
            {
                if (staged)
                {
                    c.Request.ContentLength = null;
                    c.Request.Body = new MeteredBody(
                        System.Text.Encoding.UTF8.GetBytes("token=" + guess), gate.Task, () => Interlocked.Increment(ref entered));
                }
            }));
        }

        if (staged)
        {
            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref entered) == Burst, TimeSpan.FromSeconds(30)),
                "the staged requests did not all reach their body read");
            gate.SetResult();
        }

        var replies = await Task.WhenAll(tasks);

        var compared = replies.Count(r => r.Response.StatusCode == StatusCodes.Status200OK);
        var held = replies.Where(r => r.Response.StatusCode == StatusCodes.Status429TooManyRequests).ToList();
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, compared);
        Assert.Equal(Burst - compared, held.Count);
        Assert.All(held, r => Assert.True(int.Parse(r.Response.Headers.RetryAfter.ToString(), System.Globalization.CultureInfo.InvariantCulture) >= 1));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SixteenConcurrentRightTokens_FromOneSource_AllSignIn(bool staged)
    {
        using var server = await BuildServer(networkMode: true);
        const int Burst = 16;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;

        /* In-flight reservations count against the free allowance; a sign-in that only meets those waits for
           them instead of being answered 429. */
        var tasks = new Task<HttpContext>[Burst];
        for (var i = 0; i < Burst; i++)
        {
            tasks[i] = Task.Run(() => DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token, configure: c =>
            {
                if (staged)
                {
                    c.Request.ContentLength = null;
                    c.Request.Body = new MeteredBody(
                        System.Text.Encoding.UTF8.GetBytes("token=" + Token), gate.Task, () => Interlocked.Increment(ref entered));
                }
            }));
        }

        if (staged)
        {
            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref entered) == Burst, TimeSpan.FromSeconds(30)),
                "the staged requests did not all reach their body read");
            gate.SetResult();
        }

        var replies = await Task.WhenAll(tasks);
        Assert.All(replies, r => Assert.Equal(StatusCodes.Status302Found, r.Response.StatusCode));
    }

    [Fact]
    public async Task OtherSourcesSpendingTheTotalBudget_DoNotStopASourceThatSignedIn_AndTheHoldSaysWhy()
    {
        using var server = await BuildServer(networkMode: true);

        var worker = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token);
        Assert.Equal(StatusCodes.Status302Found, worker.Response.StatusCode);

        var sources = DarlingSignInThrottle.GlobalMaxAttempts / (DarlingSignInThrottle.FreeFailures + 1);
        for (var k = 0; k < sources; k++)
        {
            for (var j = 0; j <= DarlingSignInThrottle.FreeFailures; j++)
            {
                var miss = await DarlingWebHostGateLiveTests.PostSignIn(
                    server, ListenIp, IPAddress.Parse("192.168.1." + (100 + k)), "wrong-" + k + "-" + j);
                Assert.Equal(StatusCodes.Status200OK, miss.Response.StatusCode);
            }
        }

        /* A source that has not signed in is held by the total, and the answer says so. */
        var stranger = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, IPAddress.Parse("192.168.1.51"), Token);
        var body = await BodyOf(stranger);
        Assert.Equal(StatusCodes.Status429TooManyRequests, stranger.Response.StatusCode);
        Assert.Contains("not necessarily this one", body, StringComparison.Ordinal);
        Assert.Contains("restarting the service clears it", body, StringComparison.Ordinal);
        Assert.True(int.Parse(stranger.Response.Headers.RetryAfter.ToString(), System.Globalization.CultureInfo.InvariantCulture)
            > (int)DarlingSignInThrottle.MaxDelay.TotalSeconds);

        /* The source that signed in earlier still signs in. */
        var again = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token);
        Assert.Equal(StatusCodes.Status302Found, again.Response.StatusCode);
    }

    [Fact]
    public async Task TheHeldAnswer_SaysNoTokenIsComparedAndThatARestartClearsTheWait()
    {
        using var server = await BuildServer(networkMode: true);
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "wrong-" + i);
        }

        var held = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token);
        var body = await BodyOf(held);

        Assert.Equal(StatusCodes.Status429TooManyRequests, held.Response.StatusCode);
        Assert.Contains("No token is compared during the wait", body, StringComparison.Ordinal);
        Assert.Contains("restarting the service clears it", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARightToken_GivesBackOnlyItsOwnAttempt_NeverTheCountOfOtherClientsOnTheSource()
    {
        using var server = await BuildServer(networkMode: true);
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "other-client-" + i);
        }

        var right = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, Token);
        Assert.Equal(StatusCodes.Status302Found, right.Response.StatusCode);

        /* The other client's three misses still count: its next miss starts the wait. */
        var next = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "other-client-next");
        Assert.Equal(StatusCodes.Status200OK, next.Response.StatusCode);
        var held = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "other-client-again");
        Assert.Equal(StatusCodes.Status429TooManyRequests, held.Response.StatusCode);
    }

    [Fact]
    public void BothHosts_KeyTheirThrottleBySourceKey_SoIpv6IsGroupedByItsSlash64()
    {
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs");
        var mcp = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");

        Assert.Contains("DarlingSignInThrottle.SourceKey(remote)", web, StringComparison.Ordinal);
        Assert.Contains("DarlingSignInThrottle.SourceKey(remote)", mcp, StringComparison.Ordinal);
        Assert.DoesNotContain("signInThrottle.RecordFailure", web, StringComparison.Ordinal);
        Assert.DoesNotContain("tokenThrottle.RecordFailure", mcp, StringComparison.Ordinal);
    }

    /* ---------------- a forwarded loopback address is not loopback ---------------- */

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("::1")]
    public async Task BehindAConfiguredProxy_AForwardedLoopbackAddress_IsJudgedByAllowFrom(string forwardedFor)
    {
        using var server = await BuildServer(networkMode: true, trustedProxies: OneProxy());
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, ProxyRemote, Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-For"] = forwardedFor;
        });

        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
    }

    [Fact]
    public async Task AProxyOnThisHost_ForwardingAClientOutsideAllowFrom_StillGets403()
    {
        Assert.True(DarlingHostBinding.TryParseTrustedProxies(new[] { "127.0.0.1" }, out var local, out _));
        using var server = await BuildServer(networkMode: true, trustedProxies: local);
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, IPAddress.Loopback, Token, configure: c =>
        {
            c.Request.Headers["X-Forwarded-For"] = "10.0.0.9";
        });

        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
    }

    [Fact]
    public void OnlyTheConnectionsOwnAddress_CanBeLoopback()
    {
        var loopback = IPAddress.Loopback;
        var proxy = ProxyRemote;

        Assert.True(DarlingWebHostService.IsLoopbackConnection(loopback, null));
        Assert.True(DarlingWebHostService.IsLoopbackConnection(loopback, loopback));
        Assert.True(DarlingWebHostService.IsLoopbackConnection(IPAddress.IPv6Loopback, IPAddress.IPv6Loopback));
        /* A forwarded loopback address over a connection that is not loopback. */
        Assert.False(DarlingWebHostService.IsLoopbackConnection(loopback, proxy));
        Assert.False(DarlingWebHostService.IsLoopbackConnection(IPAddress.IPv6Loopback, proxy));
        /* A real loopback connection that forwarded some other client. */
        Assert.False(DarlingWebHostService.IsLoopbackConnection(IPAddress.Parse("10.0.0.9"), loopback));
        Assert.False(DarlingWebHostService.IsLoopbackConnection(null, null));
        Assert.False(DarlingWebHostService.IsLoopbackConnection(proxy, null));
    }

    /* ---------------- the docs say what the code does ---------------- */

    [Fact]
    public void TheComposeDocs_NameTheLevelTheCodeLogsForAPlainHttpNetworkStart()
    {
        var compose = RepoFile.ReadRepoFile("Darling", "compose", "docker-compose.yml");
        var sample = RepoFile.ReadRepoFile("Darling", "compose", "darling.sample.json");
        var tls = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Hosting", "DarlingListenerTls.cs");

        Assert.Contains("logger.LogWarning(\"{Warning}\", CleartextWarning(labels));", tls, StringComparison.Ordinal);
        Assert.Contains("so at Warning on every start of a plain-HTTP network listener", compose, StringComparison.Ordinal);
        Assert.DoesNotContain("at Critical on every start", compose, StringComparison.Ordinal);
        Assert.Contains("logs a Warning line at every plain-HTTP network start", sample, StringComparison.Ordinal);
        Assert.DoesNotContain("Critical line at every plain-HTTP", sample, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHstsComment_AndTheReadme_SayItCoversTheWholeHostNameOnEveryPort()
    {
        var headers = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebSecurityHeaders.cs");
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");

        Assert.Contains("HSTS is kept per HOST NAME and ignores the port", headers, StringComparison.Ordinal);
        Assert.DoesNotContain("an address:port of its own", headers, StringComparison.Ordinal);
        Assert.Contains("HSTS is kept per host name and ignores the port", readme, StringComparison.Ordinal);
        Assert.Contains("for every port of that same name", readme, StringComparison.Ordinal);
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
