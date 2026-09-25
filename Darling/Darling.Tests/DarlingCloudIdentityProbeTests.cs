/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4214 part 2b: <see cref="DarlingCloudIdentityProbe"/> driven entirely through a fake
/// <see cref="HttpMessageHandler"/> — no real network call, no live rig. Covers every branch the ruling names:
/// an EC2 success, an Azure success, neither found (a timeout within the probe's own budget), a 401, a 404, an
/// oversized body, a redirect (never followed), and a value that fails the accept pattern.
/// </summary>
public sealed class DarlingCloudIdentityProbeTests
{
    private static HttpResponseMessage Text(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body) };

    private static HttpResponseMessage Empty(HttpStatusCode status) => new(status);

    [Fact]
    public async Task ProbeAsync_Ec2TokenAndInstanceTypeSucceed_ReturnsAwsProviderAndType()
    {
        var handler = new FakeHandler((request, _) =>
        {
            if (request.Method == HttpMethod.Put && request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "AABBCCTOKEN"));
            }

            if (request.RequestUri!.AbsolutePath == "/latest/meta-data/instance-type")
            {
                Assert.Equal("AABBCCTOKEN", request.Headers.GetValues("X-aws-ec2-metadata-token").Single());
                return Task.FromResult(Text(HttpStatusCode.OK, "t3.large"));
            }

            throw new InvalidOperationException("Azure should never be reached once EC2 succeeds: " + request.RequestUri);
        });

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal("aws", identity.Provider);
        Assert.Equal("t3.large", identity.InstanceType);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task ProbeAsync_Ec2TokenFails_FallsBackToAzureSuccess_ReturnsAzureProviderAndSize()
    {
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Empty(HttpStatusCode.Unauthorized));
            }

            if (request.RequestUri!.AbsolutePath.Contains("vmSize", StringComparison.Ordinal))
            {
                Assert.Equal("true", request.Headers.GetValues("Metadata").Single());
                return Task.FromResult(Text(HttpStatusCode.OK, "Standard_D2s_v3"));
            }

            throw new InvalidOperationException("Unexpected request: " + request.RequestUri);
        });

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal("azure", identity.Provider);
        Assert.Equal("Standard_D2s_v3", identity.InstanceType);
    }

    [Fact]
    public async Task ProbeAsync_NeitherCloudResponds_ReturnsNone_WithinItsOwnBudget()
    {
        var handler = new FakeHandler(async (_, cancellationToken) =>
        {
            /* Simulates a host with no metadata endpoint reachable at all: the request hangs until the
               probe's own 200 ms budget cancels IT, never until some external test timeout does. */
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("unreachable");
        });

        var stopwatch = Stopwatch.StartNew();
        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);
        stopwatch.Stop();

        Assert.Equal(CloudIdentity.None, identity);
        Assert.True(stopwatch.ElapsedMilliseconds < 2000, $"took {stopwatch.ElapsedMilliseconds} ms — the 200 ms budget did not bound it");
    }

    [Fact]
    public async Task ProbeAsync_Ec2NotFound_FallsBackToAzureNotFound_ReturnsNone()
    {
        var handler = new FakeHandler((_, _) => Task.FromResult(Empty(HttpStatusCode.NotFound)));

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal(CloudIdentity.None, identity);
    }

    [Fact]
    public async Task ProbeAsync_Ec2InstanceTypeBodyOversized_RejectedAndAzureAlsoNotFound_ReturnsNone()
    {
        var oversized = new string('a', 300);
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "TOKEN"));
            }

            if (request.RequestUri!.AbsolutePath == "/latest/meta-data/instance-type")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, oversized));
            }

            return Task.FromResult(Empty(HttpStatusCode.NotFound));
        });

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal(CloudIdentity.None, identity);
    }

    [Fact]
    public async Task ProbeAsync_Ec2InstanceTypeRedirects_NeverFollowed_AzureAlsoNotFound_ReturnsNone()
    {
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "TOKEN"));
            }

            if (request.RequestUri!.AbsolutePath == "/latest/meta-data/instance-type")
            {
                var redirect = Empty(HttpStatusCode.Found);
                redirect.Headers.Location = new Uri("http://169.254.169.254/latest/meta-data/instance-type/other");
                return Task.FromResult(redirect);
            }

            if (request.RequestUri!.AbsolutePath.EndsWith("/other", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("the redirect target must never be fetched (AllowAutoRedirect=false)");
            }

            return Task.FromResult(Empty(HttpStatusCode.NotFound));
        });

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal(CloudIdentity.None, identity);
        /* token PUT + instance-type GET (redirect, not followed) + azure GET = 3 — never a 4th request to the
           redirect target. */
        Assert.Equal(3, handler.Requests.Count);
    }

    [Fact]
    public async Task ProbeAsync_ValueFailsAcceptPattern_RejectedAndAzureAlsoNotFound_ReturnsNone()
    {
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "TOKEN"));
            }

            if (request.RequestUri!.AbsolutePath == "/latest/meta-data/instance-type")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "not an instance type!!"));
            }

            return Task.FromResult(Empty(HttpStatusCode.NotFound));
        });

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal(CloudIdentity.None, identity);
    }

    [Fact]
    public void CreateHandler_ReturnsASocketsHttpHandler()
    {
        using var handler = DarlingCloudIdentityProbe.CreateHandler();
        Assert.IsType<SocketsHttpHandler>(handler);
    }

    /// <summary>A fake handler recording every request it sees; the responder gets the SAME cancellation token
    /// <see cref="DarlingCloudIdentityProbe"/> passed to <c>SendAsync</c> — the timeout test awaits against it
    /// directly rather than a separate test-owned timeout, so it proves the PROBE's own budget is what stops
    /// the call.</summary>
    private sealed class FakeHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _responder;

        public List<HttpRequestMessage> Requests { get; } = [];

        public FakeHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> responder)
        {
            _responder = responder;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return _responder(request, cancellationToken);
        }
    }
}

/// <summary>
/// #4214 part 2b, ruling 3: source pins proving <see cref="DarlingStoreHostProfile.GatherStartupProfileAsync"/>
/// and <c>DarlingWorker</c> never reach <see cref="DarlingCloudIdentityProbe"/>, and ruling 2: pins on
/// <see cref="DarlingCloudIdentityProbe.CreateHandler"/>'s two safety settings. Source-text pins rather than
/// behavioral tests because the property under test IS the source: a passing behavioral suite proves the probe
/// answers correctly when called, not that a future edit never adds a call from the startup path — the same
/// reasoning <c>AlertReadFailureSurfaceTests</c> and <c>DocCommentHygieneTests</c> already rest on in this repo
/// (see the lane orders' "real gates" list). Proved once, by hand, that each assertion fails when the source it
/// pins is broken (reverted after).
/// </summary>
public sealed class DarlingCloudIdentityProbeSourcePinTests
{
    private const string ProbeClassName = "DarlingCloudIdentityProbe";

    [Fact]
    public void GatherStartupProfileAsync_NeverReachesTheProbe()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingStoreHostProfile.cs"));

        var body = ExtractMethodBody(stripped, "Task<HostProfile> GatherStartupProfileAsync(");

        Assert.DoesNotContain(ProbeClassName, body, StringComparison.Ordinal);
    }

    [Fact]
    public void DarlingWorker_NeverReachesTheProbe()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs"));

        Assert.DoesNotContain(ProbeClassName, stripped, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateHandler_SetsUseProxyFalse()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCloudIdentityProbe.cs"));

        Assert.Contains("UseProxy = false", stripped, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateHandler_SetsAllowAutoRedirectFalse()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCloudIdentityProbe.cs"));

        Assert.Contains("AllowAutoRedirect = false", stripped, StringComparison.Ordinal);
    }

    private static string ExtractMethodBody(string strippedSource, string signatureAnchor)
    {
        var at = strippedSource.IndexOf(signatureAnchor, StringComparison.Ordinal);
        Assert.True(at > 0, $"'{signatureAnchor}' not found in source");

        var open = strippedSource.IndexOf('{', at);
        Assert.True(open > 0, $"'{signatureAnchor}' has no block body");

        return CSharpSourceWalker.BraceBalanced(strippedSource, open);
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#4214 part 2b scan target not found: {path}");

        return File.ReadAllText(path);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
