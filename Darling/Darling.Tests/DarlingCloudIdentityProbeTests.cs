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
    public async Task ProbeAsync_Ec2InstanceTypeBodyExceedsCapByTrailingWhitespace_RejectedByTheCapAlone_ReturnsNone()
    {
        /* 60 valid characters, then 240 spaces: 300 bytes. After Trim() this would read as 60 valid
           characters — inside ValuePattern's 1-64 range — so ONLY the 256-byte cap (checked on the raw byte
           count, before Trim() ever runs) can be what rejects it. The 300-'a' oversized test above fails the
           pattern too even once trimmed, so it does not by itself prove the cap exists. */
        var paddedPastTheCap = new string('a', 60) + new string(' ', 240);
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "TOKEN"));
            }

            if (request.RequestUri!.AbsolutePath == "/latest/meta-data/instance-type")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, paddedPastTheCap));
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
    public async Task ProbeAsync_Ec2InstanceTypeIs404WithAValidLookingBody_RejectedByStatusNotBody_ReturnsNone()
    {
        /* "t3.large" alone would pass ValuePattern — only the IsSuccessStatusCode check (ruling 2) can be
           what rejects this. The existing 404 test above uses an empty body, which the pattern would reject
           on its own even with that status check removed, so it does not by itself prove the check exists. */
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "TOKEN"));
            }

            if (request.RequestUri!.AbsolutePath == "/latest/meta-data/instance-type")
            {
                return Task.FromResult(Text(HttpStatusCode.NotFound, "t3.large"));
            }

            return Task.FromResult(Empty(HttpStatusCode.NotFound));
        });

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal(CloudIdentity.None, identity);
    }

    [Fact]
    public async Task ProbeAsync_Ec2InstanceTypeIs302WithAValidLookingBody_RejectedByStatusNotBody_ReturnsNone()
    {
        /* Same point as the 404 test above, for the redirect status the existing redirect test (also an
           empty body) does not by itself prove the status check rejects rather than just the pattern. */
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/latest/api/token")
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "TOKEN"));
            }

            if (request.RequestUri!.AbsolutePath == "/latest/meta-data/instance-type")
            {
                var redirect = Text(HttpStatusCode.Found, "t3.large");
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
    public async Task ProbeAsync_AzureValueFailsAcceptPattern_ReturnsNone()
    {
        /* EC2 finds nothing, so Azure is reached, and AZURE's own value fails ValuePattern this time. The
           test above only exercises TryEc2Async's ValuePattern.IsMatch call — a reviewer who removed just
           TryAzureAsync's own check would leave every existing test green. */
        var handler = new FakeHandler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath.Contains("vmSize", StringComparison.Ordinal))
            {
                return Task.FromResult(Text(HttpStatusCode.OK, "bad value<script>"));
            }

            return Task.FromResult(Empty(HttpStatusCode.NotFound));
        });

        var identity = await DarlingCloudIdentityProbe.ProbeAsync(handler, CancellationToken.None);

        Assert.Equal(CloudIdentity.None, identity);
    }

    /* A Stopwatch-bound timing test (EC2's token PUT answering 401 at ~190 ms, Azure hanging, asserting total
       elapsed stays under a bound between the correct ~200 ms and a broken ~390 ms) lived here and was
       dropped: measured directly, it is racy. The SHARED outer budget still bounds the EC2 leg's own
       Task.Delay regardless of what Azure gets, so on a loaded machine EC2's delay can itself lose the race
       to that shared budget before completing "naturally" — 2 of 3 runs against a deliberately-reverted,
       per-request-timeout build still finished under the bound, for a reason unrelated to what the test meant
       to check. DarlingCloudIdentityProbeSourcePinTests.CreateHandler_ArmsExactlyOneBudget_NotAPerRequestTimeout
       pins the same "one shared budget" property deterministically instead: a per-request-timeout
       implementation needs a SECOND call to CancelAfter(, so counting them is exact where a clock is not. */

    [Fact]
    public void CreateHandler_ReturnsASocketsHttpHandler()
    {
        using var handler = DarlingCloudIdentityProbe.CreateHandler();
        var sockets = Assert.IsType<SocketsHttpHandler>(handler);

        Assert.False(sockets.UseProxy);
        Assert.False(sockets.AllowAutoRedirect);
        Assert.Equal(DarlingCloudIdentityProbe.ProbeBudget, sockets.ConnectTimeout);
        Assert.Equal(0, sockets.MaxResponseDrainSize);
        Assert.Null(sockets.ActivityHeadersPropagator);
    }

    [Fact]
    public async Task ProbeAsync_CallerTokenAlreadyCancelled_ThrowsAndCacheStoresNothing()
    {
        var handler = new FakeHandler(async (_, cancellationToken) =>
        {
            /* Never actually answers — this proves the probe's OWN 200 ms budget is not what stops it here;
               the caller's own already-cancelled token is. */
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("unreachable");
        });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => DarlingCloudIdentityProbe.ProbeAsync(handler, cts.Token));

        /* And through StoreHostProfileCache (get_store_host's caller): the exception must reach ITS caller
           too, not collapse into a cached "not found". CancellationToken.None as the cache's OWN token, not
           cts.Token, so SemaphoreSlim.WaitAsync does not short-circuit before ever calling gather — the
           assertion below needs gather to actually run and actually throw. */
        var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5));
        var gatherCalls = 0;

        async Task<HostProfile> CancelledGather(CancellationToken _)
        {
            gatherCalls++;
            await DarlingCloudIdentityProbe.ProbeAsync(handler, cts.Token);
            throw new InvalidOperationException("unreachable — ProbeAsync above always throws first");
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cache.GetOrGatherAsync(CancelledGather, CancellationToken.None));

        Task<HostProfile> SucceedingGather(CancellationToken _)
        {
            gatherCalls++;
            return Task.FromResult(FixtureProfile());
        }

        var second = await cache.GetOrGatherAsync(SucceedingGather, CancellationToken.None);

        /* 2, not 1: the second call actually re-gathered rather than reusing whatever the cancelled first
           call might have left behind, so nothing from the cancelled gather was cached. */
        Assert.Equal(2, gatherCalls);
        Assert.NotNull(second.Profile);
    }

    [Fact]
    public void ValuePattern_RejectsATrailingNewline()
    {
        /* $ (the pattern's shape before this fix) also matches immediately before a trailing \n, so
           "t3.large\n" would wrongly pass. Called directly, not through ProbeAsync, since TryEc2Async/
           TryAzureAsync both Trim() the body before matching and would hide this. */
        Assert.DoesNotMatch(DarlingCloudIdentityProbe.ValuePattern, "t3.large\n");
    }

    /// <summary>A minimal, valid <see cref="HostProfile"/> for the cache test above — the cache never reads
    /// into its fields, only caches/returns the reference, so the values themselves are arbitrary. Mirrors
    /// <c>StoreHostProfileCacheTests.FixtureProfile</c>.</summary>
    private static HostProfile FixtureProfile() => new()
    {
        Platform = "linux",
        IsContainerized = false,
        ProcessorCount = 4,
        Memory = new HostMemoryProfile(8_589_934_592, null, 8_589_934_592, true, "GlobalMemoryStatusEx"),
        DataVolume = new HostDataVolumeProfile(107_374_182_400, 53_687_091_200, "ext4", true),
        IsManagedStore = false,
        Store = new HostStoreFacts("17.4", "2.99.0", 1_000_000, 99.0, 0, 0, 0),
        Settings = Array.Empty<HostSettingProfile>(),
        Cloud = CloudIdentity.None,
    };

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
/// #4214 part 2b, ruling 3: source pins proving <see cref="DarlingCloudIdentityProbe.ProbeAsync(CancellationToken)"/>,
/// <see cref="DarlingStoreHostProfile.GatherAsync"/> and <c>GetStoreHost</c> are each reached from only the
/// call sites ruling 3 names, plus ruling 2: a pin on <see cref="DarlingCloudIdentityProbe.CreateHandler"/>'s
/// handler-disposal discipline. Source-text pins rather than behavioral tests because the property under test
/// IS the source: a passing behavioral suite proves the probe answers correctly when called, not that a future
/// edit never adds a call from the startup path or some other new call site — the same reasoning
/// <c>AlertReadFailureSurfaceTests</c> and <c>DocCommentHygieneTests</c> already rest on in this repo (see the
/// lane orders' "real gates" list). Proved once, by hand, that each assertion fails when the source it pins is
/// broken (reverted after).
/// </summary>
public sealed class DarlingCloudIdentityProbeSourcePinTests
{
    private const string ProbeClassName = "DarlingCloudIdentityProbe";

    /* GatherStartupProfileAsync_NeverReachesTheProbe and DarlingWorker_NeverReachesTheProbe used to live here
       as name pins: each checked one named method body, or one named file, for the ABSENCE of the string
       "DarlingCloudIdentityProbe". That only rules out the two spots someone thought to name — a new call
       site added anywhere else (a third file, a helper method) would pass both checks with everything green.
       Replaced by a call-site CENSUS: every .cs file under this project is scanned for each marker, and the
       set of files that contain it must equal the exact set ruling 3 names — not "does not contain" one
       named spot, but "contains, and ONLY in these spots". */

    [Fact]
    public void ProbeAsyncCallSite_OnlyInsideDarlingStoreHostProfile_GatherAsync()
    {
        var files = FilesContainingAcrossServiceProject("DarlingCloudIdentityProbe.ProbeAsync(");

        Assert.Equal(new[] { "DarlingStoreHostProfile.cs" }, files);

        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingStoreHostProfile.cs"));

        var gatherAsyncBody = ExtractMethodBody(stripped, "Task<HostProfile> GatherAsync(");
        Assert.Contains("DarlingCloudIdentityProbe.ProbeAsync(", gatherAsyncBody, StringComparison.Ordinal);

        /* The startup path (DarlingWorker's once-per-process-start profile) must never reach the probe
           (ruling 3) — kept as its own explicit assertion, not just an absence implied by the file-level
           census above, since GatherStartupProfileAsync lives in the SAME file as the one legitimate call. */
        var startupBody = ExtractMethodBody(stripped, "Task<HostProfile> GatherStartupProfileAsync(");
        Assert.DoesNotContain(ProbeClassName, startupBody, StringComparison.Ordinal);
    }

    [Fact]
    public void GatherAsyncCallSite_OnlyInCliCommandsAndTheMcpStoreHostTool()
    {
        var files = FilesContainingAcrossServiceProject("DarlingStoreHostProfile.GatherAsync(");

        Assert.Equal(new[] { "DarlingCliCommands.cs", "Mcp/DarlingMcpStoreHostTools.cs" }, files);
    }

    [Fact]
    public void GetStoreHostCallSite_OnlyInTheMcpToolAndTheWebDispatch()
    {
        var files = FilesContainingAcrossServiceProject("GetStoreHost(");

        Assert.Equal(new[] { "DarlingWebEndpoints.cs", "Mcp/DarlingMcpStoreHostTools.cs" }, files);
    }

    /// <summary>Every <c>.cs</c> file under <c>Darling/PerformanceMonitor.Darling.Service/</c> (skipping
    /// <c>bin</c>/<c>obj</c> build output), comment/string-stripped, that contains <paramref name="marker"/> —
    /// sorted, relative paths with <c>/</c> separators so the assertion reads the same on every OS.</summary>
    private static string[] FilesContainingAcrossServiceProject(string marker)
    {
        var serviceDir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");
        Assert.True(Directory.Exists(serviceDir), $"#4214 part 2b scan root not found: {serviceDir}");

        var matches = new List<string>();

        foreach (var path in Directory.EnumerateFiles(serviceDir, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(serviceDir, path).Replace('\\', '/');
            if (relative.StartsWith("bin/", StringComparison.Ordinal) || relative.StartsWith("obj/", StringComparison.Ordinal))
            {
                continue;
            }

            var stripped = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
            if (stripped.Contains(marker, StringComparison.Ordinal))
            {
                matches.Add(relative);
            }
        }

        matches.Sort(StringComparer.Ordinal);
        return [.. matches];
    }

    /* CreateHandler_SetsUseProxyFalse and CreateHandler_SetsAllowAutoRedirectFalse used to live here as
       whole-file text pins ("the string UseProxy = false appears somewhere in this file"). Replaced by
       DarlingCloudIdentityProbeTests.CreateHandler_ReturnsASocketsHttpHandler asserting the built handler's
       actual property values — a text pin passes even if the setting moved to dead code or a different type;
       a property assertion on the constructed handler cannot. */

    [Fact]
    public void OneArgumentProbeAsync_DisposesTheHandlerItCreates()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCloudIdentityProbe.cs"));

        var body = ExtractMethodBody(stripped, "Task<CloudIdentity> ProbeAsync(CancellationToken cancellationToken)");

        Assert.Contains("using var handler = CreateHandler()", body, StringComparison.Ordinal);
    }

    /// <summary>Ruling 2's "one budget for the whole probe" pinned deterministically, not with a clock. A
    /// behavioral timing test (EC2 answering just under the budget, Azure hanging, asserting total elapsed
    /// stays under a bound between "one shared budget" and "a fresh budget per request") was tried and
    /// dropped: the SHARED outer budget still bounds the EC2 leg's own delay regardless of what Azure gets, so
    /// under load EC2's delay can lose the race to that shared budget before completing "naturally", passing
    /// the assertion for a reason unrelated to what it meant to check — measured flaky (2 of 3 runs) against a
    /// deliberately broken, per-request-timeout build. Counting <c>CancelAfter(</c> call sites has no clock to
    /// race: a per-request-timeout implementation needs a SECOND one (one for EC2, one for Azure), so exactly
    /// one proves the budget is shared.</summary>
    [Fact]
    public void CreateHandler_ArmsExactlyOneBudget_NotAPerRequestTimeout()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCloudIdentityProbe.cs"));

        var occurrences = stripped.Split("CancelAfter(", StringSplitOptions.None).Length - 1;

        Assert.Equal(1, occurrences);
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
