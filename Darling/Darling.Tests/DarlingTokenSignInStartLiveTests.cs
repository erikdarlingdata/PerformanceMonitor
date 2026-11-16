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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8735: the not-started rule and the mode-dependent start lines, driven through the real <c>TryStartServerAsync</c> in process, on a
/// network-mode start with the managed store and no credential (the same shape as <c>DarlingWebHostGateLiveTests</c>). A start that
/// passes the tokenSignIn check goes on to wait for the store credential, so each start here carries a short cancellation: a start the
/// check must stop returns false at once with its Critical line, one that is wrongly let through returns false only at the cancellation
/// and without that line, so the line is what these tests assert. The positive half (the mode reaching <c>ConfigurePipeline</c>) needs
/// the managed store credential and is covered by a live smoke.
/// </summary>
public sealed class DarlingTokenSignInStartLiveTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string PlainToken = "a-long-enough-plain-token-value-1234567890";
    private const string ValidOidc = "'authority': 'https://login.example.com', 'clientId': 'client-1', 'clientSecret': 'secret-1'";

    private sealed class HostLogger(CapturingTestLogger inner) : ILogger<DarlingWebHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => inner.Log(logLevel, eventId, state, exception, formatter);
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

    private static DarlingConfig Config(int port, string networkExtra) =>
        DarlingConfig.Parse(
            ("{ 'postgres': { 'managed': true }, 'servers': [ { 'host': 'SQL2022' } ], 'web': { 'enabled': true, 'port': "
            + port + ", 'network': { 'listen': '" + ListenIp + "', 'allowFrom': '" + AllowedCidr + "', " + networkExtra + " } } }")
            .Replace('\'', '"'));

    private sealed record StartResult(bool Started, bool SecondStarted, bool Answered, CapturingTestLogger Log);

    /// <summary>Starts the host <paramref name="attempts"/> times on one instance, as the supervisor's retry does, then probes the port.</summary>
    private static async Task<StartResult> StartAsync(DarlingConfig config, int port, int attempts = 1)
    {
        var log = new CapturingTestLogger();
        var host = new DarlingWebHostService(
            new HostLogger(log), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
        var toggle = new DarlingHostBinding.EndpointToggle(true, port, DarlingHostBinding.EndpointToggleOrigin.File, false, false);
        var tryStart = typeof(DarlingWebHostService).GetMethod("TryStartServerAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var started = true;
        var secondStarted = true;
        var answered = false;
        try
        {
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                var result = await (Task<bool>)tryStart.Invoke(host, [config, toggle, cts.Token])!;
                if (attempt == 0)
                {
                    started = result;
                }
                else
                {
                    secondStarted = result;
                }
            }

            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await client.ConnectAsync(IPAddress.Loopback, port, cts.Token);
                answered = true;
            }
            catch (System.Net.Sockets.SocketException)
            {
                answered = false;
            }
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }

        return new StartResult(started, secondStarted, answered, log);
    }

    private static int CriticalLinesAbout(CapturingTestLogger log) =>
        log.Lines.Count(l => l.StartsWith("Critical:", StringComparison.Ordinal)
            && l.Contains("web.network.oidc.tokenSignIn", StringComparison.Ordinal));

    [Fact]
    public async Task Off_WithOnlyATokenSignInOidcBlock_DoesNotStart_AndLogsTheCriticalLineOnceOverTwoAttempts()
    {
        var port = FreeTcpPort();
        var config = Config(port, "'token': '" + PlainToken + "', 'oidc': { 'tokenSignIn': 'off' }");

        var result = await StartAsync(config, port, attempts: 2);

        Assert.False(result.Started);
        Assert.False(result.SecondStarted);
        Assert.False(result.Answered, "nothing may answer on the port");
        Assert.Equal(1, CriticalLinesAbout(result.Log));
        Assert.Contains(result.Log.Lines, l => l.Contains("OIDC is not configured", StringComparison.Ordinal));
        Assert.DoesNotContain(result.Log.Lines, l => l.Contains(PlainToken, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Off_WithValidOidc_AndAnUnparsableTrustedProxy_DoesNotStart_BecauseTheCheckRunsAfterTheDegrade()
    {
        var port = FreeTcpPort();
        var config = Config(port, "'trustedProxies': ['not-an-ip'], 'oidc': { " + ValidOidc + ", 'tokenSignIn': 'off' }");

        var result = await StartAsync(config, port);

        Assert.False(result.Started);
        Assert.False(result.Answered);
        Assert.Contains(result.Log.Lines, l => l.StartsWith("Critical:", StringComparison.Ordinal)
            && l.Contains("web.network.trustedProxies is misconfigured", StringComparison.Ordinal));
        var line = Assert.Single(result.Log.Lines, l => l.StartsWith("Critical:", StringComparison.Ordinal)
            && l.Contains("web.network.oidc.tokenSignIn", StringComparison.Ordinal));
        Assert.Contains("not exposed", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnknownValue_DoesNotStart_AndTheCriticalLineQuotesIt()
    {
        var port = FreeTcpPort();
        var config = Config(port, "'token': '" + PlainToken + "', 'oidc': { " + ValidOidc + ", 'tokenSignIn': 'bogus' }");

        var result = await StartAsync(config, port);

        Assert.False(result.Started);
        Assert.False(result.Answered);
        var line = Assert.Single(result.Log.Lines, l => l.StartsWith("Critical:", StringComparison.Ordinal)
            && l.Contains("web.network.oidc.tokenSignIn", StringComparison.Ordinal));
        Assert.Contains("'bogus'", line, StringComparison.Ordinal);
    }

    private static int LinesContaining(CapturingTestLogger log, string level, string text) =>
        log.Lines.Count(l => l.StartsWith(level, StringComparison.Ordinal) && l.Contains(text, StringComparison.Ordinal));

    private const string NotStartedText = "leaves OIDC as the only way to sign in";

    [Fact]
    public async Task Off_WithOidcRefusedAtValidation_LogsEachStartLineOnce_OverTwoAttempts()
    {
        var port = FreeTcpPort();
        var config = Config(port, "'token': '" + PlainToken
            + "', 'oidc': { 'authority': 'http://login.example.com', 'clientId': 'client-1', 'clientSecret': 'secret-1', 'tokenSignIn': 'off' }");

        var result = await StartAsync(config, port, attempts: 2);

        Assert.False(result.Started);
        Assert.False(result.SecondStarted);
        Assert.Equal(1, LinesContaining(result.Log, "Critical:", "web.network.oidc is misconfigured"));
        Assert.Equal(1, LinesContaining(result.Log, "Warning:", "web.network.token is set in plaintext"));
        Assert.Equal(1, LinesContaining(result.Log, "Critical:", NotStartedText));
        Assert.Contains("OIDC was refused at start", result.Log.Lines.Single(l => l.Contains(NotStartedText, StringComparison.Ordinal)), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Off_WithABadTrustedProxy_LogsEachStartLineOnce_OverTwoAttempts()
    {
        var port = FreeTcpPort();
        var config = Config(port, "'token': '" + PlainToken + "', 'trustedProxies': ['not-an-ip'], 'oidc': { " + ValidOidc + ", 'tokenSignIn': 'off' }");

        var result = await StartAsync(config, port, attempts: 2);

        Assert.False(result.Started);
        Assert.False(result.SecondStarted);
        Assert.Equal(1, LinesContaining(result.Log, "Critical:", "web.network.trustedProxies is misconfigured"));
        Assert.Equal(1, LinesContaining(result.Log, "Warning:", "web.network.token is set in plaintext"));
        Assert.Equal(1, LinesContaining(result.Log, "Critical:", NotStartedText));
    }

    [Theory]
    [InlineData("on", true, "break-glass")]
    [InlineData("loopback", true, "from this machine only")]
    [InlineData("off", false, "")]
    public async Task OidcStartLine_FollowsTheMode(string mode, bool mentionsTheSharedToken, string expectedPhrase)
    {
        var port = FreeTcpPort();
        var config = Config(port, "'token': '" + PlainToken + "', 'oidc': { " + ValidOidc + ", 'tokenSignIn': '" + mode + "' }");

        var result = await StartAsync(config, port);

        var line = Assert.Single(result.Log.Lines, l => l.Contains("OIDC sign-in enabled", StringComparison.Ordinal));
        Assert.Equal(mentionsTheSharedToken, line.Contains("The shared token", StringComparison.Ordinal));
        Assert.Contains(expectedPhrase, line, StringComparison.Ordinal);
        Assert.DoesNotContain(result.Log.Lines, l => l.Contains(PlainToken, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("on", "gates web dashboard network access")]
    [InlineData("loopback", "gates web dashboard network access")]
    [InlineData("off", "signs nobody in")]
    public async Task PlaintextTokenWarning_FollowsTheMode(string mode, string expectedPhrase)
    {
        var port = FreeTcpPort();
        var config = Config(port, "'token': '" + PlainToken + "', 'oidc': { " + ValidOidc + ", 'tokenSignIn': '" + mode + "' }");

        var result = await StartAsync(config, port);

        var line = Assert.Single(result.Log.Lines, l => l.Contains("web.network.token is set in plaintext", StringComparison.Ordinal));
        Assert.Contains(expectedPhrase, line, StringComparison.Ordinal);
    }
}
