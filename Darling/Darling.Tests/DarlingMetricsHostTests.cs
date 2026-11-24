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
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Metrics;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9013: the metrics host's start order, its refusals, its Kestrel pins and its log lines. No store: every host gets a data source
/// override that answers null and a collector that is not collecting, so <c>/metrics</c> answers the meta series only. Touches no
/// process-wide state: the token file is a per-test temp file and the real-port tests bind on a port the test obtained. The
/// ambient-configuration test, which sets an environment variable, is <see cref="DarlingMetricsHostAmbientConfigTests"/>.
/// </summary>
public sealed class DarlingMetricsHostTests : IDisposable
{
    internal const string GoodToken = "abcdefghijklmnop1234";

    private readonly string _tokenFile = Path.Combine(Path.GetTempPath(), $"darling-metrics-token-{Guid.NewGuid():N}.txt");

    public DarlingMetricsHostTests()
    {
        File.WriteAllText(_tokenFile, GoodToken);
    }

    public void Dispose() => File.Delete(_tokenFile);

    private string FileToken => "file:" + _tokenFile;

    internal sealed class CapturingLogger : ILogger<DarlingMetricsHostService>
    {
        private readonly List<(LogLevel Level, string Text)> _lines = new();

        public List<(LogLevel Level, string Text)> Lines
        {
            get
            {
                lock (_lines)
                {
                    return _lines.ToList();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add((logLevel, formatter(state, exception)));
            }
        }
    }

    private static string Quote(string value) => JsonSerializer.Serialize(value);

    internal static string MetricsOn(int port, string? token, string listen = "127.0.0.1", string? allowFrom = null, string before = "") =>
        "{" + before + "\"metrics\":{\"enabled\":true,\"port\":" + port + ",\"listen\":" + Quote(listen)
        + (token is null ? "" : ",\"token\":" + Quote(token))
        + (allowFrom is null ? "" : ",\"allowFrom\":" + Quote(allowFrom))
        + "}}";

    internal static DarlingMetricsHostService NewHost(
        CapturingLogger logger, string json, bool inContainer = false, Func<WebApplication, ValueTask>? disposeApp = null) =>
        NewHost(logger, () => DarlingConfig.Parse(json), inContainer, disposeApp);

    private static DarlingMetricsHostService NewHost(
        CapturingLogger logger, Func<DarlingConfig> loadConfig, bool inContainer = false, Func<WebApplication, ValueTask>? disposeApp = null) =>
        new(new CollectorRuntimeState(), new ReadAdmission(), new MonitoredServerRegistryState(), logger, loadConfig,
            inContainer: inContainer, disposeApp: disposeApp, dataSourceOverride: _ => Task.FromResult<NpgsqlDataSource?>(null));

    private static bool PortIsFree(int port)
    {
        var probe = new TcpListener(IPAddress.Loopback, port);
        try
        {
            probe.Start();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
        finally
        {
            probe.Stop();
        }
    }

    private static async Task AssertNothingStarted(string json, int port)
    {
        var ct = TestContext.Current.CancellationToken;
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, json);

        await host.StartAsync(ct);

        Assert.False(host.IsListening);
        Assert.Null(host.Services);
        Assert.DoesNotContain(logger.Lines, l => l.Level > LogLevel.Debug);

        /* The refused connect proves nothing when something else already holds the port, so check that first. */
        if (!PortIsFree(port))
        {
            Assert.Skip($"Port {port} is in use on this machine, so a refused connect cannot be checked.");
        }

        using var tcp = new TcpClient();
        await Assert.ThrowsAnyAsync<SocketException>(async () => await tcp.ConnectAsync(IPAddress.Loopback, port, ct));
    }

    [Fact]
    public Task NoMetricsBlock_StartsNothing() => AssertNothingStarted("{}", new MetricsConfig().Port);

    [Fact]
    public Task NullMetrics_StartsNothing() => AssertNothingStarted("{\"metrics\": null}", new MetricsConfig().Port);

    [Fact]
    public Task EnabledFalse_StartsNothing()
    {
        var port = DarlingHealthHostTests.FreePort();
        return AssertNothingStarted(
            "{\"metrics\":{\"enabled\":false,\"port\":" + port + ",\"listen\":\"127.0.0.1\",\"token\":\"" + GoodToken + "\"}}", port);
    }

    private static async Task AssertRefused(string json, params string[] named)
    {
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, json);

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
        var line = Assert.Single(logger.Lines);
        Assert.Equal(LogLevel.Critical, line.Level);
        foreach (var setting in named)
        {
            Assert.Contains(setting, line.Text, StringComparison.Ordinal);
        }
    }

    public static TheoryData<string, string[]> RefusedBlocks => new()
    {
        { MetricsOn(6110, GoodToken, before: "\"web\":{\"port\":6110},"), new[] { "metrics.port", "web.port" } },
        { MetricsOn(6111, GoodToken, before: "\"mcp\":{\"port\":6111},"), new[] { "metrics.port", "mcp.port" } },
        { MetricsOn(6112, GoodToken, before: "\"health\":{\"enabled\":true,\"port\":6112},"), new[] { "metrics.port", "health.port" } },
        { MetricsOn(6113, GoodToken, listen: "nope"), new[] { "metrics.listen" } },
        { MetricsOn(6114, token: null), new[] { "metrics.token" } },
        { MetricsOn(6115, "env:DARLING_TEST_UNSET_9013_HOST"), new[] { "metrics.token" } },
        { MetricsOn(6116, "short"), new[] { "metrics.token" } },
        { MetricsOn(6117, GoodToken, allowFrom: "10.0.0.0/33"), new[] { "metrics.allowFrom" } },
    };

    [Theory]
    [MemberData(nameof(RefusedBlocks))]
    public Task Problems_AreRefusedByTheHost(string json, string[] named)
    {
        Assert.Null(Environment.GetEnvironmentVariable("DARLING_TEST_UNSET_9013_HOST"));
        return AssertRefused(json, named);
    }

    [Fact]
    public Task FrontDoor_IsRefusedWithOneCritical() =>
        AssertRefused(MetricsOn(6118, GoodToken, before: "\"frontDoor\":{\"enabled\":true},"), "metrics needs a store; not served in front-door mode");

    [Fact]
    public async Task UnreadableConfig_DoesNotStart_AndLogsOneError()
    {
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, () => throw new JsonException("bad json"));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
        var line = Assert.Single(logger.Lines);
        Assert.Equal(LogLevel.Error, line.Level);
        Assert.Contains("metrics", line.Text, StringComparison.Ordinal);
        Assert.Contains(nameof(JsonException), line.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RegisteredAfterTheWorker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        DarlingProcessServices.Add(services, null);

        /* The registration is a factory, so the descriptor carries no implementation type: resolve the hosted services in the order
           the host starts them, which builds the metrics host through its factory. */
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToList();
        var metrics = hosted.FindIndex(h => h is DarlingMetricsHostService);
        var worker = hosted.FindIndex(h => h is DarlingWorker);

        Assert.True(metrics >= 0, "the metrics host is registered");
        Assert.True(worker >= 0, "the worker is registered");
        Assert.True(metrics > worker, "the metrics host must start after the worker");
    }

    /// <summary>#9013: the store login resolves again on every refresh while it is missing. Its refusal line names the metrics port,
    /// not the web dashboard, and is logged once for the streak, not once a minute. The login here is an unmanaged store whose
    /// postgres.webConnectionString names an unset variable, which the resolver refuses before it reads anything else.</summary>
    [Fact]
    public async Task MissingStoreLogin_LogsOneLineNamingTheMetricsPort()
    {
        var ct = TestContext.Current.CancellationToken;
        var unset = $"DARLING_9013_UNSET_{Guid.NewGuid():N}";
        Assert.Null(Environment.GetEnvironmentVariable(unset));
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig { Managed = false, ConnectionString = "Host=127.0.0.1;Username=darling;Database=darling", WebConnectionString = "env:" + unset },
            Metrics = new MetricsConfig { Enabled = true, Port = DarlingHealthHostTests.FreePort(), Listen = "127.0.0.1", Token = FileToken },
        };
        var logger = new CapturingLogger();
        await using var host = new DarlingMetricsHostService(
            new CollectorRuntimeState(), new ReadAdmission(), new MonitoredServerRegistryState(), logger, () => config, inContainer: false);
        await host.StartAsync(ct);
        Assert.True(host.IsListening);

        Assert.Null(await host.StoreDataSourceAsync(ct));
        Assert.Null(await host.StoreDataSourceAsync(ct));

        var line = Assert.Single(logger.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Equal(LogLevel.Error, line.Level);
        Assert.Contains("metrics port", line.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("web dashboard", line.Text, StringComparison.Ordinal);
        await host.StopAsync(ct);
    }

    [Fact]
    public async Task KestrelLimits_AreTheDocumentedOnes()
    {
        var port = DarlingHealthHostTests.FreePort();
        await using var host = NewHost(new CapturingLogger(), MetricsOn(port, FileToken));
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(host.IsListening);

        var options = host.Services!.GetRequiredService<IOptions<KestrelServerOptions>>().Value;

        Assert.False(options.AddServerHeader);
        Assert.Equal(64, options.Limits.MaxConcurrentConnections);
        Assert.Equal(TimeSpan.FromSeconds(15), options.Limits.KeepAliveTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), options.Limits.RequestHeadersTimeout);
        Assert.Equal(8 * 1024, options.Limits.MaxRequestHeadersTotalSize);
        Assert.Equal(0, options.Limits.MaxRequestBodySize);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>A real scrape: 200, the exposition content type, the meta series of a host that has not refreshed yet, and no Server
    /// header.</summary>
    [Fact]
    public async Task RealResponse_HasNoServerHeader()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = DarlingHealthHostTests.FreePort();
        await using var host = NewHost(new CapturingLogger(), MetricsOn(port, FileToken));
        await host.StartAsync(ct);
        Assert.True(host.IsListening);

        using var client = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, $"http://127.0.0.1:{port}/metrics");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", GoodToken);
        using var response = await client.SendAsync(request, ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.False(response.Headers.Contains("Server"));
        Assert.Equal(PrometheusText.ContentType, response.Content.Headers.ContentType?.ToString());
        Assert.Contains("darling_metrics_last_refresh_success 0\n", await response.Content.ReadAsStringAsync(ct), StringComparison.Ordinal);

        /* The real listener runs the token gate: the same scrape without the header is refused. */
        using var refused = await client.GetAsync($"http://127.0.0.1:{port}/metrics", ct);
        Assert.Equal(HttpStatusCode.Unauthorized, refused.StatusCode);
        Assert.Equal("", await refused.Content.ReadAsStringAsync(ct));
        await host.StopAsync(ct);
    }

    [Fact]
    public async Task OtherPaths_Are404_OtherMethods_Are405_WithEmptyBodies()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = DarlingHealthHostTests.FreePort();
        await using var host = NewHost(new CapturingLogger(), MetricsOn(port, FileToken));
        await host.StartAsync(ct);
        Assert.True(host.IsListening);
        using var client = new HttpClient();

        foreach (var path in new[] { "/", "/livez", "/api/fleet" })
        {
            using var response = await client.GetAsync($"http://127.0.0.1:{port}{path}", ct);
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("", await response.Content.ReadAsStringAsync(ct));
        }

        foreach (var method in new[] { "POST", "PUT", "DELETE", "PATCH", "OPTIONS" })
        {
            using var request = new HttpRequestMessage(new HttpMethod(method), $"http://127.0.0.1:{port}/metrics");
            using var response = await client.SendAsync(request, ct);
            Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
            Assert.Equal("", await response.Content.ReadAsStringAsync(ct));
        }

        await host.StopAsync(ct);
    }

    [Fact]
    public async Task LiteralToken_WarnsOnce()
    {
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(DarlingHealthHostTests.FreePort(), GoodToken));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("metrics.token", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain(logger.Lines, l => l.Text.Contains(GoodToken, StringComparison.Ordinal));
    }

    [Fact]
    public async Task FileToken_GivesNoPlaintextWarning()
    {
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(DarlingHealthHostTests.FreePort(), FileToken));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        Assert.DoesNotContain(logger.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Information);
    }

    [Fact]
    public async Task WideListen_EmptyAllowFrom_Warns()
    {
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(DarlingHealthHostTests.FreePort(), FileToken, listen: "0.0.0.0"));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("metrics.allowFrom", warning.Text, StringComparison.Ordinal);
        Assert.Contains("token alone", warning.Text, StringComparison.Ordinal);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Information);
    }

    /// <summary>#9326: allowFrom limits who can connect, not what crosses the network, so a plain-HTTP bind off loopback warns
    /// about the token whatever allowFrom holds. The warning does not say allowFrom is empty, because it is not.</summary>
    [Fact]
    public async Task WideListen_WithAllowFrom_WarnsThatTheTokenCrossesUnencrypted()
    {
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(DarlingHealthHostTests.FreePort(), FileToken, listen: "0.0.0.0", allowFrom: "10.0.0.0/8"));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("plain HTTP", warning.Text, StringComparison.Ordinal);
        Assert.Contains("unencrypted", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("allowFrom is empty", warning.Text, StringComparison.Ordinal);
    }

    /// <summary>#9326: a loopback bind never warns about plain HTTP, with or without allowFrom.</summary>
    [Fact]
    public async Task LoopbackListen_WithAllowFrom_GivesNoWarning()
    {
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(DarlingHealthHostTests.FreePort(), FileToken, listen: "127.0.0.1", allowFrom: "10.0.0.0/8"));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        Assert.DoesNotContain(logger.Lines, l => l.Level >= LogLevel.Warning);
    }

    private sealed class TlsFixture : IDisposable
    {
        private readonly string _dir = Path.Combine(Path.GetTempPath(), "darling-metrics-tls-" + Guid.NewGuid().ToString("N"));

        public TlsFixture(bool usable = true, bool dnsNameOnly = false)
        {
            Directory.CreateDirectory(_dir);
            var pfx = Path.Combine(_dir, "web.pfx");
            var secret = Path.Combine(_dir, "web.secret");
            File.WriteAllText(secret, "hunter2");
            if (usable)
            {
                using var rsa = RSA.Create(2048);
                var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                var san = new SubjectAlternativeNameBuilder();
                san.AddDnsName("localhost");
                if (!dnsNameOnly)
                {
                    san.AddIpAddress(IPAddress.Loopback);
                }
                request.CertificateExtensions.Add(san.Build());
                using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));
                File.WriteAllBytes(pfx, cert.Export(X509ContentType.Pkcs12, "hunter2"));
            }

            WebBlock = "\"web\":{\"network\":{\"tls\":{\"pfxPath\":" + Quote(pfx) + ",\"pfxPassword\":" + Quote("file:" + secret) + "}}},";
        }

        /// <summary>The <c>web</c> block that names this certificate, ready to pass as <c>before</c> to <c>MetricsOn</c>.</summary>
        public string WebBlock { get; }

        public void Dispose()
        {
            try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary>#9326: with web.network.tls set, the metrics port serves HTTPS with that certificate, the start line says https, and a
    /// plain-HTTP request to the port does not reach /metrics.</summary>
    [Fact]
    public async Task WebCertificate_MetricsPortServesHttps_NotPlainHttp()
    {
        var ct = TestContext.Current.CancellationToken;
        using var fixture = new TlsFixture();
        var port = DarlingHealthHostTests.FreePort();
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(port, FileToken, before: fixture.WebBlock));

        await host.StartAsync(ct);

        Assert.True(host.IsListening);
        Assert.Contains(logger.Lines, l => l.Level == LogLevel.Information && l.Text.Contains($"https://127.0.0.1:{port}", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Lines, l => l.Text.Contains("http://127.0.0.1", StringComparison.Ordinal));

        using var handler = new HttpClientHandler { ServerCertificateCustomValidationCallback = (_, _, _, _) => true };
        using var client = new HttpClient(handler);
        using var request = new HttpRequestMessage(HttpMethod.Get, $"https://127.0.0.1:{port}/metrics");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", GoodToken);
        using var response = await client.SendAsync(request, ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("darling_metrics_last_refresh_success", await response.Content.ReadAsStringAsync(ct), StringComparison.Ordinal);

        using var plain = new HttpRequestMessage(HttpMethod.Get, $"http://127.0.0.1:{port}/metrics");
        plain.Headers.Authorization = new AuthenticationHeaderValue("Bearer", GoodToken);
        await Assert.ThrowsAnyAsync<HttpRequestException>(async () => await client.SendAsync(plain, ct));
        await host.StopAsync(ct);
    }

    /// <summary>#9326: HTTPS removes the plain-HTTP warning. A wide bind with allowFrom and a certificate starts clean.</summary>
    [Fact]
    public async Task WebCertificate_WideListenWithAllowFrom_GivesNoBindWarning()
    {
        using var fixture = new TlsFixture();
        var logger = new CapturingLogger();
        await using var host = NewHost(
            logger, MetricsOn(DarlingHealthHostTests.FreePort(), FileToken, listen: "0.0.0.0", allowFrom: "10.0.0.0/8", before: fixture.WebBlock));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        Assert.DoesNotContain(logger.Lines, l => l.Text.Contains("not loopback", StringComparison.Ordinal));
    }

    /// <summary>#9326: the metrics port has no Host allowlist, so a certificate whose names are all DNS names works when a scraper
    /// reaches the port by that name. The start logs no warning about the bind address missing from the certificate.</summary>
    [Fact]
    public async Task DnsNameOnlyCertificate_OnTheDefaultListen_ServesHttpsWithoutAWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        using var fixture = new TlsFixture(dnsNameOnly: true);
        var port = DarlingHealthHostTests.FreePort();
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(port, FileToken, before: fixture.WebBlock));

        await host.StartAsync(ct);

        Assert.True(host.IsListening);
        Assert.DoesNotContain(logger.Lines, l => l.Level >= LogLevel.Warning);

        using var handler = new HttpClientHandler { ServerCertificateCustomValidationCallback = (_, _, _, _) => true };
        using var client = new HttpClient(handler);
        using var request = new HttpRequestMessage(HttpMethod.Get, $"https://127.0.0.1:{port}/metrics");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", GoodToken);
        using var response = await client.SendAsync(request, ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        await host.StopAsync(ct);
    }

    /// <summary>#9326: a certificate on a wide bind with an empty allowFrom removes the plain-HTTP part of the warning and keeps the
    /// allowFrom part: one Warning line, and it does not say the token crosses the network unencrypted.</summary>
    [Fact]
    public async Task WebCertificate_WideListenWithEmptyAllowFrom_WarnsAboutAllowFromOnly()
    {
        using var fixture = new TlsFixture();
        var logger = new CapturingLogger();
        await using var host = NewHost(
            logger, MetricsOn(DarlingHealthHostTests.FreePort(), FileToken, listen: "0.0.0.0", before: fixture.WebBlock));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("metrics.allowFrom", warning.Text, StringComparison.Ordinal);
        Assert.Contains("token alone", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("unencrypted", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("plain HTTP", warning.Text, StringComparison.Ordinal);
    }

    /// <summary>#9326: a web.network.tls block that cannot be loaded keeps the metrics port off, with one Critical line naming the
    /// block. The operator asked for TLS, so the port does not fall back to plain HTTP.</summary>
    [Fact]
    public async Task WebCertificateThatCannotBeLoaded_KeepsThePortOff()
    {
        var ct = TestContext.Current.CancellationToken;
        using var fixture = new TlsFixture(usable: false);
        var port = DarlingHealthHostTests.FreePort();
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(port, FileToken, before: fixture.WebBlock));

        await host.StartAsync(ct);

        Assert.False(host.IsListening);
        Assert.Null(host.Services);
        /* One Critical line, and it says what happens to this port: the shared TLS path's refusal ends "binding loopback-only" for web and MCP,
           which is not what the metrics port does. */
        var critical = Assert.Single(logger.Lines, l => l.Level == LogLevel.Critical);
        Assert.Contains("web.network.tls", critical.Text, StringComparison.Ordinal);
        Assert.Contains("the metrics port stays off", critical.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("loopback-only", critical.Text, StringComparison.Ordinal);
        if (!PortIsFree(port))
        {
            Assert.Skip($"Port {port} is in use on this machine, so a refused connect cannot be checked.");
        }

        using var tcp = new TcpClient();
        await Assert.ThrowsAnyAsync<SocketException>(async () => await tcp.ConnectAsync(IPAddress.Loopback, port, ct));
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("127.0.0.1", false)]
    [InlineData("127.0.0.1", true)]
    [InlineData("::1", false)]
    public async Task LoopbackListen_GivesNoWarning(string listen, bool inContainer)
    {
        Assert.SkipWhen(listen.Contains(':') && !Socket.OSSupportsIPv6, "This machine has no IPv6.");
        var logger = new CapturingLogger();
        await using var host = NewHost(logger, MetricsOn(DarlingHealthHostTests.FreePort(), FileToken, listen: listen), inContainer);

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(host.IsListening);
        Assert.DoesNotContain(logger.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Information);
    }

    [Fact]
    public async Task BindFailure_LogsAndDoesNotThrow()
    {
        var taken = new TcpListener(IPAddress.Loopback, 0);
        taken.Start();
        try
        {
            var port = ((IPEndPoint)taken.LocalEndpoint).Port;
            var logger = new CapturingLogger();
            await using var host = NewHost(logger, MetricsOn(port, FileToken));

            await host.StartAsync(TestContext.Current.CancellationToken);

            Assert.False(host.IsListening);
            var line = Assert.Single(logger.Lines, l => l.Level == LogLevel.Error);
            Assert.Contains("127.0.0.1", line.Text);
            Assert.Contains(port.ToString(), line.Text);
            Assert.DoesNotContain(logger.Lines, l => l.Level == LogLevel.Warning);
        }
        finally
        {
            taken.Stop();
        }
    }

    [Fact]
    public async Task FailedStart_WhoseTeardownThrows_StillDoesNotThrow()
    {
        var taken = new TcpListener(IPAddress.Loopback, 0);
        taken.Start();
        try
        {
            var port = ((IPEndPoint)taken.LocalEndpoint).Port;
            var logger = new CapturingLogger();
            var disposeCalls = 0;
            await using var host = NewHost(logger, MetricsOn(port, FileToken),
                disposeApp: async app =>
                {
                    disposeCalls++;
                    await app.DisposeAsync();
                    throw new InvalidOperationException("Kestrel failed again on teardown");
                });

            await host.StartAsync(TestContext.Current.CancellationToken);

            Assert.Equal(1, disposeCalls);
            Assert.False(host.IsListening);
            Assert.Single(logger.Lines, l => l.Level == LogLevel.Error);
        }
        finally
        {
            taken.Stop();
        }
    }

    [Fact]
    public async Task DisposeAsync_WhoseTeardownThrows_DoesNotThrow()
    {
        var port = DarlingHealthHostTests.FreePort();
        var host = NewHost(new CapturingLogger(), MetricsOn(port, FileToken),
            disposeApp: async app =>
            {
                await app.DisposeAsync();
                throw new InvalidOperationException("Kestrel failed on teardown");
            });
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(host.IsListening);

        await host.DisposeAsync();

        Assert.False(host.IsListening);
    }
}
