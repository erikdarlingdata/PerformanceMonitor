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
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5288: a live proof that the MCP host's network listener serves HTTPS when a certificate was adopted, and plain
/// HTTP when none was, over REAL Kestrel on 127.0.0.1 with an ephemeral port (not <c>TestServer</c>, which has no
/// TLS layer to prove anything about). Everything the host composes at start is the production code, in the
/// production order: <see cref="DarlingListenerTls.Resolve"/> reads the <c>mcp.network.tls</c> block with the MCP
/// labels and the normalized host name, <see cref="DarlingMcpHostService.ConfigureListeners"/> binds the listeners
/// exactly as <c>TryStartServerAsync</c> does, and <see cref="DarlingMcpHostService.ConfigurePipeline"/> installs
/// the Host guard, the bearer token and the CIDR check. The host's own glue (adopt, bail, dispose, the network-mode
/// gate) is pinned from source in <see cref="DarlingMcpHostTests"/>, because a network-mode start needs a managed
/// store and cannot be driven from a test.
///
/// <para>The client reaches <c>darling.test</c> without DNS: a <c>ConnectCallback</c> sends every connection to
/// 127.0.0.1, so the URL, the SNI name, the certificate's dNSName entry and the Host header all say
/// <c>darling.test</c>, which is what a real client reaching the endpoint by its host name sends. The certificate
/// is pinned by thumbprint, because a self-signed certificate is not in any trust store, and the name check is
/// left ON for the by-name calls: a certificate that did not name the host would fail them.</para>
/// </summary>
public sealed class DarlingMcpTlsLiveTests
{
    private const string HostName = "darling.test";
    private const string Token = "correct-mcp-token-value";
    private const string ToolsList = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}";

    /* ---- the proofs ---- */

    /// <summary>By host name over TLS with no token: the certificate is accepted by name, the Host guard admits the
    /// configured name, and the bearer gate refuses 401 before any handler runs.</summary>
    [Fact]
    public async Task ByHostNameOverTls_NoToken_Is401()
    {
        await using var rig = await StartAsync(withCertificate: true);
        Assert.Equal("https", rig.Scheme);

        using var client = Client(rig.Thumbprint, requireNameMatch: true);
        var (status, _) = await ToolsListAsync(client, $"https://{HostName}:{rig.Port}/", bearer: null);

        Assert.Equal(StatusCodes.Status401Unauthorized, status);
    }

    /// <summary>The same call with the right token gets past every gate to <c>tools/list</c>.</summary>
    [Fact]
    public async Task ByHostNameOverTls_WithToken_ToolsListIs200()
    {
        await using var rig = await StartAsync(withCertificate: true);

        using var client = Client(rig.Thumbprint, requireNameMatch: true);
        var (status, body) = await ToolsListAsync(client, $"https://{HostName}:{rig.Port}/", Token);

        Assert.True(status == StatusCodes.Status200OK, $"expected 200, got {status}: {body}");
        Assert.Contains("tools", body, StringComparison.Ordinal);
    }

    /// <summary>One port cannot speak both schemes: plain HTTP to the TLS listener fails at the handshake (or at
    /// worst gets a 400). It is never served (200) and never reaches the token gate (401), because that would mean
    /// the listener answered in the clear.</summary>
    [Fact]
    public async Task PlainHttpToTlsListener_IsRefused()
    {
        await using var rig = await StartAsync(withCertificate: true);

        using var plain = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        try
        {
            var (status, _) = await ToolsListAsync(plain, $"http://127.0.0.1:{rig.Port}/", Token);
            Assert.True(
                status == StatusCodes.Status400BadRequest,
                $"plain HTTP to the TLS listener answered {status}; only a refusal or a 400 is acceptable");
        }
        catch (HttpRequestException)
        {
            // Refused at the handshake: the expected outcome.
        }
    }

    /// <summary>A Host that is not the listen IP, loopback or the configured name is still 400 over TLS, token and
    /// all: the guard runs first. Once reached by a wrong URL name, once by a wrong Host header on the right name.</summary>
    [Fact]
    public async Task WrongHostOverTls_Is400()
    {
        await using var rig = await StartAsync(withCertificate: true);

        using var client = Client(rig.Thumbprint, requireNameMatch: false);

        var (right, rightBody) = await ToolsListAsync(client, $"https://{HostName}:{rig.Port}/", Token);
        Assert.True(right == StatusCodes.Status200OK, $"the configured name must still work: {right}: {rightBody}");

        var (byUrl, _) = await ToolsListAsync(client, $"https://evil.test:{rig.Port}/", Token);
        Assert.Equal(StatusCodes.Status400BadRequest, byUrl);

        var (byHeader, _) = await ToolsListAsync(client, $"https://{HostName}:{rig.Port}/", Token, hostHeader: "evil.test");
        Assert.Equal(StatusCodes.Status400BadRequest, byHeader);
    }

    /// <summary>With no <c>tls</c> block nothing changes: <c>Resolve</c> exposes with no certificate (after the
    /// cleartext warning), the network listener is plain HTTP exactly as before, and it does not speak TLS.</summary>
    [Fact]
    public async Task NoCertificate_NetworkListenerIsPlainHttp()
    {
        await using var rig = await StartAsync(withCertificate: false);
        Assert.Equal("http", rig.Scheme);
        Assert.Contains(rig.Log.Lines, line => line.StartsWith("Warning: MCP server is LAN-exposed WITHOUT TLS", StringComparison.Ordinal));

        using var client = Client(pinnedThumbprint: null, requireNameMatch: false);

        var (noToken, _) = await ToolsListAsync(client, $"http://{HostName}:{rig.Port}/", bearer: null);
        Assert.Equal(StatusCodes.Status401Unauthorized, noToken);

        var (withToken, body) = await ToolsListAsync(client, $"http://{HostName}:{rig.Port}/", Token);
        Assert.True(withToken == StatusCodes.Status200OK, $"expected 200 over plain HTTP, got {withToken}: {body}");

        // A TLS client gets nowhere: there is no certificate behind this listener.
        await Assert.ThrowsAnyAsync<HttpRequestException>(
            () => ToolsListAsync(client, $"https://{HostName}:{rig.Port}/", Token));
    }

    /* ---- the rig ---- */

    /// <summary>
    /// Starts the real listener the way <c>TryStartServerAsync</c> composes it in network mode: resolve the TLS
    /// block, adopt the certificate, bind through <c>ConfigureListeners</c>, install the pipeline with the
    /// normalized host name. The listen IP is loopback so the test needs no LAN address; loopback is also where the
    /// wildcard/loopback collision rule says no second set of loopback listeners is added.
    /// </summary>
    private static async Task<Rig> StartAsync(bool withCertificate)
    {
        var log = new RecordingLogger();
        var temp = Path.Combine(Path.GetTempPath(), "darling-mcp-tls-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(temp);

        WebTlsConfig? block = null;
        string? thumbprint = null;
        if (withCertificate)
        {
            using var certificate = MakeCertificate();
            thumbprint = certificate.Thumbprint;
            var pfx = Path.Combine(temp, "mcp.pfx");
            File.WriteAllBytes(pfx, certificate.Export(X509ContentType.Pkcs12, "hunter2"));
            block = new WebTlsConfig { PfxPath = pfx, PfxPassword = "hunter2" };
        }

        var outcome = DarlingListenerTls.Resolve(
            log, new McpTlsCertificateState(), ListenerTlsLabels.Mcp, block, IPAddress.Loopback, 0,
            DarlingMcpHostService.NormalizedHostName(HostName));
        Assert.True(outcome.Expose);
        Assert.Equal(withCertificate, outcome.Certificate is not null);
        var loaded = outcome.Certificate;

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = Environments.Production });
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
            DarlingMcpHostService.ConfigureListeners(options, networkMode: true, IPAddress.Loopback, 0, loaded));

        // A pool the gates never open: every case is refused, or reaches tools/list, before any tool body runs.
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        builder.Services.AddSingleton(new DarlingAnalysisService(
            postgres, planFetcher: null, logger: NullLogger.Instance, baselineCache: new BaselineCache()));
        builder.Services.AddSingleton<ILogger>(NullLogger.Instance);
        DarlingMcpHostService.ConfigureMcpServices(builder.Services, DarlingPeerDirectory.Snapshot.Empty);

        var app = builder.Build();
        var host = new DarlingMcpHostService(
            NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState());
        host.ConfigurePipeline(
            app,
            networkMode: true,
            networkListenIp: IPAddress.Loopback,
            allowedCidr: CidrAllowList.Parse("127.0.0.0/8"),
            bearerToken: Token,
            allowedHostName: DarlingMcpHostService.ResolveAllowedHostName(HostName, networkMode: true, NullLogger.Instance));
        await app.StartAsync();

        var address = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First());
        return new Rig(app, loaded, temp, address.Port, address.Scheme, thumbprint, log);
    }

    private sealed class Rig : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly DarlingWebTls.LoadedCertificate? _certificate;
        private readonly string _temp;

        public Rig(
            WebApplication app, DarlingWebTls.LoadedCertificate? certificate, string temp, int port, string scheme,
            string? thumbprint, RecordingLogger log)
        {
            _app = app;
            _certificate = certificate;
            _temp = temp;
            Port = port;
            Scheme = scheme;
            Thumbprint = thumbprint;
            Log = log;
        }

        public int Port { get; }

        public string Scheme { get; }

        public string? Thumbprint { get; }

        public RecordingLogger Log { get; }

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
            _certificate?.Dispose();
            try { Directory.Delete(_temp, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary>A client that reaches <c>darling.test</c> (or any name) at 127.0.0.1 without DNS and trusts exactly
    /// the pinned certificate. <paramref name="requireNameMatch"/> keeps the TLS stack's own name check on.</summary>
    private static HttpClient Client(string? pinnedThumbprint, bool requireNameMatch)
    {
        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (context, token) =>
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                try
                {
                    await socket.ConnectAsync(new IPEndPoint(IPAddress.Loopback, context.DnsEndPoint.Port), token);
                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            },
            SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, certificate, _, errors) =>
                    certificate is not null
                    && string.Equals(certificate.GetCertHashString(), pinnedThumbprint, StringComparison.OrdinalIgnoreCase)
                    && (!requireNameMatch || (errors & SslPolicyErrors.RemoteCertificateNameMismatch) == 0),
            },
        };
        return new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) };
    }

    private static async Task<(int Status, string Body)> ToolsListAsync(
        HttpClient client, string url, string? bearer, string? hostHeader = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(ToolsList, Encoding.UTF8, "application/json"),
        };
        request.Headers.Accept.ParseAdd("application/json");
        request.Headers.Accept.ParseAdd("text/event-stream");
        if (bearer is not null)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
        }

        if (hostHeader is not null)
        {
            request.Headers.Host = hostHeader;
        }

        using var response = await client.SendAsync(request);
        return ((int)response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    /// <summary>A self-signed certificate that names <c>darling.test</c> (dNSName) and 127.0.0.1 (iPAddress).</summary>
    private static X509Certificate2 MakeCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=darling-mcp-tls-tests", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(HostName);
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly List<string> _lines = new();

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (_lines)
                {
                    return _lines.ToArray();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add($"{logLevel}: {formatter(state, exception)}");
            }
        }
    }
}
