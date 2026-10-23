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
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web dashboard and the MCP server on loopback, started through the real start path on a free port and asked over
/// real HTTP: an enabled surface with no token does not start and says which darling.json setting to add (once per
/// process, not on every 30-second retry); a configured token is required, so a request with no token or the wrong one
/// gets 401 and the right one passes; and <c>allowTokenlessLoopback</c> serves with no token, logging that it is on.
/// The store string points at a closed port: none of these requests reaches the store.
/// </summary>
public sealed class DarlingLoopbackTokenStartTests
{
    private const string Token = "loopback-start-test-token-0123456789abcdef";

    /// <summary>The hosts take a typed logger; this one records its lines so each test can read what the start said.</summary>
    private sealed class TypedLogger<T>(CapturingTestLogger inner) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            inner.Log(logLevel, eventId, state, exception, formatter);
    }

    private static string StoreString(int closedPort) =>
        $"Host=127.0.0.1;Port={closedPort};Username=darling;Password=x;Database=darlingtest";

    private static DarlingConfig Config(string surface, int port, string surfaceExtra)
    {
        var store = StoreString(ClosedPort());
        return DarlingConfig.Parse(
            ("{ 'postgres': { 'managed': false, 'connectionString': '" + store + "', 'webConnectionString': '" + store
            + "', 'mcpConnectionString': '" + store + "' }, 'servers': [ { 'host': 'SQL2022' } ], '" + surface
            + "': { 'enabled': true, 'port': " + port + surfaceExtra + " } }")
            .Replace('\'', '"'));
    }

    private static int ClosedPort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static DarlingHostBinding.EndpointToggle Toggle(int port) =>
        new(true, port, DarlingHostBinding.EndpointToggleOrigin.File, false, false);

    private static async Task<bool> TryStartAsync(object host, DarlingConfig config, int port)
    {
        var tryStart = host.GetType().GetMethod("TryStartServerAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        return await (Task<bool>)tryStart.Invoke(host, [config, Toggle(port), CancellationToken.None])!;
    }

    private static async Task<bool> AnswersAsync(int port)
    {
        try
        {
            using var client = new System.Net.Sockets.TcpClient();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await client.ConnectAsync(IPAddress.Loopback, port, cts.Token);
            return true;
        }
        catch (System.Net.Sockets.SocketException)
        {
            return false;
        }
    }

    private static HttpClient Client(int port) =>
        new(new HttpClientHandler { AllowAutoRedirect = false, UseCookies = false })
        {
            BaseAddress = new Uri($"http://localhost:{port}"),
            Timeout = TimeSpan.FromSeconds(30),
        };

    private static DarlingWebHostService NewWebHost(CapturingTestLogger log) =>
        new(new TypedLogger<DarlingWebHostService>(log), new WebRuntimeState(), new CollectorRuntimeState(),
            new WebTlsCertificateState(), new BaselineCache());

    private static DarlingMcpHostService NewMcpHost(CapturingTestLogger log) =>
        new(new TypedLogger<DarlingMcpHostService>(log), new McpRuntimeState(), new MonitoredServerRegistryState());

    private static async Task<HttpStatusCode> McpToolsListAsync(HttpClient client, string? bearer)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/")
        {
            Content = new StringContent("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}", Encoding.UTF8, "application/json"),
        };
        request.Headers.Accept.ParseAdd("application/json");
        request.Headers.Accept.ParseAdd("text/event-stream");
        if (bearer is not null)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
        }

        using var response = await client.SendAsync(request);
        return response.StatusCode;
    }

    private static async Task<HttpResponseMessage> WebSignInAsync(HttpClient client, string token) =>
        await client.PostAsync(
            DarlingWebHostService.LoginPath,
            new FormUrlEncodedContent([new KeyValuePair<string, string>("token", token)]));

    /// <summary><c>--enable-mcp</c> / <c>--enable-web</c> say when the endpoint they just turned on will not start for
    /// want of a token, and say nothing when a token or the opt-out is set.</summary>
    [Theory]
    [InlineData(true, "mcp")]
    [InlineData(false, "web")]
    public void EnableVerb_NamesTheTokenSetting_OnlyWhenNoTokenAndNoOptOut(bool mcp, string section)
    {
        var none = DarlingCliCommands.EnableTokenNote(mcp, Config(section, 5000, ""));
        Assert.NotNull(none);
        Assert.Contains(section + ".network.token", none, StringComparison.Ordinal);
        Assert.Contains(section + ".allowTokenlessLoopback", none, StringComparison.Ordinal);

        Assert.Null(DarlingCliCommands.EnableTokenNote(mcp, Config(section, 5000, ", 'network': { 'token': 'env:SOME_TOKEN' }")));
        Assert.Null(DarlingCliCommands.EnableTokenNote(mcp, Config(section, 5000, ", 'allowTokenlessLoopback': true")));
    }

    [Fact]
    public async Task Web_EnabledWithNoToken_DoesNotStart_AndNamesTheSettingOnce()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewWebHost(log);
        bool first, second, answered;
        try
        {
            first = await TryStartAsync(host, Config("web", port, ""), port);
            second = await TryStartAsync(host, Config("web", port, ""), port);
            answered = await AnswersAsync(port);
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }

        Assert.False(first, "an enabled dashboard with no token must not start on loopback");
        Assert.False(second);
        Assert.False(answered, "nothing may answer on the dashboard port");
        Assert.Single(log.Lines, l => l.Contains(DarlingWebHostService.MissingTokenMessage, StringComparison.Ordinal));
        Assert.Contains("web.network.token", DarlingWebHostService.MissingTokenMessage, StringComparison.Ordinal);
        Assert.Contains("web.allowTokenlessLoopback", DarlingWebHostService.MissingTokenMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Web_WithToken_NoOrWrongTokenIs401_RightTokenSignsIn()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewWebHost(log);
        try
        {
            Assert.True(await TryStartAsync(host, Config("web", port, ", 'network': { 'token': '" + Token + "' }"), port), log.Joined);
            using var client = Client(port);

            using var noToken = await client.GetAsync("/api/fleet");
            Assert.Equal(HttpStatusCode.Unauthorized, noToken.StatusCode);

            using var wrong = await WebSignInAsync(client, "not-the-token");
            Assert.NotEqual(HttpStatusCode.Redirect, wrong.StatusCode);
            Assert.False(wrong.Headers.Contains("Set-Cookie"), "a wrong token must not be exchanged for a session cookie");

            using var right = await WebSignInAsync(client, Token);
            Assert.Equal(HttpStatusCode.Redirect, right.StatusCode);
            var cookie = right.Headers.GetValues("Set-Cookie").Single();
            cookie = cookie[..cookie.IndexOf(';')];

            using var withCookie = new HttpRequestMessage(HttpMethod.Get, "/some/unmapped/path");
            withCookie.Headers.Add("Cookie", cookie);
            using var passed = await client.SendAsync(withCookie);
            Assert.Equal(HttpStatusCode.NotFound, passed.StatusCode);

            Assert.DoesNotContain(log.Lines, l => l.Contains(Token, StringComparison.Ordinal));
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }
    }

    [Fact]
    public async Task Web_OptOut_ServesWithNoToken_AndSaysSo()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewWebHost(log);
        try
        {
            Assert.True(await TryStartAsync(host, Config("web", port, ", 'allowTokenlessLoopback': true"), port), log.Joined);
            using var client = Client(port);

            using var open = await client.GetAsync("/some/unmapped/path");
            Assert.Equal(HttpStatusCode.NotFound, open.StatusCode);
            Assert.Contains(log.Lines, l => l.Contains(DarlingWebHostService.TokenlessLoopbackMessage, StringComparison.Ordinal));
            Assert.DoesNotContain(log.Lines, l => l.Contains(DarlingWebHostService.MissingTokenMessage, StringComparison.Ordinal));
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }
    }

    [Fact]
    public async Task Mcp_EnabledWithNoToken_DoesNotStart_AndNamesTheSettingOnce()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewMcpHost(log);
        bool first, second, answered;
        try
        {
            first = await TryStartAsync(host, Config("mcp", port, ""), port);
            second = await TryStartAsync(host, Config("mcp", port, ""), port);
            answered = await AnswersAsync(port);
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }

        Assert.False(first, "an enabled MCP server with no token must not start on loopback");
        Assert.False(second);
        Assert.False(answered, "nothing may answer on the MCP port");
        Assert.Single(log.Lines, l => l.Contains(DarlingMcpHostService.MissingTokenMessage, StringComparison.Ordinal));
        Assert.Contains("mcp.network.token", DarlingMcpHostService.MissingTokenMessage, StringComparison.Ordinal);
        Assert.Contains("mcp.allowTokenlessLoopback", DarlingMcpHostService.MissingTokenMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Mcp_WithToken_NoOrWrongTokenIs401_RightTokenPasses()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewMcpHost(log);
        try
        {
            Assert.True(await TryStartAsync(host, Config("mcp", port, ", 'network': { 'token': '" + Token + "' }"), port), log.Joined);
            using var client = Client(port);

            Assert.Equal(HttpStatusCode.Unauthorized, await McpToolsListAsync(client, bearer: null));
            Assert.Equal(HttpStatusCode.Unauthorized, await McpToolsListAsync(client, "not-the-token"));
            Assert.Equal(HttpStatusCode.OK, await McpToolsListAsync(client, Token));
            Assert.DoesNotContain(log.Lines, l => l.Contains(Token, StringComparison.Ordinal));
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }
    }

    /// <summary>#6542 with the loopback token rule: named tokens (<c>mcp.tokens</c>) and no legacy token are tokens. The start
    /// must not stop with the "no token" refusal, and every request, loopback included, must present one of them.</summary>
    [Fact]
    public async Task Mcp_WithOnlyNamedTokens_Starts_AndGatesOnThem()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewMcpHost(log);
        try
        {
            Assert.True(await TryStartAsync(
                host, Config("mcp", port, ", 'tokens': [ { 'name': 'ops-lead', 'token': '" + Token + "', 'role': 'operator' } ]"), port), log.Joined);
            using var client = Client(port);

            Assert.Equal(HttpStatusCode.Unauthorized, await McpToolsListAsync(client, bearer: null));
            Assert.Equal(HttpStatusCode.Unauthorized, await McpToolsListAsync(client, "not-the-token"));
            Assert.Equal(HttpStatusCode.OK, await McpToolsListAsync(client, Token));
            Assert.DoesNotContain(log.Lines, l => l.Contains(DarlingMcpHostService.MissingTokenMessage, StringComparison.Ordinal));
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }
    }

    [Fact]
    public async Task Mcp_OptOut_ServesWithNoToken_AndSaysSo()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewMcpHost(log);
        try
        {
            Assert.True(await TryStartAsync(host, Config("mcp", port, ", 'allowTokenlessLoopback': true"), port), log.Joined);
            using var client = Client(port);

            Assert.Equal(HttpStatusCode.OK, await McpToolsListAsync(client, bearer: null));
            Assert.Contains(log.Lines, l => l.Contains(DarlingMcpHostService.TokenlessLoopbackMessage, StringComparison.Ordinal));
            Assert.DoesNotContain(log.Lines, l => l.Contains(DarlingMcpHostService.MissingTokenMessage, StringComparison.Ordinal));
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }
    }

    private static async Task<(HttpStatusCode Status, string Body)> SendAsync(HttpClient client, HttpMethod method, string path, string? cookie)
    {
        using var request = new HttpRequestMessage(method, path);
        if (method != HttpMethod.Get)
        {
            request.Content = new StringContent("{}", Encoding.UTF8, "application/json");
        }

        if (cookie is not null)
        {
            request.Headers.Add("Cookie", cookie);
        }

        using var response = await client.SendAsync(request);
        return (response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    private static async Task<string> SignInCookieAsync(HttpClient client, string token)
    {
        using var right = await WebSignInAsync(client, token);
        Assert.Equal(HttpStatusCode.Redirect, right.StatusCode);
        var cookie = right.Headers.GetValues("Set-Cookie").Single();
        return cookie[..cookie.IndexOf(';')];
    }

    /// <summary>The opt-out serves reads only: a read is served, the session says it cannot edit, and a write (adding a
    /// server) is refused before it reaches the route, with a message that names the token setting.</summary>
    [Fact]
    public async Task Web_OptOut_ServesReads_AndRefusesAWrite_NamingTheTokenSetting()
    {
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewWebHost(log);
        try
        {
            Assert.True(await TryStartAsync(host, Config("web", port, ", 'allowTokenlessLoopback': true"), port), log.Joined);
            using var client = Client(port);

            var session = await SendAsync(client, HttpMethod.Get, "/api/session", cookie: null);
            Assert.Equal(HttpStatusCode.OK, session.Status);
            Assert.Contains("\"can_edit\":false", session.Body.Replace(" ", ""), StringComparison.Ordinal);

            var add = await SendAsync(client, HttpMethod.Post, "/api/servers", cookie: null);
            Assert.Equal(HttpStatusCode.Forbidden, add.Status);
            Assert.Contains("web.allowTokenlessLoopback", add.Body, StringComparison.Ordinal);
            Assert.Contains("web.network.encryptedToken", add.Body, StringComparison.Ordinal);

            Assert.Contains("reads only", DarlingWebHostService.TokenlessLoopbackMessage, StringComparison.Ordinal);
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }
    }

    /// <summary>A loopback token under the length floor still signs a browser in, but the dashboard serves reads only with
    /// it, and the start says why in one line; a token that clears the floor keeps the edit seat.</summary>
    [Fact]
    public async Task Web_ShortLoopbackToken_SignsIn_ButKeepsWritesOff_AndSaysWhy()
    {
        const string shortToken = "short-token-12";
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewWebHost(log);
        try
        {
            Assert.True(await TryStartAsync(host, Config("web", port, ", 'network': { 'token': '" + shortToken + "' }"), port), log.Joined);
            using var client = Client(port);
            var cookie = await SignInCookieAsync(client, shortToken);

            var session = await SendAsync(client, HttpMethod.Get, "/api/session", cookie);
            Assert.Equal(HttpStatusCode.OK, session.Status);
            Assert.Contains("\"can_edit\":false", session.Body.Replace(" ", ""), StringComparison.Ordinal);

            var add = await SendAsync(client, HttpMethod.Post, "/api/servers", cookie);
            Assert.Equal(HttpStatusCode.Forbidden, add.Status);
            Assert.Contains("needs at least " + DarlingHostBinding.MinimumTokenLength + " characters", add.Body, StringComparison.Ordinal);

            Assert.Single(log.Lines, l => l.Contains("shorter than " + DarlingHostBinding.MinimumTokenLength, StringComparison.Ordinal)
                && l.Contains("serves reads only", StringComparison.Ordinal));
            Assert.DoesNotContain(log.Lines, l => l.Contains(shortToken, StringComparison.Ordinal));
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }

        port = ClosedPort();
        var longLog = new CapturingTestLogger();
        var longHost = NewWebHost(longLog);
        try
        {
            Assert.True(await TryStartAsync(longHost, Config("web", port, ", 'network': { 'token': '" + Token + "' }"), port), longLog.Joined);
            using var client = Client(port);
            var cookie = await SignInCookieAsync(client, Token);
            var session = await SendAsync(client, HttpMethod.Get, "/api/session", cookie);
            Assert.Contains("\"can_edit\":true", session.Body.Replace(" ", ""), StringComparison.Ordinal);
        }
        finally
        {
            await longHost.DisposeFailedStartAsync();
        }
    }

    /// <summary>The MCP twin: a short loopback token still gates every request, and with neither the writes nor the
    /// investigation tools asked for, the start names the problem once.</summary>
    [Fact]
    public async Task Mcp_ShortLoopbackToken_StillGates_AndSaysWhyOnce()
    {
        const string shortToken = "short-token-12";
        var port = ClosedPort();
        var log = new CapturingTestLogger();
        var host = NewMcpHost(log);
        try
        {
            Assert.True(await TryStartAsync(host, Config("mcp", port, ", 'network': { 'token': '" + shortToken + "' }"), port), log.Joined);
            using var client = Client(port);
            Assert.Equal(HttpStatusCode.Unauthorized, await McpToolsListAsync(client, bearer: null));
            Assert.Equal(HttpStatusCode.OK, await McpToolsListAsync(client, shortToken));
            Assert.Single(log.Lines, l => l.Contains("MCP token (mcp.network.encryptedToken or mcp.network.token) is shorter than", StringComparison.Ordinal));
            Assert.DoesNotContain(log.Lines, l => l.Contains(shortToken, StringComparison.Ordinal));
        }
        finally
        {
            await host.DisposeFailedStartAsync();
        }
    }

    /// <summary>Turning network exposure off keeps the mcp and web tokens, so each endpoint keeps asking for its token on
    /// loopback: only listen, allowFrom, tls, hostName, trustedProxies and oidc go, and a block with nothing else goes whole.</summary>
    [Fact]
    public void DisableNetwork_KeepsTheTokens_AndRemovesOnlyTheExposureKeys()
    {
        var original = ("{ 'postgres': { 'managed': true, 'network': { 'listen': '192.168.1.5', 'allowFrom': '192.168.1.0/24', 'role': 'viewer' } },\n"
            + "  'servers': [ { 'host': 'SQL2022' } ],\n"
            + "  'mcp': {\n    'enabled': true,\n    'network': {\n      'listen': '192.168.1.5',\n      'allowFrom': '192.168.1.0/24',\n"
            + "      'token': 'env:MCP_TOKEN', // kept\n      'hostName': 'box.example'\n    }\n  },\n"
            + "  'web': {\n    'enabled': true,\n    'network': {\n      'listen': '192.168.1.5',\n      'allowFrom': '192.168.1.0/24',\n"
            + "      'encryptedToken': 'AQAAAblob',\n      'trustedProxies': [ '10.0.0.5' ],\n      'tls': { 'pfxPath': 'c:/x.pfx' },\n"
            + "      'hsts': true\n    }\n  }\n}").Replace('\'', '"');

        var (afterPostgres, afterMcp, text) = DarlingCliCommands.DisabledExposureText(original);
        var config = DarlingConfig.Parse(text);

        Assert.Null(config.Postgres.Network);
        Assert.Equal("env:MCP_TOKEN", config.Mcp.Network?.Token);
        Assert.Null(config.Mcp.Network?.Listen);
        Assert.Null(config.Mcp.Network?.AllowFrom);
        Assert.Null(config.Mcp.Network?.HostName);
        Assert.Equal("AQAAAblob", config.Web.Network?.EncryptedToken);
        Assert.Null(config.Web.Network?.Listen);
        Assert.Null(config.Web.Network?.Tls);
        Assert.Null(config.Web.Network?.TrustedProxies);
        Assert.True(config.Web.Network?.Hsts);
        Assert.Contains("// kept", text, StringComparison.Ordinal);
        Assert.NotEqual(afterPostgres, afterMcp);
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, DarlingWebHostService.ResolveWebBind(config.Web, config.Postgres.Managed).Mode);

        var noToken = "{ \"servers\": [ { \"host\": \"SQL2022\" } ], \"mcp\": { \"network\": { \"listen\": \"192.168.1.5\", \"allowFrom\": \"192.168.1.0/24\" } } }";
        Assert.Null(DarlingConfig.Parse(DarlingCliCommands.DisabledExposureText(noToken).NewText).Mcp.Network);
    }
}
