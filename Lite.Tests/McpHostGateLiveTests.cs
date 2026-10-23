/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The REAL <see cref="McpHostService"/> (its constructor's serving decision, <c>ExecuteAsync</c>, the pipeline and the SDK's tool
/// registration) listening on a free localhost port, spoken to over HTTP. What a client sees: a request with no token or a wrong
/// one is refused 401; the right token lists the tools; with <c>mcp_allow_tokenless_loopback</c> on, no write or investigation tool is
/// listed whatever the other settings say; a token under 16 characters serves no write or investigation tool; New token makes the old
/// token fail at once.
/// </summary>
public sealed class McpHostGateLiveTests : IDisposable
{
    private const string Token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private const string FifteenCharToken = "abcdefghijklmno";
    private const string SixteenCharToken = "abcdefghijklmnop";
    private const string SixteenCopiesOfOneCharacter = "aaaaaaaaaaaaaaaa";
    private const string FiveDistinctCharacters = "abcdeabcdeabcdea";
    private const string SixDistinctCharacters = "abcdefabcdefabcd";

    private readonly string _folder = Path.Combine(Path.GetTempPath(), "lite-mcp-host-" + Guid.NewGuid().ToString("N"));
    private readonly List<RunningHost> _hosts = [];
    private readonly List<DuckDbInitializer> _ducks = [];

    public McpHostGateLiveTests() => Directory.CreateDirectory(_folder);

    public void Dispose()
    {
        foreach (var host in _hosts)
        {
            try { host.Service.StopAsync(CancellationToken.None).GetAwaiter().GetResult(); } catch { /* best effort */ }
            host.Client.Dispose();
        }

        foreach (var duck in _ducks)
        {
            duck.Dispose();
        }

        try { Directory.Delete(_folder, recursive: true); } catch { /* best effort */ }
    }

    /// <summary>An <see cref="IInvestigationRequestStore"/> nothing calls: tools/list reads no store.</summary>
    public class NoStore : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => null;
    }

    private sealed record RunningHost(McpHostService Service, HttpClient Client);

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    private async Task<RunningHost> StartAsync(string? token, bool allowTokenless, bool allowWrites = false, bool allowInvestigations = false)
    {
        var port = FreePort();
        var duck = new DuckDbInitializer(Path.Combine(_folder, $"m{port}.duckdb"));
        _ducks.Add(duck);
        var service = new McpHostService(
            new LocalDataService(duck),
            new ServerManager(_folder),
            new MuteRuleService(new AlertWebhookCancelTests.NoMuteRules(), new AppLoggerAdapter<MuteRuleService>()),
            duck,
            port,
            DispatchProxy.Create<IInvestigationRequestStore, NoStore>(),
            token,
            allowTokenless,
            allowWrites: allowWrites,
            allowInvestigations: allowInvestigations);
        await service.StartAsync(CancellationToken.None);

        var client = new HttpClient { BaseAddress = new Uri($"http://localhost:{port}/"), Timeout = TimeSpan.FromSeconds(30) };
        var host = new RunningHost(service, client);
        _hosts.Add(host);

        /* The listener comes up after StartAsync returns; wait until the port accepts a connection. */
        for (var attempt = 0; attempt < 100; attempt++)
        {
            try
            {
                using var probe = new TcpClient();
                await probe.ConnectAsync(IPAddress.Loopback, port);
                break;
            }
            catch (SocketException)
            {
                await Task.Delay(100);
            }
        }

        return host;
    }

    private static async Task<(int Status, string Body)> ToolsListAsync(RunningHost host, string? authorization)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/")
        {
            Content = new StringContent("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}", Encoding.UTF8, "application/json"),
        };
        request.Headers.TryAddWithoutValidation("Accept", "application/json, text/event-stream");
        if (authorization is not null)
        {
            request.Headers.TryAddWithoutValidation("Authorization", authorization);
        }

        using var response = await host.Client.SendAsync(request);
        return ((int)response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    private static string[] ToolNames(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        return document.RootElement.GetProperty("result").GetProperty("tools").EnumerateArray()
            .Select(t => t.GetProperty("name").GetString() ?? "").ToArray();
    }

    private static async Task<string[]> ListedAsync(RunningHost host, string? token)
    {
        var (status, body) = await ToolsListAsync(host, token is null ? null : "Bearer " + token);
        Assert.Equal(200, status);
        return ToolNames(body);
    }

    [Fact]
    public async Task WithAToken_NoToken_AndAWrongToken_AreRefused401_AndTheRightTokenListsTheTools()
    {
        var host = await StartAsync(Token, allowTokenless: false);

        var (noToken, _) = await ToolsListAsync(host, authorization: null);
        var (wrongToken, _) = await ToolsListAsync(host, "Bearer " + Token[..^1] + "e");
        var tools = await ListedAsync(host, Token);

        Assert.Equal(401, noToken);
        Assert.Equal(401, wrongToken);
        Assert.Contains("analyze_server", tools);
    }

    [Fact]
    public async Task WithTheOptOutOn_WritesAndInvestigationsAsked_NoWriteOrInvestigationToolIsListed()
    {
        var host = await StartAsync(token: null, allowTokenless: true, allowWrites: true, allowInvestigations: true);

        var tools = await ListedAsync(host, token: null);

        Assert.Contains("analyze_server", tools);
        Assert.DoesNotContain(tools, McpWritePolicy.IsWrite);
        Assert.DoesNotContain(tools, McpInvestigationPolicy.IsInvestigationTool);
    }

    [Fact]
    public async Task WithTheOptOutOn_InvestigationsAsked_NoInvestigationToolIsListed()
    {
        var host = await StartAsync(token: null, allowTokenless: true, allowInvestigations: true);

        var tools = await ListedAsync(host, token: null);

        Assert.DoesNotContain(tools, McpInvestigationPolicy.IsInvestigationTool);
    }

    [Fact]
    public async Task AFifteenCharacterToken_StillGatesRequests_ButServesNoWriteOrInvestigationTool()
    {
        var host = await StartAsync(FifteenCharToken, allowTokenless: false, allowWrites: true);
        var hostInvestigations = await StartAsync(FifteenCharToken, allowTokenless: false, allowInvestigations: true);

        var (noToken, _) = await ToolsListAsync(host, authorization: null);
        var writes = await ListedAsync(host, FifteenCharToken);
        var investigations = await ListedAsync(hostInvestigations, FifteenCharToken);

        Assert.Equal(401, noToken);
        Assert.DoesNotContain(writes, McpWritePolicy.IsWrite);
        Assert.DoesNotContain(investigations, McpInvestigationPolicy.IsInvestigationTool);
    }

    [Fact]
    public async Task ASixteenCharacterToken_ServesTheWriteTools_WhenAsked()
    {
        var host = await StartAsync(SixteenCharToken, allowTokenless: false, allowWrites: true);

        var tools = await ListedAsync(host, SixteenCharToken);

        Assert.Contains("mute_analysis_finding", tools);
        Assert.Equal(McpWritePolicy.WriteTools.Order(), tools.Where(McpWritePolicy.IsWrite).Order());
        Assert.DoesNotContain(tools, McpInvestigationPolicy.IsInvestigationTool);
    }

    [Theory]
    [InlineData(SixteenCopiesOfOneCharacter)]
    [InlineData(FiveDistinctCharacters)]
    public async Task ASixteenCharacterTokenWithFewerThanSixDistinctCharacters_StillGatesRequests_ButServesNoWriteOrInvestigationTool(string token)
    {
        var host = await StartAsync(token, allowTokenless: false, allowWrites: true);
        var hostInvestigations = await StartAsync(token, allowTokenless: false, allowInvestigations: true);

        var (noToken, _) = await ToolsListAsync(host, authorization: null);
        var writes = await ListedAsync(host, token);
        var investigations = await ListedAsync(hostInvestigations, token);

        Assert.Equal(401, noToken);
        Assert.DoesNotContain(writes, McpWritePolicy.IsWrite);
        Assert.DoesNotContain(investigations, McpInvestigationPolicy.IsInvestigationTool);
    }

    [Fact]
    public async Task ASixteenCharacterTokenWithSixDistinctCharacters_ServesTheWriteAndInvestigationTools_WhenAsked()
    {
        var host = await StartAsync(SixDistinctCharacters, allowTokenless: false, allowWrites: true);
        var hostInvestigations = await StartAsync(SixDistinctCharacters, allowTokenless: false, allowInvestigations: true);

        var writes = await ListedAsync(host, SixDistinctCharacters);
        var investigations = await ListedAsync(hostInvestigations, SixDistinctCharacters);

        Assert.Equal(McpWritePolicy.WriteTools.Order(), writes.Where(McpWritePolicy.IsWrite).Order());
        Assert.Equal(McpInvestigationPolicy.InvestigationTools.Order(), investigations.Where(McpInvestigationPolicy.IsInvestigationTool).Order());
    }

    [Fact]
    public void ARepetitiveToken_GetsTheCriticalLine_ThatNamesTheDistinctCharacterRequirement()
    {
        var lines = McpHostService.StartupLines(McpHostService.DecideServing(SixteenCopiesOfOneCharacter, false, allowWrites: true, allowInvestigations: false));

        Assert.Single(lines, l => l.Level == LogLevel.Critical && l.Message.Contains("6 different characters", StringComparison.Ordinal));
    }

    /// <summary>The pipeline's order, by outcome: a request whose Host is not loopback is refused 400 even with no token and even
    /// with the right one, so the Host check runs before the token gate and before MapMcp.</summary>
    [Fact]
    public async Task ARequestWithAForeignHost_IsRefused400_BeforeTheTokenGateLooksAtIt()
    {
        var host = await StartAsync(Token, allowTokenless: false);

        var (noToken, _) = await PostAsync(host, authorization: null, hostHeader: "evil.example", contentType: "application/json");
        var (rightToken, _) = await PostAsync(host, "Bearer " + Token, hostHeader: "evil.example", contentType: "application/json");
        var (loopbackNoToken, _) = await PostAsync(host, authorization: null, hostHeader: null, contentType: "application/json");

        Assert.Equal(400, noToken);
        Assert.Equal(400, rightToken);
        Assert.Equal(401, loopbackNoToken);
    }

    /// <summary>A POST that is not JSON is refused 415 by the JSON guard when the token is right, and 401 by the token gate when it
    /// is not, so the token gate runs first and the JSON guard runs before MapMcp.</summary>
    [Fact]
    public async Task ANonJsonPost_WithTheRightToken_IsRefused415_AndWithoutOneIsRefused401()
    {
        var host = await StartAsync(Token, allowTokenless: false);

        var (withToken, _) = await PostAsync(host, "Bearer " + Token, hostHeader: null, contentType: "text/plain");
        var (withoutToken, _) = await PostAsync(host, authorization: null, hostHeader: null, contentType: "text/plain");
        var (json, _) = await PostAsync(host, "Bearer " + Token, hostHeader: null, contentType: "application/json");

        Assert.Equal(415, withToken);
        Assert.Equal(401, withoutToken);
        Assert.Equal(200, json);
    }

    private static async Task<(int Status, string Body)> PostAsync(RunningHost host, string? authorization, string? hostHeader, string contentType)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/")
        {
            Content = new StringContent("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}", Encoding.UTF8),
        };
        request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(contentType);
        request.Headers.TryAddWithoutValidation("Accept", "application/json, text/event-stream");
        if (hostHeader is not null)
        {
            request.Headers.Host = hostHeader;
        }

        if (authorization is not null)
        {
            request.Headers.TryAddWithoutValidation("Authorization", authorization);
        }

        using var response = await host.Client.SendAsync(request);
        return ((int)response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task WithATokenAndWritesOff_TheInvestigationToolsAreListed_AndWithBothAsked_OnlyTheWritesAre()
    {
        var investigations = await StartAsync(Token, allowTokenless: false, allowInvestigations: true);
        var both = await StartAsync(Token, allowTokenless: false, allowWrites: true, allowInvestigations: true);

        var investigationTools = await ListedAsync(investigations, Token);
        var bothTools = await ListedAsync(both, Token);

        Assert.Equal(McpInvestigationPolicy.InvestigationTools.Order(), investigationTools.Where(McpInvestigationPolicy.IsInvestigationTool).Order());
        Assert.DoesNotContain(investigationTools, McpWritePolicy.IsWrite);
        Assert.Contains(bothTools, McpWritePolicy.IsWrite);
        Assert.DoesNotContain(bothTools, McpInvestigationPolicy.IsInvestigationTool);
    }

    [Fact]
    public async Task NewToken_MakesTheOldTokenFailAtOnce_AndTheNewOneWork()
    {
        var host = await StartAsync(Token, allowTokenless: false);
        var replacement = McpTokenStore.Generate();

        Assert.Equal(200, (await ToolsListAsync(host, "Bearer " + Token)).Status);
        Assert.True(host.Service.ReplaceToken(replacement));

        Assert.Equal(401, (await ToolsListAsync(host, "Bearer " + Token)).Status);
        Assert.Equal(200, (await ToolsListAsync(host, "Bearer " + replacement)).Status);
    }

    [Fact]
    public async Task NewToken_OnAHostWithNoToken_ChangesNothing()
    {
        var host = await StartAsync(token: null, allowTokenless: true);

        Assert.False(host.Service.ReplaceToken(McpTokenStore.Generate()));
        Assert.Equal(200, (await ToolsListAsync(host, authorization: null)).Status);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void TheHostWithNoToken_AndNoOptOut_DoesNotStart(string? token)
    {
        Assert.Throws<ArgumentException>(() => McpHostService.DecideServing(token, allowTokenlessLoopback: false, allowWrites: false, allowInvestigations: false));
    }

    [Fact]
    public void EveryToolGroupAskedForAndOff_GetsOneCriticalLine()
    {
        var optOut = McpHostService.StartupLines(McpHostService.DecideServing(null, true, allowWrites: true, allowInvestigations: true));
        var served = McpHostService.StartupLines(McpHostService.DecideServing(Token, false, allowWrites: true, allowInvestigations: false));
        var conflict = McpHostService.StartupLines(McpHostService.DecideServing(Token, false, allowWrites: true, allowInvestigations: true));

        Assert.Equal(2, optOut.Count(l => l.Level == LogLevel.Critical));
        Assert.Contains(optOut, l => l.Level == LogLevel.Critical && l.Message.Contains("write tools", StringComparison.Ordinal));
        Assert.Contains(optOut, l => l.Level == LogLevel.Critical && l.Message.Contains("investigation tools", StringComparison.Ordinal));
        Assert.DoesNotContain(served, l => l.Level == LogLevel.Critical);
        Assert.Single(conflict, l => l.Level == LogLevel.Critical);
    }
}
