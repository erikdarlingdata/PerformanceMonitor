/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// A front door passes its caller's data detail level to the store (#7023), and the store serves the stricter of that and its own. The
/// front door and the store here are real MCP hosts behind in-memory servers, joined by the real peer client, with the store at
/// <c>full</c>. A narrowed reader on the front door must get free text masked, which only the store's own tool does (the front door's
/// filter masks by key, and a report's <c>root_cause</c> is not a masked key).
/// </summary>
public sealed class DarlingFrontDoorDataDetailTests
{
    private const string StoreToken = "store-operator-token-0123456789abcdef-7023";
    private const string OperatorToken = "front-operator-token-0123456789abcdef-7023";
    private const string ReaderToken = "front-masked-reader-0123456789abcdef-7023";
    private const string MetadataReaderToken = "front-metadata-reader-0123456789abcdef-7023";
    private const string Literal = "zq-lit-7023";

    /// <summary>Test-only tools with the shape of a real one: the setting comes in as a service parameter, and free text is masked through
    /// the same masker the investigation tools use.</summary>
    [McpServerToolType]
    public sealed class DetailProbeTools
    {
        [McpServerTool(Name = "detail_probe"), Description("Test-only: answers the data detail level the call was served.")]
        public static string Level(McpDataDetailSetting? maskSetting = null) =>
            new JsonObject { ["level"] = McpDataDetailModes.Word(maskSetting?.Mode ?? McpDataDetail.Full) }.ToJsonString();

        [McpServerTool(Name = "free_text_probe"), Description("Test-only: answers a report-shaped answer whose root_cause is masked as free text.")]
        public static string FreeText(McpDataDetailSetting? maskSetting = null) =>
            new JsonObject
            {
                ["root_cause"] = McpDataDetailMasker.ForAnswer(maskSetting?.Mode ?? McpDataDetail.Full).FreeText("Bad value where b = '" + Literal + "'"),
            }.ToJsonString();
    }

    private static void RegisterCommon(WebApplicationBuilder builder)
    {
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling"));
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
        builder.Services.AddSingleton<IGcfOutputFormat>(new FixedGcfOutputFormat(false));
    }

    private static async Task<TestServer> StartAsync(Microsoft.AspNetCore.Builder.WebApplication app, Action<DarlingMcpHostService, Microsoft.AspNetCore.Builder.WebApplication> pipeline)
    {
        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());
        pipeline(host, app);
        await app.StartAsync();
        return app.GetTestServer();
    }

    /// <summary>The store: one operator-role token (the legacy one), at the service level <paramref name="storeLevel"/>.</summary>
    private static async Task<TestServer> BuildStoreAsync(McpDataDetail storeLevel)
    {
        var builder = DarlingMcpHostService.CreateHostBuilder();
        RegisterCommon(builder);
        DarlingMcpHostService.RegisterDataDetail(builder.Services, storeLevel);
        DarlingMcpHostService.ConfigureMcpServices(builder.Services, DarlingPeerDirectory.Snapshot.Empty);
        builder.Services.AddMcpServer().WithTools<DetailProbeTools>();
        return await StartAsync(builder.Build(), (host, app) =>
        {
            /* A request that comes through the test server's handler has no remote address; the host admits loopback only. */
            app.Use((context, next) =>
            {
                context.Connection.RemoteIpAddress ??= IPAddress.Loopback;
                return next(context);
            });
            host.ConfigurePipeline(
                app, networkMode: false, networkListenIp: null, allowedCidr: IPNetwork.Parse("192.168.1.0/24"), bearerToken: StoreToken,
                requireTokenWhenLoopbackOnly: true);
        });
    }

    /// <summary>The front door, at <c>full</c>: an operator, a <c>maskLiterals</c> reader and a <c>metadataOnly</c> reader, forwarding to
    /// <paramref name="store"/> through the real peer client.</summary>
    private static async Task<(TestServer Server, DarlingFrontDoorMcpPeers Peers)> BuildFrontDoorAsync(TestServer store)
    {
        var config = new FrontDoorConfig
        {
            Enabled = true,
            Stores = new List<FrontDoorStoreConfig>
            {
                new() { Name = "use1", McpUrl = "http://localhost/", McpToken = StoreToken },
            },
        };
        var directory = new FakeFrontDoorDirectory("use1") { Resolver = _ => new FrontDoorRoute.ToStore("use1") };
        var peers = new DarlingFrontDoorMcpPeers(config, new DarlingFrontDoorSecrets(config), () => directory, innerHandler: _ => store.CreateHandler());
        var router = new DarlingFrontDoorRouter(directory, peers, TimeSpan.FromSeconds(30));

        var mcp = new McpConfig
        {
            Tokens = new List<McpTokenEntry>
            {
                new() { Name = "dba", Token = OperatorToken, Role = "operator" },
                new() { Name = "dev-a", Token = ReaderToken, Role = "reader", DataDetail = "maskLiterals" },
                new() { Name = "dev-b", Token = MetadataReaderToken, Role = "reader", DataDetail = "metadataOnly" },
            },
        };
        var legacy = DarlingMcpWritePolicy.ResolveForStart(mcp, "", out _);
        var tokens = DarlingMcpTokenSet.Resolve(mcp, "", legacy, null);

        var builder = DarlingMcpHostService.CreateHostBuilder();
        RegisterCommon(builder);
        DarlingMcpHostService.RegisterDataDetail(builder.Services, McpDataDetail.Full);

        /* The probe tools are registered before ConfigureMcpServices, so the front door replaces them with its forwarding tools. */
        builder.Services.AddMcpServer().WithTools<DetailProbeTools>();
        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, writes: tokens.Union, tokens: tokens, frontDoor: router);
        var server = await StartAsync(builder.Build(), (host, app) => host.ConfigurePipeline(
            app, networkMode: false, networkListenIp: null, allowedCidr: IPNetwork.Parse("192.168.1.0/24"), bearerToken: "",
            requireTokenWhenLoopbackOnly: true, tokens: tokens));
        return (server, peers);
    }

    /// <summary>The store's own answer: a front door with no server named asks every store, and wraps each store's answer.</summary>
    private static JsonElement StoreAnswer(string text)
    {
        var root = JsonDocument.Parse(text).RootElement;
        return root.TryGetProperty("stores", out var stores) ? stores[0].GetProperty("result").Clone() : root.Clone();
    }

    private static string LevelOf(string text)
    {
        var answer = StoreAnswer(text);
        Assert.True(answer.TryGetProperty("level", out var level), "No level in the answer: " + text);
        return level.GetString() ?? "";
    }

    private static string RootCauseOf(string text) => StoreAnswer(text).GetProperty("root_cause").GetString() ?? "";

    [Fact]
    public async Task AMaskedReader_ThroughAFrontDoorOverAFullStore_GetsFreeTextMasked_AndTheOperatorDoesNot()
    {
        using var store = await BuildStoreAsync(McpDataDetail.Full);
        var (front, peers) = await BuildFrontDoorAsync(store);
        using var frontHost = front;
        await using var peerClient = peers;

        /* Alternate the callers: a client built for one level must never serve another. */
        foreach (var (bearer, level) in new[]
        {
            (ReaderToken, "maskLiterals"), (OperatorToken, "full"), (ReaderToken, "maskLiterals"),
            (MetadataReaderToken, "metadataOnly"), (OperatorToken, "full"),
        })
        {
            var (probe, _) = await StatementFilterCensus.CallRealToolAsync(front, "detail_probe", new JsonObject(), bearerToken: bearer);
            Assert.Equal(level, LevelOf(probe));
        }

        var (masked, _) = await StatementFilterCensus.CallRealToolAsync(front, "free_text_probe", new JsonObject(), bearerToken: ReaderToken);
        Assert.DoesNotContain(Literal, masked, StringComparison.Ordinal);

        var (open, _) = await StatementFilterCensus.CallRealToolAsync(front, "free_text_probe", new JsonObject(), bearerToken: OperatorToken);
        Assert.Contains(Literal, RootCauseOf(open), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFrontDoorAtFull_NeverWidensAStoreThatIsStricter()
    {
        using var store = await BuildStoreAsync(McpDataDetail.MetadataOnly);
        var (front, peers) = await BuildFrontDoorAsync(store);
        using var frontHost = front;
        await using var peerClient = peers;

        var (level, _) = await StatementFilterCensus.CallRealToolAsync(front, "detail_probe", new JsonObject(), bearerToken: OperatorToken);

        Assert.Equal("metadataOnly", LevelOf(level));
    }

    /// <summary>A request to a store with the header: any client may send it, and it can only narrow. A missing, blank or unrecognized value
    /// narrows nothing, so the store's own level stands.</summary>
    [Theory]
    [InlineData(McpDataDetail.Full, null, "full")]
    [InlineData(McpDataDetail.Full, "", "full")]
    [InlineData(McpDataDetail.Full, "   ", "full")]
    [InlineData(McpDataDetail.Full, "nonsense", "full")]
    [InlineData(McpDataDetail.Full, "full", "full")]
    [InlineData(McpDataDetail.Full, "maskLiterals", "maskLiterals")]
    [InlineData(McpDataDetail.Full, "MASKLITERALS", "maskLiterals")]
    [InlineData(McpDataDetail.Full, "metadataOnly", "metadataOnly")]
    [InlineData(McpDataDetail.MaskLiterals, "full", "maskLiterals")]
    [InlineData(McpDataDetail.MaskLiterals, "nonsense", "maskLiterals")]
    [InlineData(McpDataDetail.MaskLiterals, "metadataOnly", "metadataOnly")]
    [InlineData(McpDataDetail.MetadataOnly, "full", "metadataOnly")]
    [InlineData(McpDataDetail.MetadataOnly, "maskLiterals", "metadataOnly")]
    public async Task TheStore_ServesTheStricterOfItsOwnLevelAndTheHeader(McpDataDetail storeLevel, string? header, string expected)
    {
        using var store = await BuildStoreAsync(storeLevel);

        var context = await store.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.Headers.Authorization = "Bearer " + StoreToken;
            if (header is not null)
            {
                c.Request.Headers[DarlingMcpTokenSet.DataDetailHeader] = header;
            }

            var bytes = Encoding.UTF8.GetBytes(new JsonObject
            {
                ["jsonrpc"] = "2.0",
                ["id"] = 1,
                ["method"] = "tools/call",
                ["params"] = new JsonObject { ["name"] = "detail_probe", ["arguments"] = new JsonObject() },
            }.ToJsonString());
            c.Request.ContentType = "application/json";
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });
        using var reader = new System.IO.StreamReader(context.Response.Body);
        var raw = await reader.ReadToEndAsync();
        Assert.Equal(200, context.Response.StatusCode);
        var json = raw.TrimStart().StartsWith('{')
            ? raw
            : string.Join("\n", raw.Split('\n').Where(l => l.StartsWith("data:", StringComparison.Ordinal)).Select(l => l.Substring(5).Trim()));
        var text = JsonDocument.Parse(json).RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString();

        Assert.Equal(expected, LevelOf(text!));
    }
}
