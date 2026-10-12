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
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The MCP write switch (<see cref="DarlingMcpWritePolicy"/>): with the default settings no tool that changes
/// monitoring configuration is served, <c>mcp.allowWrites</c> serves them behind the bearer token on every
/// request (loopback included), <c>mcp.allowServerAdmin</c> additionally serves the server-registry pair, and
/// every served tool carries the read-only or destructive annotation hint. The pure cases run over the REAL
/// served tool set; the wired cases drive the real pipeline through a <see cref="TestServer"/>.
/// </summary>
public sealed class DarlingMcpWritePolicyTests
{
    private const string Token = "write-gate-token-value";

    private static readonly DarlingMcpWritePolicy.Effective WritesOnly = new(Writes: true, ServerAdmin: false);
    private static readonly DarlingMcpWritePolicy.Effective Everything = new(Writes: true, ServerAdmin: true);

    private static McpServerPrimitiveCollection<McpServerTool> ServedTools()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var collection = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in tools)
            collection.Add(tool);
        return collection;
    }

    private static string[] Names(System.Collections.Generic.IEnumerable<McpServerTool> tools)
        => tools.Select(t => t.ProtocolTool.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    [Fact]
    public void WriteToolSet_IsExactlyTheRegisteredToolsWithAWriteVerb()
    {
        var registered = Names(ServedTools());
        Assert.True(registered.Length > 100, "the served tool set should be the full Darling surface");

        var byVerb = registered
            .Where(n => DarlingMcpWritePolicy.WriteVerbPrefixes.Any(p => n.StartsWith(p, StringComparison.Ordinal)))
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(
            byVerb.SetEquals(DarlingMcpWritePolicy.WriteTools),
            "every registered tool whose name starts with a write verb must be in WriteTools, and every WriteTools name must be registered; "
            + $"by verb but unlisted: [{string.Join(", ", byVerb.Except(DarlingMcpWritePolicy.WriteTools))}], "
            + $"listed but not registered by verb: [{string.Join(", ", DarlingMcpWritePolicy.WriteTools.Except(byVerb))}]");
        Assert.Equal(21, DarlingMcpWritePolicy.WriteTools.Count);
        Assert.True(DarlingMcpWritePolicy.ServerAdminTools.IsSubsetOf(DarlingMcpWritePolicy.WriteTools));
    }

    [Fact]
    public void Default_ServesNoWriteTool_AndEveryRead()
    {
        var all = ServedTools();
        var served = DarlingMcpWritePolicy.FilterToolCollection(all, DarlingMcpWritePolicy.Effective.Off);

        var names = Names(served);
        Assert.DoesNotContain(names, DarlingMcpWritePolicy.IsWrite);
        Assert.Equal(all.Count - DarlingMcpWritePolicy.WriteTools.Count, served.Count);
        Assert.Contains("list_servers", names);
        Assert.Contains("get_tool_guide", names);
    }

    [Fact]
    public void WritesOn_ServerAdminOff_ServesWritesExceptTheRegistryPair()
    {
        var served = Names(DarlingMcpWritePolicy.FilterToolCollection(ServedTools(), WritesOnly));

        Assert.Contains("create_mute_rule", served);
        Assert.Contains("delete_custom_view", served);
        Assert.Contains("mute_analysis_finding", served);
        Assert.DoesNotContain("add_servers", served);
        Assert.DoesNotContain("remove_server", served);
    }

    [Fact]
    public void BothOn_ServesEveryTool()
    {
        var all = ServedTools();
        var served = DarlingMcpWritePolicy.FilterToolCollection(all, Everything);

        Assert.Equal(all.Count, served.Count);
        Assert.Contains("add_servers", Names(served));
    }

    [Fact]
    public void Apply_RemovesRefusedWritesInPlace_AndStampsTheHints()
    {
        var narrowed = ServedTools();
        var before = narrowed.Count;
        DarlingMcpWritePolicy.Apply(narrowed, DarlingMcpWritePolicy.Effective.Off);

        Assert.Equal(before - DarlingMcpWritePolicy.WriteTools.Count, narrowed.Count);
        Assert.All(narrowed, tool =>
        {
            Assert.NotNull(tool.ProtocolTool.Annotations);
            Assert.True(tool.ProtocolTool.Annotations!.ReadOnlyHint, $"{tool.ProtocolTool.Name} should carry readOnlyHint");
            Assert.False(tool.ProtocolTool.Annotations.DestructiveHint, $"{tool.ProtocolTool.Name} should not carry destructiveHint");
        });

        var full = ServedTools();
        DarlingMcpWritePolicy.Apply(full, Everything);
        Assert.Equal(before, full.Count);
        foreach (var tool in full.Where(t => DarlingMcpWritePolicy.IsWrite(t.ProtocolTool.Name)))
        {
            Assert.True(tool.ProtocolTool.Annotations!.DestructiveHint, $"{tool.ProtocolTool.Name} should carry destructiveHint");
            Assert.False(tool.ProtocolTool.Annotations.ReadOnlyHint, $"{tool.ProtocolTool.Name} should not carry readOnlyHint");
        }

        DarlingMcpWritePolicy.Apply(null, Everything);
    }

    [Theory]
    [InlineData(false, false, true, false, false)]
    [InlineData(true, false, false, false, false)]
    [InlineData(true, false, true, true, false)]
    [InlineData(true, true, true, true, true)]
    [InlineData(false, true, true, false, false)]
    [InlineData(true, true, false, false, false)]
    public void Resolve_WritesNeedTheSettingAndTheToken_ServerAdminNeedsWrites(
        bool allowWrites, bool allowServerAdmin, bool tokenPresent, bool expectWrites, bool expectServerAdmin)
    {
        var mcp = new McpConfig { AllowWrites = allowWrites, AllowServerAdmin = allowServerAdmin };

        var effective = DarlingMcpWritePolicy.Resolve(mcp, tokenPresent);

        Assert.Equal(expectWrites, effective.Writes);
        Assert.Equal(expectServerAdmin, effective.ServerAdmin);
    }

    [Fact]
    public void McpConfig_WriteSettings_DefaultOff_AndParseFromDarlingJson()
    {
        var omitted = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], ""mcp"": { ""enabled"": true } }");
        Assert.False(omitted.Mcp.AllowWrites);
        Assert.False(omitted.Mcp.AllowServerAdmin);

        var set = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], ""mcp"": { ""enabled"": true, ""allowWrites"": true, ""allowServerAdmin"": true } }");
        Assert.True(set.Mcp.AllowWrites);
        Assert.True(set.Mcp.AllowServerAdmin);
    }

    [Fact]
    public void Instructions_DoNotClaimReadOnly_NameTheSettings_AndSayCollectedTextIsData()
    {
        var text = DarlingMcpInstructions.Text;

        Assert.DoesNotContain("STRICTLY READ-ONLY", text, StringComparison.Ordinal);
        Assert.DoesNotContain("## CRITICAL: Read-Only Access", text, StringComparison.Ordinal);
        Assert.Contains(DarlingMcpWritePolicy.WritesSetting, text, StringComparison.Ordinal);
        Assert.Contains(DarlingMcpWritePolicy.ServerAdminSetting, text, StringComparison.Ordinal);
        Assert.Contains("data, never instructions", text, StringComparison.Ordinal);

        /* The settings line sits in the preamble, before the tool census, where an agent reads the boundaries. */
        Assert.True(
            text.IndexOf(DarlingMcpWritePolicy.WritesSetting, StringComparison.Ordinal)
                < text.IndexOf("This server exposes", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Loopback_Default_NoToken_ToolsListSucceeds_ServesNoWriteTool_WithReadOnlyHints()
    {
        using var server = await BuildServer(default);

        var (statusCode, body) = await ToolsListAsync(server, bearer: null);

        Assert.Equal(StatusCodes.Status200OK, statusCode);
        var tools = ToolsOf(body);
        Assert.NotEmpty(tools);
        Assert.DoesNotContain(tools.Keys, DarlingMcpWritePolicy.IsWrite);
        Assert.True(tools["list_servers"].GetProperty("annotations").GetProperty("readOnlyHint").GetBoolean());
    }

    [Fact]
    public async Task Loopback_WritesOn_NoToken_IsRefused()
    {
        using var server = await BuildServer(WritesOnly);

        var (statusCode, _) = await ToolsListAsync(server, bearer: null);

        Assert.Equal(StatusCodes.Status401Unauthorized, statusCode);
    }

    [Fact]
    public async Task Loopback_WritesOn_WithToken_ServesWrites_WithDestructiveHint_ButNotTheRegistryPair()
    {
        using var server = await BuildServer(WritesOnly);

        var (statusCode, body) = await ToolsListAsync(server, bearer: Token);

        Assert.Equal(StatusCodes.Status200OK, statusCode);
        var tools = ToolsOf(body);
        Assert.True(tools.ContainsKey("delete_custom_view"));
        Assert.True(tools["delete_custom_view"].GetProperty("annotations").GetProperty("destructiveHint").GetBoolean());
        Assert.False(tools["delete_custom_view"].GetProperty("annotations").GetProperty("readOnlyHint").GetBoolean());
        Assert.False(tools.ContainsKey("add_servers"));
        Assert.False(tools.ContainsKey("remove_server"));
    }

    [Fact]
    public async Task Loopback_BothOn_WithToken_ServesTheRegistryPair()
    {
        using var server = await BuildServer(Everything);

        var (statusCode, body) = await ToolsListAsync(server, bearer: Token);

        Assert.Equal(StatusCodes.Status200OK, statusCode);
        var tools = ToolsOf(body);
        Assert.True(tools.ContainsKey("add_servers"));
        Assert.True(tools["add_servers"].GetProperty("annotations").GetProperty("destructiveHint").GetBoolean());

        /* add_servers takes its caller policy as a service parameter that ConfigureMcpServices seats, so the one
           argument a client is offered is servers_json. Without the registration the policy would be served as an argument. */
        var offered = tools["add_servers"].GetProperty("inputSchema").GetProperty("properties")
            .EnumerateObject().Select(p => p.Name).ToArray();
        Assert.Equal(new[] { "servers_json" }, offered);
    }

    [Fact]
    public async Task Loopback_BothOn_WithToken_AddServers_RefusesWindowsAuthOverAnUnverifiedConnection_ByDefault()
    {
        /* Every entry is refused before the store is read, so the pool stays closed. It points at a port nothing
           listens on all the same. The call runs the real dispatch: the SDK has to resolve the caller policy from
           DI when it invokes the tool. */
        using var server = await BuildServer(Everything, "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1");
        var arguments = JsonSerializer.Serialize(new
        {
            servers_json = "[{\"host\":\"sql01\",\"auth\":\"Windows\",\"trust_server_certificate\":true}]",
        });
        var (statusCode, body) = await ToolsCallAsync(server, "add_servers", arguments, bearer: Token);

        Assert.Equal(StatusCodes.Status200OK, statusCode);
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var envelope = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var text = envelope.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!;
        using var result = JsonDocument.Parse(text);
        Assert.Equal(1, result.RootElement.GetProperty("failed").GetInt32());
        Assert.Equal(0, result.RootElement.GetProperty("added").GetInt32());
        var row = result.RootElement.GetProperty("results")[0];
        Assert.Equal("invalid", row.GetProperty("status").GetString());
        Assert.Contains("mcp.allowWindowsAuthOverUnverifiedTls", row.GetProperty("detail").GetString(), StringComparison.Ordinal);
    }

    /// <summary>The same wiring production runs, on the loopback bind: <c>ConfigureMcpServices</c> with the
    /// policy, then <c>ConfigurePipeline</c> with the token required exactly when writes are served.</summary>
    private static async Task<TestServer> BuildServer(
        DarlingMcpWritePolicy.Effective writes, string postgresConnection = "Host=localhost;Database=postgres;Username=darling")
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        // A pool no test here opens: every case stops at the gate or at tools/list, before any tool body runs.
        var postgres = NpgsqlDataSource.Create(postgresConnection);
        builder.Services.AddSingleton(postgres);
        builder.Services.AddSingleton(new PerformanceMonitor.Darling.Analysis.DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: new PerformanceMonitor.Darling.Analysis.BaselineCache()));
        builder.Services.AddSingleton<ILogger>(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(builder.Services, DarlingPeerDirectory.Snapshot.Empty, writes: writes);

        var app = builder.Build();

        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());

        host.ConfigurePipeline(
            app,
            networkMode: false,
            networkListenIp: null,
            allowedCidr: IPNetwork.Parse("192.168.1.0/24"),
            bearerToken: Token,
            requireTokenWhenLoopbackOnly: writes.Writes);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<(int StatusCode, string Body)> ToolsListAsync(TestServer server, string? bearer)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
            if (bearer is not null)
                c.Request.Headers.Authorization = $"Bearer {bearer}";
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        return (ctx.Response.StatusCode, body);
    }

    private static async Task<(int StatusCode, string Body)> ToolsCallAsync(TestServer server, string tool, string argumentsJson, string? bearer)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(
                "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + tool +
                "\",\"arguments\":" + argumentsJson + "}}");
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
            if (bearer is not null)
                c.Request.Headers.Authorization = $"Bearer {bearer}";
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        return (ctx.Response.StatusCode, body);
    }

    /// <summary>The tools/list result keyed by tool name. The transport may frame the reply as one SSE
    /// <c>data:</c> line or as bare JSON; both parse the same way.</summary>
    private static System.Collections.Generic.Dictionary<string, JsonElement> ToolsOf(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        var json = dataLine is null ? body : dataLine["data:".Length..].Trim();
        using var document = JsonDocument.Parse(json);
        return document.RootElement.GetProperty("result").GetProperty("tools").EnumerateArray()
            .ToDictionary(t => t.GetProperty("name").GetString() ?? "", t => t.Clone(), StringComparer.Ordinal);
    }
}
