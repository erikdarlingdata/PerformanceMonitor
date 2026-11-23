/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.FrontDoor.FakeFrontDoorPeers;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// The front door's tool list and tool calls (#6840 part P3), built from the host's real registration: every tool the host registers is
/// served by a front door tool that has the same schema plus an optional <c>store</c>, and a call reaches the store through the router and
/// never runs the registered tool.
/// </summary>
public sealed class DarlingFrontDoorToolTests
{
    private static readonly DarlingMcpWritePolicy.Effective Operator = new(Writes: true, ServerAdmin: true);

    private static readonly DarlingMcpWritePolicy.Effective Agent = new(Writes: false, ServerAdmin: false, Investigations: true);

    /// <summary>The services a front door host builds: the host's tool services over a data source that is never opened, then the MCP
    /// registration with the router.</summary>
    private static ServiceCollection FrontDoorServices(DarlingFrontDoorRouter router, DarlingMcpWritePolicy.Effective writes)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var postgres = NpgsqlDataSource.Create("Host=front-door.invalid");
        DarlingMcpHostService.RegisterToolServices(
            services, postgres, new MonitoredServerRegistryState(), new PostgresConfig(), analyzer: null,
            planFetcher: null, NullLogger.Instance, new BaselineCache());
        DarlingMcpHostService.ConfigureMcpServices(services, DarlingPeerDirectory.Snapshot.Empty, writes: writes, frontDoor: router);
        return services;
    }

    private static DarlingFrontDoorRouter RouterOf(FakeFrontDoorDirectory directory, FakeFrontDoorPeers peers) =>
        new(directory, peers, TimeSpan.FromSeconds(30));

    private static Dictionary<string, JsonElement> PropertiesOf(Tool tool) =>
        tool.InputSchema.TryGetProperty("properties", out var properties)
            ? properties.EnumerateObject().ToDictionary(p => p.Name, p => p.Value, StringComparer.Ordinal)
            : new Dictionary<string, JsonElement>(StringComparer.Ordinal);

    private static string[] RequiredOf(Tool tool) =>
        tool.InputSchema.TryGetProperty("required", out var required)
            ? required.EnumerateArray().Select(e => e.GetString()!).ToArray()
            : Array.Empty<string>();

    [Fact]
    public void EveryServedTool_IsAFrontDoorTool_WhoseSchemaGainsStoreAndKeepsTheRest()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        using var provider = FrontDoorServices(RouterOf(directory, new FakeFrontDoorPeers()), Operator).BuildServiceProvider();
        var served = provider.GetRequiredService<IOptions<McpServerOptions>>().Value.ToolCollection!.ToList();

        var (registered, _, _) = McpToolsListBudgetTests.BuildServedTools();
        /* The host stamps every registered tool with the write policy before the front door wraps it, and the stamp restates a
           state-changing tool's database_name description (#9065). */
        DarlingMcpWritePolicy.ApplyAnnotations(registered);
        var registeredCollection = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var registeredTool in registered)
        {
            registeredCollection.Add(registeredTool);
        }
        var offered = DarlingMcpWritePolicy.ToolNames(DarlingMcpWritePolicy.FilterToolCollection(registeredCollection, Operator));
        Assert.True(served.Count > 100);
        /* #9388: the census names what / serves, so it leaves out call_tool, which only /lean serves (the front door wraps it too). */
        Assert.Equal(offered.OrderBy(n => n, StringComparer.Ordinal), served.Select(t => t.ProtocolTool.Name).Where(n => n != "call_tool").OrderBy(n => n, StringComparer.Ordinal));
        /* #9388: the local tools stay the registered tools: find_tools answers from this request's served set, and call_tool takes store inside arguments. */
        Assert.All(served, tool => Assert.Equal(!DarlingFrontDoorRouting.LocalTools.Contains(tool.ProtocolTool.Name), tool is DarlingFrontDoorTool));

        var byName = registered.ToDictionary(t => t.ProtocolTool.Name, StringComparer.Ordinal);
        foreach (var tool in served.Where(t => !DarlingFrontDoorRouting.LocalTools.Contains(t.ProtocolTool.Name)))
        {
            var inner = byName[tool.ProtocolTool.Name].ProtocolTool;
            var before = PropertiesOf(inner);
            var after = PropertiesOf(tool.ProtocolTool);

            Assert.False(before.ContainsKey("store"), $"{inner.Name} already has a store parameter, which the front door's would replace");
            Assert.True(after.ContainsKey("store"), inner.Name);
            Assert.Equal("string", after["store"].GetProperty("type").GetString());
            Assert.Equal(new[] { "use1", "use2" }, after["store"].GetProperty("enum").EnumerateArray().Select(e => e.GetString()).ToArray());
            Assert.Equal(before.Count + 1, after.Count);
            foreach (var (name, schema) in before)
            {
                Assert.Equal(schema.GetRawText(), after[name].GetRawText());
            }

            /* store is optional: the tool's own required list is untouched. */
            Assert.Equal(RequiredOf(inner), RequiredOf(tool.ProtocolTool));
            Assert.DoesNotContain("store", RequiredOf(tool.ProtocolTool));

            Assert.Equal(inner.Description, tool.ProtocolTool.Description);
            Assert.Equal(!DarlingMcpWritePolicy.IsWrite(inner.Name) && !DarlingMcpWritePolicy.IsInvestigation(inner.Name), tool.ProtocolTool.Annotations?.ReadOnlyHint);
            Assert.Equal(DarlingMcpWritePolicy.IsWrite(inner.Name), tool.ProtocolTool.Annotations?.DestructiveHint);
        }
    }

    [Fact]
    public void ARouterWithNoStores_AddsStoreWithoutAnEnum()
    {
        var registered = McpToolsListBudgetTests.BuildServedTools().Tools.First(t => t.ProtocolTool.Name == "list_servers");

        var tool = DarlingFrontDoorTool.WithStoreArgument(registered.ProtocolTool, Array.Empty<string>());

        var store = PropertiesOf(tool)["store"];
        Assert.Equal("string", store.GetProperty("type").GetString());
        Assert.False(store.TryGetProperty("enum", out _));
    }

    private sealed class Running : IAsyncDisposable
    {
        internal required McpClient Client { get; init; }

        internal required CancellationTokenSource Stop { get; init; }

        internal required Task ServerRun { get; init; }

        internal required ServiceProvider Provider { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            await Stop.CancelAsync();
            try
            {
                await ServerRun.WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (OperationCanceledException)
            {
            }

            await Provider.DisposeAsync();
            Stop.Dispose();
        }
    }

    private static async Task<Running> StartAsync(FakeFrontDoorDirectory directory, FakeFrontDoorPeers peers, DarlingMcpWritePolicy.Effective writes)
    {
        var services = FrontDoorServices(RouterOf(directory, peers), writes);
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        services.AddMcpServer().WithStreamServerTransport(clientToServer.Reader.AsStream(), serverToClient.Writer.AsStream());

        var provider = services.BuildServiceProvider();
        var stop = new CancellationTokenSource();
        var serverRun = provider.GetRequiredService<McpServer>().RunAsync(stop.Token);
        var client = await McpClient.CreateAsync(
            new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()));
        return new Running { Client = client, Stop = stop, ServerRun = serverRun, Provider = provider };
    }

    [Fact]
    public async Task ACallThroughTheMcpPipeline_ReachesTheStoreWithoutTheStoreArgument_AndNeverRunsTheRegisteredTool()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2") { Resolver = _ => new FrontDoorRoute.ToStore("use2") };
        var peers = new FakeFrontDoorPeers { Handler = _ => Task.FromResult(Text("{\"status\":\"empty\",\"message\":\"the store's own words\"}")) };
        await using var running = await StartAsync(directory, peers, Operator);

        var listed = await running.Client.ListToolsAsync();
        Assert.All(listed.Where(t => !DarlingFrontDoorRouting.LocalTools.Contains(t.Name)), t => Assert.True(t.ProtocolTool.InputSchema.GetProperty("properties").TryGetProperty("store", out _), t.Name));

        var result = await running.Client.CallToolAsync("get_wait_stats", new Dictionary<string, object?> { ["server_name"] = "sql-a", ["hours_back"] = 4 });

        var call = Assert.Single(peers.Calls);
        Assert.Equal("use2", call.Store);
        Assert.Equal("get_wait_stats", call.Params.Name);
        Assert.Equal("{\"server_name\":\"sql-a\",\"hours_back\":4}", ArgumentsOf(call));
        Assert.Equal("{\"status\":\"empty\",\"message\":\"the store's own words\"}", TextOf(result));
    }

    [Fact]
    public async Task AWriteWithNoStore_ThroughTheMcpPipeline_IsRefusedAndSentNowhere()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();
        await using var running = await StartAsync(directory, peers, Operator);

        var result = await running.Client.CallToolAsync("create_mute_rule", new Dictionary<string, object?>());

        var root = JsonDocument.Parse(TextOf(result)).RootElement;
        Assert.Equal("invalid", root.GetProperty("status").GetString());
        Assert.Equal("store", root.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Empty(peers.Calls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheCallersRole_ThroughTheMcpPipeline_DecidesWhichTokenTheStoreSees(bool agent)
    {
        var directory = new FakeFrontDoorDirectory("use1") { Resolver = _ => new FrontDoorRoute.ToStore("use1") };
        var peers = new FakeFrontDoorPeers();
        await using var running = await StartAsync(directory, peers, agent ? Agent : Operator);

        await running.Client.CallToolAsync("get_wait_stats", new Dictionary<string, object?> { ["server_name"] = "sql-a" });

        Assert.Equal(agent, Assert.Single(peers.Calls).AgentRole);
    }

    /// <summary>The tools a store whose peer token has writes on serves, searched as that store's own find_tools would.</summary>
    private static string StoreSideFindTools(FakePeerCall call)
    {
        var (registered, _, _) = McpToolsListBudgetTests.BuildServedTools();
        DarlingMcpWritePolicy.ApplyAnnotations(registered);
        var collection = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in registered)
        {
            collection.Add(tool);
        }

        var asked = call.Params.Arguments ?? new Dictionary<string, JsonElement>();
        return McpToolSearch.Render(
            DarlingMcpWritePolicy.FilterToolCollection(collection, Operator),
            asked["query"].GetString(),
            asked.TryGetValue("detail", out var detail) ? detail.GetString() : null,
            null);
    }

    /// <summary>#9388: find_tools on a front door answers from the tools this caller is served on the front door. A store would search
    /// its own set under the front door's peer token, which has writes on, so a reader token was shown write tools it cannot call,
    /// and the schemas it was shown had no store argument.</summary>
    [Fact]
    public async Task FindTools_OnAFrontDoor_SearchesWhatTheCallerIsServedThere_AndNeverAsksAStore()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers { Handler = call => Task.FromResult(Text(StoreSideFindTools(call))) };
        var reader = new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false);
        await using var running = await StartAsync(directory, peers, reader);

        var created = await running.Client.CallToolAsync("find_tools", new Dictionary<string, object?> { ["query"] = "create", ["detail"] = "names" });
        var full = await running.Client.CallToolAsync("find_tools", new Dictionary<string, object?> { ["query"] = "wait", ["detail"] = "full", ["limit"] = 3 });

        Assert.Empty(peers.Calls);
        using var createdAnswer = JsonDocument.Parse(TextOf(created));
        var names = createdAnswer.RootElement.TryGetProperty("tools", out var listed)
            ? listed.EnumerateArray().Select(e => e.GetString()!).ToList()
            : new List<string>();
        Assert.DoesNotContain(names, n => DarlingMcpWritePolicy.IsWrite(n) || DarlingMcpWritePolicy.IsInvestigation(n) || DarlingMcpWritePolicy.IsServerAdmin(n));

        using var fullAnswer = JsonDocument.Parse(TextOf(full));
        var shown = fullAnswer.RootElement.GetProperty("tools").EnumerateArray().ToList();
        Assert.NotEmpty(shown);
        Assert.All(shown, entry =>
        {
            var properties = entry.GetProperty("input_schema").GetProperty("properties");
            Assert.True(properties.TryGetProperty("store", out _), entry.GetProperty("name").GetString());
        });
    }

    /// <summary>#9388: call_tool is served as the registered tool, with no outer store, because CallToolFilter discards one.</summary>
    [Fact]
    public async Task CallTool_OnAFrontDoor_HasNoOuterStore()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        await using var running = await StartAsync(directory, new FakeFrontDoorPeers(), Operator);

        var listed = await running.Client.ListToolsAsync();

        var callTool = Assert.Single(listed, t => t.Name == "call_tool").ProtocolTool;
        Assert.False(PropertiesOf(callTool).ContainsKey("store"));
    }
}
