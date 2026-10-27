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
        var registeredCollection = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var registeredTool in registered)
        {
            registeredCollection.Add(registeredTool);
        }
        var offered = DarlingMcpWritePolicy.ToolNames(DarlingMcpWritePolicy.FilterToolCollection(registeredCollection, Operator));
        Assert.True(served.Count > 100);
        Assert.Equal(offered.OrderBy(n => n, StringComparer.Ordinal), served.Select(t => t.ProtocolTool.Name).OrderBy(n => n, StringComparer.Ordinal));
        Assert.All(served, tool => Assert.IsType<DarlingFrontDoorTool>(tool));

        var byName = registered.ToDictionary(t => t.ProtocolTool.Name, StringComparer.Ordinal);
        foreach (var tool in served)
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
        Assert.All(listed, t => Assert.True(t.ProtocolTool.InputSchema.GetProperty("properties").TryGetProperty("store", out _), t.Name));

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
}
