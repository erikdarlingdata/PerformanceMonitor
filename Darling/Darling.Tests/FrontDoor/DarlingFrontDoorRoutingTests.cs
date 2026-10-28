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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Darling.Service.FrontDoor;
using Xunit;
using static Darling.Tests.FrontDoor.FakeFrontDoorPeers;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// The front door's routing rules (#6840 part P3), run against fakes of the directory and the peer client: an explicit store wins,
/// a server name goes through the directory, and otherwise the tool's class decides. A write never fans out.
/// </summary>
public sealed class DarlingFrontDoorRoutingTests
{
    private static DarlingFrontDoorRouter RouterOf(FakeFrontDoorDirectory directory, FakeFrontDoorPeers peers, TimeSpan? timeout = null) =>
        new(directory, peers, timeout ?? TimeSpan.FromSeconds(30));

    private static FrontDoorRoute To(string store) => new FrontDoorRoute.ToStore(store);

    private static JsonElement Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static void AssertRefusal(CallToolResult result, string parameter)
    {
        var root = Parse(TextOf(result));
        Assert.Equal("invalid", root.GetProperty("status").GetString());
        Assert.Equal(parameter, root.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.False(string.IsNullOrWhiteSpace(root.GetProperty("message").GetString()));
    }

    [Fact]
    public async Task AnExplicitStore_WinsOverEverythingElse_AndIsNotSentToTheStore()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("store", "\"USE2\""), ("hours_back", "4")), agentRole: false, CancellationToken.None);

        var call = Assert.Single(peers.Calls);
        Assert.Equal("use2", call.Store);
        Assert.Equal("get_wait_stats", call.Params.Name);
        Assert.Equal("{\"hours_back\":4}", ArgumentsOf(call));
        Assert.Contains("use2", TextOf(result));
    }

    [Fact]
    public async Task AnExplicitStore_ThatIsNotConfigured_IsRefusedAndNothingIsSent()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("store", "\"nope\"")), agentRole: false, CancellationToken.None);

        AssertRefusal(result, "store");
        Assert.Contains("use1, use2", TextOf(result));
        Assert.Empty(peers.Calls);
    }

    [Fact]
    public async Task AnExplicitStore_WithAServerNameOnAnotherStore_IsRefused()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2")
        {
            Resolver = name => name == "sql-a" ? To("use1") : new FrontDoorRoute.Refused("none"),
        };
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("store", "\"use2\""), ("server_name", "\"sql-a\"")), agentRole: false, CancellationToken.None);

        AssertRefusal(result, "server_name");
        Assert.Contains("use1", TextOf(result));
        Assert.Empty(peers.Calls);
    }

    [Fact]
    public async Task AnExplicitStore_WithAServerNameOnThatStore_IsSentThere()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2") { Resolver = _ => To("use2") };
        var peers = new FakeFrontDoorPeers();

        await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("store", "\"use2\""), ("server_name", "\"sql-a\"")), agentRole: false, CancellationToken.None);

        var call = Assert.Single(peers.Calls);
        Assert.Equal("use2", call.Store);
        Assert.Equal("{\"server_name\":\"sql-a\"}", ArgumentsOf(call));
    }

    [Fact]
    public async Task AServerName_GoesToTheStoreTheDirectoryResolvesIt_AndTheStoresAnswerComesBackUnchanged()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2") { Resolver = _ => To("use2") };
        var peers = new FakeFrontDoorPeers();
        var answer = Text("{\"status\":\"empty\",\"message\":\"nothing\"}");
        peers.Handler = _ => Task.FromResult(answer);

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("server_name", "\"sql-a\"")), agentRole: false, CancellationToken.None);

        Assert.Same(answer, result);
        Assert.Equal("use2", Assert.Single(peers.Calls).Store);
        Assert.Equal(0, directory.RefreshCalls);
    }

    [Fact]
    public async Task AServerNameThatIsNotFound_RefreshesTheDirectoryOnce_AndRoutesWhenTheRefreshFindsIt()
    {
        var found = false;
        var directory = new FakeFrontDoorDirectory("use1", "use2")
        {
            Resolver = _ => found ? To("use1") : new FrontDoorRoute.Refused("not found"),
            OnRefresh = () => found = true,
        };
        var peers = new FakeFrontDoorPeers();

        await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("server_name", "\"new-box\"")), agentRole: false, CancellationToken.None);

        Assert.Equal(1, directory.RefreshCalls);
        Assert.Equal("use1", Assert.Single(peers.Calls).Store);
    }

    [Fact]
    public async Task AServerNameThatIsStillNotFoundAfterTheRefresh_IsRefusedWithTheDirectorysMessage()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2")
        {
            Resolver = _ => new FrontDoorRoute.Refused("'ghost' matches no server on use1 or use2."),
        };
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("server_name", "\"ghost\"")), agentRole: false, CancellationToken.None);

        AssertRefusal(result, "server_name");
        Assert.Contains("matches no server on use1 or use2", TextOf(result));
        Assert.Equal(1, directory.RefreshCalls);
        Assert.Empty(peers.Calls);
    }

    [Fact]
    public async Task AServerNameThatIsAmbiguous_DoesNotRefreshTheDirectory_AndIsRefusedWithTheDirectorysMessage()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2")
        {
            Resolver = _ => new FrontDoorRoute.Refused(
                "'sql-a' matches servers on use1 and use2. Name the store.", Ambiguous: true, Stores: new[] { "use1", "use2" }),
        };
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("server_name", "\"sql-a\"")), agentRole: false, CancellationToken.None);

        AssertRefusal(result, "server_name");
        Assert.Contains("matches servers on use1 and use2", TextOf(result));
        Assert.Equal(0, directory.RefreshCalls);
        Assert.Empty(peers.Calls);
    }

    [Theory]
    [InlineData("create_mute_rule")]
    [InlineData("add_servers")]
    [InlineData("claim_investigation_request")]
    [InlineData("run_custom_view_panel")]
    [InlineData("test_custom_alert_rule")]
    [InlineData("backtest_custom_alert_rule")]
    [InlineData("wait_for_investigation_requests")]
    public async Task AToolThatNeedsAStore_WithNoStoreOrServer_IsRefusedAndNeverSentAnywhere(string tool)
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(tool, Args(), agentRole: false, CancellationToken.None);

        AssertRefusal(result, "store");
        Assert.Empty(peers.Calls);
    }

    [Fact]
    public async Task AWriteThatNamesEveryServer_IsStillRefused_NotFannedOut()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(
            "create_mute_rule", Args(("server_name", "\"*\"")), agentRole: false, CancellationToken.None);

        AssertRefusal(result, "store");
        Assert.Empty(peers.Calls);
    }

    [Fact]
    public async Task EveryNameInTheThreeStoreRequiredSets_IsRefusedWithNoStore()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();
        var router = RouterOf(directory, peers);

        var names = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpWritePolicy.WriteTools
            .Concat(PerformanceMonitor.Darling.Service.Mcp.DarlingMcpWritePolicy.InvestigationTools)
            .Concat(PerformanceMonitor.Darling.Service.Mcp.DarlingMcpWritePolicy.ServerAdminTools)
            .Concat(DarlingFrontDoorRouting.ExtraStoreRequiredTools)
            .Distinct()
            .ToList();
        Assert.True(names.Count > 80);

        foreach (var name in names)
        {
            AssertRefusal(await router.CallAsync(name, Args(), agentRole: false, CancellationToken.None), "store");
        }

        Assert.Empty(peers.Calls);
    }

    [Theory]
    [InlineData("rule_id", "7")]
    [InlineData("view_id", "7")]
    [InlineData("route_id", "7")]
    [InlineData("fallback_route_id", "7")]
    [InlineData("tag_id", "7")]
    [InlineData("parent_id", "7")]
    [InlineData("note_id", "7")]
    [InlineData("request_id", "7")]
    [InlineData("claim_id", "7")]
    [InlineData("command_id", "7")]
    [InlineData("journal_id", "7")]
    [InlineData("sweep_id", "7")]
    [InlineData("called_id", "7")]
    [InlineData("query_id", "7")]
    [InlineData("route_ids", "[1,2]")]
    [InlineData("server_ids", "[1,2]")]
    public async Task AReadThatCarriesAStoreLocalId_NeedsAStore(string argument, string json)
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync(
            "get_alert_history", Args((argument, json)), agentRole: false, CancellationToken.None);

        AssertRefusal(result, "store");
        Assert.Empty(peers.Calls);
    }

    [Fact]
    public async Task AStoreLocalId_WithAStoreNamed_GoesToThatStoreOnly()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();

        await RouterOf(directory, peers).CallAsync(
            "get_alert_history", Args(("rule_id", "7"), ("store", "\"use1\"")), agentRole: false, CancellationToken.None);

        Assert.Equal("use1", Assert.Single(peers.Calls).Store);
    }

    [Fact]
    public async Task AWriteWithAStoreNamed_IsSentToThatStoreOnly()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2", "use3");
        var peers = new FakeFrontDoorPeers();

        await RouterOf(directory, peers).CallAsync(
            "create_mute_rule", Args(("store", "\"use3\""), ("server_name", "\"*\"")), agentRole: false, CancellationToken.None);

        var call = Assert.Single(peers.Calls);
        Assert.Equal("use3", call.Store);
        Assert.Equal("{\"server_name\":\"*\"}", ArgumentsOf(call));
    }

    [Fact]
    public async Task APureTool_GoesToTheFirstHealthyStore_OnceAndNotToTheOthers()
    {
        var directory = new FakeFrontDoorDirectory(
            ("use1", FrontDoorStoreHealth.Unreachable), ("use2", FrontDoorStoreHealth.AuthFailed), ("use3", FrontDoorStoreHealth.Ok), ("use4", FrontDoorStoreHealth.Ok));
        var peers = new FakeFrontDoorPeers();

        await RouterOf(directory, peers).CallAsync("analyze_plan_xml", Args(("plan_xml", "\"<x/>\"")), agentRole: false, CancellationToken.None);

        Assert.Equal("use3", Assert.Single(peers.Calls).Store);
    }

    [Fact]
    public async Task APureTool_WhoseFirstStoreCannotBeAsked_GoesToTheNextOne()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers
        {
            Handler = call => call.Store == "use1"
                ? throw new FrontDoorPeerException("use1", FrontDoorPeerFailure.Unreachable, "use1 is down")
                : Task.FromResult(Text("{\"status\":\"ok\"}")),
        };

        var result = await RouterOf(directory, peers).CallAsync("get_tool_guide", Args(), agentRole: false, CancellationToken.None);

        Assert.Equal(new[] { "use1", "use2" }, peers.Calls.Select(c => c.Store).ToArray());
        Assert.Equal("{\"status\":\"ok\"}", TextOf(result));
    }

    [Fact]
    public async Task ARead_WithNoServerAndNoStore_IsAskedOfEveryStore_AndEachAnswerIsKeptApart()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers
        {
            Handler = call => Task.FromResult(Text(
                call.Store == "use1" ? "{\"status\":\"empty\",\"message\":\"none\"}" : "{\"status\":\"not_collected\",\"engine\":\"postgres\"}")),
        };

        var result = await RouterOf(directory, peers).CallAsync("get_fleet_overview", Args(), agentRole: false, CancellationToken.None);

        Assert.Equal(2, peers.Calls.Count);
        Assert.False(result.IsError);
        var root = Parse(TextOf(result));
        Assert.True(root.GetProperty("front_door").GetBoolean());
        Assert.Equal("get_fleet_overview", root.GetProperty("tool").GetString());
        var stores = root.GetProperty("stores").EnumerateArray().ToList();
        Assert.Equal(new[] { "use1", "use2" }, stores.Select(s => s.GetProperty("store").GetString()).ToArray());
        Assert.All(stores, s => Assert.False(s.GetProperty("is_error").GetBoolean()));
        Assert.Equal("empty", stores[0].GetProperty("result").GetProperty("status").GetString());
        Assert.Equal("not_collected", stores[1].GetProperty("result").GetProperty("status").GetString());
    }

    [Fact]
    public async Task ARead_WithServerNameStar_IsAskedOfEveryStore()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers();

        await RouterOf(directory, peers).CallAsync("get_wait_stats", Args(("server_name", "\"*\"")), agentRole: false, CancellationToken.None);

        Assert.Equal(new[] { "use1", "use2" }, peers.Calls.Select(c => c.Store).OrderBy(s => s, StringComparer.Ordinal).ToArray());
        Assert.Equal(0, directory.RefreshCalls);
        Assert.Equal(0, directory.ResolveCalls);
    }

    [Fact]
    public async Task AFanOut_WithOnePeerFailing_KeepsTheOthersAnswers_AndIsNotAnError()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2", "use3");
        var peers = new FakeFrontDoorPeers
        {
            Handler = call => call.Store == "use2"
                ? throw new FrontDoorPeerException("use2", FrontDoorPeerFailure.Unreachable, "use2 could not be reached")
                : Task.FromResult(Text("{\"status\":\"ok\"}")),
        };

        var result = await RouterOf(directory, peers).CallAsync("list_servers", Args(), agentRole: false, CancellationToken.None);

        Assert.False(result.IsError);
        var stores = Parse(TextOf(result)).GetProperty("stores").EnumerateArray().ToList();
        Assert.Equal(3, stores.Count);
        Assert.Equal("unavailable", stores[1].GetProperty("status").GetString());
        Assert.Equal("Store use2: use2 could not be reached", stores[1].GetProperty("message").GetString());
        Assert.Equal("ok", stores[0].GetProperty("result").GetProperty("status").GetString());
        Assert.Equal("ok", stores[2].GetProperty("result").GetProperty("status").GetString());
    }

    [Fact]
    public async Task AFanOut_WithEveryPeerFailing_IsAnError()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers
        {
            Handler = call => throw new FrontDoorPeerException(call.Store, FrontDoorPeerFailure.Unreachable, $"{call.Store} could not be reached"),
        };

        var result = await RouterOf(directory, peers).CallAsync("list_servers", Args(), agentRole: false, CancellationToken.None);

        Assert.True(result.IsError);
        var stores = Parse(TextOf(result)).GetProperty("stores").EnumerateArray().ToList();
        Assert.All(stores, s => Assert.Equal("unavailable", s.GetProperty("status").GetString()));
    }

    [Fact]
    public async Task AFanOut_WhereEveryStoreAnswersWithAnErrorResult_IsAnError_AndOneErrorResultAmongGoodOnesIsNot()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2");
        var peers = new FakeFrontDoorPeers
        {
            Handler = _ => Task.FromResult(Text("{\"status\":\"error\",\"message\":\"boom\"}", isError: true)),
        };
        var router = RouterOf(directory, peers);

        var allBad = await router.CallAsync("list_servers", Args(), agentRole: false, CancellationToken.None);
        Assert.True(allBad.IsError);
        Assert.Equal("boom", Parse(TextOf(allBad)).GetProperty("stores")[0].GetProperty("result").GetProperty("message").GetString());

        peers.Handler = call => Task.FromResult(call.Store == "use1" ? Text("{\"status\":\"error\"}", isError: true) : Text("{\"status\":\"ok\"}"));
        var oneBad = await router.CallAsync("list_servers", Args(), agentRole: false, CancellationToken.None);
        Assert.False(oneBad.IsError);
    }

    [Fact]
    public async Task AFanOut_KeepsAnAnswerThatIsNotJsonAsAString()
    {
        var directory = new FakeFrontDoorDirectory("use1");
        var peers = new FakeFrontDoorPeers { Handler = _ => Task.FromResult(Text("plain words, not JSON")) };

        var result = await RouterOf(directory, peers).CallAsync("list_servers", Args(), agentRole: false, CancellationToken.None);

        var entry = Parse(TextOf(result)).GetProperty("stores")[0];
        Assert.Equal(JsonValueKind.String, entry.GetProperty("result").ValueKind);
        Assert.Equal("plain words, not JSON", entry.GetProperty("result").GetString());
    }

    [Fact]
    public async Task AFanOut_DoesNotCallAStoreThatIsHeld_AndSaysItIsUnavailable()
    {
        var directory = new FakeFrontDoorDirectory(("use1", FrontDoorStoreHealth.Ok), ("use2", FrontDoorStoreHealth.AuthFailed));
        var peers = new FakeFrontDoorPeers();

        var result = await RouterOf(directory, peers).CallAsync("list_servers", Args(), agentRole: false, CancellationToken.None);

        Assert.Equal("use1", Assert.Single(peers.Calls).Store);
        Assert.False(result.IsError);
        var stores = Parse(TextOf(result)).GetProperty("stores").EnumerateArray().ToList();
        Assert.Equal("unavailable", stores[1].GetProperty("status").GetString());
    }

    [Fact]
    public async Task AFanOut_AsksEveryStoreAtTheSameTime()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2", "use3");
        var arrived = 0;
        var allArrived = new TaskCompletionSource();
        var peers = new FakeFrontDoorPeers
        {
            Handler = async _ =>
            {
                if (Interlocked.Increment(ref arrived) == 3)
                {
                    allArrived.TrySetResult();
                }

                await allArrived.Task.WaitAsync(TimeSpan.FromSeconds(10));
                return Text("{\"status\":\"ok\"}");
            },
        };

        var result = await RouterOf(directory, peers).CallAsync("list_servers", Args(), agentRole: false, CancellationToken.None);

        Assert.False(result.IsError);
        Assert.Equal(3, arrived);
    }

    [Fact]
    public async Task ASingleStoreAnswer_BusyIncluded_ComesBackUnchanged_WithOneCallAndNoRetry()
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2") { Resolver = _ => To("use1") };
        var peers = new FakeFrontDoorPeers();
        var busy = Text("{\"status\":\"busy\",\"message\":\"too many heavy reads\",\"retry_after_seconds\":5}", isError: true);
        peers.Handler = _ => Task.FromResult(busy);

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("server_name", "\"sql-a\"")), agentRole: false, CancellationToken.None);

        Assert.Same(busy, result);
        Assert.Single(peers.Calls);
    }

    [Fact]
    public async Task ASingleStoreThatCannotBeAsked_IsAnErrorResultWithThePeersMessage()
    {
        var directory = new FakeFrontDoorDirectory("use1") { Resolver = _ => To("use1") };
        var peers = new FakeFrontDoorPeers
        {
            Handler = _ => throw new FrontDoorPeerException("use1", FrontDoorPeerFailure.AuthFailed, "use1 refused the front door's token"),
        };

        var result = await RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("server_name", "\"sql-a\"")), agentRole: false, CancellationToken.None);

        Assert.True(result.IsError);
        var root = Parse(TextOf(result));
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.Equal("Store use1: use1 refused the front door's token", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task ACallersCancel_ReachesThePeerCallsToken_AndEndsTheCall()
    {
        var directory = new FakeFrontDoorDirectory("use1") { Resolver = _ => To("use1") };
        var reached = new TaskCompletionSource();
        var peers = new FakeFrontDoorPeers
        {
            Handler = async call =>
            {
                reached.TrySetResult();
                await Task.Delay(Timeout.Infinite, call.Token);
                return Text("never");
            },
        };
        using var caller = new CancellationTokenSource();

        var pending = RouterOf(directory, peers).CallAsync(
            "get_wait_stats", Args(("server_name", "\"sql-a\"")), agentRole: false, caller.Token);
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var peerToken = Assert.Single(peers.Calls).Token;
        Assert.False(peerToken.IsCancellationRequested);

        caller.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.True(peerToken.IsCancellationRequested);
    }

    [Fact]
    public async Task APeerThatDoesNotAnswerInTime_IsUnavailable_NotACancelledCall()
    {
        var directory = new FakeFrontDoorDirectory("use1") { Resolver = _ => To("use1") };
        var peers = new FakeFrontDoorPeers
        {
            Handler = async call =>
            {
                await Task.Delay(Timeout.Infinite, call.Token);
                return Text("never");
            },
        };

        var result = await RouterOf(directory, peers, TimeSpan.FromMilliseconds(100)).CallAsync(
            "get_wait_stats", Args(("server_name", "\"sql-a\"")), agentRole: false, CancellationToken.None);

        Assert.True(result.IsError);
        Assert.Equal("unavailable", Parse(TextOf(result)).GetProperty("status").GetString());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheCallersRole_IsPassedToThePeerClient(bool agentRole)
    {
        var directory = new FakeFrontDoorDirectory("use1", "use2") { Resolver = _ => To("use1") };
        var peers = new FakeFrontDoorPeers();
        var router = RouterOf(directory, peers);

        await router.CallAsync("get_wait_stats", Args(("server_name", "\"sql-a\"")), agentRole, CancellationToken.None);
        await router.CallAsync("list_servers", Args(), agentRole, CancellationToken.None);
        await router.CallAsync("get_tool_guide", Args(), agentRole, CancellationToken.None);

        Assert.Equal(4, peers.Calls.Count);
        Assert.All(peers.Calls, c => Assert.Equal(agentRole, c.AgentRole));
    }
}
