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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>#6840 part P2: the front door's directory against in-process fake stores.</summary>
public class DarlingFrontDoorDirectoryTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task An_exact_name_in_one_store_beats_a_longer_name_that_contains_it_in_another()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var use2 = await FakeMcpPeer.StartAsync(new[] { "SQL01-REPLICA" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1), ("use2", use2) });
        await rig.Directory.RefreshAsync(Ct);

        var route = rig.Directory.ResolveServer("SQL01");

        Assert.Equal(new FrontDoorRoute.ToStore("use1"), route);
    }

    [Fact]
    public async Task The_same_exact_name_in_two_stores_is_refused_and_names_both_stores()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var use2 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1), ("use2", use2) });
        await rig.Directory.RefreshAsync(Ct);

        var refused = Assert.IsType<FrontDoorRoute.Refused>(rig.Directory.ResolveServer("SQL01"));

        Assert.Contains("use1", refused.Message);
        Assert.Contains("use2", refused.Message);
        Assert.Contains("SQL01", refused.Message);
    }

    [Fact]
    public async Task A_primary_in_one_store_and_its_replica_in_another_make_a_partial_name_ambiguous()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "PROD-SQL01" });
        await using var use2 = await FakeMcpPeer.StartAsync(new[] { "PROD-SQL01:RO" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1), ("use2", use2) });
        await rig.Directory.RefreshAsync(Ct);

        var refused = Assert.IsType<FrontDoorRoute.Refused>(rig.Directory.ResolveServer("sql01"));

        Assert.Contains("use1: PROD-SQL01", refused.Message);
        Assert.Contains("use2: PROD-SQL01:RO", refused.Message);

        /* The full name of either one is exact in its own store and routes. */
        Assert.Equal(new FrontDoorRoute.ToStore("use2"), rig.Directory.ResolveServer("PROD-SQL01:RO"));
    }

    [Fact]
    public async Task A_partial_name_found_in_one_store_routes_there()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "ALPHA-01" });
        await using var use2 = await FakeMcpPeer.StartAsync(new[] { "BETA-01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1), ("use2", use2) });
        await rig.Directory.RefreshAsync(Ct);

        Assert.Equal(new FrontDoorRoute.ToStore("use2"), rig.Directory.ResolveServer("beta"));
    }

    [Fact]
    public async Task A_star_is_a_fan_out_and_an_unknown_name_is_refused_with_the_store_list()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var use2 = await FakeMcpPeer.StartAsync(new[] { "SQL02", "SQL03" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1), ("use2", use2) });
        await rig.Directory.RefreshAsync(Ct);

        Assert.IsType<FrontDoorRoute.FanOut>(rig.Directory.ResolveServer("*"));

        var refused = Assert.IsType<FrontDoorRoute.Refused>(rig.Directory.ResolveServer("nothing-like-this"));
        Assert.Contains("use1 (1 servers)", refused.Message);
        Assert.Contains("use2 (2 servers)", refused.Message);
    }

    [Fact]
    public async Task A_store_that_stops_answering_keeps_its_last_list_and_is_stale()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        var use2 = await FakeMcpPeer.StartAsync(new[] { "SQL02" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1), ("use2", use2) });
        await rig.Directory.RefreshAsync(Ct);
        Assert.Equal(FrontDoorStoreHealth.Ok, rig.State("use2").Health);

        await use2.DisposeAsync();
        await rig.Directory.RefreshAsync(Ct);

        var state = rig.State("use2");
        Assert.Equal(FrontDoorStoreHealth.Stale, state.Health);
        Assert.Equal("SQL02", Assert.Single(state.Servers).ServerName);
        Assert.NotNull(state.LastOkUtc);
        Assert.Equal(FrontDoorStoreHealth.Ok, rig.State("use1").Health);

        /* The stale store still routes by its last known list. */
        Assert.Equal(new FrontDoorRoute.ToStore("use2"), rig.Directory.ResolveServer("SQL02"));
    }

    [Fact]
    public async Task A_store_that_never_answered_is_unreachable_and_the_others_still_work()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        var use2 = await FakeMcpPeer.StartAsync(new[] { "SQL02" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1), ("use2", use2) });
        await use2.DisposeAsync();

        await rig.Directory.RefreshAsync(Ct);

        var state = rig.State("use2");
        Assert.Equal(FrontDoorStoreHealth.Unreachable, state.Health);
        Assert.Empty(state.Servers);
        Assert.Null(state.LastOkUtc);
        Assert.Equal(new FrontDoorRoute.ToStore("use1"), rig.Directory.ResolveServer("SQL01"));
    }

    [Fact]
    public async Task A_429_holds_the_store_for_its_retry_after_and_no_call_goes_to_it_meanwhile()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });
        await rig.Directory.RefreshAsync(Ct);
        use1.Refuse(429, retryAfterSeconds: 600);

        await rig.Directory.RefreshAsync(Ct);

        Assert.Equal(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
        var requestsWhenHeld = use1.HttpRequests;

        /* Neither a refresh nor a miss refresh sends anything while the hold runs. */
        await rig.Directory.RefreshAsync(Ct);
        await rig.Directory.RefreshOnMissAsync(Ct);
        rig.Clock.Advance(TimeSpan.FromSeconds(599));
        await rig.Directory.RefreshAsync(Ct);
        Assert.Equal(requestsWhenHeld, use1.HttpRequests);
        Assert.Equal(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);

        /* The hold ends, the store has recovered, and the next refresh finds it. Its list was kept all along. */
        rig.Clock.Advance(TimeSpan.FromSeconds(2));
        use1.Refuse(0);
        Assert.NotEqual(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
        Assert.Equal("SQL01", Assert.Single(rig.State("use1").Servers).ServerName);
        await rig.Directory.RefreshAsync(Ct);
        Assert.Equal(FrontDoorStoreHealth.Ok, rig.State("use1").Health);
    }

    [Fact]
    public async Task A_401_without_a_retry_after_holds_the_store_for_five_minutes()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        use1.Refuse(401);
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });

        await rig.Directory.RefreshAsync(Ct);
        Assert.Equal(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
        var requestsWhenHeld = use1.HttpRequests;

        rig.Clock.Advance(TimeSpan.FromSeconds(299));
        await rig.Directory.RefreshAsync(Ct);
        Assert.Equal(requestsWhenHeld, use1.HttpRequests);

        rig.Clock.Advance(TimeSpan.FromSeconds(2));
        await rig.Directory.RefreshAsync(Ct);
        Assert.True(use1.HttpRequests > requestsWhenHeld, "the hold ended, so the store is asked again");
    }

    [Fact]
    public async Task A_miss_refreshes_once_to_find_a_server_added_since_and_at_most_once_per_thirty_seconds()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });
        await rig.Directory.RefreshAsync(Ct);
        use1.AddServer("NEWBOX");
        Assert.IsType<FrontDoorRoute.Refused>(rig.Directory.ResolveServer("NEWBOX"));
        var readsBefore = use1.ToolCalls("list_servers");

        await rig.Directory.RefreshOnMissAsync(Ct);

        Assert.Equal(new FrontDoorRoute.ToStore("use1"), rig.Directory.ResolveServer("NEWBOX"));
        Assert.Equal(readsBefore + 1, use1.ToolCalls("list_servers"));

        /* A second miss inside the gap does not read again, so a caller that keeps asking for a name that is not there cannot
           turn every call into a read of every store. */
        use1.AddServer("ANOTHER");
        rig.Clock.Advance(TimeSpan.FromSeconds(29));
        await rig.Directory.RefreshOnMissAsync(Ct);
        Assert.Equal(readsBefore + 1, use1.ToolCalls("list_servers"));
        Assert.IsType<FrontDoorRoute.Refused>(rig.Directory.ResolveServer("ANOTHER"));

        rig.Clock.Advance(TimeSpan.FromSeconds(2));
        await rig.Directory.RefreshOnMissAsync(Ct);
        Assert.Equal(readsBefore + 2, use1.ToolCalls("list_servers"));
        Assert.Equal(new FrontDoorRoute.ToStore("use1"), rig.Directory.ResolveServer("ANOTHER"));
    }

    [Fact]
    public async Task Stores_are_listed_in_file_order_and_found_ignoring_case()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var use2 = await FakeMcpPeer.StartAsync(new[] { "SQL02" });
        await using var rig = new FrontDoorRig(new[] { ("use2", use2), ("use1", use1) });

        Assert.Equal(new[] { "use2", "use1" }, rig.Directory.Stores.Select(s => s.Store.Name));
        Assert.Equal("use1", rig.Directory.Find("USE1")?.Name);
        Assert.Null(rig.Directory.Find("use3"));
    }

    private static FrontDoorConfig ConfigWith(int refreshSeconds, int peerTimeoutSeconds = FrontDoorConfig.DefaultPeerTimeoutSeconds) => new()
    {
        Enabled = true,
        RefreshSeconds = refreshSeconds,
        PeerTimeoutSeconds = peerTimeoutSeconds,
        Stores = new List<FrontDoorStoreConfig> { new() { Name = "use1", McpUrl = "http://127.0.0.1:1/mcp", McpToken = "t" } },
    };

    [Theory]
    [InlineData(0, FrontDoorConfig.MinRefreshSeconds)]
    [InlineData(-1, FrontDoorConfig.MinRefreshSeconds)]
    [InlineData(int.MinValue, FrontDoorConfig.MinRefreshSeconds)]
    [InlineData(90, 90)]
    [InlineData(int.MaxValue, FrontDoorConfig.MaxRefreshSeconds)]
    public void The_refresh_interval_is_held_between_the_config_limits_even_when_the_file_was_never_validated(int configured, int expectedSeconds)
    {
        using var directory = new DarlingFrontDoorDirectory(ConfigWith(configured), new FakeFrontDoorPeers());

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), directory.RefreshInterval);
    }

    [Theory]
    [InlineData(0, FrontDoorConfig.MinPeerTimeoutSeconds)]
    [InlineData(-1, FrontDoorConfig.MinPeerTimeoutSeconds)]
    [InlineData(int.MaxValue, FrontDoorConfig.MaxPeerTimeoutSeconds)]
    public void The_peer_timeout_is_held_between_the_config_limits_even_when_the_file_was_never_validated(int configured, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), ConfigWith(30, configured).EffectivePeerTimeout());
    }

    [Fact]
    public async Task The_refresh_service_with_a_negative_refresh_interval_keeps_running_and_waits_between_reads()
    {
        var peers = new FakeFrontDoorPeers();
        using var directory = new DarlingFrontDoorDirectory(ConfigWith(-1), peers);
        using var service = new DarlingFrontDoorDirectoryService(directory);

        await service.StartAsync(Ct);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (peers.Calls.Count == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20, Ct);
            }

            Assert.NotEmpty(peers.Calls);
            await Task.Delay(300, Ct);

            Assert.False(service.ExecuteTask!.IsCompleted);
            Assert.Equal(1, peers.Calls.Count(c => c.Params.Name == "list_servers"));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }
}
