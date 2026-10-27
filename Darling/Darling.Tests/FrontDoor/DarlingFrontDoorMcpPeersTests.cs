/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Darling.Service.FrontDoor;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>#6840 part P2: the front door's MCP client to its stores, against in-process fake stores.</summary>
public class DarlingFrontDoorMcpPeersTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CallToolRequestParams Call(string tool) => new() { Name = tool };

    private static string Text(CallToolResult result) => result.Content.OfType<TextContentBlock>().Single().Text;

    [Fact]
    public async Task A_call_reaches_the_store_with_the_operator_token_and_the_answer_comes_back()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });

        var result = await rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct);

        Assert.Equal("operator", Text(result));
        Assert.Contains("Bearer " + FakeMcpPeer.OperatorToken, use1.Authorizations);
    }

    [Fact]
    public async Task An_agent_role_call_carries_the_stores_agent_token_and_never_the_operator_one()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });

        var agent = await rig.Peers.CallAsync("use1", Call("whoami"), agentRole: true, Ct);
        var operatorAnswer = await rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct);

        Assert.Equal("agent", Text(agent));
        Assert.Equal("operator", Text(operatorAnswer));
        Assert.Contains("Bearer " + FakeMcpPeer.AgentToken, use1.Authorizations);
    }

    [Fact]
    public async Task An_agent_role_call_is_refused_when_the_store_has_no_agent_token_and_nothing_is_sent()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) }, hasAgentToken: _ => false);

        var refused = await Assert.ThrowsAsync<FrontDoorPeerException>(
            () => rig.Peers.CallAsync("use1", Call("whoami"), agentRole: true, Ct));

        Assert.Equal(FrontDoorPeerFailure.NoToken, refused.Kind);
        Assert.Equal("use1", refused.Store);
        Assert.Equal(0, use1.HttpRequests);
        Assert.Equal(0, use1.ToolCalls("whoami"));
    }

    [Fact]
    public async Task A_busy_answer_is_passed_back_and_not_retried()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });

        var result = await rig.Peers.CallAsync("use1", Call("get_busy"), agentRole: false, Ct);

        Assert.Contains("\"busy\"", Text(result));
        Assert.Equal(1, use1.ToolCalls("get_busy"));
        Assert.NotEqual(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
    }

    [Fact]
    public async Task A_caller_that_cancels_cancels_the_request_the_store_is_running()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        use1.BlockWaitStats = true;
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var call = rig.Peers.CallAsync("use1", Call("get_wait_stats"), agentRole: false, caller.Token);
        await use1.WaitStatsStarted.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);
        await caller.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);
        await use1.WaitStatsCancelled.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);
    }

    [Fact]
    public async Task A_429_holds_the_store_and_the_next_call_is_refused_without_being_sent()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });
        await rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct);
        use1.Refuse(429, retryAfterSeconds: 600);

        var first = await Assert.ThrowsAsync<FrontDoorPeerException>(
            () => rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct));

        Assert.Equal(FrontDoorPeerFailure.AuthFailed, first.Kind);
        Assert.Equal(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
        var requestsWhenHeld = use1.HttpRequests;

        var second = await Assert.ThrowsAsync<FrontDoorPeerException>(
            () => rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct));

        Assert.Equal(FrontDoorPeerFailure.AuthFailed, second.Kind);
        Assert.Equal(requestsWhenHeld, use1.HttpRequests);

        /* The hold is the store's Retry-After, not the five minute floor. */
        rig.Clock.Advance(TimeSpan.FromSeconds(301));
        Assert.Equal(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
        rig.Clock.Advance(TimeSpan.FromSeconds(300));
        Assert.NotEqual(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
    }

    [Fact]
    public async Task A_wrong_token_is_refused_by_the_store_and_no_message_carries_a_token()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" }, operatorToken: "some-other-token");
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });

        var refused = await Assert.ThrowsAsync<FrontDoorPeerException>(
            () => rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct));

        Assert.Equal(FrontDoorPeerFailure.AuthFailed, refused.Kind);
        Assert.DoesNotContain(FakeMcpPeer.OperatorToken, refused.ToString());
        Assert.DoesNotContain("some-other-token", refused.ToString());
        Assert.Equal(FrontDoorStoreHealth.AuthFailed, rig.State("use1").Health);
    }

    [Fact]
    public async Task A_store_that_is_down_is_unreachable_and_a_store_that_comes_back_is_called_again()
    {
        var down = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        var port = down.Url.Port;
        await using var rig = new FrontDoorRig(new[] { ("use1", down) });
        Assert.Equal("operator", Text(await rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct)));

        await down.DisposeAsync();
        var failure = await Assert.ThrowsAsync<FrontDoorPeerException>(
            () => rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct));
        Assert.Equal(FrontDoorPeerFailure.Unreachable, failure.Kind);

        await using var back = await FakeMcpPeer.StartAsync(new[] { "SQL01" }, port: port);
        Assert.Equal("operator", Text(await rig.Peers.CallAsync("use1", Call("whoami"), agentRole: false, Ct)));
    }

    [Fact]
    public async Task A_store_name_that_is_not_configured_is_refused()
    {
        await using var use1 = await FakeMcpPeer.StartAsync(new[] { "SQL01" });
        await using var rig = new FrontDoorRig(new[] { ("use1", use1) });

        await Assert.ThrowsAsync<FrontDoorPeerException>(
            () => rig.Peers.CallAsync("use9", Call("whoami"), agentRole: false, Ct));
    }
}
