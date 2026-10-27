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
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace Darling.Tests.FrontDoor;

/// <summary>A clock a test moves by hand, so a 30 second gap or a 5 minute hold is not waited out.</summary>
internal sealed class ManualClock : TimeProvider
{
    private DateTimeOffset _now = new(2026, 10, 27, 12, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan span) => _now += span;
}

/// <summary>
/// A front door's directory and MCP peer client wired to fake stores (#6840), the way the process wires them. Each store gets the
/// fake's operator token and, unless <paramref name="hasAgentToken"/> says otherwise, its agent token.
/// </summary>
internal sealed class FrontDoorRig : IAsyncDisposable
{
    public FrontDoorRig(IReadOnlyList<(string Name, FakeMcpPeer Peer)> stores, Func<string, bool>? hasAgentToken = null)
    {
        Config = new FrontDoorConfig
        {
            Enabled = true,
            Stores = stores.Select(s => new FrontDoorStoreConfig
            {
                Name = s.Name,
                McpUrl = s.Peer.Url.ToString(),
                McpToken = FakeMcpPeer.OperatorToken,
                McpAgentToken = hasAgentToken is null || hasAgentToken(s.Name) ? FakeMcpPeer.AgentToken : null,
            }).ToList(),
        };
        var secrets = new DarlingFrontDoorSecrets(Config);
        DarlingFrontDoorDirectory? directory = null;
        Peers = new DarlingFrontDoorMcpPeers(Config, secrets, () => directory!);
        directory = new DarlingFrontDoorDirectory(Config, Peers, clock: Clock);
        Directory = directory;
    }

    public FrontDoorConfig Config { get; }

    public ManualClock Clock { get; } = new();

    public DarlingFrontDoorMcpPeers Peers { get; }

    public DarlingFrontDoorDirectory Directory { get; }

    public FrontDoorStoreState State(string store)
        => Directory.Stores.Single(s => string.Equals(s.Store.Name, store, StringComparison.OrdinalIgnoreCase));

    public async ValueTask DisposeAsync()
    {
        await Peers.DisposeAsync();
        Directory.Dispose();
    }
}
