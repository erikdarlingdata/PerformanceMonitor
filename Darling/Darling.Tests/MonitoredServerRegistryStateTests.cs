/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2298 worker→MCP registry seam. The behavioral contract is small and every piece is load-bearing:
/// null-before-first-publish is what routes the MCP host to its darling.json fallback, the snapshot swap is
/// what lets the per-fetch resolver heal without a restart, and first-wins on a duplicate id mirrors the
/// resolver map this state replaced (and the worker's FirstOrDefault over runtimes) — last-wins would make
/// the MCP host resolve a different server than the worker collects from.
/// </summary>
public sealed class MonitoredServerRegistryStateTests
{
    private static MonitoredServer Server(string name) => new() { Name = name, Host = name };

    [Fact]
    public void NullBeforeFirstPublish_SoTheReaderTakesItsFileFallback()
    {
        var state = new MonitoredServerRegistryState();

        Assert.Null(state.Read());
    }

    [Fact]
    public void PublishedSnapshotCarriesTheSetAndItsIdMap()
    {
        var state = new MonitoredServerRegistryState();
        var alpha = Server("alpha");
        var bravo = Server("bravo");

        state.Publish(new List<MonitoredServer> { alpha, bravo });

        var snapshot = state.Read();
        Assert.NotNull(snapshot);
        Assert.Equal(2, snapshot!.Servers.Count);
        Assert.Same(alpha, snapshot.ById[alpha.ServerId]);
        Assert.Same(bravo, snapshot.ById[bravo.ServerId]);
    }

    [Fact]
    public void RepublishSwapsTheWholeSnapshot_SoAResolverSeesTheNewSetOnItsNextRead()
    {
        var state = new MonitoredServerRegistryState();
        var original = Server("original");
        var added = Server("added-through-add-servers");

        state.Publish(new List<MonitoredServer> { original });
        var before = state.Read();

        state.Publish(new List<MonitoredServer> { original, added });
        var after = state.Read();

        Assert.NotSame(before, after);
        Assert.False(before!.ById.ContainsKey(added.ServerId));
        Assert.True(after!.ById.ContainsKey(added.ServerId));
    }

    [Fact]
    public void FirstEntryWinsOnADuplicateServerId()
    {
        var state = new MonitoredServerRegistryState();
        /* Same name + host → the same derived ServerId, the duplicate the resolver map deduped first-wins. */
        var first = Server("twin");
        var second = Server("twin");
        Assert.Equal(first.ServerId, second.ServerId);

        state.Publish(new List<MonitoredServer> { first, second });

        Assert.Same(first, state.Read()!.ById[first.ServerId]);
    }
}
