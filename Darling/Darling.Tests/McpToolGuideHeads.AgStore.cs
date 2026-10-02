/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for <c>get_ag_health</c> (<c>DarlingMcpAgTools.cs</c>) and <c>get_store_query_stats</c>
/// (<c>DarlingMcpStoreQueryStatsTools.cs</c>). Both are Darling-only reads (no Lite twin), so this file carries
/// no D6 lockstep pin. See <see cref="McpToolGuideHeadsHealthParserTests"/> for the pattern.
/// </summary>
public sealed class McpToolGuideHeadsAgStoreTests
{
    private static readonly string[] AgStoreTools =
    [
        "get_ag_health",
        "get_store_query_stats",
    ];

    [Fact]
    public void EveryHead_StaysAtOrUnder620Characters_AndKeepsItsParametersUnder200()
    {
        foreach (var tool in AgStoreTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200));
        }
    }

    /// <summary>D9: <c>get_ag_health</c> reports ONE ROW PER REPLICA'S VIEW of an AG (not merged), severities
    /// restate the DMVs' own verdicts and are NOT banded on lag or queue depth, lag reads 0 while suspended, and
    /// a stale <c>collection_time</c> can outlive a dropped AG — none of that is visible from the JSON alone.</summary>
    [Fact]
    public void AgHealthHead_CarriesThePerspectiveAndBandingGuardrails()
    {
        var served = McpToolGuideTests.Served("get_ag_health").Served;
        Assert.Contains("One row per REPLICA's view: a multi-replica AG appears once per replica, not merged.", served, StringComparison.Ordinal);
        Assert.Contains(
            "Severities restate DMV verdicts only, un-banded on lag/queue depth; lag reads 0 while suspended, so check secondary_lag_seconds and is_suspended yourself.",
            served, StringComparison.Ordinal);
        Assert.Contains("collection_time can be stale after an AG is dropped.", served, StringComparison.Ordinal);
    }

    /// <summary>D9: <c>get_ag_health</c> answers TWO different miss words for two different causes. Empty is a
    /// real "nothing collected" (fleet-wide or for the named server); not_collected fires only when the caller
    /// scoped to one server whose engine never runs AG collection at all (Azure SQL Database, PostgreSQL). A
    /// fleet-wide read never returns not_collected, because no one engine speaks for the whole fleet.</summary>
    [Fact]
    public void AgHealthHead_SeparatesEmptyFromNotCollected()
    {
        var served = McpToolGuideTests.Served("get_ag_health").Served;
        Assert.Contains("Empty: none collected fleet-wide or on the server.", served, StringComparison.Ordinal);
        Assert.Contains("Scoped to a server whose engine never runs AG collection: not_collected.", served, StringComparison.Ordinal);
    }

    /// <summary>D9: <c>get_store_query_stats</c> has no <c>server_name</c> parameter at all (the store, not a
    /// monitored server, is the subject), and its role split answers a TIME SHARE question, not a perceived-speed
    /// one (<c>BuildNote</c>: "not how slow that role's calls felt").</summary>
    [Fact]
    public void StoreQueryStatsHead_CarriesTheSubjectAndRoleShareGuardrails()
    {
        var served = McpToolGuideTests.Served("get_store_query_stats").Served;
        Assert.Contains("No server_name: the store is the subject.", served, StringComparison.Ordinal);
        Assert.Contains("by_role is each role's TIME SHARE, not how slow it felt.", served, StringComparison.Ordinal);
    }

    /// <summary>D9: a store with zero matching statements answers a NORMAL payload with empty arrays, not a
    /// status empty envelope — this tool has no empty/unavailable branch at all. The only non-JSON status is
    /// precondition, gated on pg_stat_statements being missing, not loaded or too old, its reader not built yet, or
    /// the role having no grant (<c>PreconditionReason</c> has all five).</summary>
    [Fact]
    public void StoreQueryStatsHead_SeparatesZeroRowsFromThePreconditionGate()
    {
        var served = McpToolGuideTests.Served("get_store_query_stats").Served;
        Assert.Contains("Zero matches: a normal payload with empty arrays, not status empty.", served, StringComparison.Ordinal);
        Assert.Contains(
            "Gated: status precondition, with the remedy, when pg_stat_statements is missing, not loaded or too old, its reader isn't built yet, or this role has no grant.",
            served, StringComparison.Ordinal);
    }
}
