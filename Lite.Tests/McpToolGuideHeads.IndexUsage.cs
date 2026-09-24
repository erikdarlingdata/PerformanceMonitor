/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 D3 head pins for <c>get_index_usage</c> (wave E, lane e10). A twin: Darling's copy of this class lives
/// in <c>Darling/Darling.Tests/McpToolGuideHeads.IndexUsage.cs</c> and pins the same head text. See
/// <see cref="McpToolGuideHeadsHealthParserTests"/> for the pattern this file follows.
/// </summary>
public sealed class McpToolGuideHeadsIndexUsageTests
{
    /// <summary>The exact served head, byte-identical on both products (D6): the D2 size cap for this lane was
    /// 493 served chars (Lite's original, unconverted length), not the usual 620, so Lite does not grow.</summary>
    private const string Head =
        "Per-index usage (seeks, scans, lookups, updates) from the latest daily snapshot, classed Unused, "
        + "Write-only, or Active. Unused/write-only sort first as drop candidates: on a server with many, "
        + "results can be one database's unused indexes, hiding Active ones elsewhere. Counters reset at the "
        + "last restart or index rebuild, so Write-only means no reads since then. last_user_access is UTC "
        + "(de-skewed): compare directly with get_collection_log and list_servers.";

    [Fact]
    public void Head_CarriesTheOrderingTrap_TheRestartFloor_AndTheUtcDeSkew_AndStaysAtOrUnder493Served()
    {
        var served = McpToolGuideTests.Served("get_index_usage");
        Assert.NotNull(served.Tail);
        Assert.Equal(Head, served.Served[..^McpToolGuide.GuidePointer.Length]);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);

        /* D9: unused/write-only always sort ahead of Active, so a capped, all-Unused answer is not proof the
           server has few Active indexes — the trap #2636 exists to name. As of #2636, Lite carries the same
           database_name filter and truncated field as Darling, so the head's caution applies identically on
           both products now. */
        Assert.Contains(
            "Unused/write-only sort first as drop candidates: on a server with many, results can be one "
            + "database's unused indexes, hiding Active ones elsewhere.",
            served.Served,
            StringComparison.Ordinal);

        /* D9: a low or zero counter can mean "reset by a restart or a rebuild", not "never used". */
        Assert.Contains("Counters reset at the last restart or index rebuild, so Write-only means no reads since then.", served.Served, StringComparison.Ordinal);

        /* D9: last_user_access is the one field on this payload with no UTC neighbour to cross-check against. */
        Assert.Contains(
            "last_user_access is UTC (de-skewed): compare directly with get_collection_log and list_servers.",
            served.Served,
            StringComparison.Ordinal);

        /* Lane e10's cap: 493 served chars total (head + the 31-char pointer), so Lite does not grow past its
           pre-conversion length. */
        Assert.True(served.Served.Length <= 493, $"get_index_usage: served head {served.Served.Length} is over the 493 lane cap");

        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200));
    }

    /// <summary>#2636: Lite gained the same database_name filter, matching_index_count, and truncated signal
    /// Darling has always had, so the tail now tells the same truth on both products — mirrored verbatim from
    /// Darling's tail rather than kept as a near-duplicate wording of an identical behavior.</summary>
    [Fact]
    public void Tail_CarriesTheDatabaseFilterGuidance_AndTheClassificationRule()
    {
        var tail = McpToolGuideTests.Served("get_index_usage").Tail!;

        Assert.Contains(
            "Pass database_name to ask about one database, which is almost always what you want; the response "
            + "carries matching_index_count and truncated so a short answer is never mistaken for an absent one.",
            tail,
            StringComparison.Ordinal);

        Assert.Contains(
            "Classification: Unused is zero seeks, scans and lookups AND zero updates; Write-only is zero of "
            + "the first three but at least one update; everything else is Active.",
            tail,
            StringComparison.Ordinal);

        /* #2636: database_name matching zero rows while the server has index data elsewhere is status empty,
           not unavailable — a different answer from a truly uncollected server. */
        Assert.Contains("status is empty, not unavailable", tail, StringComparison.Ordinal);
        Assert.Contains("this tool never reports a truly empty server as empty", tail, StringComparison.Ordinal);
    }
}
