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
           server has few Active indexes — sharper here than on Darling, since Lite has no database_name filter
           and no truncated field to say so. */
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

    /// <summary>Nothing the original Lite description said was lost, and the tail names the trap that is
    /// sharper on Lite than on Darling: no database_name filter and no truncation signal at all, so a capped
    /// answer here carries no sign that it was capped.</summary>
    [Fact]
    public void Tail_CarriesTheClassificationRule_AndTheNoTruncationSignalCaveat()
    {
        var tail = McpToolGuideTests.Served("get_index_usage").Tail!;

        Assert.Contains(
            "Classification: Unused is zero seeks, scans and lookups AND zero updates; Write-only is zero of "
            + "the first three but at least one update; everything else is Active.",
            tail,
            StringComparison.Ordinal);

        Assert.Contains(
            "This read has no database_name filter and no truncation signal: it always returns every "
            + "database's indexes on the server together, capped at 200 rows, unused and write-only first.",
            tail,
            StringComparison.Ordinal);

        Assert.Contains("this tool never returns status empty", tail, StringComparison.Ordinal);
    }
}
