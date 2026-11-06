/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7984: Lite's Overview search matches the names of the tags above a card's tags, the same rule as the Darling
/// Viewer's Overview and sidebar: a server tagged HOSTS under PROD &gt; BULK is found by "BULK" and "PROD", while its
/// pill still shows only HOSTS. The pills are built through the pill class's own builder, and the fields through the
/// window's private search seam, so the test needs no window.
/// </summary>
public sealed class OverviewSearchParentTagsTests
{
    private static ServerTag Tag(int id, string name, int? parentId = null) => new(id, name, parentId, 0, null);

    /* PROD > BULK > HOSTS and PROD > BULK > LISTENERS, plus DR as a second root. */
    private static List<ServerTag> Forest() => new()
    {
        Tag(1, "PROD"), Tag(2, "BULK", 1), Tag(3, "HOSTS", 2), Tag(4, "LISTENERS", 2), Tag(5, "DR"),
    };

    /// <summary>The pill builder, reached by name so this file compiles against a Lite that stamps pills in its
    /// window code and fails here, at an assert, rather than at the compiler.</summary>
    private static IReadOnlyList<ServerTagPill> Pills(IReadOnlyList<ServerTag> tags, params int[] assigned)
    {
        var builder = typeof(ServerTagPill).GetMethod("ForServer", BindingFlags.Public | BindingFlags.Static);
        Assert.True(builder is not null,
            "ServerTagPill.ForServer does not exist: a pill carries only its own tag's name, so a search for the name of a tag above it matches no server.");
        return (IReadOnlyList<ServerTagPill>)builder!.Invoke(null, new object[] { tags, assigned })!;
    }

    /// <summary>The fields the Overview filter matches a card against: the window's private static seam.</summary>
    private static string?[] SearchFields(ServerSummaryItem card)
    {
        var fields = typeof(MainWindow).GetMethod("SearchFields", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(fields is not null, "MainWindow.SearchFields is the Overview search seam and must exist.");
        return (string?[])fields!.Invoke(null, new object[] { card })!;
    }

    private static ServerSummaryItem Card(string name, int id, IReadOnlyList<ServerTag> tags, params int[] assigned) =>
        new() { DisplayName = name, ServerName = name + ".corp", ServerId = id, TagPills = Pills(tags, assigned) };

    private static bool Matches(string term, ServerSummaryItem card) => ServerOverviewFilter.Matches(term, SearchFields(card));

    [Fact]
    public void Search_MatchesTheNameOfEveryTagAboveAnAssignedTag()
    {
        var host = Card("bulk-host-1", 1, Forest(), 3);
        var listener = Card("ag-listener", 2, Forest(), 4);
        var standby = Card("standby", 3, Forest(), 5);

        Assert.True(Matches("BULK", host));
        Assert.True(Matches("BULK", listener));
        Assert.False(Matches("BULK", standby));

        Assert.True(Matches("prod", host));
        Assert.True(Matches("prod", listener));
        Assert.False(Matches("prod", standby));

        Assert.True(Matches("HOSTS", host));
        Assert.False(Matches("HOSTS", listener));
        Assert.True(Matches("DR", standby));
    }

    [Fact]
    public void ThePill_StillShowsOnlyTheAssignedTag_OrderedByName()
    {
        Assert.Equal(new[] { "HOSTS", "LISTENERS" }, Pills(Forest(), 4, 3).Select(p => p.Name));
        Assert.Empty(Pills(Forest(), 42));
    }

    [Fact]
    public void AParentCycle_ADanglingParent_AndADeepChain_AllEndTheWalk()
    {
        var cycle = new List<ServerTag> { Tag(1, "A", 2), Tag(2, "B", 1) };
        var looped = Card("looped", 1, cycle, 1);
        Assert.True(Matches("B", looped));
        Assert.Equal(new[] { "A", "B" }, SearchFields(looped).Skip(2));

        var dangling = new List<ServerTag> { Tag(1, "ORPHAN", 99) };
        var orphan = Card("orphaned", 2, dangling, 1);
        Assert.True(Matches("orphan", orphan));
        Assert.Equal(new[] { "ORPHAN" }, SearchFields(orphan).Skip(2));

        var chain = Enumerable.Range(1, 200).Select(i => Tag(i, "level-" + i, i == 1 ? null : i - 1)).ToList();
        var deep = Card("deep", 3, chain, 200);
        Assert.True(Matches("level-199", deep));
        Assert.True(Matches("level-150", deep));
    }
}
