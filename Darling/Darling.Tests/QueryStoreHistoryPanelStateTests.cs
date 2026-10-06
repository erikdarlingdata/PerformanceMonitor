/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web Query Store history panel's open state (#5234): one panel per grid row, a Plan button that opens only its
/// own cell, a redraw set that stays bounded across grid rebuilds, and a chart axis that stays with the data it was
/// read for. The shipped <c>query-store-history.js</c> runs under Node through the harness that
/// <see cref="QueryStoreHistoryBehaviourTests"/> uses; Node is skipped when it is not installed.
/// </summary>
public sealed class QueryStoreHistoryPanelStateTests
{
    private static JsonElement Run(string scenario) => QueryStoreHistoryBehaviourTests.Run(scenario);

    private static bool[] Flags(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetBoolean()).ToArray();

    private static string[] Strs(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static int[] Ints(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetInt32()).ToArray();

    private static void AssertOpenPlans(JsonElement r, string name, int a, int b, int grid)
    {
        var o = r.GetProperty(name);
        Assert.Equal(a, o.GetProperty("a").GetInt32());
        Assert.Equal(b, o.GetProperty("b").GetInt32());
        Assert.Equal(grid, o.GetProperty("grid").GetInt32());
    }

    [Fact]
    public void HistoryOpenedOnOneGridRow_LeavesTheOtherRowsOfTheQueryClosed()
    {
        var r = Run("twoRows");
        Assert.Equal(new[] { true, false, false, false }, Flags(r, "afterFirst"));
        Assert.Equal(1, r.GetProperty("fetchesAfterFirst").GetInt32());
        Assert.Equal(new[] { "srv-a|Orders|42|7|Regular|PRIMARY" }, Strs(r, "keysAfterFirst"));
        Assert.Equal(new[] { true, true, false, false }, Flags(r, "afterSecond"));
        Assert.Equal(new[] { false, true, false, false }, Flags(r, "afterHide"));
        Assert.Equal(new[] { "srv-a|Orders|42|9|Regular|PRIMARY" }, Strs(r, "keysAfterHide"));
    }

    [Fact]
    public void ThePlanButtonInAHistoryTable_OpensOnlyItsOwnCell()
    {
        var r = Run("planButtonScope");
        Assert.Equal(1, r.GetProperty("planReads").GetInt32());
        AssertOpenPlans(r, "afterTableButton", a: 1, b: 0, grid: 0);
        AssertOpenPlans(r, "afterGridButton", a: 1, b: 0, grid: 1);
        AssertOpenPlans(r, "afterOtherTableButton", a: 1, b: 1, grid: 1);
        Assert.Equal(3, r.GetProperty("planPanels").GetInt32());
    }

    /// <summary>Three rows a build, rebuilt twelve times without a click: first the same rows each time, then new rows each
    /// time. The set holds the grid on the page and the one being built, so six cells at most.</summary>
    [Fact]
    public void RebuildingTheGridWithoutAClick_KeepsTheRedrawSetBounded()
    {
        var r = Run("rebuildsBounded");
        foreach (var phase in new[] { "sameRows", "newRows" })
        {
            var p = r.GetProperty(phase);
            var after = Ints(p, "after");
            Assert.All(after, n => Assert.InRange(n, 3, 6));
            Assert.Equal(6, after[^1]);
            // The cells of the build in progress are not on the page yet, and a later cell of that build must not drop them.
            Assert.Equal(6, Ints(p, "during")[^1]);
        }
        Assert.Equal(6, r.GetProperty("keys").GetInt32());
    }

    [Fact]
    public void UnderAPreset_ARedrawKeepsTheWindowTheDataWasLoadedFor_AndANewReadTakesItsOwn()
    {
        const long hour = 3600000L;
        var r = Run("presetWindow");
        var end = DateTimeOffset.Parse("2026-03-04T06:00:00Z").ToUnixTimeMilliseconds();
        var loaded = r.GetProperty("loaded");
        Assert.Equal(end, loaded.GetProperty("end").GetInt64());
        Assert.Equal(end - hour, loaded.GetProperty("start").GetInt64());

        // Half an hour on, the page's rebuild draws the open panel from the same read, so the axis stays where it was.
        Assert.Equal(0, r.GetProperty("rebuiltRefetched").GetInt32());
        var rebuilt = r.GetProperty("rebuilt");
        Assert.Equal(end, rebuilt.GetProperty("end").GetInt64());
        Assert.Equal(end - hour, rebuilt.GetProperty("start").GetInt64());

        // A read for a wider window takes the axis of that window, anchored at the time of that read.
        Assert.Equal(1, r.GetProperty("widenedRefetched").GetInt32());
        var widened = r.GetProperty("widened");
        Assert.Equal(end + hour / 2, widened.GetProperty("end").GetInt64());
        Assert.Equal(end + hour / 2 - 6 * hour, widened.GetProperty("start").GetInt64());
    }
}
