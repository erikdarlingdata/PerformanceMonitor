/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Unit coverage for #3953's grid/MCP top gate (ruling issuecomment-5836972848; review D4R items H3, M1):
/// <see cref="QueryStoreIntervalWide.UseTable"/>, each clause alone flipping the answer to raw. Mirrors
/// <c>PlanRegressionIntervalTableEquivalenceTests</c>' own <c>UseTable</c> theory for V143's gate. The live
/// equality, clamp and end-to-end gate tests belong beside the grid/MCP top reads that call this decision.
/// </summary>
public sealed class QueryStoreIntervalWideGateTests
{
    private static readonly DateTime WindowEnd = new(2026, 9, 25, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = WindowEnd.AddHours(-24);
    private static readonly TimeSpan MinWindow = QueryStoreIntervalWide.GridWideMinWindow;

    /// <summary>Every argument defaults to a value that alone passes its own clause, so a test overrides only
    /// the one input the clause under test cares about.</summary>
    private static bool Rule(
        DateTime? filledSince = null, bool hasPending = false, DateTime? rawFloor = null,
        DateTime? windowStart = null, DateTime? windowEnd = null, DateTime? literalWindowEnd = null,
        DateTime? appliedThrough = null, DateTime? tableFloor = null, TimeSpan? minWindow = null) =>
        QueryStoreIntervalWide.UseTable(
            filledSince ?? WindowStart.AddDays(-1), hasPending, rawFloor,
            windowStart ?? WindowStart, windowEnd ?? WindowEnd, literalWindowEnd,
            appliedThrough ?? WindowEnd, tableFloor ?? WindowStart.AddDays(-2), minWindow ?? MinWindow);

    [Fact]
    public void EveryClauseHolding_ReadsTheTable() => Assert.True(Rule());

    [Fact]
    public void Clause1_NoCoverageRow_ReadsRaw() =>
        // A real DateTime? null (no coverage row at all) — bypasses Rule's "unset means default" sentinel.
        Assert.False(QueryStoreIntervalWide.UseTable(
            filledSince: null, hasPending: false, rawFloor: null, windowStart: WindowStart, windowEnd: WindowEnd,
            literalWindowEnd: null, appliedThrough: WindowEnd, tableFloor: WindowStart.AddDays(-2), minWindow: MinWindow));

    [Fact]
    public void Clause1_PendingBatch_ReadsRaw_EvenWithGoodCoverage() => Assert.False(Rule(hasPending: true));

    [Fact]
    public void Clause2_FilledSinceAfterMaxOfRawFloorAndWindowStart_ReadsRaw() =>
        // filledSince sits after both the (absent) raw floor and the window start: the table's claim starts
        // later than the window needs.
        Assert.False(Rule(filledSince: WindowEnd));

    [Fact]
    public void Clause3_TableFloorShortOfTheOneDayMargin_ReadsRaw() =>
        // table floor only reaches 23h before window start — short of the full one-day margin clause 3 needs —
        // and raw has no floor (a plain store): the table cannot answer for the earliest part of the window.
        Assert.False(Rule(tableFloor: WindowStart.AddHours(-23)));

    [Fact]
    public void Clause3_TableFloorAtOrBeyondTheOneDayMargin_ReadsTheTable() =>
        Assert.True(Rule(tableFloor: WindowStart.AddDays(-1)));

    [Fact]
    public void Clause3_RawFloorAtOrAboveTableFloor_ReadsTheTable_EvenOutsideTheMargin() =>
        Assert.True(Rule(rawFloor: WindowStart.AddDays(-3), tableFloor: WindowStart.AddDays(-3)));

    [Fact]
    public void Clause4_OpenEnd_SkipsTheAppliedThroughComparison() =>
        // appliedThrough sits after the window end (as a viewer clock running behind the store's would produce),
        // but literalWindowEnd is null (a preset): clause 4 never fires.
        Assert.True(Rule(literalWindowEnd: null, appliedThrough: WindowEnd.AddMinutes(5)));

    [Fact]
    public void Clause4_LiteralEndBeforeAppliedThrough_ReadsRaw() =>
        Assert.False(Rule(literalWindowEnd: WindowEnd, appliedThrough: WindowEnd.AddMinutes(5)));

    [Fact]
    public void Clause4_LiteralEndAtOrAfterAppliedThrough_ReadsTheTable() =>
        Assert.True(Rule(literalWindowEnd: WindowEnd, appliedThrough: WindowEnd));

    [Fact]
    public void Clause5_WindowShorterThanTheMinimum_ReadsRaw() =>
        Assert.False(Rule(windowStart: WindowEnd.AddHours(-1)));

    [Fact]
    public void Clause5_WindowAtExactlyTheMinimum_ReadsTheTable() =>
        Assert.True(Rule(windowStart: WindowEnd - MinWindow));

    [Fact]
    public void ClampedStart_UsesTheRawFloor_OnlyWhenItIsAfterTheWindowStart()
    {
        Assert.Equal(WindowStart, QueryStoreIntervalWide.ClampedStart(null, WindowStart));
        Assert.Equal(WindowStart, QueryStoreIntervalWide.ClampedStart(WindowStart.AddHours(-1), WindowStart));

        var laterFloor = WindowStart.AddHours(1);
        Assert.Equal(laterFloor, QueryStoreIntervalWide.ClampedStart(laterFloor, WindowStart));
    }

    private static readonly DateTime Floor = WindowStart.AddDays(4);

    [Theory]
    [InlineData(null, 0, true)]
    [InlineData(5, 0, true)]
    [InlineData(60, 60, true)]
    [InlineData(61, 5, false)]
    [InlineData(5, 61, false)]
    [InlineData(0, 5, false)]
    public void CadenceAllowsBelowFloor_SixtyMinutesIsTheLimit(int? cadence, int gapMinutes, bool expected) =>
        Assert.Equal(expected, QueryStoreIntervalWide.CadenceAllowsBelowFloor(cadence, TimeSpan.FromMinutes(gapMinutes == 0 ? 5 : gapMinutes)));

    [Fact]
    public void CadenceAllowsBelowFloor_UnknownGap_IsRefused() =>
        Assert.False(QueryStoreIntervalWide.CadenceAllowsBelowFloor(5, null));

    [Fact]
    public void PurgeEdgeMargin_IsOneDayTwoHours() =>
        Assert.Equal(TimeSpan.FromHours(26), QueryStoreIntervalWide.PurgeEdgeMargin);

    [Fact]
    public void ExactBelowFloorStart_NoFloorOrFloorAtOrBelowWindowStart_IsNull()
    {
        Assert.Null(QueryStoreIntervalWide.ExactBelowFloorStart(null, WindowStart, WindowStart, WindowStart.AddDays(-9)));
        Assert.Null(QueryStoreIntervalWide.ExactBelowFloorStart(WindowStart, WindowStart, WindowStart, WindowStart.AddDays(-9)));
    }

    [Fact]
    public void ExactBelowFloorStart_NoTableFloor_IsNull() =>
        Assert.Null(QueryStoreIntervalWide.ExactBelowFloorStart(Floor, WindowStart, WindowStart, null));

    [Fact]
    public void ExactBelowFloorStart_AllBoundsBelowWindow_IsWindowStart() =>
        Assert.Equal(WindowStart, QueryStoreIntervalWide.ExactBelowFloorStart(Floor, WindowStart, WindowStart.AddDays(-1), WindowStart.AddDays(-5))!.Value.Start);

    [Fact]
    public void ExactBelowFloorStart_FilledSinceLater_IsFilledSince() =>
        Assert.Equal(WindowStart.AddDays(1), QueryStoreIntervalWide.ExactBelowFloorStart(Floor, WindowStart, WindowStart.AddDays(1), WindowStart.AddDays(-5))!.Value.Start);

    [Fact]
    public void ExactBelowFloorStart_TableFloorPlusMarginLater_IsThatBound() =>
        Assert.Equal(WindowStart.AddDays(2) + QueryStoreIntervalWide.PurgeEdgeMargin,
            QueryStoreIntervalWide.ExactBelowFloorStart(Floor, WindowStart, WindowStart, WindowStart.AddDays(2))!.Value.Start);

    [Fact]
    public void ExactBelowFloorStart_BoundAtOrPastTheFloor_IsNull()
    {
        Assert.Null(QueryStoreIntervalWide.ExactBelowFloorStart(Floor, WindowStart, Floor, WindowStart.AddDays(-5)));
        Assert.Null(QueryStoreIntervalWide.ExactBelowFloorStart(Floor, WindowStart, WindowStart, Floor));
    }

    [Fact]
    public void WideReadPlan_EffectiveStart_IsReadStartWhenTheTableServes() =>
        Assert.Equal(WindowStart.AddDays(1), new QueryStoreIntervalWide.WideReadPlan(true, Floor, WindowStart.AddDays(1), WindowStart.AddDays(1)).EffectiveStart);

    [Fact]
    public void WideReadPlan_EffectiveStart_IsNullWhenTheTableDoesNotServe() =>
        Assert.Null(new QueryStoreIntervalWide.WideReadPlan(false, WindowStart, WindowStart, null).EffectiveStart);

    [Fact]
    public void ExactBelowFloorStart_NamesTheBoundThatWon()
    {
        var w = WindowStart;
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.Window,
            QueryStoreIntervalWide.ExactBelowFloorStart(Floor, w, w.AddDays(-1), w.AddDays(-5))!.Value.Bound);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.FilledSince,
            QueryStoreIntervalWide.ExactBelowFloorStart(Floor, w, w.AddDays(1), w.AddDays(-5))!.Value.Bound);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.TablePurgeEdge,
            QueryStoreIntervalWide.ExactBelowFloorStart(Floor, w, w, w.AddDays(2))!.Value.Bound);
    }

    [Fact]
    public void ExactBelowFloorStart_TiesPreferFilledSinceThenPurgeEdgeThenWindow()
    {
        var w = WindowStart;
        // filled_since equals the purge edge: the coverage claim wins.
        var edge = w.AddDays(2);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.FilledSince,
            QueryStoreIntervalWide.ExactBelowFloorStart(Floor, w, edge + QueryStoreIntervalWide.PurgeEdgeMargin, edge)!.Value.Bound);
        // the purge edge equals the window start: the purge edge wins over the window.
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.TablePurgeEdge,
            QueryStoreIntervalWide.ExactBelowFloorStart(Floor, w, w.AddDays(-5), w - QueryStoreIntervalWide.PurgeEdgeMargin)!.Value.Bound);
    }
}
