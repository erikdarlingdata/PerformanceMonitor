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
}
