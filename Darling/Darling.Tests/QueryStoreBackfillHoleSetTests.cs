/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #9352: the state rule both products share for a database's recorded Query Store holes. The backfill digs a hole from its
/// ceiling down and saves the ceiling as it goes. A clamp hole recorded while an older hole is mid-dig used to widen the one
/// saved hole, which put its ceiling back up at the clamp point and sent the dig over ranges it had already filled and ranges
/// the live collector already holds, storing a second copy of every row in them.
/// </summary>
public sealed class QueryStoreBackfillHoleSetTests
{
    private static readonly DateTime Now = new(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime FloorLimit = Now.AddDays(-30);

    /// <summary>Plays the backfill scan and slices over a recorded value the way both workers do: the newest hole is dug from
    /// its ceiling down one bounded slice at a time, then the dug range leaves the value. Returns every range dug, in order.</summary>
    private static List<(DateTime From, DateTime To)> DigAll(string? state)
    {
        var dug = new List<(DateTime From, DateTime To)>();
        var slices = 0;
        while (state is not null)
        {
            Assert.True(QueryStoreBackfillState.TryDecodeNewestHole(state, out var holeFrom, out var holeTo));
            if (holeTo <= FloorLimit)
            {
                break;
            }

            var holeFloor = holeFrom > FloorLimit ? holeFrom : FloorLimit;
            var sliceFloor = QueryStoreBackfillState.BoundSliceFloor(holeFloor, holeTo);
            dug.Add((sliceFloor, holeTo));

            state = QueryStoreBackfillState.RemoveDugRange(state, sliceFloor, holeTo, holeFloor);
            Assert.InRange(++slices, 1, 5000);
        }

        return dug;
    }

    private static void AssertNoRangeDugTwice(List<(DateTime From, DateTime To)> dug)
    {
        for (var i = 0; i < dug.Count; i++)
        {
            for (var j = i + 1; j < dug.Count; j++)
            {
                Assert.False(dug[i].From < dug[j].To && dug[j].From < dug[i].To,
                    $"slice {i} ({dug[i].From:o}..{dug[i].To:o}) and slice {j} ({dug[j].From:o}..{dug[j].To:o}) dig the same range");
            }
        }
    }

    [Fact]
    public void AClampHoleAboveAHoleThatIsMidDig_NeverSendsTheDigOverARangeAlreadyFilled()
    {
        /* The tail's hole was dug from the top down to ten days ago: everything above that is stored. A clamp then opens a
           two-hour hole at the top. The span between the two is stored, by the dig above its ceiling and by live collection. */
        var midDigCeiling = Now.AddDays(-10);
        var pending = QueryStoreBackfillState.EncodeHole(FloorLimit, midDigCeiling);
        var clampFrom = Now.AddHours(-3);
        var clampTo = Now.AddHours(-1);

        var state = QueryStoreBackfillState.AddHole(pending, clampFrom, clampTo);
        var dug = DigAll(state);

        AssertNoRangeDugTwice(dug);

        var alreadyFilledFrom = midDigCeiling;
        var alreadyFilledTo = clampFrom;
        foreach (var (from, to) in dug)
        {
            Assert.False(from < alreadyFilledTo && alreadyFilledFrom < to,
                $"the dig walked {from:o}..{to:o}, inside the stored span {alreadyFilledFrom:o}..{alreadyFilledTo:o}");
        }

        /* Both holes are dug in full: the newest first, then the older one resumed at its own saved ceiling. */
        Assert.Equal(clampTo, dug[0].To);
        Assert.Equal(clampFrom, dug.FindLast(d => d.To > clampFrom).From);
        Assert.Contains(dug, d => d.To == midDigCeiling);
        Assert.Equal(FloorLimit, dug[^1].From);
    }

    [Fact]
    public void ManyClampHolesWhileTheTailDigs_NeverSendTheDigOverARangeTwice()
    {
        /* The shape seen in the field: a fresh clamp hole lands every few hours while the long dig is still going. */
        var state = (string?)QueryStoreBackfillState.EncodeHole(FloorLimit, Now.AddDays(-2));
        var clampFloor = Now.AddDays(-2);
        var clamps = 0;
        var dug = new List<(DateTime From, DateTime To)>();
        var sliceCount = 0;
        while (state is not null)
        {
            Assert.True(QueryStoreBackfillState.TryDecodeNewestHole(state, out var holeFrom, out var holeTo));
            var holeFloor = holeFrom > FloorLimit ? holeFrom : FloorLimit;
            var sliceFloor = QueryStoreBackfillState.BoundSliceFloor(holeFloor, holeTo);
            dug.Add((sliceFloor, holeTo));
            state = QueryStoreBackfillState.RemoveDugRange(state, sliceFloor, holeTo, holeFloor);

            if (++sliceCount % 40 == 0 && clamps < 8)
            {
                clamps++;
                clampFloor = clampFloor.AddHours(5);
                state = QueryStoreBackfillState.AddHole(state, clampFloor, clampFloor.AddHours(1));
            }

            Assert.InRange(sliceCount, 1, 5000);
        }

        Assert.Equal(8, clamps);
        AssertNoRangeDugTwice(dug);
    }

    [Fact]
    public void AClampHoleThatOverlapsThePendingHole_StillMergesIntoOne()
    {
        var pending = QueryStoreBackfillState.EncodeHole(Now.AddDays(-5), Now.AddDays(-1));
        var merged = QueryStoreBackfillState.AddHole(pending, Now.AddDays(-2), Now.AddHours(-1));

        Assert.Equal(QueryStoreBackfillState.EncodeHole(Now.AddDays(-5), Now.AddHours(-1)), merged);
    }

    [Fact]
    public void ARangeDugWhileAClampHoleWasRecorded_LeavesTheClampedPartInPlace()
    {
        /* A slice is running over the top 1 h of a hole when the live collector records a hole that overlaps it and reaches
           higher. The slice's own save must take out only what it dug. */
        var holeTo = Now.AddHours(-6);
        var running = QueryStoreBackfillState.EncodeHole(FloorLimit, holeTo);
        var sliceFloor = holeTo.AddHours(-1);

        var afterClamp = QueryStoreBackfillState.AddHole(running, holeTo.AddHours(-0.5), Now.AddHours(-1));
        Assert.Equal(QueryStoreBackfillState.EncodeHole(FloorLimit, Now.AddHours(-1)), afterClamp);

        var afterSlice = QueryStoreBackfillState.RemoveDugRange(afterClamp, sliceFloor, holeTo, FloorLimit);

        Assert.True(QueryStoreBackfillState.TryDecodeHoles(afterSlice, out var holes));
        Assert.Equal(new[] { (holeTo, Now.AddHours(-1)), (FloorLimit, sliceFloor) }, holes);
    }

    [Fact]
    public void ARangeDugDownToTheFloor_LeavesNothingAndNoSliverBelowTheHorizon()
    {
        var hole = QueryStoreBackfillState.EncodeHole(FloorLimit.AddDays(-3), FloorLimit.AddHours(1));

        Assert.Null(QueryStoreBackfillState.RemoveDugRange(hole, FloorLimit, FloorLimit.AddHours(1), FloorLimit));
    }

    [Fact]
    public void ASingleHole_SavesExactlyWhatTheDigAlwaysSaved_AndAnOlderBuildStillReadsIt()
    {
        var hole = QueryStoreBackfillState.EncodeHole(FloorLimit, Now);
        var saved = QueryStoreBackfillState.RemoveDugRange(hole, Now.AddHours(-1), Now, FloorLimit);

        Assert.Equal(QueryStoreBackfillState.EncodeHole(FloorLimit, Now.AddHours(-1)), saved);
        Assert.True(QueryStoreBackfillState.TryDecodeHole(saved!, out var from, out var to));
        Assert.Equal((FloorLimit, Now.AddHours(-1)), (from, to));
    }

    [Fact]
    public void AValueWithTwoHoles_IsMalformedToTheSingleHoleDecoderAnOlderBuildHas()
    {
        var two = QueryStoreBackfillState.AddHole(
            QueryStoreBackfillState.EncodeHole(FloorLimit, Now.AddDays(-10)), Now.AddHours(-3), Now.AddHours(-1));

        /* An older build reads this as "no hole recorded", the conservative direction: it loses the backfill
           opportunity and never writes wrong data. */
        Assert.False(QueryStoreBackfillState.TryDecodeHole(two, out _, out _));
        Assert.True(QueryStoreBackfillState.TryDecodeHoles(two, out var holes));
        Assert.Equal(2, holes.Count);
        Assert.Equal(Now.AddHours(-1), holes[0].ToUtc);
    }
}
