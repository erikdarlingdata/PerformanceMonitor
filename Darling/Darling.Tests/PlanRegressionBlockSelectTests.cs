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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Which valid blocks the PLAN_REGRESSION read takes (#9225): only a block whose every day is a whole built day the read already took, and never
/// two blocks over the same day. A block that is not taken leaves its days in the daily list, so the read still sees them.
/// </summary>
public sealed class PlanRegressionBlockSelectTests
{
    private static readonly DateOnly Start = new(2026, 11, 1);

    private static List<PlanRegressionDaily.BuiltDay> Whole(DateOnly first, int count) =>
        Enumerable.Range(0, count).Select(i => new PlanRegressionDaily.BuiltDay(first.AddDays(i), null)).ToList();

    [Fact]
    public void ABlockOfWholeBuiltDays_IsTaken_AndItsDaysLeaveTheDailyList()
    {
        var built = Whole(Start.AddDays(-2), PlanRegressionBlock.BlockDays + 4);

        var (blocks, days) = PlanRegressionBlock.Select(built, [Start]);

        Assert.Equal([Start], blocks);
        Assert.Equal(built.Count - PlanRegressionBlock.BlockDays, days.Count);
        for (var i = 0; i < PlanRegressionBlock.BlockDays; i++)
        {
            Assert.DoesNotContain(Start.AddDays(i), days);
        }

        Assert.Contains(Start.AddDays(-1), days);
        Assert.Contains(Start.AddDays(PlanRegressionBlock.BlockDays), days);
    }

    [Fact]
    public void ABlockWithADayMissingFromTheBuiltDays_IsNotTaken_AndEveryBuiltDayStaysInTheDailyList()
    {
        var built = Whole(Start, PlanRegressionBlock.BlockDays).Where(b => b.Day != Start.AddDays(2)).ToList();

        var (blocks, days) = PlanRegressionBlock.Select(built, [Start]);

        Assert.Empty(blocks);
        Assert.Equal(built.Select(b => b.Day), days);
    }

    [Fact]
    public void ABlockWithADayThatIsNotWhole_IsNotTaken_AndThatDayStaysInTheDailyList()
    {
        var built = Whole(Start, PlanRegressionBlock.BlockDays);
        var partial = Start.AddDays(3);
        built[3] = new PlanRegressionDaily.BuiltDay(partial, partial.ToDateTime(new TimeOnly(9, 0)));

        var (blocks, days) = PlanRegressionBlock.Select(built, [Start]);

        Assert.Empty(blocks);
        Assert.Contains(partial, days);
        Assert.Equal(built.Select(b => b.Day), days);
    }

    [Fact]
    public void TwoOverlappingBlocks_AreNotBothTaken_TheEarlierWinsAndTheOtherIsLeftToDailyRows()
    {
        var overlapping = Start.AddDays(2);
        var built = Whole(Start, PlanRegressionBlock.BlockDays + 2);

        var (blocks, days) = PlanRegressionBlock.Select(built, [overlapping, Start]);

        Assert.Equal([Start], blocks);
        Assert.Equal([Start.AddDays(PlanRegressionBlock.BlockDays), Start.AddDays(PlanRegressionBlock.BlockDays + 1)], days);
    }
}
