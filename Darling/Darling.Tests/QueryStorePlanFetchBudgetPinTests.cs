/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5813: the plan-XML fetch's time budget, the arithmetic half. <see cref="QueryStoreReadBudget.PlanFetchWidth"/> sizes
/// each chunk from the database's last measured time per plan, and <see cref="QueryStoreReadBudget.NextMsPerPlan"/> is
/// what a pass leaves for the next one, including the halving after a cut that received nothing.
/// <see cref="QueryStoreReadBudgetLiveTests"/> drives both through the runner's real plan fetch on a fake target.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStorePlanFetchBudgetPinTests
{
    private static readonly TimeSpan Thirty = TimeSpan.FromSeconds(30);

    [Fact]
    public void TheBudgetIsThirtySeconds_AndLeavesAQuarterOfHeadroom()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), QueryStoreReadBudget.PlanFetchBudget);
        Assert.Equal(0.75, QueryStoreReadBudget.PlanFetchHeadroom);
    }

    [Theory]
    /* No measurement yet: the full statement width, as before #5813. */
    [InlineData(null, 400)]
    /* The field's slow database, 2 s a plan: floor(30000 x 0.75 / 2000) = 11 plans, not 136 cut at the budget. */
    [InlineData(2000.0, 11)]
    /* 100 ms a plan: 225. */
    [InlineData(100.0, 225)]
    /* A healthy database keeps the full width: the upper clamp. */
    [InlineData(5.0, 400)]
    [InlineData(56.0, 400)]
    /* Slower than the whole budget per plan: the lower clamp, one plan, so the database still progresses. */
    [InlineData(45000.0, 1)]
    [InlineData(1_000_000.0, 1)]
    public void TheFirstChunk_IsTheBudgetOverTheTimePerPlan_ClampedToOneAndTheStatementWidth(double? msPerPlan, int expected) =>
        Assert.Equal(expected, QueryStoreReadBudget.PlanFetchWidth(Thirty, msPerPlan, 400, firstChunk: true));

    [Fact]
    public void ALaterChunk_StopsWhenTheTimeLeftFitsNoPlan_AndNoTimeLeftIsAlwaysAStop()
    {
        /* 1 s left at 2 s a plan: a later chunk stops (0) where a first chunk would still ask for one. */
        Assert.Equal(0, QueryStoreReadBudget.PlanFetchWidth(TimeSpan.FromSeconds(1), 2000.0, 400, firstChunk: false));
        Assert.Equal(1, QueryStoreReadBudget.PlanFetchWidth(TimeSpan.FromSeconds(1), 2000.0, 400, firstChunk: true));
        Assert.Equal(7, QueryStoreReadBudget.PlanFetchWidth(TimeSpan.FromSeconds(20), 2000.0, 400, firstChunk: false));

        Assert.Equal(0, QueryStoreReadBudget.PlanFetchWidth(TimeSpan.Zero, null, 400, firstChunk: true));
        Assert.Equal(0, QueryStoreReadBudget.PlanFetchWidth(TimeSpan.FromSeconds(-1), 5.0, 400, firstChunk: true));
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(-3.0)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void AMeaninglessTimePerPlan_IsNoMeasurement(double msPerPlan) =>
        Assert.Equal(400, QueryStoreReadBudget.PlanFetchWidth(Thirty, msPerPlan, 400, firstChunk: true));

    [Fact]
    public void APassThatReceivedPlans_MeasuresItsTargetTimeOverThePlansReceived()
    {
        Assert.Equal(2000.0, QueryStoreReadBudget.NextMsPerPlan(null, 30_000, 15, cut: false, cutWidth: 15));
        /* A partly cut pass still measured: its wasted time can only narrow the next width. */
        Assert.Equal(3000.0, QueryStoreReadBudget.NextMsPerPlan(10.0, 30_000, 10, cut: true, cutWidth: 400));
    }

    [Fact]
    public void ACutThatReceivedNothing_HalvesTheNextWidth_AndNeverRepeatsIt()
    {
        /* 400 plans cut empty at 30 s: the time per plan is at least 75 ms; recording twice that makes the next width
           floor(22500 / 150) = 150, under half the cut one, so the same width is never cut empty twice. */
        var next = QueryStoreReadBudget.NextMsPerPlan(null, 30_000, 0, cut: true, cutWidth: 400);
        Assert.Equal(150.0, next);
        var width = QueryStoreReadBudget.PlanFetchWidth(Thirty, next, 400, firstChunk: true);
        Assert.Equal(150, width);
        Assert.True(width <= 400 / 2);

        /* And again from there: each empty cut at least halves the width, down to the one-plan floor. */
        for (var cutWidth = width; cutWidth > 1; cutWidth = width)
        {
            next = QueryStoreReadBudget.NextMsPerPlan(next, 30_000, 0, cut: true, cutWidth: cutWidth);
            width = QueryStoreReadBudget.PlanFetchWidth(Thirty, next, 400, firstChunk: true);
            Assert.True(width <= Math.Max(1, cutWidth / 2), $"cut {cutWidth} -> next {width}");
        }

        Assert.Equal(1, width);
    }

    [Fact]
    public void APassThatNeitherReceivedNorWasCut_KeepsThePreviousFigure()
    {
        Assert.Equal(42.0, QueryStoreReadBudget.NextMsPerPlan(42.0, 900, 0, cut: false, cutWidth: 400));
        Assert.Null(QueryStoreReadBudget.NextMsPerPlan(null, 900, 0, cut: false, cutWidth: 400));
    }
}
