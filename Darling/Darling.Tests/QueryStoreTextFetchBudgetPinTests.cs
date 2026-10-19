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
/// #6102: the statement-text fetch's time budget, the arithmetic half, the twin of
/// <see cref="QueryStorePlanFetchBudgetPinTests"/>. <see cref="QueryStoreReadBudget.TextFetchWidth"/> sizes each chunk from
/// the database's last measured time per text id, and <see cref="QueryStoreReadBudget.NextMsPerText"/> is what a pass
/// leaves for the next one, including the halving after a cut that received nothing.
/// <see cref="QueryStoreReadBudgetLiveTests"/> drives both through the runner's real text fetch on a fake target.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStoreTextFetchBudgetPinTests
{
    private static readonly TimeSpan Thirty = TimeSpan.FromSeconds(30);

    [Fact]
    public void TheTextBudget_IsItsOwn_AndLeavesAQuarterOfHeadroom()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), QueryStoreReadBudget.TextFetchBudget);
        Assert.Equal(0.75, QueryStoreReadBudget.TextFetchHeadroom);
    }

    [Theory]
    /* No measurement yet: the full statement width, as before #6102. */
    [InlineData(null, 1000)]
    /* The field's text fetch, about 45 ms an id: floor(30000 x 0.75 / 45) = 500 ids, not 580 run to 27 s. */
    [InlineData(45.0, 500)]
    /* A slow store at 1 s an id: 22 ids. */
    [InlineData(1000.0, 22)]
    /* A healthy database keeps the full width: the upper clamp. */
    [InlineData(5.0, 1000)]
    [InlineData(22.0, 1000)]
    /* Slower than the whole budget per id: the lower clamp, one id, so the database still progresses. */
    [InlineData(45000.0, 1)]
    [InlineData(1_000_000.0, 1)]
    public void TheFirstChunk_IsTheBudgetOverTheTimePerId_ClampedToOneAndTheStatementWidth(double? msPerText, int expected) =>
        Assert.Equal(expected, QueryStoreReadBudget.TextFetchWidth(Thirty, msPerText, 1000, firstChunk: true));

    [Fact]
    public void ALaterChunk_StopsWhenTheTimeLeftFitsNoId_AndNoTimeLeftIsAlwaysAStop()
    {
        Assert.Equal(0, QueryStoreReadBudget.TextFetchWidth(TimeSpan.FromSeconds(1), 2000.0, 1000, firstChunk: false));
        Assert.Equal(1, QueryStoreReadBudget.TextFetchWidth(TimeSpan.FromSeconds(1), 2000.0, 1000, firstChunk: true));
        Assert.Equal(7, QueryStoreReadBudget.TextFetchWidth(TimeSpan.FromSeconds(20), 2000.0, 1000, firstChunk: false));

        Assert.Equal(0, QueryStoreReadBudget.TextFetchWidth(TimeSpan.Zero, null, 1000, firstChunk: true));
        Assert.Equal(0, QueryStoreReadBudget.TextFetchWidth(TimeSpan.FromSeconds(-1), 5.0, 1000, firstChunk: true));
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(-3.0)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void AMeaninglessTimePerId_IsNoMeasurement(double msPerText) =>
        Assert.Equal(1000, QueryStoreReadBudget.TextFetchWidth(Thirty, msPerText, 1000, firstChunk: true));

    [Fact]
    public void APassThatReceivedTexts_MeasuresItsTargetTimeOverTheTextsReceived()
    {
        Assert.Equal(50.0, QueryStoreReadBudget.NextMsPerText(null, 25_000, 500, cut: false, cutWidth: 500));
        /* A partly cut pass still measured: its wasted time can only narrow the next width. */
        Assert.Equal(3000.0, QueryStoreReadBudget.NextMsPerText(10.0, 30_000, 10, cut: true, cutWidth: 1000));
    }

    [Fact]
    public void ACutThatReceivedNothing_HalvesTheNextWidth_AndNeverRepeatsIt()
    {
        /* 1000 ids cut empty at 30 s: at least 30 ms an id; recording twice that asks for at most half the cut width. */
        var next = QueryStoreReadBudget.NextMsPerText(null, 30_000, 0, cut: true, cutWidth: 1000);
        Assert.Equal(60.0, next);
        Assert.True(QueryStoreReadBudget.TextFetchWidth(Thirty, next, 1000, firstChunk: true) <= 1000 / 2);
    }

    [Fact]
    public void APassThatReceivedNothingAndWasNotCut_KeepsThePreviousFigure()
    {
        Assert.Equal(12.5, QueryStoreReadBudget.NextMsPerText(12.5, 4000, 0, cut: false, cutWidth: 0));
        Assert.Null(QueryStoreReadBudget.NextMsPerText(null, 4000, 0, cut: false, cutWidth: 0));
    }

    [Fact]
    public void ThePlanSideIsUnchanged_ByTheSharedArithmetic()
    {
        Assert.Equal(11, QueryStoreReadBudget.PlanFetchWidth(Thirty, 2000.0, 400, firstChunk: true));
        Assert.Equal(3000.0, QueryStoreReadBudget.NextMsPerPlan(10.0, 30_000, 10, cut: true, cutWidth: 400));
    }
}
