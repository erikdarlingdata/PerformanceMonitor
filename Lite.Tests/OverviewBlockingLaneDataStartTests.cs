/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// What the Overview's blocking chart names as its data start (#4966): the later of its two series' starts, each the earlier of the
/// series' coverage floor and its earliest bar drawn. Pure logic, no store and no WPF object.
/// </summary>
public sealed class OverviewBlockingLaneDataStartTests
{
    private static readonly DateTime Day = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Unspecified);

    private static Task<DateTime?> Answer(DateTime? start) => Task.FromResult(start);

    private static readonly TrendPoint[] NoBars = [];

    private static TrendPoint Bar(DateTime at, int count = 1) => new() { Time = at, Count = count };

    [Fact]
    public async Task TwoStarts_NameTheLaterOne_WhicheverSeriesHasIt()
    {
        Assert.Equal(Day.AddDays(2), await LiteBlockingLaneDataStart.ChooseAsync(Answer(Day.AddDays(2)), Answer(Day), NoBars, NoBars));
        Assert.Equal(Day.AddDays(2), await LiteBlockingLaneDataStart.ChooseAsync(Answer(Day), Answer(Day.AddDays(2)), NoBars, NoBars));
    }

    [Fact]
    public async Task OneSeriesOnly_IsNamed_AndNeitherNamesNothing()
    {
        Assert.Equal(Day.AddDays(1), await LiteBlockingLaneDataStart.ChooseAsync(Answer(Day.AddDays(1)), Answer(null), NoBars, NoBars));
        Assert.Equal(Day.AddDays(1), await LiteBlockingLaneDataStart.ChooseAsync(Answer(null), Answer(Day.AddDays(1)), NoBars, NoBars));
        Assert.Null(await LiteBlockingLaneDataStart.ChooseAsync(Answer(null), Answer(null), NoBars, NoBars));
    }

    [Fact]
    public async Task ABarBeforeTheFloor_MovesThatSeriesStartBack_AndNoFloorLetsTheBarName()
    {
        Assert.Equal(Day.AddDays(2), await LiteBlockingLaneDataStart.ChooseAsync(
            Answer(Day.AddDays(2)), Answer(Day.AddDays(3)), NoBars, [Bar(Day.AddDays(1)), Bar(Day.AddDays(4))]));
        Assert.Equal(Day.AddDays(1), await LiteBlockingLaneDataStart.ChooseAsync(
            Answer(null), Answer(Day.AddDays(3)), [Bar(Day.AddDays(1))], [Bar(Day.AddDays(1))]));
        Assert.Equal(Day.AddHours(5), await LiteBlockingLaneDataStart.ChooseAsync(Answer(null), Answer(null), [Bar(Day.AddHours(5)), Bar(Day.AddHours(9))], NoBars));
    }

    [Fact]
    public async Task AZeroBucket_IsNotABar()
    {
        Assert.Equal(Day.AddDays(2), await LiteBlockingLaneDataStart.ChooseAsync(
            Answer(Day.AddDays(2)), Answer(Day.AddDays(2)), [Bar(Day, 0), Bar(Day.AddDays(1), 0)], NoBars));
        Assert.Null(await LiteBlockingLaneDataStart.ChooseAsync(Answer(null), Answer(null), [Bar(Day, 0)], [Bar(Day, 0)]));
        Assert.Equal(Day.AddHours(7), LiteBlockingLaneDataStart.SeriesStart(null, [Bar(Day, 0), Bar(Day.AddHours(7)), Bar(Day.AddHours(9))]));
    }

    [Fact]
    public async Task AProbeThatThrows_DropsItsSeries_AndNeverThrowsOut()
    {
        var failed = Task.FromException<DateTime?>(new InvalidOperationException("the store went away"));
        Assert.Equal(Day.AddDays(2), await LiteBlockingLaneDataStart.ChooseAsync(failed, Answer(Day.AddDays(2)), NoBars, NoBars));
        Assert.Null(await LiteBlockingLaneDataStart.ChooseAsync(failed, Task.FromException<DateTime?>(new TimeoutException()), [Bar(Day)], [Bar(Day)]));
        Assert.Null(await LiteBlockingLaneDataStart.ChooseAsync(failed, Task.FromException<DateTime?>(new TimeoutException()), NoBars, NoBars));
    }

    /// <summary>A failed blocking probe gives no start even with a late bar; the deadlock series answers alone, so its start (the window start) is named.</summary>
    [Fact]
    public async Task AFailedProbe_WithALateBar_DoesNotRaiseTheNote()
    {
        var failed = Task.FromException<DateTime?>(new InvalidOperationException("the store went away"));
        var result = await LiteBlockingLaneDataStart.ChooseAsync(failed, Answer(Day), [Bar(Day.AddDays(5))], NoBars);
        Assert.Equal(Day, result);
    }

    /// <summary>The probing half of the step: both relations are asked over the window given, and the later floor wins.</summary>
    [Fact]
    public async Task StartAsync_AsksBothRelations_AndNamesTheLaterFloor()
    {
        var asked = new List<QueryWindowRelation>();
        var start = await LiteBlockingLaneDataStart.StartAsync(
            relation => { asked.Add(relation); return Task.FromResult<DateTime?>(relation == QueryWindowRelation.Deadlocks ? Day.AddDays(3) : Day.AddDays(1)); },
            Day, Day.AddDays(7), NoBars, NoBars);

        Assert.Equal(Day.AddDays(3), start);
        Assert.Contains(QueryWindowRelation.BlockedProcessReports, asked);
        Assert.Contains(QueryWindowRelation.Deadlocks, asked);
    }

    /// <summary>A window of 90 minutes or less calls neither probe.</summary>
    [Fact]
    public async Task StartAsync_ShortWindow_CallsNeitherProbe()
    {
        var calls = 0;
        Assert.Null(await LiteBlockingLaneDataStart.StartAsync(_ => { calls++; return Task.FromResult<DateTime?>(Day); }, Day, Day.AddMinutes(90), [Bar(Day)], [Bar(Day)]));
        Assert.Equal(0, calls);
    }

    /// <summary>A window of 90 minutes or less starts no probe, and a probe that throws drops only its series.</summary>
    [Fact]
    public async Task StartAsync_ShortWindowStartsNoProbe_AndAThrowingProbeDropsOneSeries()
    {
        var probes = 0;
        Assert.Null(await LiteBlockingLaneDataStart.StartAsync(_ => { probes++; return Task.FromResult<DateTime?>(Day); }, Day, Day.AddMinutes(90), NoBars, NoBars));
        Assert.Equal(0, probes);

        Assert.Equal(Day.AddDays(2), await LiteBlockingLaneDataStart.StartAsync(
            relation => relation == QueryWindowRelation.Deadlocks ? Task.FromResult<DateTime?>(Day.AddDays(2)) : throw new InvalidOperationException("probe failed"),
            Day, Day.AddDays(7), NoBars, NoBars));
    }
}
