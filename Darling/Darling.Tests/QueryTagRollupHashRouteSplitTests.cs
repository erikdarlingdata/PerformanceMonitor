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
/// #6800 part 2, step 2: how <see cref="QueryTagRollupHashRoute.Split"/> divides a server's span between the table and the wide table at
/// the hour edges. The edges are where a double count or a hole would come from: the partial first hour, a window end on an hour start,
/// a stale pair in the middle, a window inside one hour.
/// </summary>
public sealed class QueryTagRollupHashRouteSplitTests
{
    private static readonly DateTime Day = new(2026, 10, 20, 0, 0, 0, DateTimeKind.Unspecified);

    private static DateTime T(int hour, int minute = 0) => Day.AddHours(hour).AddMinutes(minute);

    private static QueryTagRollupHashRoute.Plan Split(DateTime start, DateTime end, IEnumerable<int> builtHours, params (int Hour, int Server)[] stale) =>
        QueryTagRollupHashRoute.Split(
            new Dictionary<int, DateTime> { [1] = start }, end, builtHours.Select(h => T(h)).ToHashSet(), stale.Select(s => (T(s.Hour), s.Server)).ToHashSet());

    private static string Ranges(QueryTagRollupHashRoute.Plan plan) =>
        string.Join(" ", plan.WideRanges.Select(r => $"[{(r.From - Day).TotalMinutes / 60:0.##},{(r.To - Day).TotalMinutes / 60:0.##})"));

    private static string Hours(QueryTagRollupHashRoute.Plan plan) => string.Join(",", plan.TablePairs.Select(p => (int)(p.Hour - Day).TotalHours));

    [Fact]
    public void AnUnalignedStart_KeepsThePartialFirstHourAndTheOpenTailOnTheWideTable()
    {
        var plan = Split(T(1, 17), T(12, 20), Enumerable.Range(0, 10));
        Assert.Equal("2,3,4,5,6,7,8,9", Hours(plan));
        Assert.Equal("[1.28,2) [10,13)", Ranges(plan));
    }

    [Fact]
    public void AnAlignedStart_StartsTheTableAtTheStartItself_AndAWindowEndOnAnHourStartKeepsThatHourInTheTail()
    {
        var plan = Split(T(1), T(12), Enumerable.Range(0, 12));
        Assert.Equal("1,2,3,4,5,6,7,8,9,10,11", Hours(plan));
        Assert.Equal("[12,13)", Ranges(plan));
    }

    [Fact]
    public void AStalePairAndAnUnbuiltHourInTheMiddle_BecomeWideRangesAndNeighborsMerge()
    {
        var plan = Split(T(1, 17), T(12, 20), [1, 2, 3, 4, 5, 6, 8, 9], (4, 1));
        Assert.Equal("2,3,5,6,8,9", Hours(plan));
        Assert.Equal("[1.28,2) [4,5) [7,8) [10,13)", Ranges(plan));
    }

    [Fact]
    public void AStaleFirstWholeHour_MergesWithThePartialHourBeforeIt()
    {
        var plan = Split(T(1, 17), T(5, 20), [2, 3], (2, 1));
        Assert.Equal("3", Hours(plan));
        Assert.Equal("[1.28,3) [4,6)", Ranges(plan));
    }

    [Fact]
    public void AStalePairOfAnotherServer_DoesNotMoveThisServer_AndNothingBuiltMeansNoTableHour()
    {
        var plan = Split(T(1, 17), T(5, 20), [2, 3], (2, 2));
        Assert.Equal("2,3", Hours(plan));
        var none = Split(T(1, 17), T(5, 20), []);
        Assert.Empty(none.TablePairs);
        Assert.Equal("[1.28,6)", Ranges(none));
    }

    [Fact]
    public void AWindowInsideOneHour_IsOneWideRange()
    {
        var plan = Split(T(1, 10), T(1, 50), [1]);
        Assert.Empty(plan.TablePairs);
        Assert.Equal("[1.17,2)", Ranges(plan));
    }

    [Fact]
    public void EachServerKeepsItsOwnStart()
    {
        var plan = QueryTagRollupHashRoute.Split(
            new Dictionary<int, DateTime> { [1] = T(1, 17), [2] = T(3) }, T(6, 5), Enumerable.Range(0, 7).Select(h => T(h)).ToHashSet(), new HashSet<(DateTime, int)>());
        Assert.Equal("2,3,4,5,3,4,5", Hours(plan));
        Assert.Equal("[1.28,2) [6,7) [6,7)", Ranges(plan));
        Assert.Equal(new[] { 1, 1, 2 }, plan.WideRanges.Select(r => r.ServerId).ToArray());
    }
}
