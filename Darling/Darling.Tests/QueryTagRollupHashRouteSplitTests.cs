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
/// the hour edges. The edges are where a double count or a hole would come from: the start snapped up to the hour (step 3a), a window end on
/// an hour start, a stale pair in the middle, a window inside one hour.
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
    public void AnUnalignedStart_SnapsUpToTheNextHour_AndTheOpenTailStaysOnTheWideTable()
    {
        var plan = Split(T(1, 17), T(12, 20), Enumerable.Range(0, 10));
        Assert.Equal("2,3,4,5,6,7,8,9", Hours(plan));
        Assert.Equal("[10,13)", Ranges(plan));
        Assert.Equal(T(2), plan.Starts[1]);
        Assert.Equal(T(1, 17), plan.RequestedStart);
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
        Assert.Equal("[4,5) [7,8) [10,13)", Ranges(plan));
    }

    [Fact]
    public void AStaleFirstHour_IsTheHeadOfTheWideRanges()
    {
        var plan = Split(T(1, 17), T(5, 20), [2, 3], (2, 1));
        Assert.Equal("3", Hours(plan));
        Assert.Equal("[2,3) [4,6)", Ranges(plan));
    }

    [Fact]
    public void AStalePairOfAnotherServer_DoesNotMoveThisServer_AndNothingBuiltMeansNoTableHour()
    {
        var plan = Split(T(1, 17), T(5, 20), [2, 3], (2, 2));
        Assert.Equal("2,3", Hours(plan));
        var none = Split(T(1, 17), T(5, 20), []);
        Assert.Empty(none.TablePairs);
        Assert.Equal("[2,6)", Ranges(none));
    }

    [Fact]
    public void AStartInTheWindowsLastHour_SnapsPastTheEnd_AndReadsNothing_AnAlignedWindowEndStillReadsItsInstant()
    {
        var plan = Split(T(1, 10), T(1, 50), [1]);
        Assert.Empty(plan.TablePairs);
        Assert.Empty(plan.WideRanges);
        var atEnd = Split(T(1), T(1), [0]);
        Assert.Empty(atEnd.TablePairs);
        Assert.Equal("[1,2)", Ranges(atEnd));
    }

    [Fact]
    public void EachServerKeepsItsOwnStart()
    {
        var plan = QueryTagRollupHashRoute.Split(
            new Dictionary<int, DateTime> { [1] = T(1, 17), [2] = T(3) }, T(6, 5), Enumerable.Range(0, 7).Select(h => T(h)).ToHashSet(), new HashSet<(DateTime, int)>());
        Assert.Equal("2,3,4,5,3,4,5", Hours(plan));
        Assert.Equal("[6,7) [6,7)", Ranges(plan));
        Assert.Equal(new[] { 1, 2 }, plan.WideRanges.Select(r => r.ServerId).ToArray());
    }

    /// <summary>
    /// #6800 part 2, step 3a: for random layouts (several servers, read starts and window ends on and off the hour, built hours, stale pairs),
    /// every instant of a server's <c>[snapped start, window end]</c> is in exactly one source: a table hour <c>[h, h + 1 h)</c> or a wide range
    /// <c>[from, to)</c> cut at the window end (the statement's own <c>collection_time &lt;= window end</c>). Never both (a double count), never
    /// neither (a hole), and nothing is read before the snapped start. A table hour is only a built hour of a server whose pair is not stale.
    /// The instants checked are every hour edge, every range edge and the window's own edges, each one tick before, at and after.
    /// </summary>
    [Fact]
    public void EveryInstantOfEveryServersSpan_IsInExactlyOneSource_ForSeededRandomLayouts()
    {
        const int Cases = 3000;
        var random = new Random(6800);
        var withTable = 0;
        var withWide = 0;
        var movedStarts = 0;
        for (var c = 0; c < Cases; c++)
        {
            var windowEnd = Edge(random, 30 + random.Next(0, 40), random.Next(0, 3) == 0);
            var starts = new Dictionary<int, DateTime>();
            var servers = random.Next(1, 7);
            for (var s = 1; s <= servers; s++)
            {
                /* Spans from under an hour to over a day, the ones inside the last hour included. */
                var back = random.Next(0, 4) == 0 ? TimeSpan.FromMinutes(random.Next(0, 120)) : TimeSpan.FromMinutes(random.Next(0, 36 * 60));
                var start = windowEnd - back;
                starts[s] = random.Next(0, 3) == 0 ? new DateTime(start.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour) : start;
            }

            var built = new HashSet<DateTime>();
            var density = random.NextDouble();
            for (var h = windowEnd.AddHours(-40); h <= windowEnd.AddHours(1); h = h.AddHours(1))
            {
                if (random.NextDouble() < density)
                {
                    built.Add(new DateTime(h.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour));
                }
            }

            var stale = new HashSet<(DateTime, int)>();
            foreach (var hour in built)
            {
                for (var s = 1; s <= servers; s++)
                {
                    if (random.Next(0, 8) == 0)
                    {
                        stale.Add((hour, s));
                    }
                }
            }

            var plan = QueryTagRollupHashRoute.Split(starts, windowEnd, built, stale);
            foreach (var (server, requested) in starts)
            {
                var snapped = plan.Starts[server];
                Assert.True(snapped >= requested && snapped - requested < TimeSpan.FromHours(1) && snapped.Ticks % TimeSpan.TicksPerHour == 0, $"case {c}: the snapped start rounds up to the hour");
                movedStarts += snapped != requested ? 1 : 0;
                var tables = plan.TablePairs.Where(p => p.ServerId == server).Select(p => p.Hour).ToList();
                var wides = plan.WideRanges.Where(r => r.ServerId == server).ToList();
                withTable += tables.Count > 0 ? 1 : 0;
                withWide += wides.Count > 0 ? 1 : 0;
                Assert.All(tables, h => Assert.True(built.Contains(h) && !stale.Contains((h, server)), $"case {c}: a table hour is built and not stale"));

                var edges = new SortedSet<DateTime> { snapped, windowEnd, requested };
                for (var h = snapped.AddHours(-2); h <= windowEnd.AddHours(2); h = h.AddHours(1))
                {
                    edges.Add(h);
                }

                foreach (var r in wides)
                {
                    edges.Add(r.From);
                    edges.Add(r.To);
                }

                var probes = new List<DateTime>();
                foreach (var edge in edges)
                {
                    probes.Add(edge.AddTicks(-1));
                    probes.Add(edge);
                    probes.Add(edge.AddTicks(1));
                    probes.Add(edge.AddMinutes(30));
                }

                foreach (var t in probes)
                {
                    var sources =
                        tables.Count(h => h <= t && t < h.AddHours(1))
                        + wides.Count(r => r.From <= t && t < r.To && t <= windowEnd);
                    var expected = t >= snapped && t <= windowEnd ? 1 : 0;
                    Assert.True(sources == expected, $"case {c} server {server}: instant {t:O} (start {requested:O}, snapped {snapped:O}, end {windowEnd:O}) is in {sources} sources, expected {expected}");
                }
            }
        }

        /* The generator reaches the shapes the property is about: table hours, wide ranges and starts that move. */
        Assert.True(withTable > Cases && withWide > Cases && movedStarts > Cases, $"{withTable}/{withWide}/{movedStarts}");
    }

    private static DateTime Edge(Random random, int hoursIn, bool aligned)
    {
        var edge = Day.AddHours(hoursIn);
        return aligned ? edge : edge.AddSeconds(random.Next(1, 3600)).AddTicks(random.Next(0, 10_000_000));
    }
}
