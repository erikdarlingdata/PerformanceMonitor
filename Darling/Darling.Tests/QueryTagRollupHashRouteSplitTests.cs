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
        var withDay = 0;
        var movedStarts = 0;
        for (var c = 0; c < Cases; c++)
        {
            /* One case in three is a multi-day window (up to 6 days back), the others the 36 hours of step 3a. */
            var longWindow = random.Next(0, 3) == 0;
            var maxBackHours = longWindow ? 6 * 24 : 36;
            var windowEnd = Edge(random, 30 + random.Next(0, 40) + (longWindow ? 6 * 24 : 0), random.Next(0, 3) == 0);
            var starts = new Dictionary<int, DateTime>();
            var servers = random.Next(1, 7);
            for (var s = 1; s <= servers; s++)
            {
                /* Spans from under an hour to over a day (or six), the ones inside the last hour included. */
                var back = random.Next(0, 4) == 0 ? TimeSpan.FromMinutes(random.Next(0, 120)) : TimeSpan.FromMinutes(random.Next(0, maxBackHours * 60));
                var start = windowEnd - back;
                starts[s] = random.Next(0, 3) == 0 ? new DateTime(start.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour) : start;
            }

            var built = new HashSet<DateTime>();
            var density = random.NextDouble();
            for (var h = windowEnd.AddHours(-(maxBackHours + 4)); h <= windowEnd.AddHours(1); h = h.AddHours(1))
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

            /* Valid days, as the ledger rule makes them: all 24 hours built and the server's pairs of the day not stale. Days that cut the
               server's range, or the window end, are offered too: Split must leave those alone. */
            var validDays = new HashSet<(DateTime, int)>();
            for (var day = new DateTime(windowEnd.Date.Ticks).AddDays(-(maxBackHours / 24) - 2); day <= windowEnd.Date; day = day.AddDays(1))
            {
                for (var s = 1; s <= servers; s++)
                {
                    if (random.Next(0, 5) < 3)
                    {
                        validDays.Add((day, s));
                        for (var h = 0; h < 24; h++)
                        {
                            built.Add(day.AddHours(h));
                            stale.Remove((day.AddHours(h), s));
                        }
                    }
                }
            }

            var plan = QueryTagRollupHashRoute.Split(starts, windowEnd, built, stale, validDays);
            var wholeEnd = new DateTime(windowEnd.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour);
            foreach (var (server, requested) in starts)
            {
                var snapped = plan.Starts[server];
                Assert.True(snapped >= requested && snapped - requested < TimeSpan.FromHours(1) && snapped.Ticks % TimeSpan.TicksPerHour == 0, $"case {c}: the snapped start rounds up to the hour");
                movedStarts += snapped != requested ? 1 : 0;
                var tables = plan.TablePairs.Where(p => p.ServerId == server).Select(p => p.Hour).ToList();
                var wides = plan.WideRanges.Where(r => r.ServerId == server).ToList();
                var days = plan.DayPairs.Where(p => p.ServerId == server).Select(p => p.Day).ToList();
                withTable += tables.Count > 0 ? 1 : 0;
                withWide += wides.Count > 0 ? 1 : 0;
                withDay += days.Count > 0 ? 1 : 0;
                Assert.All(tables, h => Assert.True(built.Contains(h) && !stale.Contains((h, server)), $"case {c}: a table hour is built and not stale"));
                Assert.All(days, d => Assert.True(validDays.Contains((d, server)) && d >= snapped && d.AddDays(1) <= wholeEnd, $"case {c}: a day is valid and whole inside the server's range"));

                /* Every valid whole day inside the server's range is taken (the day first), none other is. */
                var expectedDays = validDays.Where(v => v.Item2 == server && v.Item1 >= snapped && v.Item1.AddDays(1) <= wholeEnd).Select(v => v.Item1).OrderBy(d => d).ToList();
                Assert.True(expectedDays.SequenceEqual(days.OrderBy(d => d)), $"case {c} server {server}: the days taken are the valid whole days inside the range");

                var edges = new SortedSet<DateTime> { snapped, windowEnd, requested };
                for (var h = snapped.AddHours(-2); h <= windowEnd.AddHours(2); h = h.AddHours(1))
                {
                    edges.Add(h);
                }

                foreach (var d in days)
                {
                    edges.Add(d);
                    edges.Add(d.AddDays(1));
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
                        + days.Count(d => d <= t && t < d.AddDays(1))
                        + wides.Count(r => r.From <= t && t < r.To && t <= windowEnd);
                    var expected = t >= snapped && t <= windowEnd ? 1 : 0;
                    Assert.True(sources == expected, $"case {c} server {server}: instant {t:O} (start {requested:O}, snapped {snapped:O}, end {windowEnd:O}) is in {sources} sources, expected {expected}");
                }
            }
        }

        /* The generator reaches the shapes the property is about: table hours, wide ranges, days and starts that move. */
        Assert.True(withTable > Cases && withWide > Cases && movedStarts > Cases && withDay > Cases / 2, $"{withTable}/{withWide}/{withDay}/{movedStarts}");
    }

    private static QueryTagRollupHashRoute.Plan SplitWithDays(
        DateTime start, DateTime end, IEnumerable<int> builtHours, IEnumerable<(int Day, int Server)> validDays, params (int Hour, int Server)[] stale) =>
        QueryTagRollupHashRoute.Split(
            new Dictionary<int, DateTime> { [1] = start, [2] = start }, end, builtHours.Select(h => T(h)).ToHashSet(),
            stale.Select(s => (T(s.Hour), s.Server)).ToHashSet(), validDays.Select(d => (T(d.Day * 24), d.Server)).ToHashSet());

    [Fact]
    public void AValidDay_IsTakenFirst_TheHoursAroundItStayTableHoursAndWideRanges_AndADayThatCutsTheRangeIsNot()
    {
        /* Start 21:17 on day 0 (snapped 22:00), end 10:20 on day 3 (whole hour end 10:00). Day 1 and day 2 are whole inside it, day 0 and day 3 are not. */
        var built = Enumerable.Range(0, 24 * 3 + 10);
        var plan = SplitWithDays(T(21, 17), T(3 * 24 + 10, 20), built, [(0, 1), (1, 1), (2, 1), (3, 1)]);

        Assert.Equal("1/1,2/1", string.Join(",", plan.DayPairs.Where(p => p.ServerId == 1).Select(p => $"{(int)(p.Day - Day).TotalDays}/{p.ServerId}")));
        Assert.True(plan.UsesDays);
        /* Server 1: hours 22 and 23 of day 0 and hours 0 to 9 of day 3 from the table, none inside the days, the tail [10, 11) wide. Server 2 has no valid day: every hour is a table hour. */
        Assert.Equal(
            "22,23,72,73,74,75,76,77,78,79,80,81",
            string.Join(",", plan.TablePairs.Where(p => p.ServerId == 1).Select(p => (int)(p.Hour - Day).TotalHours)));
        Assert.Equal(60, plan.TablePairs.Count(p => p.ServerId == 2));
        Assert.Equal(new[] { T(3 * 24 + 10) }, plan.WideRanges.Where(r => r.ServerId == 1).Select(r => r.From));
    }

    [Fact]
    public void ADayInvalidForOneServer_IsReadFromHoursAndWideForThatServerOnly()
    {
        /* Day 1 is valid for server 2 only. Server 1's day 1 has two stale hours, 30 and 40, so server 1 reads its hours and wide ranges there. */
        var plan = SplitWithDays(T(0), T(3 * 24, 20), Enumerable.Range(0, 24 * 3), [(1, 2)], (30, 1), (40, 1));

        Assert.Equal("2", string.Join(",", plan.DayPairs.Select(p => p.ServerId)));
        var server1Hours = plan.TablePairs.Where(p => p.ServerId == 1).Select(p => (int)(p.Hour - Day).TotalHours).ToList();
        Assert.Contains(24, server1Hours);
        Assert.Contains(39, server1Hours);
        Assert.DoesNotContain(30, server1Hours);
        Assert.DoesNotContain(40, server1Hours);
        Assert.Equal(new[] { T(30), T(40) }, plan.WideRanges.Where(r => r.ServerId == 1).Select(r => r.From).Take(2));
        Assert.DoesNotContain(plan.TablePairs, p => p.ServerId == 2 && p.Hour >= T(24) && p.Hour < T(48));
    }

    [Fact]
    public void ADayAloneIsEnoughToRoute_AndADayRightAtTheEdgesIsTaken()
    {
        /* A window of exactly one day, aligned both ends, no hour built: the day alone answers it, and only the instant at the end stays wide. */
        var plan = SplitWithDays(T(24), T(48), [], [(1, 1), (1, 2)]);
        Assert.Empty(plan.TablePairs);
        Assert.Equal(2, plan.DayPairs.Count);
        Assert.Equal(new[] { T(48), T(48) }, plan.WideRanges.Select(r => r.From));
    }

    private static DateTime Edge(Random random, int hoursIn, bool aligned)
    {
        var edge = Day.AddHours(hoursIn);
        return aligned ? edge : edge.AddSeconds(random.Next(1, 3600)).AddTicks(random.Next(0, 10_000_000));
    }

    private static ISet<DateTime> Backlog(params int[] hours) => hours.Select(h => T(h)).ToHashSet();

    /// <summary>#6865: the read starts at the end of the newest backlog hour. The backlog hours (4) and the whole hours the read leaves out (8, from the first whole hour to the cut) are two numbers.</summary>
    [Fact]
    public void SpanPastBacklog_StartsAfterTheNewestBacklogHour_AndCountsBothTheBacklogHoursAndTheHoursLeftOut()
    {
        var (starts, waiting, leftOut) = QueryTagRollupHashRoute.SpanPastBacklog(new Dictionary<int, DateTime> { [1] = T(1, 17) }, T(1, 17), T(12, 20), Backlog(2, 3, 5, 9));
        Assert.Equal(T(10), starts[1]);
        Assert.Equal(4, waiting);
        Assert.Equal(8, leftOut);

        /* The span that results: table hours after the cut, the open tail wide, nothing older read. */
        var plan = Split(starts[1], T(12, 20), Enumerable.Range(0, 12));
        Assert.Equal("10,11", Hours(plan));
        Assert.Equal("[12,13)", Ranges(plan));
    }

    [Fact]
    public void SpanPastBacklog_IgnoresBacklogHoursOutsideTheWindow_AndWithNoneInItLeavesTheStartsAlone()
    {
        var starts = new Dictionary<int, DateTime> { [1] = T(4), [2] = T(6, 30) };
        var (moved, waiting, leftOut) = QueryTagRollupHashRoute.SpanPastBacklog(starts, T(4), T(12, 20), Backlog(1, 2, 12, 13));
        Assert.Empty(moved);
        Assert.Equal(0, waiting);
        Assert.Equal(0, leftOut);
        var (none, noneWaiting, noneLeftOut) = QueryTagRollupHashRoute.SpanPastBacklog(starts, T(4), T(12, 20), Backlog());
        Assert.Empty(none);
        Assert.Equal(0, noneWaiting);
        Assert.Equal(0, noneLeftOut);
    }

    [Fact]
    public void SpanPastBacklog_MovesEveryServerToTheCut_ButKeepsALaterStart()
    {
        var starts = new Dictionary<int, DateTime> { [1] = T(2), [2] = T(9, 30), [3] = T(7) };
        var (moved, waiting, leftOut) = QueryTagRollupHashRoute.SpanPastBacklog(starts, T(2), T(12, 20), Backlog(3, 7));
        Assert.Equal(T(8), moved[1]);
        Assert.Equal(T(9, 30), moved[2]);
        Assert.Equal(T(8), moved[3]);
        Assert.Equal(2, waiting);
        /* Server 2's own start (10, snapped) is past the cut (8), so the figures start at 10 and the count runs from hour 2 to hour 10. */
        Assert.Equal(8, leftOut);
    }

    /// <summary>
    /// #6865: the hours left out are counted to the instant every surface names as the start of the figures, the latest server's start. A server
    /// added later (start 9) is already past the cut (6), so the count is 7 (hour 2 to hour 9), not the 4 hours to the cut.
    /// </summary>
    [Fact]
    public void SpanPastBacklog_CountsTheHoursLeftOutToTheLatestStart_WhenAServerStartsAfterTheCut()
    {
        var starts = new Dictionary<int, DateTime> { [1] = T(2), [2] = T(2), [3] = T(9) };
        var (moved, waiting, leftOut) = QueryTagRollupHashRoute.SpanPastBacklog(starts, T(2), T(12, 20), Backlog(5));
        Assert.Equal(T(6), moved[1]);
        Assert.Equal(T(6), moved[2]);
        Assert.Equal(T(9), moved[3]);
        Assert.Equal(1, waiting);
        Assert.Equal(7, leftOut);
        Assert.Equal(T(9), moved.Values.Select(QueryTagRollupHashRoute.CeilHour).Max());
    }

    /// <summary>
    /// #6865: the hours left out are counted from the window's own first whole hour, the instant the sentence calls "the window", not from the earliest
    /// start the gate allows. Here the interval table's floor already cut every start to hour 4 of a window that began at hour 0, and the one backlog
    /// hour (5) moves the figures to hour 6. The read leaves out 6 hours, 4 of them before the table's floor, and 1 of the 6 is backlog.
    /// </summary>
    [Fact]
    public void SpanPastBacklog_CountsTheHoursLeftOutFromTheWindowsStart_WhenTheTablesFloorAlreadyCutTheStarts()
    {
        var starts = new Dictionary<int, DateTime> { [1] = T(4), [2] = T(4) };
        var (moved, waiting, leftOut) = QueryTagRollupHashRoute.SpanPastBacklog(starts, T(0), T(12, 20), Backlog(5));
        Assert.Equal(T(6), moved[1]);
        Assert.Equal(1, waiting);
        Assert.Equal(6, leftOut);
    }
}
