/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3678: the compression band assigns its three measured-heaviest hypertables by WEIGHT and spreads them
/// across the band, and the band itself does not move.
///
/// <para><b>What was wrong, in one sentence.</b> Every hypertable's minute came from
/// <c>CompressionPhaseMinutes[index % Count]</c> over <see cref="TimescaleSupport.CompressionPhaseOrder"/> —
/// its position in the collector catalog, which is a fact about the order collectors were written in.
/// <c>query_stats</c>, <c>query_snapshots</c> and <c>query_store_stats</c> sit at catalog indices 28, 29 and
/// 30, so the three longest compression runs this product has started on three CONSECUTIVE minutes and
/// convoyed at the daily chunk close.</para>
///
/// <para><b>Two separate claims are pinned here, and they are separate on purpose.</b> The first is that the
/// SPREAD exists: the heaviest set is evenly spaced and every hypertable still gets exactly one minute at no
/// more than <see cref="TimescaleSupport.CompressionPhaseMaxPerMinute"/> per minute. The second is that the
/// GRID did not move to buy it — the four constants the hour is tiled from hold the values dev shipped, so a
/// permutation cannot quietly become a re-derivation. A change that spread the heaviest three by widening
/// the band would satisfy the first and fail the second, which is the failure this file exists to
/// catch.</para>
/// </summary>
public sealed class CompressionPhaseAssignmentTests
{
    /// <summary>
    /// The trio's BEFORE and AFTER minutes, stated literally. BEFORE is what <c>index % Count</c> produced at
    /// today's catalog and band — three consecutive minutes — and it is computed here from the old rule
    /// rather than quoted, so the claim "they were consecutive" is derived from the same inputs the fix saw.
    /// </summary>
    [Fact]
    public void TheMeasuredHeaviestThree_WereConsecutive_AndAreNowEvenlySpread()
    {
        var minutes = TimescaleSupport.CompressionPhaseMinutes;
        var order = TimescaleSupport.CompressionPhaseOrder;

        /* BEFORE: the replaced rule, re-run. Indices 28/29/30 of the catalog, so slots 4/5/6 of a 24-minute
           band -> :40, :41, :42. Asserted CONSECUTIVE rather than asserted equal to three literals, because
           the consecutiveness is the defect and the literals are only where it happened to land. */
        var before = TimescaleSupport.HeaviestCompressionTables
            .Select(table =>
            {
                var index = order.ToList().IndexOf(table);
                Assert.True(index >= 0, $"{table} is not on the compression phase order");
                return minutes[index % minutes.Count];
            })
            .OrderBy(minute => minute)
            .ToArray();

        Assert.Equal(new[] { 40, 41, 42 }, before);
        Assert.Equal(new[] { minutes[4], minutes[5], minutes[6] }, before);
        for (var index = 1; index < before.Length; index++)
        {
            Assert.Equal(before[index - 1] + 1, before[index]);
        }

        /* AFTER: slots 0, 8 and 16 of the same band, in the list's own order — longest run first, which is
           the band's widest clearance first. */
        Assert.Equal(3, TimescaleSupport.HeaviestCompressionTables.Count);
        var after = TimescaleSupport.HeaviestCompressionTables
            .Select(table =>
            {
                Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var minute), table);
                return minute;
            })
            .ToArray();

        Assert.Equal(new[] { minutes[0], minutes[8], minutes[16] }, after);
        Assert.Equal(new[] { 36, 44, 52 }, after);
    }

    /// <summary>
    /// The spacing is the property, stated as the inequality the fix is for rather than as three literals:
    /// every PAIR is at least a whole share of the band apart. At three members over 24 minutes that is 8,
    /// and the previous grid's spacing was 1.
    /// </summary>
    [Fact]
    public void EveryPairOfTheHeaviestSet_IsAtLeastOneBandShareApart()
    {
        var minutes = TimescaleSupport.CompressionPhaseMinutes;
        var share = minutes.Count / TimescaleSupport.HeaviestCompressionTables.Count;

        Assert.True(share > 1, $"a band share of {share} minute(s) cannot separate anything");

        var assigned = TimescaleSupport.HeaviestCompressionTables
            .Select(table =>
            {
                Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var minute), table);
                return minute;
            })
            .ToArray();

        for (var left = 0; left < assigned.Length; left++)
        {
            for (var right = left + 1; right < assigned.Length; right++)
            {
                var apart = Math.Abs(assigned[left] - assigned[right]);

                Assert.True(
                    apart >= share,
                    $"{TimescaleSupport.HeaviestCompressionTables[left]} at "
                    + $":{assigned[left].ToString("00", CultureInfo.InvariantCulture)} and "
                    + $"{TimescaleSupport.HeaviestCompressionTables[right]} at "
                    + $":{assigned[right].ToString("00", CultureInfo.InvariantCulture)} are {apart} minute(s) "
                    + $"apart, inside the {share}-minute share of the band each member is given — the three "
                    + "longest compression runs this product has can convoy again (#3678)");
            }
        }

        /* Longest run first is the LOAD-BEARING half of the order: clearance falls across the band, so the
           first member must hold the widest clearance of the three. */
        var clearances = assigned.Select(TimescaleSupport.CompressionMinuteClearanceSeconds).ToArray();
        for (var index = 1; index < clearances.Length; index++)
        {
            Assert.True(
                clearances[index] < clearances[index - 1],
                $"clearance did not fall from {clearances[index - 1]}s to {clearances[index]}s across the "
                + "heaviest set, so the list is no longer ordered longest-run-first and the largest run is "
                + "not on the widest minute (#3678)");
        }
    }

    /// <summary>
    /// The map is TOTAL and the ceiling still holds: every hypertable this product owns gets exactly one
    /// minute, every minute of the band is used, and no minute carries more than
    /// <see cref="TimescaleSupport.CompressionPhaseMaxPerMinute"/> policies — which is the count of
    /// simultaneous chunk rewrites at the daily boundary and the quantity the band's width is derived from.
    /// </summary>
    [Fact]
    public void EveryHypertableGetsExactlyOneMinute_AndNoMinuteIsOverSubscribed()
    {
        var byMinute = new Dictionary<int, List<string>>();

        foreach (var table in TimescaleSupport.CompressionPhaseOrder)
        {
            Assert.True(
                TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var minute),
                $"{table} carries a compression policy but got no minute from the slot map");
            Assert.Contains(minute, TimescaleSupport.CompressionPhaseMinutes);

            if (!byMinute.TryGetValue(minute, out var tables))
            {
                byMinute[minute] = tables = new List<string>();
            }

            tables.Add(table);
        }

        /* Exactly one: the totals have to agree, which is what rules out a table counted twice or dropped. */
        Assert.Equal(TimescaleSupport.CompressionPhaseOrder.Count, byMinute.Values.Sum(v => v.Count));
        Assert.Equal(TimescaleSupport.HypertableCount, byMinute.Values.Sum(v => v.Count));
        Assert.Equal(CollectorCatalog.All.Count + 1, byMinute.Values.Sum(v => v.Count));

        /* Every minute of the band is occupied — the band is sized to hold the whole catalog at the ceiling,
           so an unused minute means the fill stopped spreading. */
        Assert.Equal(TimescaleSupport.CompressionPhaseMinutes.Count, byMinute.Count);

        var crowded = byMinute
            .Where(kv => kv.Value.Count > TimescaleSupport.CompressionPhaseMaxPerMinute)
            .ToArray();

        Assert.True(
            crowded.Length == 0,
            "the slot map put more than the per-minute ceiling on a minute, which is that many simultaneous "
            + "chunk rewrites at the daily boundary: "
            + string.Join("; ", crowded.Select(kv =>
                $":{kv.Key.ToString("00", CultureInfo.InvariantCulture)}={kv.Value.Count} "
                + $"({string.Join(", ", kv.Value)})")));

        /* And the assignment is a FUNCTION: asking twice gives the same answer, qualified or bare. */
        foreach (var table in TimescaleSupport.CompressionPhaseOrder)
        {
            Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var first));
            Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var again));
            Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor("collect." + table, out var qualified));
            Assert.Equal(first, again);
            Assert.Equal(first, qualified);
        }

        /* A hypertable this product does not own is still FOREIGN — unphased, never given a minute this code
           has no basis for choosing. */
        Assert.False(TimescaleSupport.TryCompressionPhaseMinutesFor("someone_elses_hypertable", out _));
        Assert.False(TimescaleSupport.TryCompressionPhaseMinutesFor("", out _));
    }

    /// <summary>
    /// Every member of <see cref="TimescaleSupport.HeaviestCompressionTables"/> exists on the phase order —
    /// the misspelling guard, as a PIN rather than as a throwing static initializer.
    ///
    /// <para><b>Why a test and not a throw.</b> A static initializer on <c>TimescaleSupport</c> that throws
    /// takes the whole type down with a <c>TypeInitializationException</c> for the life of the process: every
    /// retention horizon, every compression statement, every refresh policy. That file's
    /// <c>LightBandHoldsUnboundedRefreshCount</c> doc records the measurement — a throwing map took 72 tests
    /// red across four unrelated files — and decides the house rule this follows: the map degrades to the
    /// ordinary fill and the report is separate. This is the report.</para>
    /// </summary>
    [Fact]
    public void EveryHeaviestSetMember_IsAHypertableWeActuallyOwn()
    {
        Assert.NotEmpty(TimescaleSupport.HeaviestCompressionTables);

        foreach (var table in TimescaleSupport.HeaviestCompressionTables)
        {
            Assert.Contains(table, TimescaleSupport.CompressionPhaseOrder);
            Assert.True(
                TimescaleSupport.TryCompressionPhaseMinutesFor(table, out _),
                $"{table} is named as a measured-heaviest hypertable but is not on the compression phase "
                + "order, so its measured placement silently degraded to the ordinary registry fill — fix the "
                + "name, or remove it with its reading (#3678)");
        }

        Assert.Equal(
            TimescaleSupport.HeaviestCompressionTables.Count,
            TimescaleSupport.HeaviestCompressionTables.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// THE CONTROL: the grid does not move. #3678 is a PERMUTATION of which hypertable holds which minute,
    /// so every constant the hour is tiled from holds the value it shipped with.
    ///
    /// <para>Stated as literals on purpose, with the derivations beside them. A spread bought by widening the
    /// compression band would spend it out of the heaviest refresh's window — the trade
    /// <c>CompressionPhaseMinutes</c>' doc declines — and would pass every other test in this file.</para>
    /// </summary>
    [Fact]
    public void TheGridDoesNotMove_TheAssignmentIsAPermutationOfTheSameMinutes()
    {
        Assert.Equal(24, TimescaleSupport.CompressionPhaseBandMinutes);
        Assert.Equal(24, TimescaleSupport.CompressionPhaseMinutes.Count);
        Assert.Equal(18, TimescaleSupport.HeaviestRefreshWindowMinutes);
        Assert.Equal(35, TimescaleSupport.AggregateCompressionBandMinute);
        Assert.Equal(3, TimescaleSupport.CompressionPhaseMaxPerMinute);

        Assert.Equal(
            TimescaleSupport.CompressionPhaseBandMinutes,
            TimescaleSupport.CompressionPhaseMinutes.Count);
        Assert.Equal(
            new[] { 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59 },
            TimescaleSupport.CompressionPhaseMinutes.ToArray());

        /* The assignment's IMAGE is the band and nothing but the band, which is the permutation claim stated
           over the whole registry rather than over the three that moved. */
        var used = TimescaleSupport.CompressionPhaseOrder
            .Select(table =>
            {
                Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var minute), table);
                return minute;
            })
            .Distinct()
            .OrderBy(minute => minute)
            .ToArray();

        Assert.Equal(TimescaleSupport.CompressionPhaseMinutes.ToArray(), used);
    }
}
