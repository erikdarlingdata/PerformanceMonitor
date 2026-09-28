/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4510: every heavy raw hypertable gets its own <c>compress_after</c> a few hours past the shared
/// <see cref="TimescaleSupport.CompressAfterDays"/> day, so the nightly compression burst spreads across
/// hours instead of landing on every heavy table's newest chunk at the same UTC midnight.
///
/// <para>Pure pins only: <see cref="TimescaleSupport.HeavyCompressAfterOffsetHours"/>,
/// <see cref="TimescaleSupport.CompressAfterFor(string)"/> and <see cref="TimescaleSupport.HeavyOffsetHoursFor(string)"/>
/// are static C# with no store round trip. The converge itself is <c>CompressAfterStaggerLiveTests</c>.</para>
/// </summary>
public sealed class CompressAfterStaggerTests
{
    /// <summary>
    /// Every heavy table's stagger keeps its <c>compress_after</c> at or above
    /// <see cref="TimescaleSupport.HourlyRefreshStartSpan"/> \u2014 the same floor
    /// <see cref="TimescaleSupport.CompressAfterDays"/> alone is pinned against, and one #4510 must not
    /// quietly cross under: the hourly aggregate refresh reads a day back and would refresh a chunk that had
    /// disappeared into a compression rewrite mid-refresh.
    /// </summary>
    [Fact]
    public void EveryHeavyTable_CompressAfterAtLeastHourlyRefreshStartSpan()
    {
        Assert.NotEmpty(TimescaleSupport.HeavyCompressAfterOffsetHours);

        foreach (var (table, extraHours) in TimescaleSupport.HeavyCompressAfterOffsetHours)
        {
            var compressAfter = TimeSpan.FromDays(TimescaleSupport.CompressAfterDays) + TimeSpan.FromHours(extraHours);

            Assert.True(
                compressAfter >= TimescaleSupport.HourlyRefreshStartSpan,
                $"{table}'s compress_after ({compressAfter}) must stay at least HourlyRefreshStartSpan ({TimescaleSupport.HourlyRefreshStartSpan}).");
        }
    }

    /// <summary>
    /// The eligibility hours {N, N+12} \u2014 which cover both a 24-hour and a 12-hour chunked table \u2014 are
    /// pairwise disjoint across every heavy table, and none of them is hour 0 (the unstaggered instant every
    /// table used to share, and the whole burst this fixes).
    /// </summary>
    [Fact]
    public void EligibilityHours_PairwiseDisjointAndExcludeHourZero()
    {
        var hours = TimescaleSupport.HeavyCompressAfterOffsetHours
            .SelectMany(pair => new[] { pair.Value, pair.Value + 12 })
            .ToArray();

        Assert.DoesNotContain(0, hours);
        Assert.DoesNotContain(12, hours);
        Assert.Equal(hours.Length, hours.Distinct().Count());
    }

    /// <summary>
    /// Every key in <see cref="TimescaleSupport.HeavyCompressAfterOffsetHours"/> is a real hypertable on
    /// <see cref="TimescaleSupport.CompressionPhaseOrder"/> \u2014 a misspelled or retired table would silently
    /// stop staggering, the same failure shape <see cref="TimescaleSupport.HeaviestCompressionTables"/>'s own
    /// membership pin exists to catch.
    /// </summary>
    [Fact]
    public void EveryMapKey_IsARealHypertableOnThePhaseOrder()
    {
        foreach (var table in TimescaleSupport.HeavyCompressAfterOffsetHours.Keys)
        {
            Assert.Contains(table, TimescaleSupport.CompressionPhaseOrder, StringComparer.Ordinal);
        }
    }

    /// <summary>
    /// The largest offset stays well inside a day \u2014 past 11 it starts to look like a second
    /// <see cref="TimescaleSupport.CompressAfterDays"/> rather than a stagger, and that is a design change
    /// this map must not drift into by simply growing.
    /// </summary>
    [Fact]
    public void MaxOffset_AtMost11Hours()
    {
        Assert.True(
            TimescaleSupport.HeavyCompressAfterOffsetHours.Values.Max() <= 11,
            "the largest HeavyCompressAfterOffsetHours entry must stay <= 11 hours");
    }

    /// <summary>
    /// <see cref="TimescaleSupport.AddCompressionPolicySql(string)"/> for <c>query_store_stats</c> renders the
    /// staggered interval literal, not the plain <see cref="TimescaleSupport.CompressAfterDays"/> literal
    /// every other test file still expects for a non-heavy table.
    /// </summary>
    [Fact]
    public void AddCompressionPolicySql_QueryStoreStats_RendersOneDayPlusOneHour()
    {
        var sql = TimescaleSupport.AddCompressionPolicySql("query_store_stats");

        Assert.Contains($"compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days 01:00:00'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A table with no entry in <see cref="TimescaleSupport.HeavyCompressAfterOffsetHours"/> keeps the plain
    /// <c>'1 days'</c> literal byte-identical to before #4510 \u2014 the change must not touch any table it was
    /// not asked to.
    /// </summary>
    [Fact]
    public void AddCompressionPolicySql_NonHeavyTable_StaysOneDay()
    {
        var sql = TimescaleSupport.AddCompressionPolicySql("wait_stats_history_placeholder_not_heavy");

        Assert.Contains($"compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("days 0", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A qualified name (<c>collect.query_stats</c>) resolves the same offset as the bare name \u2014
    /// <see cref="TimescaleSupport.AddCompressionPolicySql(string)"/>'s raw-name overload is reachable with
    /// either, the same contract <see cref="TimescaleSupport.TryCompressionPhaseMinutesFor"/> already keeps.
    /// </summary>
    [Fact]
    public void HeavyOffsetHoursFor_AcceptsQualifiedName()
    {
        Assert.Equal(
            TimescaleSupport.HeavyOffsetHoursFor("query_stats"),
            TimescaleSupport.HeavyOffsetHoursFor("collect.query_stats"));
        Assert.True(TimescaleSupport.HeavyOffsetHoursFor("collect.query_stats") > 0);
    }

    /// <summary>
    /// #4510's coordinator note: the phase MINUTES the stagger's tables already hold stay untouched \u2014 an
    /// external hourly reader depends on compression never starting before :36 of the hour. This change
    /// alters WHICH HOUR a heavy table becomes eligible, never which minute its policy starts on.
    /// </summary>
    [Fact]
    public void EveryCompressionJob_PhaseMinuteAtLeast36()
    {
        foreach (var table in TimescaleSupport.CompressionPhaseOrder)
        {
            if (TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var minute))
            {
                Assert.True(minute >= 36, $"{table}'s compression phase minute ({minute}) must stay >= 36.");
            }
        }
    }
}
