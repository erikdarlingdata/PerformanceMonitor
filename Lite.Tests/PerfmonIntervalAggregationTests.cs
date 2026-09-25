/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2234: Lite's half of the "a zero delta must be readable" contract, and the aggregate that makes it
/// so.
///
/// <para><c>cntr_value</c> and <c>delta_cntr_value</c> are additive across a counter's instance rows —
/// summing Transactions/sec over every database is a meaningful total. <c>sample_interval_seconds</c> is
/// NOT: it is one measured sweep gap repeated once per instance, so <c>SUM</c> multiplies the denominator
/// by the instance count. Measured on the production fleet, Transactions/sec, Log Flushes/sec, Log Bytes
/// Flushed/sec and Log Flush Write Time carry a median of 12 and up to 17 rows per <c>collection_time</c>
/// (the Lock* counters 15), so a summed denominator yields rates 12-17x too LOW.</para>
///
/// <para>Both Darling surfaces carry a dedicated pin for this (<c>DarlingTrendReader</c> and the Viewer's
/// <c>PerfmonTrendsSql</c>); Lite had none, which a review caught. Lite's queries are inline
/// <c>CommandText</c> rather than exposed constants, so this reads the source the way
/// <see cref="ParitySource"/> exists to — the alternative is a live multi-instance DuckDB fixture, and
/// <c>McpStatusEnvelopeTests</c> seeds one row per counter, so the case that breaks is exactly the one no
/// existing test exercises.</para>
/// </summary>
public sealed class PerfmonIntervalAggregationTests
{
    private const string ReadPath = "Lite/Services/LocalDataService.Perfmon.cs";

    /// <summary>Both trend reads — the single-counter one and its batched sibling — must take the interval
    /// as MAX. Two occurrences, because a fix applied to only one of the pair is the likelier mistake.
    /// </summary>
    [Fact]
    public void BothPerfmonTrendReads_TakeTheIntervalAsMax()
    {
        var source = ParitySource.ReadFile(ReadPath);

        var max = CountOccurrences(source, "MAX(sample_interval_seconds)");
        Assert.Equal(2, max);
    }

    /// <summary>The failure mode itself: a summed interval across a collection's INSTANCE rows is the
    /// 12-17x denominator inflation, and it is silent — the column is populated, the query succeeds, and
    /// only the derived rate is wrong.
    /// <para>#4234: the batched trend's bucketing legitimately adds a SECOND, different SUM over
    /// <c>sample_interval_seconds</c> — the outer GROUP BY <c>counter_name, bucket_start</c> summing each
    /// COLLECTION's own already-<c>MAX</c>'d interval across the collections a bucket holds (the ruling's
    /// summed-intervals rate), never raw instance rows. That SUM always carries this exact
    /// <c>FILTER (WHERE sample_interval_seconds &gt; 0)</c> right after it, so stripping just that
    /// substring isolates exactly the original bug shape: a bare, unfiltered
    /// <c>SUM(sample_interval_seconds)</c> grouped over instance rows.</para></summary>
    [Fact]
    public void NoPerfmonTrendRead_SumsTheIntervalAcrossInstanceRows()
    {
        var source = ParitySource.ReadFile(ReadPath);

        var withoutTheBucketedSum = source.Replace(
            "SUM(sample_interval_seconds) FILTER (WHERE sample_interval_seconds > 0)", string.Empty);
        Assert.DoesNotContain("SUM(sample_interval_seconds)", withoutTheBucketedSum, StringComparison.Ordinal);
    }

    /// <summary>Guards the fix from being over-applied: the two columns that ARE additive must stay sums,
    /// in both reads. A well-meant "make it consistent" edit that turned these into MAX would silently
    /// report one instance's value as the whole counter.
    /// <para>#4234: <c>delta_cntr_value</c> is 3, not 2 — the batched trend's bucketing adds one more SUM,
    /// the outer GROUP BY <c>counter_name, bucket_start</c> summing each collection's already-summed delta
    /// across the bucket's collections (the ruling's summed-deltas rate), same additive column, one layer
    /// higher. <c>cntr_value</c> stays 2: that outer layer AVERAGES (the ruling's gauge rule), so it adds no
    /// third SUM.</para></summary>
    [Fact]
    public void TheAdditiveColumnsStaySummed()
    {
        var source = ParitySource.ReadFile(ReadPath);

        Assert.Equal(2, CountOccurrences(source, "SUM(cntr_value)"));
        Assert.Equal(3, CountOccurrences(source, "SUM(delta_cntr_value)"));
        Assert.DoesNotContain("MAX(cntr_value)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(delta_cntr_value)", source, StringComparison.Ordinal);
    }

    /// <summary>The interval has to reach the caller, or the distinction dies at the API boundary and a
    /// Lite caller is back to an ambiguous zero — the half of #2234 that only landed for Darling first.
    /// </summary>
    [Fact]
    public void LitesPerfmonTrendTool_ProjectsTheInterval()
    {
        /* #3960: the per-collection inline projection this pinned moved into the shared TrendPayloads.PerfmonTrend
           builder (PerformanceMonitor.Common), which both SKUs now call — so the interval reaching the caller is
           proven at the ONE site that projects it, and by McpPerfmonTools.cs calling that builder rather than
           building its own envelope. */
        var tool = ParitySource.ReadFile("Lite/Mcp/McpPerfmonTools.cs");
        Assert.Contains("TrendPayloads.PerfmonTrend(", tool, StringComparison.Ordinal);

        var shared = ParitySource.ReadFile("PerformanceMonitor.Common/Mcp/TrendPayloads.cs");
        Assert.Contains("[\"sample_interval_seconds\"] = seconds", shared, StringComparison.Ordinal);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal);
             i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
