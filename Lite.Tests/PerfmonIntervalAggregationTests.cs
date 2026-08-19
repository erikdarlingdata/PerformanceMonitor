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

    /// <summary>The failure mode itself: a summed interval anywhere in this read path is the 12-17x
    /// denominator inflation, and it is silent — the column is populated, the query succeeds, and only
    /// the derived rate is wrong.</summary>
    [Fact]
    public void NoPerfmonTrendRead_SumsTheInterval()
    {
        var source = ParitySource.ReadFile(ReadPath);

        Assert.DoesNotContain("SUM(sample_interval_seconds)", source, StringComparison.Ordinal);
    }

    /// <summary>Guards the fix from being over-applied: the two columns that ARE additive must stay sums,
    /// in both reads. A well-meant "make it consistent" edit that turned these into MAX would silently
    /// report one instance's value as the whole counter.</summary>
    [Fact]
    public void TheAdditiveColumnsStaySummed()
    {
        var source = ParitySource.ReadFile(ReadPath);

        Assert.Equal(2, CountOccurrences(source, "SUM(cntr_value)"));
        Assert.Equal(2, CountOccurrences(source, "SUM(delta_cntr_value)"));
        Assert.DoesNotContain("MAX(cntr_value)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(delta_cntr_value)", source, StringComparison.Ordinal);
    }

    /// <summary>The interval has to reach the caller, or the distinction dies at the API boundary and a
    /// Lite caller is back to an ambiguous zero — the half of #2234 that only landed for Darling first.
    /// </summary>
    [Fact]
    public void LitesPerfmonTrendTool_ProjectsTheInterval()
    {
        var source = ParitySource.ReadFile("Lite/Mcp/McpPerfmonTools.cs");

        Assert.Contains("sample_interval_seconds = p.SampleIntervalSeconds", source, StringComparison.Ordinal);
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
