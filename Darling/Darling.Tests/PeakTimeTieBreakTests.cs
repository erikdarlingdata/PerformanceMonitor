/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license text.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4731: a tile's peak time must not depend on which of several tied rows the engine reaches first, and must
/// not be the time of a sample that has no value. Every per-tile peak-time aggregate orders by the value
/// <c>DESC NULLS LAST</c>, then by <c>collection_time DESC</c>: two rows tied on the peak report the later
/// collection time on every run, and a NULL value (which PostgreSQL sorts first under <c>DESC</c>) never wins
/// the way the tile's <c>MAX</c>, which ignores NULLs, does not let it. Census over the detector sources, so a
/// new peak-time aggregate added without either half fails here rather than showing a different time on a rerun.
/// </summary>
public sealed class PeakTimeTieBreakTests
{
    /// <summary>The detector sources that carry a per-tile <c>array_agg(collection_time ORDER BY ...)[1]</c>, and how many each holds.</summary>
    private static readonly (string File, int Sites)[] DetectorFiles =
    {
        ("PgAnomalyDetector.cs", 5),
        ("PgTargetAnomalyDetector.cs", 4),
        ("PgTargetAnomalyDetector.Kernel.cs", 1),
        ("PgTargetAnomalyDetector.WaitsSampled.cs", 1),
        ("PgTargetAnomalyDetector.Wal.cs", 1),
        ("PgTargetAnomalyDetector.Io.cs", 1),
        ("PgTargetAnomalyDetector.Blocking.cs", 1),
    };

    /// <summary>What every peak-time ORDER BY ends with: the value's NULLs last, then the later collection time.</summary>
    private const string PeakOrderSuffix = " DESC NULLS LAST, collection_time DESC";

    private static readonly Regex PeakTimeAggregate = new(
        @"array_agg\(collection_time ORDER BY (?<order>.+?)\)\)\[1\]", RegexOptions.Compiled);

    private static string Source(string file) =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", file);

    [Fact]
    public void EveryPeakTimeAggregate_OrdersByTheValue_ThenByTheLaterCollectionTime()
    {
        foreach (var (file, sites) in DetectorFiles)
        {
            var matches = PeakTimeAggregate.Matches(Source(file));
            Assert.True(sites == matches.Count, $"{file}: expected {sites} peak-time aggregate(s), found {matches.Count}");
            foreach (Match match in matches)
            {
                var order = match.Groups["order"].Value.Trim();
                Assert.True(
                    order.EndsWith(", collection_time DESC", StringComparison.Ordinal),
                    $"{file}: '{order}' leaves tied rows to the engine - end the ORDER BY with ', collection_time DESC'");
            }
        }
    }

    [Fact]
    public void EveryPeakTimeAggregate_SortsASampleWithNoValueLast_SoThePeakTimeIsARowThePeakValueCounted()
    {
        /* PostgreSQL sorts NULLs FIRST under DESC. The tile's peak value is a MAX, which ignores NULLs, so an ORDER BY
           without NULLS LAST can report the time of a sample that has no value while the value beside it is another
           row's. Every site says so, not only the ones whose value can be NULL today, so there is one shape. */
        foreach (var (file, _) in DetectorFiles)
        {
            foreach (Match match in PeakTimeAggregate.Matches(Source(file)))
            {
                var order = match.Groups["order"].Value.Trim();
                Assert.True(
                    order.EndsWith(PeakOrderSuffix, StringComparison.Ordinal),
                    $"{file}: ORDER BY '{order}' lets a sample with no value sort first - end the ORDER BY with '{PeakOrderSuffix}'");
            }
        }
    }

    [Fact]
    public void TheIoPeakSample_AndItsReadCount_PickTheSameTiedRow()
    {
        /* Two arrays ride one row: the peak sample's time and that sample's reads. They must share one ORDER BY,
           tie-break and NULLS LAST included, or a tie (or a sample with no latency) can pair one row's time with
           another row's reads. */
        var io = Source("PgTargetAnomalyDetector.Io.cs");
        Assert.Contains("(array_agg(collection_time ORDER BY ms_per_read DESC NULLS LAST, collection_time DESC))[1] AS peak_sample", io, StringComparison.Ordinal);
        Assert.Contains("(array_agg(reads ORDER BY ms_per_read DESC NULLS LAST, collection_time DESC))[1]", io, StringComparison.Ordinal);
    }
}
