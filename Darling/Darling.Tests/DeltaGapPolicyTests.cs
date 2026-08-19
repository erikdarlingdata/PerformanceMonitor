/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2233 / #2234: the gap policy every delta collector shares, and the invariant that makes a stored
/// zero readable.
///
/// <para>The old policy rejected any baseline older than 300 s and returned 0. Measured against the
/// fleet that threshold sat on the MEDIAN sweep gap — 99,717 consecutive perfmon gaps over 52 servers
/// and 7 days: p50 299 s, p90 580 s, p99 830 s, max 2,514 s — so it fired 50.0% of the time during
/// ordinary operation instead of after the restarts it was written for, and each firing stored a 0 that
/// is indistinguishable from a genuinely idle interval.</para>
///
/// <para>The pins below are behavioral, not textual: the 600 s case is the one that flips with the
/// threshold (0 before, a real delta after), and the reset case pins the invariant
/// <c>interval == 0 &lt;=&gt; no delta was knowable</c> that the whole (delta, interval) reading rests
/// on.</para>
/// </summary>
public sealed class DeltaGapPolicyTests
{
    private const int ServerId = 1;
    private const string Collector = "perfmon";
    private const string Key = "SQLServer:SQL Statistics|Batch Requests/sec|";

    private static DateTime T0 => new(2026, 8, 13, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The measured fleet median. A gap this size is ORDINARY, and the old 300 s policy
    /// rejected it — this is the 50% of output that was a fabricated zero.</summary>
    [Fact]
    public void AGapAtTheFleetMedian_YieldsARealDelta_NotAFabricatedZero()
    {
        var calc = new CollectorDeltaCalculator();

        var first = calc.CalculateDeltaWithInterval(ServerId, Collector, Key, 1_000, out var firstInterval,
            collectionTime: T0, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        Assert.Equal(0, first);          /* first sighting: baseline only */
        Assert.Equal(0, firstInterval);

        /* 299 s — the measured p50. Under the old 300 s policy this squeaked through; at p90 (580 s)
           it did not, which is why half the fleet's points were zeros. */
        var second = calc.CalculateDeltaWithInterval(ServerId, Collector, Key, 1_500, out var secondInterval,
            collectionTime: T0.AddSeconds(299), maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        Assert.Equal(500, second);
        Assert.Equal(299, secondInterval);
    }

    /// <summary>The pin that flips with the fix: 600 s is past the OLD 300 s policy and inside the new
    /// one. 8.3% of real fleet gaps land here.</summary>
    [Fact]
    public void AGapPastTheOldPolicyButInsideTheNewOne_YieldsARealDelta()
    {
        var calc = new CollectorDeltaCalculator();
        calc.CalculateDelta(ServerId, Collector, Key, 20_000_000,
            collectionTime: T0, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var delta = calc.CalculateDeltaWithInterval(ServerId, Collector, Key, 20_055_000, out var interval,
            collectionTime: T0.AddSeconds(600), maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        /* Exactly the shape #2234 reported from production: the cumulative counter advanced ~55,000
           while delta_value came back 0. It must now report the advance and the span it covered. */
        Assert.Equal(55_000, delta);
        Assert.Equal(600, interval);
    }

    /// <summary>The guard still guards: past an hour the baseline is treated as too stale to subtract
    /// from, and the interval says so rather than implying an idle hour.</summary>
    [Fact]
    public void AGapPastTheNewPolicy_YieldsZeroAndReportsNoInterval()
    {
        var calc = new CollectorDeltaCalculator();
        calc.CalculateDelta(ServerId, Collector, Key, 1_000,
            collectionTime: T0, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var delta = calc.CalculateDeltaWithInterval(ServerId, Collector, Key, 9_999, out var interval,
            collectionTime: T0.AddSeconds(CollectorDeltaCalculator.DefaultMaxGapSeconds + 1),
            maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);
    }

    /// <summary>The invariant, and the case that used to break it: a counter reset makes the delta
    /// unknowable, so the interval must be 0 too. Reporting 0 work over a REAL interval is a claim
    /// that nothing happened for that long — the one place that claim would be false.</summary>
    [Fact]
    public void ACounterReset_YieldsZeroAndReportsNoInterval_SoItCannotReadAsIdle()
    {
        var calc = new CollectorDeltaCalculator();
        calc.CalculateDelta(ServerId, Collector, Key, 5_000,
            collectionTime: T0, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        /* Counter went backwards (instance restart / plan cache eviction). */
        var delta = calc.CalculateDeltaWithInterval(ServerId, Collector, Key, 12, out var interval,
            collectionTime: T0.AddSeconds(120), maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);
    }

    /// <summary>A normal sweep with real work: both halves non-zero, which is the only combination a
    /// consumer may read as a rate.</summary>
    [Fact]
    public void AnOrdinarySweep_ReportsBothTheDeltaAndTheSpanItCovered()
    {
        var calc = new CollectorDeltaCalculator();
        calc.CalculateDelta(ServerId, Collector, Key, 100,
            collectionTime: T0, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var delta = calc.CalculateDeltaWithInterval(ServerId, Collector, Key, 700, out var interval,
            collectionTime: T0.AddSeconds(60), maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        Assert.Equal(600, delta);
        Assert.Equal(60, interval);
        Assert.Equal(10.0, delta / (double)interval);   /* the rate a caller derives */
    }

    /// <summary>A genuinely idle interval is the one case that legitimately stores a 0 delta — and it
    /// keeps a real interval, which is exactly what distinguishes it from the three unknowns.</summary>
    [Fact]
    public void AGenuinelyIdleInterval_KeepsItsRealInterval()
    {
        var calc = new CollectorDeltaCalculator();
        calc.CalculateDelta(ServerId, Collector, Key, 4_242,
            collectionTime: T0, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var delta = calc.CalculateDeltaWithInterval(ServerId, Collector, Key, 4_242, out var interval,
            collectionTime: T0.AddSeconds(180), maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        Assert.Equal(0, delta);
        Assert.Equal(180, interval);
    }

    /// <summary>No collector may reintroduce a hard-coded gap. The literal is what drifted for 41 call
    /// sites and five doc comments, so the pin is on the literal, not on any one collector.</summary>
    [Fact]
    public void NoCollectorPassesAHardCodedGap()
    {
        var collectors = Path.Combine(RepoRoot(), "PerformanceMonitor.Collectors");
        Assert.True(Directory.Exists(collectors), $"collectors directory not found at {collectors}");

        var offenders = Directory.EnumerateFiles(collectors, "*.cs", SearchOption.AllDirectories)
            .Select(path => (path, text: File.ReadAllText(path)))
            .Where(f => System.Text.RegularExpressions.Regex.IsMatch(f.text, @"maxGapSeconds: *[0-9]"))
            .Select(f => Path.GetFileName(f.path))
            .ToList();

        Assert.Empty(offenders);
    }

    /// <summary>The perfmon collector must MEASURE its interval. It wrote a literal 60 — the configured
    /// cadence — while the real median gap was 299 s, so every rate derived from it was up to 5x high.
    /// </summary>
    [Fact]
    public void ThePerfmonCollectorMeasuresItsInterval_RatherThanAssertingTheCadence()
    {
        var source = File.ReadAllText(Path.Combine(
            RepoRoot(), "PerformanceMonitor.Collectors", "PerfmonStatsCollector.cs"));

        Assert.Contains("CalculateDeltaWithInterval", source, StringComparison.Ordinal);
        Assert.Contains(".Value(sampleIntervalSeconds)", source, StringComparison.Ordinal);
        /* The literal it used to write, as the payload value. */
        Assert.DoesNotContain(".Value(60)", source, StringComparison.Ordinal);
    }

    /// <summary>The read has to carry the denominator, or the distinction dies at the API boundary —
    /// which is the half of #2234 that made a fabricated zero unfalsifiable from outside.
    /// <para>And it must AGGREGATE it correctly: the value and delta are additive across a counter's
    /// instance rows, the interval is not — it is one measured gap repeated per instance. Fleet-measured,
    /// Transactions/sec carries a median of 12 (max 17) rows per collection_time, so SUM would report a
    /// rate 12-17x too low. Pinning the aggregate by name because a review caught exactly that.</para>
    /// </summary>
    [Fact]
    public void ThePerfmonTrendReadProjectsTheInterval_AggregatedAsMaxNotSum()
    {
        var sql = PerformanceMonitor.Darling.Service.Mcp.DarlingTrendReader.PerfmonTrendSql;

        Assert.Contains("MAX(sample_interval_seconds)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(sample_interval_seconds)", sql, StringComparison.Ordinal);
        /* The additive pair must stay additive — this guards the fix from being over-applied. */
        Assert.Contains("SUM(cntr_value)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_cntr_value)", sql, StringComparison.Ordinal);
    }

    private static string RepoRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
