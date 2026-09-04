/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Decision-table pins for the shared <see cref="CollectorHealthClassifier"/> (#1573) - the one banding
/// the Lite grid, the Darling viewer's grid + Overview "collectors failing" count, and the service's
/// get_collection_health / web fleet reader all delegate to. This SAME table is pinned identically in
/// Darling.Tests so the two SKUs' collector-health banding cannot drift.
///
/// <para>
/// The load-bearing case is the motivating bug: index_object_stats is a DAILY collector
/// (<see cref="CollectorScheduleDefaults"/> cadence 1440 min) that succeeds every run, yet the old flat
/// 4h-STALE / 24h-FAILING thresholds flagged it STALE past 4h and FAILING past 24h. The thresholds are
/// now relative to each collector's cadence (FAILING = max(24, 2 x freqHours); STALE = max(4, 1.5 x
/// freqHours)), with floors that keep every FREQUENT collector byte-for-byte identical.
/// </para>
/// </summary>
public sealed class CollectorHealthClassifierTests
{
    private const int OneMinute = 1;      // wait_stats, cpu_utilization, etc. - the fastest cadence.
    private const int Hourly = 60;        // database_size_stats - still floor-bound (1.5h/2h < 4h/24h).
    private const int Daily = 1440;       // index_object_stats - the collector that relaxes.
    private const int OnLoadFreq = 0;     // config snapshots run on connect, not on the loop.

    [Theory]
    /* NEVER_RUN wins first, whatever else is set (including on-load). */
    [InlineData(0, 0, 0, 0, 0, 999, 999, OneMinute, false, CollectorHealthClassifier.NeverRun)]
    [InlineData(0, 0, 0, 0, 0, 0, 0, OnLoadFreq, true, CollectorHealthClassifier.NeverRun)]

    /* NO_PERMISSIONS (only permission denials) is checked before the on-load branch and before STOPPED -
       a collector whose every run this window was a denied grant is still actively RUNNING (hoursSinceLastRun
       is small), so this is not a STOPPED case, but the precedence is worth pinning regardless. */
    [InlineData(5, 0, 0, 5, 0, 999, 2, OneMinute, false, CollectorHealthClassifier.NoPermissions)]
    [InlineData(5, 0, 0, 5, 0, 999, 2, OnLoadFreq, true, CollectorHealthClassifier.NoPermissions)]

    /* On-load collectors are staleness-exempt: banded by failure rate only, never STOPPED/STALE/FAILING. */
    [InlineData(10, 10, 0, 0, 0, 500, 500, OnLoadFreq, true, CollectorHealthClassifier.Healthy)]
    [InlineData(10, 7, 3, 0, 0, 500, 500, OnLoadFreq, true, CollectorHealthClassifier.Warning)]   // 30% > 20%

    /* Frequent (1-min) collector - the floors mean these are IDENTICAL to the old flat thresholds for the
       cases where hoursSinceLastRun stays recent. */
    [InlineData(10, 10, 0, 0, 0, 2, 2, OneMinute, false, CollectorHealthClassifier.Healthy)]    // < 4h
    [InlineData(10, 7, 3, 0, 0, 2, 2, OneMinute, false, CollectorHealthClassifier.Warning)]     // recent, 30% fail
    [InlineData(10, 10, 0, 0, 0, 5, 5, OneMinute, false, CollectorHealthClassifier.Stale)]      // > 4h, < 24h

    /* STOPPED vs FAILING is the whole point of this pair: same hoursSinceLastSuccess (30h, past the 24h
       cutoff), but one collector is STILL BEING INVOKED (a recent failed attempt, hoursSinceLastRun=2) and
       the other has gone completely dark (hoursSinceLastRun=30, nothing attempted since that old success). */
    [InlineData(20, 15, 5, 0, 0, 30, 2, OneMinute, false, CollectorHealthClassifier.Failing)]   // recent failure -> still trying
    [InlineData(10, 10, 0, 0, 0, 25, 25, OneMinute, false, CollectorHealthClassifier.Stopped)]  // nothing since -> gone dark

    /* Ran, never a success (999 sentinel on hoursSinceLastSuccess) - but the collector is still being
       invoked (hoursSinceLastRun=1), so this correctly stays FAILING rather than reading STOPPED. This is
       the exact interaction #1573's original 999-sentinel comment described, now proven against STOPPED too. */
    [InlineData(5, 0, 5, 0, 0, 999, 1, OneMinute, false, CollectorHealthClassifier.Failing)]

    /* Hourly (60-min) collector - 1.5h/2h are both under the 4h/24h floors, so still floor-bound. */
    [InlineData(10, 10, 0, 0, 0, 5, 5, Hourly, false, CollectorHealthClassifier.Stale)]         // > 4h floor
    [InlineData(10, 10, 0, 0, 0, 25, 25, Hourly, false, CollectorHealthClassifier.Stopped)]     // > 24h floor, dark since

    /* Daily (1440-min) collector - the FIX. STALE line 36h, FAILING/STOPPED line 48h. */
    [InlineData(30, 30, 0, 0, 0, 27, 27, Daily, false, CollectorHealthClassifier.Healthy)]      // THE #1573 BUG: was FAILING, now HEALTHY
    [InlineData(30, 30, 0, 0, 0, 35, 35, Daily, false, CollectorHealthClassifier.Healthy)]      // still under the 36h stale line
    [InlineData(30, 30, 0, 0, 0, 37, 37, Daily, false, CollectorHealthClassifier.Stale)]        // > 36h, < 48h
    [InlineData(30, 30, 0, 0, 0, 47, 47, Daily, false, CollectorHealthClassifier.Stale)]        // still stale, not failing
    [InlineData(30, 30, 0, 0, 0, 49, 49, Daily, false, CollectorHealthClassifier.Stopped)]      // > 48h, dark since (was FAILING)
    [InlineData(10, 7, 3, 0, 0, 1, 1, Daily, false, CollectorHealthClassifier.Warning)]         // recent, 30% fail -> WARNING not STALE

    /* ---- #2804: ABANDONED cycles. ------------------------------------------------------------------
       An abandoned run is the #2673 whole-server wall-clock budget giving up: it stores nothing and
       advances no watermark. Before this it incremented total_runs and NOTHING else, so it grew the
       failure-rate DENOMINATOR while contributing nothing to the numerator, and never touched the success
       clock. TOTAL abandonment still reaches FAILING through staleness, because no success lands at all;
       the gap these rows close is the PARTIAL case, where the collector keeps succeeding often enough to
       stay fresh and so reads HEALTHY forever while losing cycles.

       The threshold (0.5%) is positioned by the fleet, not by taste: over 24h across 1,639
       (server, collector) pairs and 520,455 runs, only FOUR pairs abandoned anything at all and the
       per-pair rate distribution is p50 = p75 = p90 = p95 = p99 = 0.000% with a max of 2.157%. 0.5 sits
       below that observed floor (0.60%) and above the 99th percentile. */

    /* THE BUG, at the exact shape measured in production: 24 abandoned of 1,226 runs (1.96%), zero
       errors, and a success recent enough that neither STALE nor FAILING can fire. Read HEALTHY before
       this change. The sibling row is the same server's query_stats at 14 of 1,226 (1.14%). */
    [InlineData(1226, 1202, 0, 0, 24, 1, 1, OneMinute, false, CollectorHealthClassifier.Warning)]
    [InlineData(1226, 1212, 0, 0, 14, 1, 1, OneMinute, false, CollectorHealthClassifier.Warning)]

    /* The boundary, either side of 0.5% of 1,226 runs (= 6.13 runs). Six abandoned is 0.489% and stays
       quiet; seven is 0.571% and bands. This is what keeps a single isolated abandonment from flapping a
       collector in any window under ~400 runs - "one in a thousand is noise, twenty-four is a finding". */
    [InlineData(1226, 1220, 0, 0, 6, 1, 1, OneMinute, false, CollectorHealthClassifier.Healthy)]
    [InlineData(1226, 1219, 0, 0, 7, 1, 1, OneMinute, false, CollectorHealthClassifier.Warning)]

    /* One abandonment in a small window stays HEALTHY (1 of 500 = 0.2%), and the fleet's own resting
       state - zero abandoned - is untouched. */
    [InlineData(500, 499, 0, 0, 1, 1, 1, OneMinute, false, CollectorHealthClassifier.Healthy)]
    [InlineData(1226, 1226, 0, 0, 0, 1, 1, OneMinute, false, CollectorHealthClassifier.Healthy)]

    /* On-load collectors are staleness-exempt and banded by rate ONLY, so abandonment has to be asked on
       that branch too - otherwise the one class of collector that cannot reach the staleness safety net
       would be the one class where abandonment stays invisible. */
    [InlineData(1000, 980, 0, 0, 20, 1, 1, OnLoadFreq, true, CollectorHealthClassifier.Warning)]
    [InlineData(1000, 998, 0, 0, 2, 1, 1, OnLoadFreq, true, CollectorHealthClassifier.Healthy)]

    /* Precedence: abandonment sits at the WARNING tier, so the louder facts still win. A collector that
       has not succeeded in 30h is FAILING whatever its abandon rate, and one that has gone dark entirely
       is STOPPED - abandonment never downgrades either, and NEVER_RUN still wins outright. */
    [InlineData(1226, 1000, 0, 0, 226, 30, 2, OneMinute, false, CollectorHealthClassifier.Failing)]
    [InlineData(1226, 1000, 0, 0, 226, 30, 30, OneMinute, false, CollectorHealthClassifier.Stopped)]
    [InlineData(1226, 1200, 0, 0, 26, 5, 5, OneMinute, false, CollectorHealthClassifier.Stale)]
    [InlineData(0, 0, 0, 0, 0, 999, 999, OneMinute, false, CollectorHealthClassifier.NeverRun)]

    /* A collector abandoning EVERY cycle already had a band - it lands no successes, so the success clock
       runs out and staleness carries it to FAILING. Pinned so the new rate cannot be mistaken for the only
       thing standing between total abandonment and a green row. */
    [InlineData(100, 0, 0, 0, 100, 999, 1, OneMinute, false, CollectorHealthClassifier.Failing)]
    public void Classify_BandsTheDecisionTable(
        long totalRuns, long successCount, long errorCount, long permissionDeniedCount, long abandonedCount,
        double hoursSinceLastSuccess, double hoursSinceLastRun, int frequencyMinutes, bool isOnLoad, string expected)
    {
        Assert.Equal(expected, CollectorHealthClassifier.Classify(
            totalRuns, successCount, errorCount, permissionDeniedCount, abandonedCount,
            hoursSinceLastSuccess, hoursSinceLastRun, frequencyMinutes, isOnLoad));
    }

    /// <summary>
    /// #2926, Lite's half of the era-invariance the four Darling banding reads are pinned for in
    /// <c>Darling.Tests/CollectorHealthClassifierTests</c>. The abandonment count must be keyed on the
    /// ROW - nothing stored, plus the budget note - and not on <c>ABANDONED</c> alone, because
    /// <c>collection_log</c> is append-only and a window can still hold cycles abandoned before #2803
    /// gave abandonment its own status. Those carry <c>status = 'SUCCESS'</c>, so a status-only count read
    /// them as zero abandonments AND as successes: the collector banded HEALTHY while losing cycles.
    ///
    /// <para>By REFERENCE to the constant, not by grepping the file: this repo's SQL carries discursive
    /// <c>--</c> commentary that quotes column names and predicates, and a source scan cannot tell the
    /// query from the prose beside it.</para>
    /// </summary>
    [Fact]
    public void TheCollectionHealthRead_CountsAbandonmentByTheRow_NotByTheStatusAlone()
    {
        var sql = LocalDataService.CollectionHealthSql;

        Assert.Contains("AS abandoned_count", sql, StringComparison.Ordinal);
        Assert.Contains(EnumeratedCollectorDriver.AbandonedRunPredicateSql, sql, StringComparison.Ordinal);

        /* And the same rows leave the success bucket, so the Success and Abandoned columns that sit side
           by side in the Collection Health grid cannot both claim one run. */
        Assert.Contains("AND NOT " + EnumeratedCollectorDriver.AbandonedByNotePredicateSql, sql, StringComparison.Ordinal);
        Assert.Contains("AS success_count", sql, StringComparison.Ordinal);

        /* The status-only form is gone. The line under it is this negative's POSITIVE CONTROL - the same
           Contains form over the sibling bucket this read demonstrably carries - so a DoesNotContain that
           passed by matching nothing cannot hide here. */
        Assert.DoesNotContain("SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END)", sql, StringComparison.Ordinal);

        /* The predicate reads rows_collected, and this query's aggregates sit outside a subquery that
           ENUMERATES its columns - so the column has to be projected through it or the read fails at the
           store and nowhere earlier. */
        Assert.Contains("rows_collected,", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The field shape this band exists for: a collector whose gate flipped off for a target (agent_status
    /// and running_jobs on an RDS SQL Server instance where SQL Agent job data is not reachable) simply
    /// stops being invoked. Its last historical success sits well inside the health window and ages past
    /// the FAILING cutoff exactly like a genuinely broken collector would - the field symptom this whole
    /// band exists to stop misreporting.
    /// </summary>
    [Fact]
    public void Classify_LongDormantCollector_WithNoRecentActivityOfAnyKind_IsStoppedNotFailing()
    {
        Assert.Equal(CollectorHealthClassifier.Stopped, CollectorHealthClassifier.Classify(
            totalRuns: 882, successCount: 882, errorCount: 0, permissionDeniedCount: 0, abandonedCount: 0,
            hoursSinceLastSuccess: 120, hoursSinceLastRun: 120, frequencyMinutes: 5, isOnLoad: false));
    }

    [Theory]
    [InlineData(OneMinute, 24.0)]     // floor
    [InlineData(Hourly, 24.0)]        // 2 x 1h = 2h < 24h floor
    [InlineData(720, 24.0)]           // 2 x 12h = 24h, exactly the floor
    [InlineData(Daily, 48.0)]         // 2 x 24h = 48h relaxes past the floor
    public void FailingThresholdHours_IsMaxOfFloorAndTwiceCadence(int frequencyMinutes, double expected) =>
        Assert.Equal(expected, CollectorHealthClassifier.FailingThresholdHours(frequencyMinutes), 3);

    [Theory]
    [InlineData(OneMinute, 4.0)]      // floor
    [InlineData(Hourly, 4.0)]         // 1.5 x 1h = 1.5h < 4h floor
    [InlineData(Daily, 36.0)]         // 1.5 x 24h = 36h relaxes past the floor
    public void StaleThresholdHours_IsMaxOfFloorAndOnePointFiveCadence(int frequencyMinutes, double expected) =>
        Assert.Equal(expected, CollectorHealthClassifier.StaleThresholdHours(frequencyMinutes), 3);

    [Theory]
    [InlineData("server_config", true)]
    [InlineData("database_config", true)]
    [InlineData("database_scoped_config", true)]
    [InlineData("trace_flags", true)]
    [InlineData("server_properties", true)]
    [InlineData("SERVER_CONFIG", true)]        // case-insensitive
    [InlineData("wait_stats", false)]
    [InlineData("index_object_stats", false)]
    public void IsOnLoadCollector_MatchesTheFreqZeroSet(string collectorName, bool expected) =>
        Assert.Equal(expected, CollectorHealthClassifier.IsOnLoadCollector(collectorName));

    /// <summary>The on-load set is exactly the FrequencyMinutes == 0 entries in the shared schedule table -
    /// the invariant that lets a caller resolve cadence and on-load-ness from the same source.</summary>
    [Fact]
    public void OnLoadSet_EqualsTheFreqZeroScheduleEntries()
    {
        foreach (var (name, entry) in CollectorScheduleDefaults.All)
        {
            Assert.Equal(entry.FrequencyMinutes == 0, CollectorHealthClassifier.IsOnLoadCollector(name));
        }
    }

    /* --- The bug, reproduced through Lite's real CollectorHealthRow (it resolves the cadence from
       CollectorScheduleDefaults by name). --- */

    [Fact]
    public void IndexObjectStats_DefaultCadence_IsDaily()
    {
        // Pins the exact input that produced the field false-positive: a 1440-min (daily) collector.
        Assert.Equal(1440, CollectorScheduleDefaults.All["index_object_stats"].FrequencyMinutes);
    }

    [Fact]
    public void CollectorHealthRow_IndexObjectStats_At27Hours_IsHealthyNotFailing()
    {
        /* The motivating bug: index_object_stats succeeds daily; 27h since last success is well within a
           daily cadence. Under the old flat 24h-FAILING threshold this read FAILING; it now reads HEALTHY. */
        var row = new CollectorHealthRow
        {
            CollectorName = "index_object_stats",
            TotalRuns = 7,
            SuccessCount = 7,
            LastSuccessTime = DateTime.UtcNow.AddHours(-27),
        };
        Assert.Equal("HEALTHY", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_IndexObjectStats_At37Hours_IsStale()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "index_object_stats",
            TotalRuns = 7,
            SuccessCount = 7,
            LastSuccessTime = DateTime.UtcNow.AddHours(-37),
        };
        Assert.Equal("STALE", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_IndexObjectStats_At49Hours_IsStoppedNotFailing()
    {
        /* This row never sets LastRunTime, so HoursSinceLastRun falls back to HoursSinceLastSuccess (49h) -
           a 100%-success collector whose last success was 49h ago has, by definition, attempted NOTHING
           since (no later success, no failure either), which is the STOPPED signature, not FAILING's
           "still running and erroring". Reclassified from FAILING now that the two clocks are distinguished. */
        var row = new CollectorHealthRow
        {
            CollectorName = "index_object_stats",
            TotalRuns = 7,
            SuccessCount = 7,
            LastSuccessTime = DateTime.UtcNow.AddHours(-49),
        };
        Assert.Equal("STOPPED", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_FrequentCollector_GoneDarkFor30Hours_IsStoppedNotFailing()
    {
        /* A 1-min collector unaffected by the relative thresholds (the floors dominate): 30h past its last
           (and, since 100% success, its ONLY recent) activity now reads STOPPED - it has gone completely
           dark, which is a different fact from "it keeps running and erroring" (FAILING, proven by the
           direct decision-table cases above). Reclassified from FAILING for the same reason as the
           index_object_stats case: no LastRunTime set, so it falls back to HoursSinceLastSuccess. */
        var row = new CollectorHealthRow
        {
            CollectorName = "wait_stats",
            TotalRuns = 100,
            SuccessCount = 100,
            LastSuccessTime = DateTime.UtcNow.AddHours(-30),
        };
        Assert.Equal("STOPPED", row.HealthStatus);
    }
}
