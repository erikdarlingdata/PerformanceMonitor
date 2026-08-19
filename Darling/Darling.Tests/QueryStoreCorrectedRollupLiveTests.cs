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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1849 and #1869 end-to-end against a REAL TimescaleDB: the corrected Query Store rollups actually remove
/// the cumulative-snapshot double-count, the day-grain daily removes the hour-straddle residual that leaves
/// behind, and no purge can outrun either of them.
///
/// <para>These cannot be unit tests, for the same reason #1759's could not: every load-bearing fact here is
/// the ENGINE's, not ours. That a continuous aggregate may not bucket on a non-dimension column, that
/// <c>last(x, collection_time)</c> is accepted where <c>row_number()</c> is not, and — the one that shaped
/// this design and is in no documentation — that a hierarchical CAGG whose bucket EQUALS its parent's is a
/// LEAF that nothing can be built on, which is why #1869's re-dedup level had to WIDEN to be legal. A C#
/// test can assert we emit a string; only a live store can say whether TimescaleDB accepts it and what it
/// then computes.</para>
///
/// <para><b>#1776 own-store</b> — mints its own scratch database rather than sharing the live fixture, so it
/// is deliberately NOT in the <c>live-postgres</c> collection. It creates continuous aggregates and retention
/// policies the shared fixture must never inherit.</para>
/// </summary>
public sealed class QueryStoreCorrectedRollupLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -918491;

    /// <summary>
    /// How many times one Query Store interval gets re-collected inside a single hour. This is the shape
    /// #1849 measured on a live store (up to 496x), reproduced exactly: <c>execution_count</c> is CUMULATIVE
    /// within an interval, so the honest answer for the interval is its LAST snapshot — this number — while
    /// the old rollup's <c>sum(execution_count)</c> adds up every snapshot it ever saw.
    /// </summary>
    private const int ReCollections = 496;

    /// <summary>The inflated total the OLD rollup produces for that one interval: 1+2+...+496. Written as the
    /// closed form so the expectation is derived, not a magic number transcribed from a test run.</summary>
    private const long InflatedSum = ReCollections * (ReCollections + 1L) / 2L;

    /// <summary>
    /// L1's retention horizon in days (#2223), from the product's own TIMESPAN rather than re-parsing the
    /// interval string.
    ///
    /// <para>Review catch: the first cut split <c>IntervalRetentionInterval</c> on whitespace and parsed the
    /// leading number, reimplementing a conversion that already exists typed — and one that would quietly
    /// produce a wrong seed depth if the interval were ever written <c>"1 week"</c>. It matters more here than
    /// it looks: this feeds a static initializer, so a bad parse fails as a
    /// <c>TypeInitializationException</c> across the whole class rather than as anything readable.
    /// <c>TimescaleContinuousAggregateTests</c> pins the span equal to the string, so this cannot drift from
    /// the SQL the policy is created with.</para>
    /// </summary>
    private static readonly int L1RetentionDays = TimescaleSupport.IntervalRetentionSpan.Days;

    /// <summary>
    /// How deep every retention-arming test in this class seeds its history — DERIVED from L1's horizon, with
    /// two days of margin, so nothing a test seeds is ever eligible for deletion by a policy that test itself
    /// arms.
    ///
    /// <para><b>#2223, and it was a class-wide contradiction rather than one test's bug.</b> All four tests here
    /// that call <c>EnsureRetentionPoliciesAsync</c> used to seed a hardcoded 9 days while step 1 of each arms
    /// L1's retention at <c>drop_after =&gt; INTERVAL '7 days'</c>. Each therefore armed a policy guaranteed to
    /// want to delete two days of its own fixture, and then asserted against floors those buckets defined.</para>
    ///
    /// <para><b>How it failed.</b> In the re-hold test, <c>l1Floor</c> is read once and the backfill it is
    /// compared against runs ~20 lines later. If the armed retention job fires inside that window it drops L1's
    /// two oldest buckets, so the rebuilt consumer can only backfill to <c>now - 7 days</c> while the assertion
    /// still holds the pre-deletion floor. The reported failure was consumer <c>08-05</c> against L1
    /// <c>08-03</c> on a fixture seeded from <c>08-03</c> — the consumer floor was EXACTLY
    /// <c>now - IntervalRetentionInterval</c>, which is what identified the mechanism. Intermittent because it
    /// depends on the job firing in that window: solo runs passed every time, full-suite runs failed about half,
    /// and more wall clock means more chances.</para>
    ///
    /// <para>Derived rather than re-hardcoded to a smaller number so that changing the product horizon moves
    /// every fixture with it, instead of silently reintroducing this the next time it is tuned. The relationship
    /// is pinned by <see cref="SeededHistoryStaysInsideTheHorizonItArms"/>.</para>
    /// </summary>
    private static readonly int SeedDepthDays = L1RetentionDays - 2;

    /// <summary>
    /// The #2223 invariant itself: a test may not seed history that a policy it arms would delete. Pinned as a
    /// test rather than trusted as arithmetic, because the product horizon is tunable and the whole failure mode
    /// was a fixture silently drifting outside it.
    /// </summary>
    [Fact]
    public void SeededHistoryStaysInsideTheHorizonItArms()
    {
        Assert.True(SeedDepthDays < L1RetentionDays,
            $"seeded history ({SeedDepthDays}d) must stay inside L1's retention horizon " +
            $"({TimescaleSupport.IntervalRetentionInterval}) or the retention job these tests arm can delete " +
            $"the buckets they assert on — that is #2223.");

        /* And deep enough to still be the scenario: the coverage gap has to span more than one daily bucket. */
        Assert.True(SeedDepthDays >= 3,
            $"seeded history ({SeedDepthDays}d) must span at least three days for a multi-bucket coverage gap.");
    }

    [Fact]
    public async Task CorrectedRollups_DedupTheReCollectedInterval_WhileTheOldPairKeepsInflating()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1849 corrected-rollup test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Fixed instants, not now-relative: every assertion below is an exact arithmetic identity, and a
           window that drifted across an hour boundary between seeding and asserting would turn a real
           regression into a flake. Kind-Unspecified — naive-UTC storage, see PgCollectorRowWriter. */
        var hour = new DateTime(2026, 3, 4, 10, 0, 0, DateTimeKind.Unspecified);

        /* ── SEED 1: ONE interval, re-collected 496 times inside the 10:00 hour. Honest answer: 496. ── */
        await SeedIntervalAsync(connection, hour, intervalId: 1001, queryId: 42, planId: 77,
            firstExecution: hour.AddSeconds(5), snapshots: ReCollections, secondsApart: 7,
            avgDurationUs: 100, avgCpuUs: 50, ct: ct);

        /* ── SEED 2: a SECOND interval in the SAME hour, 10 snapshots. Honest hour total: 496 + 10 = 506. ── */
        await SeedIntervalAsync(connection, hour.AddMinutes(30), intervalId: 1002, queryId: 43, planId: 78,
            firstExecution: hour.AddMinutes(30), snapshots: 10, secondsApart: 10,
            avgDurationUs: 200, avgCpuUs: 80, ct: ct);

        /* ── SEED 3: LEGACY rows — collected before V41, so runtime_stats_interval_id is NULL and the tier-1
               PROXY (first_execution_time) is the only identity there has ever been for them. Five snapshots
               of one interval in the 12:00 hour; honest answer 5. This is the case the L1 GROUP BY includes
               ON PURPOSE rather than excluding: excluding them is the easier claim to make true and would
               silently drop every pre-upgrade hour out of the corrected rollup. ── */
        await SeedIntervalAsync(connection, hour.AddHours(2), intervalId: null, queryId: 44, planId: 79,
            firstExecution: hour.AddHours(2).AddSeconds(3), snapshots: 5, secondsApart: 60,
            avgDurationUs: 300, avgCpuUs: 90, ct: ct);

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);
        await RefreshAllAsync(connection, ct);

        /* ── 1. THE DEDUP ITSELF. L1 collapses 496 raw rows to ONE, holding the interval's LAST snapshot.
               Asserted as a row COUNT and a VALUE together: a count alone passes if the aggregate picked the
               first snapshot or the minimum, and a value alone passes if it happened to land on one row. ── */
        var l1 = await ReadIntervalRowsAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);

        var reCollected = Assert.Single(l1, r => r.QueryId == 42);
        Assert.Equal(ReCollections, reCollected.ExecutionCount);
        Assert.Equal(ReCollections, reCollected.SampleCount);
        Assert.Equal(1001L, reCollected.IntervalId);

        /* The legacy row deduped too, keyed on the proxy alone — tier-1 fidelity, which is exactly what
           #1853 says a legacy-only window degrades to. */
        var legacy = Assert.Single(l1, r => r.QueryId == 44);
        Assert.Null(legacy.IntervalId);
        Assert.Equal(5, legacy.ExecutionCount);

        /* ── 2. THE SIDE-BY-SIDE. The corrected hourly reports the honest 506 for the 10:00 hour while the
               ORIGINAL hourly, still standing and still fed from the same raw rows, reports the inflated sum.
               Asserting BOTH in one test is the point: a corrected-only assertion would still pass if the old
               view had been quietly reshaped, which is the one thing #1759/#1793 forbid. ── */
        var correctedHour = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, hour, ct);
        var legacyHour = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsHourlyView, hour, ct);

        Assert.Equal(506, correctedHour.ExecutionCountSum);
        Assert.Equal(InflatedSum + 55, legacyHour.ExecutionCountSum);

        /* The correction is worth stating as a ratio, because the ratio is the product claim. */
        Assert.True(legacyHour.ExecutionCountSum / (double)correctedHour.ExecutionCountSum > 200,
            $"expected the old rollup to be at least 200x inflated against the corrected one for this shape, " +
            $"got {legacyHour.ExecutionCountSum} vs {correctedHour.ExecutionCountSum}");

        /* ── 3. THE WEIGHTED MEAN STILL COMPOSES. The whole reason the corrected view carries weighted SUMS
               rather than a materialized average: duration_us_weighted_sum / execution_count_sum must be the
               true execution-weighted mean, never an avg-of-avgs. Hand-computed:
               (496 x 100 + 10 x 200) / 506. ── */
        var expectedMean = ((496d * 100d) + (10d * 200d)) / 506d;
        Assert.Equal(expectedMean, correctedHour.DurationWeightedSum / correctedHour.ExecutionCountSum, 6);

        /* ── 4. THE HAND-COMPUTED DAY. The corrected daily is sourced from L1 DIRECTLY (a sibling of the
               corrected hourly, not its child — an identity-width hierarchical CAGG is a leaf), so it must
               agree with summing the corrected hours: 506 from the 10:00 hour + 5 legacy from 12:00. ── */
        var correctedDay = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedDailyView, hour.Date, ct);
        Assert.Equal(511, correctedDay.ExecutionCountSum);

        var correctedLegacyHour = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, hour.AddHours(2), ct);
        Assert.Equal(5, correctedLegacyHour.ExecutionCountSum);
        Assert.Equal(correctedHour.ExecutionCountSum + correctedLegacyHour.ExecutionCountSum, correctedDay.ExecutionCountSum);
    }

    /// <summary>
    /// WATCHED (mutation): drop <c>query_store_stats_interval_hourly</c> from
    /// <see cref="TimescaleSupport.RawTierCoverage"/>'s coverage list and this goes red on the FIRST assertion
    /// — the raw purge arms while the corrected rollups hold nothing, which is the #1790-class race in one
    /// step.
    ///
    /// <para>query_store_stats is the only raw table with two rollup families reading it, and the gate is an
    /// AND over both. The original hourly can be fully caught up while the corrected one is empty (exactly the
    /// state every existing store is in the moment it takes this build), so a gate that checked either one
    /// alone would let raw drop the only copy of history the corrected rollups have never seen.</para>
    /// </summary>
    [Fact]
    public async Task RawPurgeArming_HeldWhileTheCorrectedIntervalLayerIsShort_ThenReleasesWhenItCovers()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1849 arming-gate test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var oldest = now.Date.AddDays(-SeedDepthDays);

        /* Pre-existing raw history, one interval per hour, going back well past any refresh policy's 3-day
           window — the shape every real store upgrading into this build is in. */
        for (var offset = 0; oldest.AddHours(offset) < now; offset += 3)
        {
            await SeedIntervalAsync(connection, oldest.AddHours(offset), intervalId: 2000 + offset, queryId: 7, planId: 9,
                firstExecution: oldest.AddHours(offset), snapshots: 3, secondsApart: 120,
                avgDurationUs: 100, avgCpuUs: 40, ct: ct);
        }

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);

        /* Bring the ORIGINAL pair fully up to date, and leave the corrected one SHORT. This is the precise
           state that makes a single-consumer gate wrong: by the old rule raw is "covered". */
        await RefreshRangeAsync(connection, TimescaleSupport.QueryStoreStatsHourlyView, oldest.AddDays(-1), now.AddDays(1), ct);

        var legacyFloor = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsHourlyView, ct);
        var rawOldestBefore = await RawOldestAsync(connection, ct);
        Assert.NotNull(rawOldestBefore);
        Assert.True(legacyFloor <= rawOldestBefore,
            "the ORIGINAL rollup must fully cover raw here, or the test is not exercising the case where a " +
            "single-consumer gate would wrongly arm.");

        /* L1 must NOT cover raw. Asserted as "does not reach raw's oldest row" rather than "is empty",
           because empty is the wrong invariant to pin: this is a live store whose refresh policies were just
           removed, and TimescaleDB runs a new policy's first check IMMEDIATELY (#1564) — so under suite load
           a job could materialize the recent window before the removal lands, and the test would fail on a
           technicality while the property it exists to prove still held. Shallow-or-empty is the real
           precondition, and it is what the gate itself reads. */
        var l1FloorBefore = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);
        Assert.True(l1FloorBefore is null || l1FloorBefore > rawOldestBefore,
            $"the corrected interval layer should not yet reach raw's oldest row (L1 {l1FloorBefore:O}, raw {rawOldestBefore:O}).");

        /* ── 1. HELD. The policy is created and left PAUSED, because L1 covers nothing. ── */
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.False(await IsArmedAsync(connection, "query_store_stats", ct),
            "raw's purge armed while the corrected interval layer had materialized NOTHING — the gate is " +
            "reading only one of query_store_stats' two rollup families.");

        /* ── 2. Coverage arrives. Materialize L1 back over everything raw holds, exactly as the backfill
               verb's slices do. ── */
        await RefreshRangeAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, oldest.AddDays(-1), now.AddDays(1), ct);

        var l1Floor = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);
        var rawOldest = await RawOldestAsync(connection, ct);
        Assert.NotNull(l1Floor);
        Assert.NotNull(rawOldest);
        Assert.True(l1Floor <= rawOldest,
            $"the interval layer must reach at least as far back as raw before the gate can release (L1 {l1Floor:O}, raw {rawOldest:O})");

        /* ── 3. RELEASED, by itself, on the next sweep — no manual step. That self-healing property is what
               #1759 replaced a caveat with, and adding a second consumer must not break it. ── */
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.True(await IsArmedAsync(connection, "query_store_stats", ct),
            "both rollup families now cover raw, so the held purge should have armed itself on this sweep.");
    }

    /// <summary>
    /// #1869, against a real store: an interval whose snapshots straddle an hour boundary is counted ONCE at
    /// the daily grain, where <see cref="TimescaleSupport.QueryStoreStatsCorrectedDailyView"/> counts it once
    /// per collection HOUR.
    ///
    /// <para>The seed is the exact shape #1869 published — an interval read 3 by 14:55 and 9 by 15:25 — plus a
    /// second straddle that puts almost all its work in the FIRST hour, which is where the residual approaches
    /// its 2x bound, plus a non-straddling interval that must come out identical on both. Every expectation is
    /// hand-computed from those three numbers rather than transcribed from a run.</para>
    ///
    /// <para>WATCHED (mutation): change L2's <c>last(execution_count, bucket)</c> to <c>sum</c> and the
    /// day-grain daily reports the corrected daily's 1,013 — the residual back in full.</para>
    /// </summary>
    [Fact]
    public async Task DayGrainDaily_CountsAnHourStraddlingIntervalOnce_WhereTheCorrectedDailyCountsItTwice()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1869 day-grain test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var day = new DateTime(2026, 3, 4, 0, 0, 0, DateTimeKind.Unspecified);

        /* ── A: STRADDLES 14:00/15:00. #1869's own example — 3 by the end of hour 14, 9 by the end of the
               interval. True contribution 9; the corrected daily books 3 + 9 = 12. ── */
        await SeedSnapshotsAsync(connection, intervalId: 1001, queryId: 42, planId: 77,
            intervalStart: day.AddHours(14), avgDurationUs: 100, avgCpuUs: 50,
            snapshots: new[]
            {
                (day.AddHours(14).AddMinutes(20), 1L),
                (day.AddHours(14).AddMinutes(40), 2L),
                (day.AddHours(14).AddMinutes(55), 3L),
                (day.AddHours(15).AddMinutes(5), 7L),
                (day.AddHours(15).AddMinutes(25), 9L),
            }, ct);

        /* ── B: STRADDLES 16:00/17:00 with nearly all the work already done in the first hour — the ~2x worst
               case. True contribution 496; the corrected daily books 495 + 496 = 991. ── */
        await SeedSnapshotsAsync(connection, intervalId: 1002, queryId: 42, planId: 78,
            intervalStart: day.AddHours(16), avgDurationUs: 200, avgCpuUs: 60,
            snapshots: new[]
            {
                (day.AddHours(16).AddMinutes(10), 494L),
                (day.AddHours(16).AddMinutes(50), 495L),
                (day.AddHours(17).AddMinutes(3), 496L),
            }, ct);

        /* ── C: no straddle at all. Both dailies must agree on it, or the new level is changing something it
               has no business changing. ── */
        await SeedSnapshotsAsync(connection, intervalId: 1003, queryId: 42, planId: 79,
            intervalStart: day.AddHours(10), avgDurationUs: 300, avgCpuUs: 70,
            snapshots: new[]
            {
                (day.AddHours(10).AddMinutes(5), 4L),
                (day.AddHours(10).AddMinutes(35), 7L),
                (day.AddHours(10).AddMinutes(55), 10L),
            }, ct);

        /* ── D: STRADDLES MIDNIGHT, on its own pair of days so it cannot pollute the arithmetic above. This is
               the residual the day grain does NOT remove, asserted rather than merely documented. ── */
        await SeedSnapshotsAsync(connection, intervalId: 1004, queryId: 42, planId: 80,
            intervalStart: day.AddDays(1).AddHours(23), avgDurationUs: 400, avgCpuUs: 80,
            snapshots: new[]
            {
                (day.AddDays(1).AddHours(23).AddMinutes(30), 20L),
                (day.AddDays(1).AddHours(23).AddMinutes(50), 25L),
                (day.AddDays(2).AddMinutes(10), 30L),
                (day.AddDays(2).AddMinutes(40), 33L),
            }, ct);

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);
        await RefreshAllAsync(connection, ct);

        /* ── 1. L2 ITSELF: one row per interval per DAY, holding that interval's last snapshot of the day. The
               straddling intervals are back to a single row; the midnight one is legitimately two. ── */
        var l2 = await ReadIntervalRowsAsync(connection, TimescaleSupport.QueryStoreStatsIntervalDailyView, ct);

        Assert.Equal(9, Assert.Single(l2, r => r.IntervalId == 1001).ExecutionCount);
        Assert.Equal(496, Assert.Single(l2, r => r.IntervalId == 1002).ExecutionCount);
        Assert.Equal(10, Assert.Single(l2, r => r.IntervalId == 1003).ExecutionCount);

        /* sample_count still counts RAW SNAPSHOTS, summed up the chain — 5 for A, not the 2 L1 rows it came
           from. That keeps it comparable with every other rollup's sample_count. */
        Assert.Equal(5, Assert.Single(l2, r => r.IntervalId == 1001).SampleCount);

        /* ── 2. THE HEADLINE, side by side in one test so neither number can be quietly re-based: the exact
               day total is 9 + 496 + 10 = 515, and the corrected daily books 12 + 991 + 10 = 1,013. ── */
        var dayGrain = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsDayGrainDailyView, day, ct);
        var corrected = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedDailyView, day, ct);

        Assert.Equal(515, dayGrain.ExecutionCountSum);
        Assert.Equal(1013, corrected.ExecutionCountSum);

        /* The residual, stated as the ratio the issue is costed against. */
        Assert.True(corrected.ExecutionCountSum / (double)dayGrain.ExecutionCountSum > 1.9,
            $"expected the hour-grain daily to be near 2x the day-grain one for this shape, got " +
            $"{corrected.ExecutionCountSum} vs {dayGrain.ExecutionCountSum}");

        /* ── 3. THE WEIGHTED MEAN STILL COMPOSES off the corrected values, never an avg-of-avgs:
               (9 x 100 + 496 x 200 + 10 x 300) / 515. ── */
        var expectedWeightedSum = (9d * 100d) + (496d * 200d) + (10d * 300d);
        Assert.Equal(expectedWeightedSum, dayGrain.DurationWeightedSum, 6);
        Assert.Equal(expectedWeightedSum / 515d, dayGrain.DurationWeightedSum / dayGrain.ExecutionCountSum, 6);

        /* ── 4. THE HOURLY IS UNTOUCHED. The straddle residual is irreducible at the hourly grain, so #1869
               must not have moved the corrected hourly at all: hour 14 still reads 3 and hour 15 still reads
               9, which is the same 12 the corrected daily inherits. ── */
        Assert.Equal(3, (await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, day.AddHours(14), ct)).ExecutionCountSum);
        Assert.Equal(9, (await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, day.AddHours(15), ct)).ExecutionCountSum);

        /* ── 5. THE RESIDUAL THAT REMAINS, pinned rather than hand-waved. An interval straddling MIDNIGHT is
               still counted once per collection DAY, so both dailies report 25 and 33 against a true 33. It is
               the identical argument one grain up and equally irreducible there — this assertion exists so the
               documentation cannot quietly become a lie. ── */
        var midnightDay1 = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsDayGrainDailyView, day.AddDays(1), ct);
        var midnightDay2 = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsDayGrainDailyView, day.AddDays(2), ct);

        Assert.Equal(25, midnightDay1.ExecutionCountSum);
        Assert.Equal(33, midnightDay2.ExecutionCountSum);
        Assert.Equal(midnightDay1.ExecutionCountSum,
            (await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedDailyView, day.AddDays(1), ct)).ExecutionCountSum);
    }

    /// <summary>
    /// WATCHED (mutation): drop <see cref="TimescaleSupport.QueryStoreStatsIntervalDailyView"/> from the
    /// interval-hourly layer's coverage list and this goes red on the first assertion.
    ///
    /// <para>This is the gate #1869 calls out by name. A store taking this build has a fully caught-up L1 and
    /// an EMPTY interval-grain daily, so a gate that still read only the two #1849 consumers would arm L1's
    /// purge and drop the exact history the day-grain daily has never materialized — and unlike raw's, that
    /// history has no other copy at interval grain.</para>
    /// </summary>
    [Fact]
    public async Task IntervalLayerPurgeArming_HeldWhileTheDayGrainLayerIsShort_ThenReleasesWhenItCovers()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1869 arming-gate test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var oldest = now.Date.AddDays(-SeedDepthDays);

        for (var offset = 0; oldest.AddHours(offset) < now; offset += 3)
        {
            await SeedIntervalAsync(connection, oldest.AddHours(offset), intervalId: 3000 + offset, queryId: 11, planId: 13,
                firstExecution: oldest.AddHours(offset), snapshots: 3, secondsApart: 120,
                avgDurationUs: 100, avgCpuUs: 40, ct: ct);
        }

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);

        /* Materialize L1 and BOTH #1849 consumers over everything — the state a store running that build is
           in. Only the #1869 layer is left short, so the gate's verdict turns entirely on the third consumer. */
        var span = (From: oldest.AddDays(-1), To: now.AddDays(1));
        foreach (var view in new[]
        {
            TimescaleSupport.QueryStoreStatsIntervalHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedDailyView,
        })
        {
            await RefreshRangeAsync(connection, view, span.From, span.To, ct);
        }

        var l1Floor = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);
        Assert.NotNull(l1Floor);
        foreach (var consumer in new[]
        {
            TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedDailyView,
        })
        {
            var floor = await FloorAsync(connection, consumer, ct);
            Assert.True(floor <= l1Floor,
                $"{consumer} must fully cover L1 here, or the test is not exercising the case where a gate " +
                $"reading only the #1849 consumers would wrongly arm ({consumer} {floor:O}, L1 {l1Floor:O}).");
        }

        /* Shallow-or-empty, not empty: refresh policies were stripped, but TimescaleDB runs a new policy's
           first check IMMEDIATELY (#1564), so under suite load a job can materialize the recent window before
           the removal lands. Shallow is the real precondition and it is what the gate itself reads. */
        var dayGrainSourceFloorBefore = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalDailyView, ct);
        Assert.True(dayGrainSourceFloorBefore is null || dayGrainSourceFloorBefore > l1Floor,
            $"the day-grain dedup layer should not yet reach L1's oldest bucket (L2 {dayGrainSourceFloorBefore:O}, L1 {l1Floor:O}).");

        /* ── 1. HELD. L1's purge stays paused because its THIRD consumer holds nothing. ── */
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.False(await IsArmedAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct),
            "the interval-grain HOURLY layer's purge armed while the interval-grain DAILY layer had " +
            "materialized nothing — the gate is reading only the #1849 consumers.");

        /* ── 2. Coverage arrives, exactly as the backfill verb's slices deliver it. ── */
        await RefreshRangeAsync(connection, TimescaleSupport.QueryStoreStatsIntervalDailyView, span.From, span.To, ct);

        var dayGrainSourceFloor = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalDailyView, ct);
        Assert.NotNull(dayGrainSourceFloor);
        Assert.True(dayGrainSourceFloor <= l1Floor,
            $"the day-grain dedup layer must reach at least as far back as L1 before the gate can release " +
            $"(L2 {dayGrainSourceFloor:O}, L1 {l1Floor:O})");

        /* ── 3. RELEASED by itself on the next sweep, with no manual step — the self-healing property #1759
               replaced a caveat with, which a third consumer must not break. ── */
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.True(await IsArmedAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct),
            "all three consumers now cover L1, so the held purge should have armed itself on this sweep.");
    }

    /// <summary>
    /// #1877, the case the gate could not reach: a policy this store ALREADY ARMED, whose coverage list then
    /// GREW a consumer. RED under an arm-only gate at step 3.
    ///
    /// <para>The test above proves the gate holds a policy it is creating. This one starts from the opposite
    /// state — L1's purge armed and running, every consumer caught up, exactly what a store on the #1849 build
    /// looks like — and then does what #1869 did to it: hands it a new consumer that holds nothing.
    /// <c>add_retention_policy(if_not_exists =&gt; true)</c> returns -1 for the policy that already exists, so
    /// nothing pauses it, and under the old code L1 kept purging its own source while the new consumer could
    /// only ever be backfilled as deep as what survived.</para>
    ///
    /// <para>The consumer is added by DROPping and rebuilding it (WITH NO DATA) rather than by editing the
    /// coverage list, because that is what an upgrading store experiences: the relation appears, born empty,
    /// under a policy that is already armed. Dropping it CASCADEs to the day-grain daily that reads it, and the
    /// ensure sweep rebuilds both — so this is the real #1869 upgrade shape, not a simulation of it.</para>
    ///
    /// <para>WATCHED (mutation): make the <c>Short</c> branch of the sweep skip its hold statement — i.e. put
    /// the gate back to arm-only — and step 3 goes red while every other assertion here still passes.</para>
    /// </summary>
    [Fact]
    public async Task ArmedIntervalPurge_IsReHeldWhenItsCoverageListGainsAnEmptyConsumer_ThenReleasesOnBackfill()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1877 re-hold test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var oldest = now.Date.AddDays(-SeedDepthDays);
        var span = (From: oldest.AddDays(-1), To: now.AddDays(1));

        for (var offset = 0; oldest.AddHours(offset) < now; offset += 3)
        {
            await SeedIntervalAsync(connection, oldest.AddHours(offset), intervalId: 4000 + offset, queryId: 17, planId: 19,
                firstExecution: oldest.AddHours(offset), snapshots: 3, secondsApart: 120,
                avgDurationUs: 100, avgCpuUs: 40, ct: ct);
        }

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);

        /* ── 1. A HEALTHY, FULLY-COVERED STORE. Every consumer materialized over everything L1 holds. ── */
        foreach (var view in new[]
        {
            TimescaleSupport.QueryStoreStatsIntervalHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedDailyView,
            TimescaleSupport.QueryStoreStatsIntervalDailyView,
            TimescaleSupport.QueryStoreStatsDayGrainDailyView,
        })
        {
            await RefreshRangeAsync(connection, view, span.From, span.To, ct);
        }

        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.True(await IsArmedAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct),
            "L1's purge must be ARMED here or the test is not starting from the state #1877 is about — an " +
            "already-armed policy is the only thing the arm-only gate could not re-hold.");

        /* ── 2. THE UPGRADE. The coverage list gains a consumer, born empty, exactly as #1869 delivered one to
               stores already running #1849. The policy is untouched and stays armed through this. ── */
        await using (var drop = new NpgsqlCommand(
            $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.QueryStoreStatsIntervalDailyView} CASCADE", connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);

        var l1Floor = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);
        Assert.NotNull(l1Floor);

        /* Shallow-or-empty rather than empty: the rebuild re-attaches a refresh policy whose first check runs
           IMMEDIATELY (#1564), so under suite load the recent window can materialize before the strip lands. */
        var newConsumerFloor = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalDailyView, ct);
        Assert.True(newConsumerFloor is null || newConsumerFloor > l1Floor,
            $"the rebuilt consumer should not already cover L1, or there is no coverage gap to re-hold on " +
            $"(new consumer {newConsumerFloor:O}, L1 {l1Floor:O}).");

        /* ── 3. RE-HELD. The property #1877 exists for: a running purge is STOPPED once its own coverage list
               no longer reaches, rather than left arming-only and quietly outrunning the new tier. ── */
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.False(await IsArmedAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct),
            "L1's purge is still armed while its newly-added consumer holds nothing — the gate is arm-only, so " +
            "it keeps dropping the only interval-grain copy of history that consumer has never seen (#1877).");

        /* ── 4. SELF-RELEASE, through the existing arming path and with no manual step — the same way a
               first-time hold releases. This is what makes the re-hold safe to ship: it is not a latch. ── */
        await RefreshRangeAsync(connection, TimescaleSupport.QueryStoreStatsIntervalDailyView, span.From, span.To, ct);

        var backfilledFloor = await FloorAsync(connection, TimescaleSupport.QueryStoreStatsIntervalDailyView, ct);
        Assert.NotNull(backfilledFloor);
        Assert.True(backfilledFloor <= l1Floor,
            $"the backfill must carry the new consumer past L1's oldest bucket before the gate can release " +
            $"(consumer {backfilledFloor:O}, L1 {l1Floor:O})");

        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.True(await IsArmedAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct),
            "the new consumer now covers L1, so the re-held purge should have armed itself on this sweep.");
    }

    /// <summary>
    /// The property that makes #1877's re-hold shippable at all: a probe that FAILS is not a coverage
    /// regression. #1877 filed the naive fix — "unsafe implies disarm" — as materially worse than the gap,
    /// because the coverage probe is deliberately fail-closed and one bad probe on a busy store would have
    /// stopped purging across every tier at once, growing disk without bound.
    ///
    /// <para>Driven with a genuinely missing relation, which is the strongest available stand-in for the
    /// timeout or permission blip that motivates the rule: the probe's statement throws 42P01 rather than
    /// returning a measurement, and there is no way for the gate to tell that apart from any other failure.</para>
    ///
    /// <para>Three separate things are asserted, because a hold-nothing implementation would pass the first
    /// alone: the armed policy whose probe threw stays ARMED, a DIFFERENT policy whose probe succeeded is still
    /// judged on its own merits in the same sweep, and a policy that is NOT yet armed when its probe throws
    /// stays PAUSED — the original fail-closed direction, which the tristate must not have inverted.</para>
    ///
    /// <para>WATCHED (mutation): return <c>Short</c> instead of <c>Unknown</c> from the probe's catch and the
    /// first assertion goes red — that mutation is precisely the fleet-wide purge stop #1877 refused to ship.</para>
    /// </summary>
    [Fact]
    public async Task RetentionSweep_TreatsAFailedProbeAsUnknown_NeverAsACoverageRegression()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1877 fail-closed test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var oldest = now.Date.AddDays(-SeedDepthDays);
        var span = (From: oldest.AddDays(-1), To: now.AddDays(1));

        for (var offset = 0; oldest.AddHours(offset) < now; offset += 3)
        {
            await SeedIntervalAsync(connection, oldest.AddHours(offset), intervalId: 5000 + offset, queryId: 23, planId: 29,
                firstExecution: oldest.AddHours(offset), snapshots: 3, secondsApart: 120,
                avgDurationUs: 100, avgCpuUs: 40, ct: ct);
        }

        /* The second subject: a raw tier on a completely different rollup family, so "one bad probe does not
           stop the others" is a claim about independent policies rather than about one relation twice. */
        await SeedQueryStatsAsync(connection, oldest, now, ct);

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);

        foreach (var view in new[]
        {
            TimescaleSupport.QueryStoreStatsIntervalHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedDailyView,
            TimescaleSupport.QueryStoreStatsIntervalDailyView,
            TimescaleSupport.QueryStoreStatsDayGrainDailyView,
            TimescaleSupport.QueryStatsHourlyView,
        })
        {
            await RefreshRangeAsync(connection, view, span.From, span.To, ct);
        }

        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.True(await IsArmedAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct),
            "L1's purge must be armed before the probe is broken, or there is nothing for a failed probe to " +
            "wrongly disarm.");
        Assert.True(await IsArmedAsync(connection, "query_stats", ct),
            "raw query_stats' purge must be armed here so the isolation assertion below means something.");

        /* THE BREAK: L1's third consumer stops existing, so its probe throws instead of measuring. */
        await using (var drop = new NpgsqlCommand(
            $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.QueryStoreStatsIntervalDailyView} CASCADE", connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);

        Assert.True(await IsArmedAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct),
            "a FAILED coverage probe disarmed a running purge — an unreadable store is not evidence that " +
            "coverage regressed, and treating it as one stops retention fleet-wide on one bad probe (#1877).");
        Assert.True(await IsArmedAsync(connection, "query_stats", ct),
            "one relation's failed probe changed a DIFFERENT policy's verdict — the per-policy judgment is " +
            "supposed to be independent.");

        /* And the original direction, unchanged: an unmeasurable store may not ARM either. Re-created from
           scratch (its policy removed, its consumer dropped) so the sweep meets it as a new policy. */
        await using (var removePolicy = new NpgsqlCommand(
            "SELECT remove_retention_policy('collect.query_stats', if_exists => true)", connection))
        {
            await removePolicy.ExecuteNonQueryAsync(ct);
        }

        await using (var drop = new NpgsqlCommand(
            $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.QueryStatsHourlyView} CASCADE", connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "query_stats", ct),
            "an unreadable coverage state must still answer 'not safe to drop' — the catalog sweep and the " +
            "tiered policy have to keep judging the same drop the same way (#1793).");

        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.False(await IsArmedAsync(connection, "query_stats", ct),
            "a policy whose coverage could not be established ARMED itself — the fail-closed direction the " +
            "tristate had to preserve.");
    }

    /// <summary>
    /// #1907 against the real TimescaleDB: what the corrected rollups compute when an interval arrives as ONE
    /// row per collection (the post-fix collector) versus as the two tied slice rows every pre-fix build
    /// stored, proving the defect reached the materialized rollup and that the fix resolves it exactly.
    ///
    /// <para>Query Store returns the flushed and the still-in-memory slice of one runtime_stats_interval_id as
    /// two ADDITIVE rows. Stored as-is they share the L1 GROUP BY key AND <c>collection_time</c>, so
    /// <c>last(execution_count, collection_time)</c> — the aggregate #1853 probed and chose because a CAGG
    /// cannot contain a window function — is ordering by a value identical for both rows. It cannot sum them
    /// and it cannot even choose between them. Whatever it returns is a SLICE, so the rollup understates the
    /// interval no matter which row wins; on the live Azure store that was 8 reported against 94 true.</para>
    ///
    /// <para>Both halves are asserted in ONE test on purpose. The post-fix number alone would pass against a
    /// build that never had the bug, and the pre-fix number alone says nothing about whether the fix works —
    /// it is the pair, in one store on one refresh, that shows the rollup arithmetic actually changed.</para>
    /// </summary>
    [Fact]
    public async Task CorrectedRollups_SeeTheWholeInterval_OnlyWhenTheSlicesAreCombinedAtCollection()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1907 slice-aggregation rollup test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var postFixHour = new DateTime(2026, 5, 6, 9, 0, 0, DateTimeKind.Unspecified);
        var preFixHour = postFixHour.AddHours(1);

        /* ── POST-FIX: the collector combines the slices, so each collection contributes exactly one row and
              execution_count is the interval's running TOTAL. 40 -> 90 -> 125 is the live SQL 2022 repro's
              arithmetic (100 flushed + 25 in memory = 125, cross-checked against dm_exec_procedure_stats)
              arriving over three cycles. Honest answer for the interval: its last snapshot, 125. ── */
        await SeedSnapshotsAsync(connection, intervalId: 7001, queryId: 61, planId: 91,
            intervalStart: postFixHour, avgDurationUs: 100, avgCpuUs: 50,
            snapshots:
            [
                (postFixHour.AddMinutes(5), 40L),
                (postFixHour.AddMinutes(10), 90L),
                (postFixHour.AddMinutes(15), 125L),
            ], ct: ct);

        /* A second interval in the same hour, so the hour total is a sum of two deduped intervals rather than
           one value that could be right by accident. */
        await SeedSnapshotsAsync(connection, intervalId: 7002, queryId: 62, planId: 92,
            intervalStart: postFixHour.AddMinutes(30), avgDurationUs: 200, avgCpuUs: 80,
            snapshots:
            [
                (postFixHour.AddMinutes(35), 10L),
                (postFixHour.AddMinutes(40), 30L),
            ], ct: ct);

        /* ── PRE-FIX: the SAME interval and the SAME true total of 125, but stored the way it used to be —
              the flushed slice (100) and the in-memory slice (25) as two rows at ONE collection_time. ── */
        await SeedTiedSlicesAsync(connection, intervalId: 7003, queryId: 63, planId: 93,
            intervalStart: preFixHour, collectionTime: preFixHour.AddMinutes(5),
            sliceCounts: [100L, 25L], avgDurationUs: 100, avgCpuUs: 50, ct: ct);

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);
        await RefreshAllAsync(connection, ct);

        /* ── 1. L1 collapses each interval to one row. Post-fix that row IS the interval's truth. ── */
        var l1 = await ReadIntervalRowsAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);

        var combined = Assert.Single(l1, r => r.QueryId == 61);
        Assert.Equal(125, combined.ExecutionCount);
        Assert.Equal(7001L, combined.IntervalId);

        var second = Assert.Single(l1, r => r.QueryId == 62);
        Assert.Equal(30, second.ExecutionCount);

        /* ── 2. THE HAND-COMPUTED HOUR. 125 + 30 = 155, and the weighted mean composes off the same two
              deduped rows: (125 x 100 + 30 x 200) / 155. ── */
        var correctedHour = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, postFixHour, ct);
        Assert.Equal(155, correctedHour.ExecutionCountSum);
        Assert.Equal(
            ((125d * 100d) + (30d * 200d)) / 155d,
            correctedHour.DurationWeightedSum / correctedHour.ExecutionCountSum,
            6);

        /* ── 3. THE SPLIT SLICES, same store, same refresh. L1 still produces ONE row — the two slices share
              its whole grouping key — but the value it carries is one SLICE, never the 125 they add up to.
              Which slice is not asserted, because last() has no tie-break and picking one is not something
              the engine promises; that it cannot reach 125 is the point, and it is what makes this an
              under-count rather than a coin flip with a correct face. ── */
        var split = Assert.Single(l1, r => r.QueryId == 63);
        Assert.Equal(7003L, split.IntervalId);
        Assert.Contains(split.ExecutionCount, new long[] { 100L, 25L });
        Assert.NotEqual(125, split.ExecutionCount);

        var splitHour = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, preFixHour, ct);
        Assert.Equal(split.ExecutionCount, splitHour.ExecutionCountSum);
        Assert.True(splitHour.ExecutionCountSum < 125,
            $"the split-slice hour must UNDER-report the interval's true 125, got {splitHour.ExecutionCountSum}");

        /* ── 4. The daily tier inherits both, so the defect and its fix are visible where history is kept
              indefinitely — which is exactly why the pre-fix residual needed its own issue (#1912) rather
              than an assumption that retention would carry it away. ── */
        var correctedDay = await ReadCompositeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedDailyView, postFixHour.Date, ct);
        Assert.Equal(155 + split.ExecutionCount, correctedDay.ExecutionCountSum);
    }

    /* ─────────────────────────── seeding + reading helpers ─────────────────────────── */

    /// <summary>
    /// Plants ONE Query Store interval as the pre-#1907 shape: several slice rows sharing a single
    /// <c>collection_time</c> and the entire dedup key, differing only in <c>execution_count</c> — the flushed
    /// slice and the still-in-memory slice as <c>sys.query_store_runtime_stats</c> hands them back and as
    /// every build before #1907 stored them.
    ///
    /// <para>Separate from <see cref="SeedSnapshotsAsync"/> because that helper derives one collection_id per
    /// collection_time, which is precisely what cannot be done here: these rows SHARE a collection time, so
    /// the id has to come from the slice's position instead.</para>
    /// </summary>
    private static async Task SeedTiedSlicesAsync(
        NpgsqlConnection connection, long intervalId, long queryId, long planId, DateTime intervalStart,
        DateTime collectionTime, long[] sliceCounts, long avgDurationUs, long avgCpuUs, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
VALUES
    ((extract(epoch FROM $1)::bigint * 100000) + ($2 * 10) + $10, $1, $3, 'SQL01', 'AdventureWorks', 'dbo.GetOrders', '0xABCD',
     $4, $5, 'Regular', 'PRIMARY', $2, $6, $6, $7, $8, $9, 900, 400)";

        for (var slice = 0; slice < sliceCounts.Length; slice++)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue(collectionTime);
            command.Parameters.AddWithValue(intervalId);
            command.Parameters.AddWithValue(TestServerId);
            command.Parameters.AddWithValue(queryId);
            command.Parameters.AddWithValue(planId);
            command.Parameters.AddWithValue(intervalStart);
            command.Parameters.AddWithValue(sliceCounts[slice]);
            command.Parameters.AddWithValue(avgDurationUs);
            command.Parameters.AddWithValue(avgCpuUs);
            command.Parameters.AddWithValue(slice);
            await command.ExecuteNonQueryAsync(ct);
        }
    }


    /// <summary>
    /// Plants one Query Store interval as <paramref name="snapshots"/> CUMULATIVE re-collections of the same
    /// interval — <c>execution_count</c> running 1..n, which is what the collector actually stores when it
    /// re-fetches an open interval. A null <paramref name="intervalId"/> plants the PRE-V41 shape, where the
    /// proxy is the only identity.
    /// </summary>
    private static async Task SeedIntervalAsync(
        NpgsqlConnection connection, DateTime firstCollection, long? intervalId, long queryId, long planId,
        DateTime firstExecution, int snapshots, int secondsApart, long avgDurationUs, long avgCpuUs,
        CancellationToken ct)
    {
        /* collection_id is the NOT NULL prefix id every collector table carries; one per snapshot, derived
           from the row's position so it is unique without needing a sequence. */
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
SELECT
    (extract(epoch FROM $1)::bigint * 100000) + ($4 * 1000) + n,
    $1 + (n * ($2 || ' seconds')::interval),
    $3, 'SQL01', 'AdventureWorks', 'dbo.GetOrders', '0xABCD',
    $4, $5, 'Regular', 'PRIMARY',
    $6, $7, $8,
    n, $9, $10, 900, 400
FROM generate_series(1, $11) AS n";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(firstCollection);
        command.Parameters.AddWithValue(secondsApart.ToString(CultureInfo.InvariantCulture));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(intervalId.HasValue ? intervalId.Value : DBNull.Value);
        command.Parameters.AddWithValue(intervalId.HasValue ? firstCollection : (object)DBNull.Value);
        command.Parameters.AddWithValue(firstExecution);
        command.Parameters.AddWithValue(avgDurationUs);
        command.Parameters.AddWithValue(avgCpuUs);
        command.Parameters.AddWithValue(snapshots);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Plants plain <c>query_stats</c> rows, one per hour — the second, independent raw tier the fail-closed
    /// test needs so "one relation's broken probe did not change another policy's verdict" is a claim about two
    /// unrelated rollup families rather than about one relation examined twice.
    /// </summary>
    private static async Task SeedQueryStatsAsync(
        NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
SELECT
    extract(epoch FROM g)::bigint * 100,
    g,
    $3, 'SQL01', 'AdventureWorks',
    decode(md5('pm1877'), 'hex'),
    decode(md5('pm1877handle'), 'hex'),
    1000, 2000, 10
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g", connection);

        insert.Parameters.AddWithValue(from);
        insert.Parameters.AddWithValue(to);
        insert.Parameters.AddWithValue(TestServerId);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Plants one interval as EXPLICIT (collection time, cumulative count) snapshots, rather than
    /// <see cref="SeedIntervalAsync"/>'s evenly-spaced 1..n run.
    ///
    /// <para>#1869 is entirely about WHERE an interval's snapshots fall relative to a bucket boundary, so the
    /// placement is the test rather than a detail of it. Spelling each snapshot out is what lets the
    /// expectations be read straight off the seed — 3 by 14:55 and 9 by 15:25 is the issue's own worked
    /// example, and a derived-from-spacing seed would hide exactly the fact under examination.</para>
    /// </summary>
    private static async Task SeedSnapshotsAsync(
        NpgsqlConnection connection, long intervalId, long queryId, long planId, DateTime intervalStart,
        long avgDurationUs, long avgCpuUs, (DateTime When, long Count)[] snapshots, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
VALUES
    ((extract(epoch FROM $1)::bigint * 100000) + $2, $1, $3, 'SQL01', 'AdventureWorks', 'dbo.GetOrders', '0xABCD',
     $4, $5, 'Regular', 'PRIMARY', $2, $6, $6, $7, $8, $9, 900, 400)";

        foreach (var (when, count) in snapshots)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue(when);
            command.Parameters.AddWithValue(intervalId);
            command.Parameters.AddWithValue(TestServerId);
            command.Parameters.AddWithValue(queryId);
            command.Parameters.AddWithValue(planId);
            command.Parameters.AddWithValue(intervalStart);
            command.Parameters.AddWithValue(count);
            command.Parameters.AddWithValue(avgDurationUs);
            command.Parameters.AddWithValue(avgCpuUs);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// Builds the aggregates, then strips every REFRESH policy the sweep attached.
    ///
    /// <para>TimescaleDB runs a new policy's first check IMMEDIATELY rather than on its next interval
    /// (#1564/#1567), so the background scheduler starts materializing within seconds of the ensure sweep.
    /// These tests refresh manually and deterministically and assert on exact floors and exact sums, so a
    /// concurrent background refresh is pure interference: it moves a floor mid-assertion and it collides
    /// with a manual refresh as 55P03. Same reason PayloadDimensionLiveTests strips them.</para>
    /// </summary>
    private static async Task EnsureAggregatesWithoutRefreshPoliciesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            await using var remove = new NpgsqlCommand(
                $"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)", connection);
            await remove.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task RefreshAllAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Dependency order, and it now runs THREE deep (#1869): L1 before everything that reads it, and the
           interval-grain DAILY before the day-grain daily that reads THAT. Refreshing out of order
           materializes nothing and reports success. The original pair is refreshed too, because half of what
           these tests prove is what the OLD views still report. */
        foreach (var view in new[]
        {
            TimescaleSupport.QueryStoreStatsIntervalHourlyView,
            TimescaleSupport.QueryStoreStatsHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedDailyView,
            TimescaleSupport.QueryStoreStatsDailyView,
            TimescaleSupport.QueryStoreStatsIntervalDailyView,
            TimescaleSupport.QueryStoreStatsDayGrainDailyView,
        })
        {
            /* Same 55P03 window as the ranged refresh below, and the same answer — this loop runs immediately
               after the aggregates are built, which is exactly when the scheduler is busiest. */
            await RefreshWithRetryAsync(
                () => new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}', NULL, NULL)", connection),
                ct);
        }
    }

    /// <summary>
    /// Materializes a range, retrying while TimescaleDB reports a CONCURRENT REFRESH (55P03).
    ///
    /// <para><b>Why the retry, and why it is not papering over anything.</b>
    /// <see cref="EnsureAggregatesWithoutRefreshPoliciesAsync"/> strips every refresh policy, but it can only do
    /// so AFTER <c>EnsureContinuousAggregatesAsync</c> has created them — and TimescaleDB runs a new policy's
    /// first check IMMEDIATELY rather than on its next interval (#1564/#1567). So there is a window, measured in
    /// the time between two statements, where the background scheduler is already materializing the view this
    /// test is about to refresh by hand, and the two collide as 55P03.</para>
    ///
    /// <para>The window is real but tiny, which is why it went unnoticed until CI lost the race: it is a
    /// scheduler-timing coincidence, so it fires under load rather than reproducibly, and more classes building
    /// aggregates concurrently on one cluster makes it likelier. Retrying is the correct response rather than a
    /// workaround — a refresh is idempotent, the losing side is simply early, and the ALTERNATIVE (asserting the
    /// first attempt wins) is asserting something the engine never promised. Bounded, so a genuine deadlock
    /// still fails the test rather than hanging it.</para>
    /// </summary>
    private static Task RefreshRangeAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct) =>
        RefreshWithRetryAsync(
            () =>
            {
                var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}', $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(from);
                refresh.Parameters.AddWithValue(to);
                return refresh;
            },
            ct);

    /// <summary>
    /// Runs a refresh, retrying while TimescaleDB reports a CONCURRENT REFRESH (55P03).
    ///
    /// <para><b>Why the retry, and why it is not papering over anything.</b>
    /// <see cref="EnsureAggregatesWithoutRefreshPoliciesAsync"/> strips every refresh policy, but it can only do
    /// so AFTER <c>EnsureContinuousAggregatesAsync</c> has created them — and TimescaleDB runs a new policy's
    /// first check IMMEDIATELY rather than on its next interval (#1564/#1567). So there is a window, the length
    /// of two statements, in which the background scheduler is already materializing the very view the test is
    /// about to refresh by hand, and the two collide as 55P03.</para>
    ///
    /// <para>The window is real but tiny, which is why it went unnoticed until CI lost the race: it is a
    /// scheduler-timing coincidence, so it fires under load rather than reproducibly, and more classes building
    /// aggregates concurrently on one cluster makes it likelier. Retrying is the correct response rather than a
    /// workaround — a refresh is idempotent, the losing side is merely early, and the alternative is asserting
    /// that the first attempt wins, which the engine never promised. Bounded, so a genuine stall still fails
    /// the test rather than hanging it.</para>
    /// </summary>
    private static async Task RefreshWithRetryAsync(Func<NpgsqlCommand> command, CancellationToken ct)
    {
        const int maxAttempts = 12;

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var refresh = command();
                await refresh.ExecuteNonQueryAsync(ct);
                return;
            }
            catch (PostgresException ex) when (ex.SqlState == "55P03" && attempt < maxAttempts)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
            }
        }
    }

    private sealed record IntervalRow(long QueryId, long? IntervalId, long ExecutionCount, long SampleCount);

    private static async Task<List<IntervalRow>> ReadIntervalRowsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var rows = new List<IntervalRow>();
        await using var command = new NpgsqlCommand(
            $"SELECT query_id, runtime_stats_interval_id, execution_count, sample_count FROM collect.{view} ORDER BY bucket, query_id", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new IntervalRow(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3)));
        }

        return rows;
    }

    private sealed record CompositeRow(long ExecutionCountSum, double DurationWeightedSum);

    private static async Task<CompositeRow> ReadCompositeAsync(NpgsqlConnection connection, string view, DateTime bucket, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT sum(execution_count_sum)::bigint, sum(duration_us_weighted_sum)::double precision FROM collect.{view} WHERE bucket = $1", connection);
        command.Parameters.AddWithValue(bucket);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"no row in {view} for bucket {bucket:O}");
        Assert.False(reader.IsDBNull(0), $"{view} materialized nothing for bucket {bucket:O}");
        return new CompositeRow(reader.GetInt64(0), reader.GetDouble(1));
    }

    private static async Task<DateTime?> FloorAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{view}", connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull or null ? null : (DateTime)value;
    }

    private static async Task<DateTime?> RawOldestAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT min(collection_time) FROM collect.query_store_stats", connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull or null ? null : (DateTime)value;
    }

    private static async Task<bool> IsArmedAsync(NpgsqlConnection connection, string relation, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT COALESCE(bool_or(j.scheduled), false)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = $1", connection);
        command.Parameters.AddWithValue(relation);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
