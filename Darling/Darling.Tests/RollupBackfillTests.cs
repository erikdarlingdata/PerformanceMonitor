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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1759 Phase 2: the staged, disk-preflighted rollup backfill.
///
/// <para>The hazard this whole verb is shaped around: the #1680 arming gate is ALL-OR-NOTHING, so a store with a
/// year of raw must materialize the WHOLE history before the first purge arms and reclaims anything. Peak disk
/// comes BEFORE any relief, on the exact stores worst affected. Every pin below defends one of the three things
/// that keeps that from becoming a disk-exhaustion event: a preflight that refuses with numbers, an order that
/// cannot silently under-cover, and a convergence check that believes DATA rather than a returned success.</para>
/// </summary>
public sealed class RollupBackfillTests
{
    private static readonly DateTime Now = new(2026, 7, 25, 0, 0, 0, DateTimeKind.Unspecified);

    private static DateTime DaysAgo(double days) => Now.AddDays(-days);

    private const long Gb = 1024L * 1024 * 1024;

    /* ─────────────────────────── the disk preflight ─────────────────────────── */

    /// <summary>
    /// WATCHED (mutation): bypass the preflight and the verb walks into insufficient space. The refusal is the
    /// only thing standing between "materialize a year of history" and a full volume on a production store that
    /// was already down to ~150 GB free — and because peak disk comes before ANY reclaim, filling the volume
    /// does not even buy the space it was trying to free.
    /// </summary>
    [Theory]
    /* Comfortably clear: estimate + 25% headroom + 10 GB reserve all fit. */
    [InlineData(10, 100, true)]
    /* Exactly on the line (10 GB estimate needs 12.5 + 10 = 22.5 GB). */
    [InlineData(10, 23, true)]
    [InlineData(10, 22, false)]
    /* The field shape: a large backfill against a nearly-full volume. */
    [InlineData(200, 150, false)]
    /* Free space that would cover the estimate but eat the reserve — still refused. PostgreSQL needs room for
       WAL and temp files, and a store driven to zero is a worse outcome than a refused backfill. */
    [InlineData(100, 110, false)]
    public void HasRoom_RequiresTheEstimatePlusHeadroomPlusTheReserve(long estimateGb, long freeGb, bool expected) =>
        Assert.Equal(expected, RollupBackfill.HasRoom(estimateGb * Gb, freeGb * Gb));

    /// <summary>The reserve is never negotiable: even a zero-byte backfill leaves it alone, so the arithmetic
    /// cannot degenerate into "any free space will do".</summary>
    [Fact]
    public void RequiredBytes_NeverDropsBelowTheReserve()
    {
        Assert.Equal(RollupBackfill.ReserveBytes, RollupBackfill.RequiredBytes(0));
        Assert.True(RollupBackfill.RequiredBytes(100 * Gb) > 100 * Gb + RollupBackfill.ReserveBytes,
            "the safety factor must add headroom ON TOP of the estimate, not replace it");
    }

    /// <summary>
    /// The refusal has to be usable by someone at 2am who did not read this issue. It must name the SHORTFALL
    /// (not just "insufficient space"), and it must give both real options — grow the disk, or accept waiting —
    /// including the part that is easy to get wrong: waiting is SAFE. Nothing is being lost while the purges are
    /// held, so an operator who cannot grow the volume today needs to know they are not in an emergency.
    /// </summary>
    [Fact]
    public void FormatDiskRefusal_NamesTheShortfallAndBothOptions()
    {
        var refusal = DarlingCliCommands.FormatDiskRefusal(estimatedBytes: 200 * Gb, freeBytes: 150 * Gb);

        Assert.Contains("REFUSING", refusal, StringComparison.Ordinal);
        Assert.Contains("SHORT BY", refusal, StringComparison.Ordinal);
        Assert.Contains("200 GB", refusal, StringComparison.Ordinal);
        Assert.Contains("150 GB", refusal, StringComparison.Ordinal);

        /* Option 1: grow the disk, by a NAMED amount. */
        Assert.Contains("Grow the volume by at least", refusal, StringComparison.Ordinal);

        /* Option 2: waiting is safe — and says WHY, so it does not read as a shrug. */
        Assert.Contains("Accept waiting", refusal, StringComparison.Ordinal);
        Assert.Contains("nothing is being lost", refusal, StringComparison.Ordinal);

        /* And the honest cost of waiting, so it is not oversold either. */
        Assert.Contains("Raw keeps growing", refusal, StringComparison.Ordinal);
    }

    /// <summary>Sizes are formatted invariantly — this text goes to an operator's console and into a refusal, so
    /// a machine with a comma decimal separator must not render "12,5 GB" in one place and "12.5 GB" in
    /// another.</summary>
    [Fact]
    public void FormatBytes_IsInvariantAndHumanReadable()
    {
        Assert.Equal("512 B", RollupBackfillPlan.FormatBytes(512));
        Assert.Equal("1 KB", RollupBackfillPlan.FormatBytes(1024));
        Assert.Equal("1.5 GB", RollupBackfillPlan.FormatBytes(Gb + (Gb / 2)));
        Assert.Equal("2 TB", RollupBackfillPlan.FormatBytes(2L * 1024 * Gb));
    }

    /* ─────────────────────────── the plan ─────────────────────────── */

    /// <summary>
    /// IDEMPOTENT AND RESUMABLE, which is one property with two names: the plan is always computed from the
    /// MEASURED coverage floor, so a completed run converges to a no-op and an interrupted one continues from
    /// where it stopped. Without this the verb would either re-materialize a year of history on every
    /// invocation, or need external state to remember where it was.
    /// </summary>
    [Fact]
    public void Plan_IsComputedFromTheMeasuredFloor_SoItResumesAndConverges()
    {
        /* A fresh #1759 store: raw reaches back a year, the rollup only to the policy's 3-day window. */
        var initial = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: DaysAgo(365), coverageOldestUtc: DaysAgo(3),
            materializedBuckets: 72, materializedBytes: 72 * 1024 * 1024,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(initial.IsComplete);
        Assert.Equal(DaysAgo(365).Date, initial.FromUtc);
        Assert.Equal(DaysAgo(3), initial.ToUtc);

        /* Interrupted half way: the next pass plans only the REMAINING span, not the whole thing again. */
        var resumed = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: DaysAgo(365), coverageOldestUtc: DaysAgo(180),
            materializedBuckets: 4_500, materializedBytes: 4_500L * 1024 * 1024,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(resumed.IsComplete);
        Assert.True(resumed.Slices < initial.Slices, "a resumed run must plan strictly less work than the first");
        Assert.True(resumed.EstimatedBytes < initial.EstimatedBytes);

        /* Complete: coverage reaches raw, so there is nothing to do and the verb says so rather than working. */
        var complete = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: DaysAgo(365), coverageOldestUtc: DaysAgo(365),
            materializedBuckets: 8_760, materializedBytes: 9 * Gb,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.True(complete.IsComplete);
        Assert.Equal(0, complete.Slices);
        Assert.NotNull(complete.SkipReason);

        /* Coverage DEEPER than raw (raw has already been purged past it) is also complete — not negative work. */
        var deeper = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: DaysAgo(4), coverageOldestUtc: DaysAgo(21),
            materializedBuckets: 500, materializedBytes: Gb, rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.True(deeper.IsComplete);
    }

    /// <summary>A fresh store has no raw rows, so there is nothing to materialize — and no plan that could
    /// divide by an empty span.</summary>
    [Fact]
    public void Plan_EmptyRawTable_IsComplete()
    {
        var plan = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView, sourceOldestUtc: null, coverageOldestUtc: null,
            materializedBuckets: 0, materializedBytes: 0, rawBytes: 0, bucketWidth: TimeSpan.FromHours(1));

        Assert.True(plan.IsComplete);
        Assert.Equal(0, plan.EstimatedBytes);
    }

    /// <summary>
    /// The estimate is CALIBRATED from what the rollup has already materialized wherever a sample exists — which
    /// on the affected stores it always does, since their refresh policies have been materializing a trailing
    /// 3-day window all along. With no sample it falls back to a bound and FLAGS itself, because an operator
    /// deciding whether to grow a volume needs to know which of the two numbers they are reading.
    /// </summary>
    [Fact]
    public void Plan_CalibratesFromTheMaterializedSample_AndFlagsWhenItCannot()
    {
        /* 72 buckets occupying 72 MB → 1 MB/bucket. A year of hourly buckets is 8,760 of them. */
        var calibrated = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: DaysAgo(365), coverageOldestUtc: DaysAgo(3),
            materializedBuckets: 72, materializedBytes: 72 * 1024 * 1024,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.True(calibrated.Calibrated);
        /* ~362 days of hourly buckets at 1 MB each — within a bucket of 8,688. */
        Assert.InRange(calibrated.EstimatedBytes, 8_600L * 1024 * 1024, 8_800L * 1024 * 1024);

        /* Nothing materialized: no sample, so the estimate is an upper bound and says so. */
        var uncalibrated = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: DaysAgo(365), coverageOldestUtc: null,
            materializedBuckets: 0, materializedBytes: 0,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(uncalibrated.Calibrated);

        /* And it errs HIGH. The two mistakes are not symmetric: over-estimating refuses a backfill that would
           have fit (recoverable — grow the disk, or re-run once the policy has left a sample); under-estimating
           fills a production volume (not recoverable in the moment). */
        Assert.True(uncalibrated.EstimatedBytes > calibrated.EstimatedBytes,
            "an uncalibrated estimate must bound HIGH, since guessing low is the failure that fills the volume");
    }

    /// <summary>
    /// The planned range must CLOSE on a bucket boundary, not just open on one. Found on the gated live leg,
    /// not by reading: TimescaleDB rejects a refresh window narrower than one bucket outright
    /// (<c>22023 refresh window too small</c>), and the ragged tail of a day-wide slice IS narrower than one
    /// bucket for a DAILY rollup, whose bucket is a day. The range end is a coverage floor or "now" — an
    /// arbitrary instant that lands mid-bucket most of the time — so this was not an edge case, it aborted the
    /// whole daily tier of the run.
    /// </summary>
    [Theory]
    /* Daily grain: the end truncates back to midnight, leaving whole days. */
    [InlineData(24, 9.5, 9)]
    [InlineData(24, 10.0, 10)]
    [InlineData(24, 0.9, 0)]
    /* Hourly grain: the same rule at a finer bucket — the tail keeps whole hours. */
    [InlineData(1, 9.99, 9.958333)]
    public void Plan_TruncatesTheRangeEndToAWholeNumberOfBuckets(int bucketHours, double coverageDaysAgo, double expectedSpanDays)
    {
        var plan = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsDailyView,
            sourceOldestUtc: Now.Date.AddDays(-10), coverageOldestUtc: Now.Date.AddDays(-10 + coverageDaysAgo),
            materializedBuckets: 10, materializedBytes: 10 * 1024 * 1024,
            rawBytes: Gb, bucketWidth: TimeSpan.FromHours(bucketHours));

        if (expectedSpanDays == 0)
        {
            Assert.True(plan.IsComplete, "a range shorter than one bucket is nothing to do, never a sub-bucket refresh");
            return;
        }

        Assert.False(plan.IsComplete);

        var span = plan.ToUtc - plan.FromUtc;
        Assert.Equal(expectedSpanDays, span.TotalDays, 4);

        /* The invariant behind the arithmetic: the span is a whole number of buckets, so no slice — including
           the last — can ever be narrower than one. */
        Assert.Equal(0, span.Ticks % TimeSpan.FromHours(bucketHours).Ticks);

        /* P2b, RE-VERIFIED ACROSS THE DESCENDING REWRITE. The alignment property was originally established
           against ascending slicing, where the ragged remainder sat at the TOP of the range. Reversing the
           order moved it to the BOTTOM — a different slice, produced by a different branch of the loop (the
           clamp to `from` rather than the clamp to `to`) — so the property had to be re-established, not
           assumed to have carried. Both ENDS of every slice are checked, not just the width: a slice one
           bucket wide but half a bucket out of phase is still refused with 22023. */
        var bucket = TimeSpan.FromHours(bucketHours);
        foreach (var (sliceFrom, sliceTo) in RollupBackfill.Slices(plan.FromUtc, plan.ToUtc))
        {
            Assert.True((sliceTo - sliceFrom) >= bucket,
                $"slice {sliceFrom:O} -> {sliceTo:O} is narrower than one {bucketHours}h bucket; TimescaleDB rejects that with 22023");

            Assert.Equal(0, (sliceFrom - plan.FromUtc).Ticks % bucket.Ticks);
            Assert.Equal(0, (sliceTo - plan.FromUtc).Ticks % bucket.Ticks);
        }

        /* And the remainder really is at the bottom now — the pin that would catch a silent revert to
           ascending, which would put it back on top and re-open the false-DONE path. */
        var slices = RollupBackfill.Slices(plan.FromUtc, plan.ToUtc).ToArray();
        Assert.Equal(plan.FromUtc, slices[^1].FromUtc);
        Assert.Equal(plan.ToUtc, slices[0].ToUtc);
    }

    /// <summary>
    /// THE ESTIMATOR'S UNITS MUST MATCH. The calibration divides <c>materialized_bytes</c> by the sample's
    /// bucket count to get bytes-per-BUCKET, then multiplies by a count of BUCKETS. Feeding it a ROW count
    /// instead — which is what <c>count(*)</c> returns, and what shipped — divides by a number inflated by the
    /// distinct queries seen per hour, so the estimate comes out under by exactly that factor. Measured 205x
    /// under on a 200-row/hour store; a real fleet box is far worse.
    ///
    /// <para>This is the direction the whole preflight exists to prevent: guessing high refuses a backfill that
    /// would have fit (recoverable), guessing low fills a production volume (not). So the probe is pinned to
    /// <c>count(DISTINCT bucket)</c> and the arithmetic is pinned against a sample where rows and buckets
    /// differ — a 1:1 fixture cannot tell the two apart, which is precisely why the bug shipped.</para>
    /// </summary>
    [Fact]
    public void Estimate_IsPerBucket_NotPerRow()
    {
        /* A sample of 10 BUCKETS occupying 100 MB → 10 MB per bucket. On a store holding 20 rows per bucket
           the row count would be 200, and a row-fed estimator would compute 0.5 MB per bucket — 20x under. */
        const long sampleBuckets = 10;
        const long sampleBytes = 100L * 1024 * 1024;

        var plan = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: Now.AddHours(-100), coverageOldestUtc: Now,
            materializedBuckets: sampleBuckets, materializedBytes: sampleBytes,
            rawBytes: 500 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.True(plan.Calibrated);

        /* bytes-per-bucket x buckets-to-add, within a bucket's rounding. */
        var expectedPerBucket = (double)sampleBytes / sampleBuckets;
        Assert.Equal(expectedPerBucket * plan.BucketsToAdd, plan.EstimatedBytes, expectedPerBucket);

        /* And the pin that makes the fix visible in the SQL: a ROW count here is the shipped defect. */
        var probe = RollupBackfill.ProbeSql(TimescaleSupport.QueryStatsHourlyView, "query_stats", "collection_time");
        Assert.Contains($"count(DISTINCT bucket) FROM collect.{TimescaleSupport.QueryStatsHourlyView}", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("count(*)", probe, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE PREFLIGHT MUST BOUND THE WHOLE JOB, including work a mid-run re-plan discovers.
    ///
    /// <para>A hierarchical daily converges to its HOURLY, and that hourly is deepened earlier in the SAME run.
    /// The preflight has to total everything before committing to any of it, so it estimates from floors taken
    /// before a single slice has run — and for a daily that pre-backfill floor is shallower than what the run
    /// will actually aim at. Estimating from it under-counts exactly the region the run then writes, on the
    /// store shape (deep raw, everything held) where free space is tightest and the printed number is the
    /// commitment the operator accepted.</para>
    ///
    /// <para>The property under test is <b>printed bound &gt;= actual work</b>, not any particular estimator:
    /// planning with the eventual floor must span at least as much as planning with the floor the run will
    /// eventually see. Asserted on buckets and bytes, since the operator is shown both.</para>
    /// </summary>
    [Fact]
    public void Preflight_BoundsTheWorkAMidRunReplanWillFind()
    {
        /* The sick-store shape: raw holds a year (purges held), the hourly has only its 3-day policy window,
           and the daily is empty. The run will deepen the hourly to raw FIRST, so by the time the daily runs
           its source reaches back a year — not the three days the preflight would naively have seen. */
        var rawOldest = DaysAgo(365);
        var hourlyFloorAtPreflight = DaysAgo(3);

        var eventual = RollupBackfill.EventualSourceFloor(hourlyFloorAtPreflight, rawOldest);
        Assert.Equal(rawOldest, eventual);

        var preflight = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsDailyView, eventual, coverageOldestUtc: null,
            materializedBuckets: 10, materializedBytes: 10L * 1024 * 1024,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromDays(1));

        /* What the run re-plans once the hourly has been deepened. */
        var afterReplan = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsDailyView, rawOldest, coverageOldestUtc: null,
            materializedBuckets: 10, materializedBytes: 10L * 1024 * 1024,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromDays(1));

        Assert.True(preflight.BucketsToAdd >= afterReplan.BucketsToAdd,
            $"the preflight planned {preflight.BucketsToAdd} buckets but the run will do {afterReplan.BucketsToAdd} — the disk gate was cleared against a number smaller than the job");

        /* Bytes too, since the operator is shown both. Note this is the ESTIMATE — deterministic arithmetic
           over the planned span times a per-bucket sample — NOT bytes measured off the store. A live
           byte assertion would be flaky, because compression and page slack make the same span occupy
           different amounts on different days. */
        Assert.True(preflight.EstimatedBytes >= afterReplan.EstimatedBytes,
            $"the preflight estimated {preflight.EstimatedBytes} bytes but the run will write about {afterReplan.EstimatedBytes}");

        /* THE PROPERTY STATED THE OTHER WAY ROUND, and the more direct form: the daily's up-front plan must
           span to the hourly's TARGET, not to where the hourly currently stops. Compare against the hourly's
           OWN up-front plan on the same fixture — if the daily starts no earlier than the hourly will, the
           preflight has already counted every bucket the run can reach. */
        var hourlyPlan = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView, rawOldest, hourlyFloorAtPreflight,
            materializedBuckets: 72, materializedBytes: 72L * 1024 * 1024,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(hourlyPlan.IsComplete, "the hourly must have work, or the daily's target cannot move under it");
        Assert.True(preflight.FromUtc <= hourlyPlan.FromUtc,
            $"the daily's up-front plan starts at {preflight.FromUtc:O} but its hourly will reach {hourlyPlan.FromUtc:O} — planned against the source's CURRENT floor rather than the source's target");

        /* And the bound is not vacuous: taking the pre-backfill floor instead really is smaller, which is what
           makes this a bound rather than an identity. */
        var naive = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsDailyView, hourlyFloorAtPreflight, coverageOldestUtc: null,
            materializedBuckets: 10, materializedBytes: 10L * 1024 * 1024,
            rawBytes: 145 * Gb, bucketWidth: TimeSpan.FromDays(1));

        Assert.True(naive.BucketsToAdd < afterReplan.BucketsToAdd,
            "the pre-backfill floor must under-count the run, or this fixture is not exercising the gap");

        /* An HOURLY is unaffected — its source IS raw, so both candidates are the same instant. */
        Assert.Equal(rawOldest, RollupBackfill.EventualSourceFloor(rawOldest, rawOldest));

        /* Nulls degrade to whichever side is known rather than collapsing the bound to nothing. */
        Assert.Equal(rawOldest, RollupBackfill.EventualSourceFloor(null, rawOldest));
        Assert.Equal(hourlyFloorAtPreflight, RollupBackfill.EventualSourceFloor(hourlyFloorAtPreflight, null));
        Assert.Null(RollupBackfill.EventualSourceFloor(null, null));
    }

    /// <summary>
    /// A source timestamp can be garbage, and one garbage row must not become a plan. Observed live: a single
    /// stray ancient value produced a 739,825-slice plan spanning 3.7 days of pure planning CPU before anything
    /// was materialized. Refusing beats spanning, and the refusal has to NAME the timestamp — a row that old is
    /// corruption someone has to go and look at, not history to back fill.
    /// </summary>
    [Fact]
    public void Plan_RefusesAnAbsurdSpan_AndNamesTheOffendingTimestamp()
    {
        var epochGarbage = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        var plan = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: epochGarbage, coverageOldestUtc: Now,
            materializedBuckets: 10, materializedBytes: 10 * 1024 * 1024,
            rawBytes: Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.True(plan.IsComplete, "a refused plan must not be handed to the slice runner");
        Assert.Equal(0, plan.Slices);
        Assert.NotNull(plan.Refusal);
        Assert.Contains("1970-01-01", plan.Refusal, StringComparison.Ordinal);
        Assert.Contains("corrupt row", plan.Refusal, StringComparison.Ordinal);

        /* A refusal is NOT a skip: "nothing to do" is a success an operator scrolls past, and collapsing the
           two would bury the one line that needs acting on. */
        Assert.Null(plan.SkipReason);

        /* Ten years of real history still plans normally — the clamp must not refuse a long-lived store. */
        var longLived = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsHourlyView,
            sourceOldestUtc: Now.AddDays(-3_000), coverageOldestUtc: Now,
            materializedBuckets: 10, materializedBytes: 10 * 1024 * 1024,
            rawBytes: Gb, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(longLived.IsComplete);
        Assert.Null(longLived.Refusal);
    }

    /// <summary>The time budget uses the throughput actually measured on the field host class, so an operator can
    /// decide whether this fits tonight's window before starting it.</summary>
    [Fact]
    public void EstimatedDuration_UsesTheMeasuredFieldThroughput()
    {
        /* 16 MB/s → 16 MB in one second, ~1 GB per minute, ~18 hours for a 1 TB materialization. */
        Assert.Equal(1, RollupBackfill.EstimatedDuration(16 * 1024 * 1024).TotalSeconds, 3);
        Assert.InRange(RollupBackfill.EstimatedDuration(1024 * Gb).TotalHours, 17, 19);
    }

    /* ─────────────────────────── slicing ─────────────────────────── */

    /// <summary>
    /// Slices run NEWEST-FIRST, contiguously, covering the range exactly.
    ///
    /// <para>The direction is the data-safety property, not a preference. <c>min(bucket)</c> is the resume
    /// point, the completion verdict, and what the #1680 arming gate reads before letting a raw purge drop
    /// chunks. Oldest-first drove that single number to its FINAL value on the first slice, so every later
    /// slice was invisible to all three — an interrupted run left holes, the next run said "nothing to do,
    /// DONE", and the gate armed the 4-day purge over a window raw held the only copy of. Descending makes
    /// the floor a true cursor: it reaches the bottom only when the last slice has run.</para>
    /// </summary>
    [Fact]
    public void Slices_AreNewestFirst_Contiguous_AndCoverTheRangeExactly()
    {
        var from = DaysAgo(10);
        var to = DaysAgo(3);
        var slices = RollupBackfill.Slices(from, to).ToArray();

        Assert.Equal(7, slices.Length);

        /* Starts at the TOP (the current coverage floor) and ends at the BOTTOM (raw's oldest). */
        Assert.Equal(to, slices[0].ToUtc);
        Assert.Equal(from, slices[^1].FromUtc);

        for (var i = 1; i < slices.Length; i++)
        {
            Assert.Equal(slices[i - 1].FromUtc, slices[i].ToUtc);
            Assert.True(slices[i].FromUtc < slices[i - 1].FromUtc, "slices must run newest-first");
        }

        /* No gaps and no overlap: the slices tile [from, to) exactly. */
        Assert.Equal(to - from, slices.Aggregate(TimeSpan.Zero, (total, s) => total + (s.ToUtc - s.FromUtc)));

        /* A ragged remainder lands at the BOTTOM now (the last slice run), never overshooting the target —
           materializing past it is wasted disk on the one operation whose whole risk is disk. */
        var ragged = RollupBackfill.Slices(from, from.AddHours(30)).ToArray();
        Assert.Equal(2, ragged.Length);
        Assert.Equal(from.AddHours(30), ragged[0].ToUtc);
        Assert.Equal(from, ragged[^1].FromUtc);
        Assert.Equal(TimeSpan.FromHours(6), ragged[^1].ToUtc - ragged[^1].FromUtc);

        /* An empty or inverted range yields nothing rather than looping. */
        Assert.Empty(RollupBackfill.Slices(to, to));
        Assert.Empty(RollupBackfill.Slices(to, from));
    }

    /// <summary>
    /// THE INVARIANT THE ORDER BUYS, stated as arithmetic: after running the first N slices of a plan, the
    /// lowest bucket touched equals the bottom of the range ONLY when N is every slice. That is why
    /// <c>floor &lt;= raw_oldest</c> can be trusted as a completion verdict — under oldest-first it was true
    /// after the FIRST slice, which is exactly how an interrupted run reported success over a hole.
    /// </summary>
    [Fact]
    public void PartialRun_NeverReachesTheBottomOfTheRange_UntilTheLastSlice()
    {
        var from = DaysAgo(10);
        var to = DaysAgo(3);
        var slices = RollupBackfill.Slices(from, to).ToArray();

        for (var completed = 1; completed < slices.Length; completed++)
        {
            var lowestTouched = slices.Take(completed).Min(s => s.FromUtc);
            Assert.True(lowestTouched > from,
                $"after {completed} of {slices.Length} slices the plan's bottom was already reached — an " +
                "interrupted run would be indistinguishable from a complete one, which is #1759's data-loss path");
        }

        Assert.Equal(from, slices.Min(s => s.FromUtc));
    }

    /// <summary>One slice is ONE source chunk. That is what bounds the lock window: a wider slice would sit
    /// across more chunks and, on a store whose compression policy is working the same ones, hold a window long
    /// enough to matter (the #1778 deadlock watch).</summary>
    [Fact]
    public void SliceWidth_IsOneSourceChunk() =>
        Assert.Equal(TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays), RollupBackfill.SliceWidth);

    /* ─────────────────────────── hierarchical order ─────────────────────────── */

    /// <summary>
    /// WATCHED (mutation): invert the target order so dailies run before their hourlies, and the failure is
    /// SILENT rather than loud. A daily continuous aggregate reads its HOURLY one, so refreshing a daily over a
    /// range whose hourly has not been materialized reads an empty source, materializes nothing, returns
    /// success — and CONSUMES the invalidation records covering that range, so a later correct-order pass
    /// no-ops over the hole and reports success too. The order is a correctness invariant, not a preference.
    /// </summary>
    [Fact]
    public void Targets_RunEveryRawSourcedRollupBeforeAnyThatReadsOne()
    {
        var firstHierarchical = Array.FindIndex(RollupBackfill.Targets, t => t.IsHierarchical);
        var lastRawSourced = Array.FindLastIndex(RollupBackfill.Targets, t => !t.IsHierarchical);

        Assert.True(firstHierarchical > lastRawSourced,
            "every raw-sourced rollup must be refreshed before any rollup that reads one — the reverse order " +
            "materializes nothing while reporting success AND burns the invalidations that would have let a " +
            "later pass repair it.");

        /* Each hierarchical rollup's ACTUAL SOURCE appears earlier in the list. Asserted against Source
           rather than by rewriting "_daily" to "_hourly" in the view name, which is what this used to do:
           that guess silently held only while every hierarchical rollup was a daily named after its parent.
           query_store_stats_corrected_daily breaks both halves — it is a daily whose source is
           query_store_stats_INTERVAL_hourly (its sibling's parent, not its sibling), so the name rewrite
           would have looked for a view that does not exist and Assert.Contains would have failed for the
           wrong reason. Reading the edge the backfill actually traverses cannot drift from it. */
        foreach (var hierarchical in RollupBackfill.Targets.Where(t => t.IsHierarchical))
        {
            var sourceIndex = Array.FindIndex(RollupBackfill.Targets,
                t => string.Equals(t.View, hierarchical.Source, StringComparison.Ordinal));

            Assert.True(sourceIndex >= 0,
                $"{hierarchical.View} reads {hierarchical.Source}, which is not itself a backfill target — it " +
                "would never be materialized, so this rollup could never converge.");

            Assert.True(sourceIndex < Array.FindIndex(RollupBackfill.Targets,
                    t => string.Equals(t.View, hierarchical.View, StringComparison.Ordinal)),
                $"{hierarchical.View} must be refreshed after its source {hierarchical.Source}.");
        }
    }

    /// <summary>
    /// A rollup's BUCKET WIDTH is its own fact, not a synonym for "is hierarchical" (#1849). It is divided
    /// into the backfill window to get a bucket count, which drives the slice count AND the disk estimate, so
    /// a width that is 24x too wide under-counts buckets by 24x — and under-estimating is the one direction
    /// the preflight exists to prevent. <c>query_store_stats_corrected_hourly</c> is the case that separates
    /// the two: it reads another rollup (hierarchical) but its buckets are HOURS.
    /// </summary>
    [Fact]
    public void Targets_CarryTheirOwnBucketWidth_NotOneInferredFromHierarchy()
    {
        var correctedHourly = RollupBackfill.Targets.Single(t =>
            string.Equals(t.View, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, StringComparison.Ordinal));

        Assert.True(correctedHourly.IsHierarchical);
        Assert.Equal(TimeSpan.FromHours(1), correctedHourly.BucketWidth);

        /* Every target's width matches the one declared beside its view in the single source list. */
        foreach (var target in RollupBackfill.Targets)
        {
            var declared = TimescaleSupport.RollupViews.Single(r =>
                string.Equals(r.View, target.View, StringComparison.Ordinal));
            Assert.Equal(declared.BucketWidth, target.BucketWidth);
        }
    }

    /// <summary>
    /// The Query Store dedup chain is THREE levels deep since #1869 — raw → interval_hourly → interval_daily →
    /// daygrain_daily — and the ordering must hold across both hops, not just the first. Every other rollup
    /// here is one hop from raw, so "hierarchical" was enough to sort them; two hierarchical rollups where one
    /// reads the other are indistinguishable to that boolean, and got their order from the DECLARATION ORDER of
    /// RollupViews. This pins the edges the backfill actually traverses, so re-declaring or re-pointing a level
    /// fails here rather than silently materializing nothing while reporting success.
    /// </summary>
    [Fact]
    public void Targets_OrderTheThreeLevelQueryStoreChain_ByItsRealEdges()
    {
        int IndexOf(string view) => Array.FindIndex(RollupBackfill.Targets,
            t => string.Equals(t.View, view, StringComparison.Ordinal));

        var l1 = IndexOf(TimescaleSupport.QueryStoreStatsIntervalHourlyView);
        var l2 = IndexOf(TimescaleSupport.QueryStoreStatsIntervalDailyView);
        var l3 = IndexOf(TimescaleSupport.QueryStoreStatsDayGrainDailyView);

        Assert.True(l1 >= 0 && l2 >= 0 && l3 >= 0, "all three levels of the corrected chain must be backfill targets.");
        Assert.True(l1 < l2, "the interval-grain DAILY reads the interval-grain HOURLY, so it must run after it.");
        Assert.True(l2 < l3, "the day-grain daily reads the interval-grain daily, so it must run after it.");

        /* And the edges themselves, so the ordering above is checking the real graph rather than a coincidence
           of names: the day-grain daily's source is L2, NOT L1 (which is what its corrected sibling reads). */
        var dayGrain = RollupBackfill.Targets[l3];
        Assert.Equal(TimescaleSupport.QueryStoreStatsIntervalDailyView, dayGrain.Source);
        Assert.Equal(TimescaleSupport.QueryStoreStatsIntervalHourlyView, RollupBackfill.Targets[l2].Source);
        Assert.Equal("query_store_stats", dayGrain.RawTable);

        /* Both new levels are DAY-bucketed. The explicit width matters most here: L2 is a hierarchical rollup
           whose source is itself hierarchical, so nothing about its position in the list implies its grain. */
        Assert.Equal(TimeSpan.FromDays(1), RollupBackfill.Targets[l2].BucketWidth);
        Assert.Equal(TimeSpan.FromDays(1), dayGrain.BucketWidth);
    }

    /// <summary>
    /// The backfill covers exactly the rollups the ROUTER can route to, EXCEPT the six #3653 LC froze. A
    /// rollup the router uses but the backfill skips would ordinarily stay permanently un-materialized — its
    /// windows served from raw forever, and its raw purge held forever, which is #1759 unfixed for that table
    /// — but a frozen rollup's watermark never advances again by construction (no refresh policy —
    /// <see cref="TimescaleSupport.FrozenRollupAggregates"/>), so there is nothing left for a backfill to
    /// converge toward, and its raw purge is no longer gated on it either (<see cref="TimescaleSupport.RawTierCoverage"/>
    /// moved that to the successors). The router still routes to it below the successor's floor, which is why
    /// it stays in <see cref="TimescaleSupport.RollupViews"/> even though it leaves this list.
    /// </summary>
    [Fact]
    public void Targets_CoverEveryRoutedRollup_WithItsOwnRawTable()
    {
        var routedAndBackfillable = TimescaleSupport.RollupViews
            .Select(r => r.View)
            .Where(v => !TimescaleSupport.IsFrozenRollupAggregate(v))
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            routedAndBackfillable,
            RollupBackfill.Targets.Select(t => t.View).OrderBy(v => v, StringComparer.Ordinal).ToArray());

        /* The frozen six are routed but deliberately absent from the backfill plan. */
        foreach (var (_, view) in TimescaleSupport.FrozenRollupAggregates)
        {
            Assert.Contains(view, TimescaleSupport.RollupViews.Select(r => r.View));
            Assert.DoesNotContain(view, RollupBackfill.Targets.Select(t => t.View));
        }

        /* And each remaining target names the same raw table the router falls back to, or the backfill would
           chase a coverage target the router never compares against. */
        foreach (var target in RollupBackfill.Targets)
        {
            Assert.Equal(RollupCoverage.RawTableFor(target.View), target.RawTable);
        }
    }

    /* ─────────────────────────── the SQL ─────────────────────────── */

    /// <summary>
    /// The slice refresh must BIND and CAST both bounds. <c>window_start</c>/<c>window_end</c> are declared
    /// <c>"any"</c> on the 2.28.1 signature, so an untyped literal leaves PostgreSQL with nothing to resolve the
    /// polymorphic argument against.
    ///
    /// <para><c>force</c> is now always positional because <c>options</c> sits behind it, so "plain" means
    /// <c>false</c> passed explicitly rather than omitted — but it must still be FALSE unless asked for. A
    /// speculative forced refresh is strictly more work and re-batches every bucket in range, and the repair
    /// path is the only caller entitled to it.</para>
    /// </summary>
    [Fact]
    public void RefreshSliceSql_BindsAndCastsBothBounds_AndForcesOnlyOnDemand()
    {
        var plain = RollupBackfill.RefreshSliceSql(TimescaleSupport.QueryStatsHourlyView);
        Assert.Contains("CALL refresh_continuous_aggregate('collect.query_stats_hourly'::regclass, $1::timestamp, $2::timestamp, false, $3::jsonb)", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("true", plain, StringComparison.Ordinal);

        var forced = RollupBackfill.RefreshSliceSql(TimescaleSupport.QueryStatsHourlyView, force: true);
        Assert.Contains("$1::timestamp, $2::timestamp, true, $3::jsonb)", forced, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>refresh_newest_first</c> is DECLARED on any engine that accepts it, and the degraded shape exists
    /// only for the 2.18–2.20 window where <c>options</c> does not.
    /// </summary>
    [Fact]
    public void RefreshSliceSql_DeclaresNewestFirst_AndKeepsADegradedShapeForOlderStores()
    {
        foreach (var sql in new[]
        {
            RollupBackfill.RefreshSliceSql(TimescaleSupport.QueryStatsHourlyView),
            RollupBackfill.RefreshSliceSql(TimescaleSupport.QueryStatsHourlyView, force: true),
        })
        {
            Assert.Contains("$3::jsonb", sql, StringComparison.Ordinal);
            Assert.Equal(5, sql[(sql.IndexOf('(') + 1)..sql.LastIndexOf(')')].Split(',').Length);
        }

        Assert.Contains("\"refresh_newest_first\": true", RollupBackfill.RefreshOptionsJson, StringComparison.Ordinal);
        Assert.DoesNotContain("buckets_per_batch", RollupBackfill.RefreshOptionsJson, StringComparison.Ordinal);

        var degraded = RollupBackfill.RefreshSliceSql(TimescaleSupport.QueryStatsHourlyView, force: false, withOptions: false);
        Assert.DoesNotContain("jsonb", degraded, StringComparison.Ordinal);
        Assert.Equal(3, degraded[(degraded.IndexOf('(') + 1)..degraded.LastIndexOf(')')].Split(',').Length);
        Assert.Equal("42883", RollupBackfill.UndefinedFunctionSqlState);
    }

    /// <summary>
    /// THE DEGRADE MUST REACH A HUMAN, and this tests the SEAM rather than the sides.
    ///
    /// <para>The shipped-then-caught bug was not in the SQL and not in the SQLSTATE constant — both were
    /// correct and both were pinned. It was that the disclosure sink was a trailing optional parameter which
    /// both production call sites silently omitted, so the null-conditional invoke was a no-op and a pre-2.21
    /// store degraded in total silence while the call-site doc and a passing test both asserted the contract
    /// was carried. Green suite, green CI, and a self-review all missed it, because nothing had to acknowledge
    /// the parameter existed.</para>
    ///
    /// <para>The primary fix is the compiler — the sink is now REQUIRED, so every call site must decide. This
    /// pins the behaviour the compiler cannot: that the latch flips (one failed call per RUN, not per slice)
    /// and that the operator is told exactly once, however many slices and rollups follow.</para>
    /// </summary>
    [Fact]
    public void RefreshDisclosure_LatchesOncePerRun_AndTellsTheOperatorExactlyOnce()
    {
        var told = new List<string>();
        var disclosure = new RefreshDisclosure(told.Add);

        Assert.True(disclosure.OptionsSupported);

        disclosure.OptionsUnavailable("this store predates the options parameter");

        /* Latched: every later slice must take the degraded shape without re-attempting the 5-argument call. */
        Assert.False(disclosure.OptionsSupported);

        /* Said once, not once per slice — a 365-slice backfill must not print 365 identical notes. */
        disclosure.OptionsUnavailable("this store predates the options parameter");
        disclosure.OptionsUnavailable("this store predates the options parameter");
        Assert.Single(told);
        Assert.Contains("predates the options parameter", told[0], StringComparison.Ordinal);

        /* A sink is not optional — that is exactly the shape that shipped a silent degrade. */
        Assert.Throws<ArgumentNullException>(() => new RefreshDisclosure(null!));
    }

    /// <summary>
    /// WATCHED (mutation): make convergence CALL-based and silent under-coverage passes. A refresh that stops on
    /// its internal batch cap logs server-side and returns success to the client, so the only honest signal is
    /// re-reading <c>min(bucket)</c> — the same expression the arming gate reads, so "covered enough to route"
    /// and "covered enough to purge" can never drift apart.
    /// </summary>
    [Fact]
    public void ConvergenceIsReadFromData_UsingTheSameExpressionTheArmingGateUses()
    {
        Assert.Equal(
            "SELECT min(bucket) FROM collect.query_stats_hourly",
            RollupBackfill.CoverageFloorSql(TimescaleSupport.QueryStatsHourlyView));

        Assert.Contains(
            $"min(bucket) FROM collect.{TimescaleSupport.QueryStatsHourlyView}",
            TimescaleSupport.RetentionArmSafetySql("query_stats", "collection_time", new[] { TimescaleSupport.QueryStatsHourlyView }),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The probe reads the MATERIALIZATION hypertable's size, resolved through
    /// <c>timescaledb_information.continuous_aggregates</c>. <c>pg_total_relation_size</c> on a hypertable's
    /// parent reports almost nothing (its chunks are separate relations), so measuring the view directly would
    /// silently calibrate every estimate at roughly zero — the worst possible direction for a disk preflight.
    /// </summary>
    [Fact]
    public void ProbeSql_SizesTheMaterializationHypertable_NotTheViewsParent()
    {
        var sql = RollupBackfill.ProbeSql(TimescaleSupport.QueryStatsHourlyView, "query_stats", "collection_time");

        Assert.Contains("hypertable_size(", sql, StringComparison.Ordinal);
        Assert.Contains("materialization_hypertable_name", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_total_relation_size", sql, StringComparison.Ordinal);

        /* Raw's oldest row is the coverage TARGET, and the rollup's own floor is the resume point. */
        Assert.Contains("min(collection_time) FROM collect.query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("min(bucket) FROM collect.query_stats_hourly", sql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── the verb ─────────────────────────── */

    /// <summary>
    /// The verb has to be reachable. #1581's lesson: an argument the allow-list does not recognize falls through
    /// into a REAL service startup and spawns a second instance fighting the first over the bundled PostgreSQL —
    /// the outage. So a new verb that is implemented but not registered is worse than one that does not exist.
    /// </summary>
    [Fact]
    public void BackfillRollupsVerb_IsRegistered_AndDocumented()
    {
        Assert.True(DarlingCliCommands.IsBackfillRollupsVerb("--backfill-rollups"));
        Assert.True(DarlingCliCommands.IsBackfillRollupsVerb("--BACKFILL-ROLLUPS"));
        Assert.False(DarlingCliCommands.IsBackfillRollupsVerb("--backfill"));

        Assert.True(DarlingCliCommands.IsKnownVerb("--backfill-rollups"));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { "--backfill-rollups" }));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { "--backfill-rollups", "--dry-run" }));

        var usage = DarlingCliCommands.UsageText();
        Assert.Contains("--backfill-rollups", usage, StringComparison.Ordinal);
        Assert.Contains("--dry-run", usage, StringComparison.Ordinal);
        Assert.All(usage, ch => Assert.True(ch < 128, $"usage text must be ASCII; found U+{(int)ch:X4}"));
    }
}
