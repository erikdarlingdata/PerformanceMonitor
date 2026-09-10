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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The staged, disk-preflighted backfill of the query-acceleration rollups (#1759 Phase 2) — the capacity
/// operation that turns Phase 1's correct-but-slow raw fallback back into an accelerated read, and lets the
/// #1680 arming gate release the held raw purges by itself.
///
/// <para><b>Why this is an operator verb and not a startup step.</b> The arming gate is all-or-nothing: a rollup
/// either covers raw or it does not, so a store that has been collecting for a year has to materialize the WHOLE
/// history before the first purge arms and reclaims anything. Peak disk therefore comes BEFORE any relief. Doing
/// that automatically at service start, on the exact stores worst affected — one of which was already down to
/// roughly 150 GB free — is a plausible disk-exhaustion event. So: explicit invocation, a preflight that refuses
/// with numbers rather than filling the volume, and progress an operator can watch.</para>
///
/// <para><b>Slicing is supervision, not re-implementation.</b> A manual <c>refresh_continuous_aggregate</c>
/// already batches internally (2.28.1 defaults: 10 buckets per batch, each committing via
/// <c>SPI_commit_and_chain</c>), so the memory and lock-duration arguments for hand-rolled slicing are already
/// handled upstream. The outer slices here exist for three things the engine's batching does not give: progress
/// an operator can see on a multi-hour run, a resume point, and a lock window bounded to ONE source chunk (the
/// concurrent-compression deadlock watch from #1778). A batch cap hit is LOG-only and silent to the client, so
/// a returned success proves nothing — convergence is judged from data, once, at the end.</para>
///
/// <para><b>Slices run NEWEST-FIRST, and that direction is a data-safety property.</b> See
/// <see cref="Slices"/>: <c>min(bucket)</c> is the resume point, the completion verdict, AND what the #1680
/// arming gate reads before it lets a raw purge drop chunks. Oldest-first drove that one number to its final
/// value on the first slice, so an interrupted run left holes that were invisible to all three — the next run
/// reported DONE and the gate armed over a window raw held the only copy of. Descending makes
/// <c>floor &lt;= source_oldest</c> imply completeness by construction.</para>
///
/// <para><b>Convergence is SOURCE-relative, not raw-relative (#1798).</b> Each rollup is backfilled down to
/// the oldest row its own SOURCE holds — raw for the hourlies, the HOURLY VIEW for every daily, since all four
/// dailies are hierarchical. Converging the dailies to raw instead looked equivalent and is not: the #1680
/// gate for an hourly-tier policy asks whether the daily covers what the HOURLY holds, and on a store whose
/// raw purges are already armed the hourlies hold weeks while raw holds days. A daily converged "to raw" stops
/// short, the gate correctly refuses, and the verb reported DONE over it. Worse, a hierarchical daily added
/// after its hourly on such a store enters a hold nothing can clear, because the pre-raw region exists only in
/// the hourly and a verb aiming at raw never targets it. Aiming at the source makes the DONE contract's
/// promise of zero held policies true on every store shape rather than only on the held-purge one.</para>
/// </summary>
public static class RollupBackfill
{
    /// <summary>How much history one refresh call covers. Deliberately ONE source chunk
    /// (<see cref="TimescaleSupport.ChunkIntervalDays"/>): the slice's read set is then a single chunk, which
    /// is what keeps the lock window short enough not to sit across a compression job (#1778). Slices are
    /// midnight-aligned, so they always land on bucket boundaries for both the hourly and daily grains — an
    /// unaligned window would be silently widened to bucket boundaries by TimescaleDB anyway, making progress
    /// arithmetic lie.</summary>
    public static readonly TimeSpan SliceWidth = TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays);

    /// <summary>Measured compression throughput on the field host class this verb exists for (~16 MB/s). Used
    /// ONLY to turn an estimate into a duration an operator can plan around — it is a budgeting figure, never a
    /// gate.</summary>
    public const double FieldThroughputBytesPerSecond = 16.0 * 1024 * 1024;

    /// <summary>Estimates are estimates: require this much more free space than the estimate says is needed, so
    /// a rollup that materializes denser than its sample does not run the volume to zero mid-pass.</summary>
    public const double SafetyFactor = 1.25;

    /// <summary>Free space never to consume, on top of the estimate. PostgreSQL needs room for WAL, temp files
    /// and the compression of what is being written; a volume driven to zero takes the store down, which is a
    /// far worse outcome than a refused backfill.</summary>
    public const long ReserveBytes = 10L * 1024 * 1024 * 1024;

    /// <summary>
    /// The rollups to backfill, in DEPENDENCY ORDER: every hourly rollup first, then the dailies that are
    /// sourced FROM those hourlies.
    ///
    /// <para>The order is load-bearing, not cosmetic. A daily continuous aggregate reads its hourly one, so
    /// refreshing a daily over a range whose hourly has not been materialized reads an empty source and
    /// materializes nothing — while REPORTING success, and while CONSUMING the invalidation records that
    /// covered the range. A later correct-order pass then no-ops over the hole. Inverting this list does not
    /// merely reorder work; it can leave permanent under-coverage that looks like completion.</para>
    ///
    /// <para>DERIVED from <see cref="TimescaleSupport.RollupViews"/> rather than restated. That list already
    /// carries every rollup's raw table, source relation, source time column and bucket width; a second
    /// hand-kept copy here is a drift surface, and #1798 is what drift between two notions of "covered"
    /// costs.</para>
    ///
    /// <para><b>Hierarchy and grain are two facts, not one (#1849).</b> They used to be derived from the same
    /// test — a rollup whose source is keyed on <c>bucket</c> reads another rollup, and every such rollup was
    /// a daily — so one boolean served as both the ordering key and the bucket width.
    /// <c>query_store_stats_corrected_hourly</c> is the counter-example: hierarchical, but bucketed in HOURS.
    /// The width is read from the source list.</para>
    ///
    /// <para><b>Ordering is by DEPTH from raw, not by the hierarchical flag (#1869).</b> The flag sorts a chain
    /// only two levels tall. <c>query_store_stats_daygrain_daily</c> reads
    /// <c>query_store_stats_interval_daily</c>, which itself reads a rollup — the first three-level chain here
    /// — and both are "hierarchical", so the boolean cannot separate them. It happened to work while
    /// <see cref="TimescaleSupport.RollupViews"/> declared them in the right order and LINQ's sort is stable,
    /// which is a silent dependency on the ORDER OF A LIST for a property the type comments call a correctness
    /// invariant. Depth reads the edge itself, so a future rollup declared anywhere still lands after its
    /// source.</para>
    /// </summary>
    public static readonly RollupBackfillTarget[] Targets =
        TimescaleSupport.RollupViews
            .Select(r => new RollupBackfillTarget(
                r.View,
                r.RawTable,
                r.Source,
                r.SourceTimeColumn,
                IsHierarchical: string.Equals(r.SourceTimeColumn, "bucket", StringComparison.Ordinal),
                BucketWidth: r.BucketWidth))
            .OrderBy(SourceDepth)
            .ToArray();

    /// <summary>How many rollups sit between <paramref name="target"/> and the raw table underneath it: 0 for a
    /// raw-sourced rollup, 1 for one reading a raw-sourced rollup, and so on. The backfill's ordering key —
    /// refreshing a rollup before its source materializes nothing while REPORTING success (see
    /// <see cref="Targets"/>). Walking stops at the first source that is not itself a rollup, and the hop count
    /// is bounded by the list length so a malformed list cannot spin here.</summary>
    private static int SourceDepth(RollupBackfillTarget target)
    {
        var depth = 0;
        var source = target.Source;

        for (var hop = 0; hop < TimescaleSupport.RollupViews.Length; hop++)
        {
            var current = source;
            var parent = Array.FindIndex(TimescaleSupport.RollupViews,
                r => string.Equals(r.View, current, StringComparison.Ordinal));

            if (parent < 0)
            {
                break;
            }

            depth++;
            source = TimescaleSupport.RollupViews[parent].Source;
        }

        return depth;
    }

    /* ─────────────────────────── the probe ─────────────────────────── */

    /// <summary>
    /// One round trip per rollup answering everything the plan needs: how far back raw reaches (the target),
    /// how far back the rollup has materialized (the resume point), and the size + bucket count of what it HAS
    /// materialized (the calibration sample for the estimate).
    ///
    /// <para>The size comes from the MATERIALIZATION hypertable, resolved through
    /// <c>timescaledb_information.continuous_aggregates</c>, because <c>pg_total_relation_size</c> on a
    /// hypertable's parent reports almost nothing — the chunks are separate relations. Reached through a
    /// scalar subquery over the information view rather than a direct reference, so a store without the
    /// extension fails the whole probe cleanly instead of erroring halfway.</para>
    ///
    /// <para><b><c>count(DISTINCT bucket)</c>, NOT <c>count(*)</c>, and the difference is the whole estimate.</b>
    /// This column is divided into <c>materialized_bytes</c> to get bytes-per-BUCKET, which is then multiplied
    /// by a count of TIME BUCKETS. A plain <c>count(*)</c> counts ROWS, and a rollup holds one row per
    /// (server, database, query_hash, sql_handle, BUCKET) — so the two units differ by the number of distinct
    /// queries seen per hour, and the estimate comes out under by exactly that factor. Measured 205x under on a
    /// 200-row/hour store; a real fleet box is far worse. Under-estimating is the one direction this whole
    /// preflight exists to prevent, so the units have to match here or the refusal never fires.</para>
    /// </summary>
    public static string ProbeSql(string view, string source, string sourceTimeColumn) => $@"
SELECT
    (SELECT min({sourceTimeColumn}) FROM collect.{source}) AS source_oldest,
    (SELECT min(bucket) FROM collect.{view}) AS coverage_oldest,
    (SELECT count(DISTINCT bucket) FROM collect.{view}) AS materialized_buckets,
    (SELECT hypertable_size(format('%I.%I', c.materialization_hypertable_schema, c.materialization_hypertable_name)::regclass)
     FROM timescaledb_information.continuous_aggregates AS c
     WHERE c.view_schema = 'collect' AND c.view_name = '{view}') AS materialized_bytes";

    /// <summary>
    /// A BOUNDED refresh over one slice. Both bounds are bound parameters and explicitly cast:
    /// <c>window_start</c>/<c>window_end</c> are declared <c>"any"</c> on the 2.28.1 signature, so an untyped
    /// literal leaves PostgreSQL with no type to resolve the polymorphic argument against.
    ///
    /// <para>Deliberately the NON-forced form. On an aggregate created <c>WITH NO DATA</c> the plain refresh is
    /// enough — creation writes an infinite <c>[-infinity, +infinity]</c> invalidation ("initially, everything
    /// is invalid") that <c>WITH NO DATA</c> does not skip — and <c>force</c> only exists from TimescaleDB
    /// 2.18, so an older bring-your-own store raises 42883 on the 4-argument call. Where force IS needed is
    /// REPAIR, and the caller escalates to it only on a MEASURED shortfall (see
    /// <see cref="RunSliceAsync"/>), never speculatively: a refresh consumes invalidations, so a pass cut short
    /// leaves a region whose entries are gone and a later plain refresh no-ops over the hole while reporting
    /// success.</para>
    ///
    /// <para><b><c>refresh_newest_first</c> IS PASSED EXPLICITLY where the engine accepts it.</b> The resume
    /// story depends on newest-first batching: a killed slice is safe to resume from the measured floor with
    /// no bookkeeping ONLY because the batches that committed inside it were the NEWEST ones, leaving the
    /// floor inside that slice for the next run's top slice to re-cover. That behaviour is NOT a GUC — there
    /// is no such setting in <c>pg_settings</c> — it rides the <c>options</c> jsonb, so omitting the parameter
    /// means inheriting an UNSTATED ENGINE DEFAULT. Declaring it is strictly stronger than trusting it: a
    /// declared option can only be broken by an engine ignoring its own documented contract, where an
    /// undeclared default can flip in a release note.</para>
    ///
    /// <para><paramref name="withOptions"/> is the compatibility half. <c>options</c> arrived in TimescaleDB
    /// 2.21 while <c>force</c> goes back to 2.18, and nothing in this product gates a bring-your-own store's
    /// version, so an older store answers 42883 to the 5-argument call. Hard-failing there would withhold the
    /// remedy from a store that is BY DEFINITION already in the broken #1759 state, so the run degrades to the
    /// shape that was shipped and safe before — LATCHED ONCE per run in
    /// <see cref="RefreshDisclosure"/>, not retried per slice, and DISCLOSED to the operator, because on that
    /// store the guarantee really is an inherited default and saying otherwise would be the very failure this
    /// change removes.</para>
    ///
    /// <para>The degraded path is not left unguarded either: it is exactly what
    /// <c>MidSliceCancellation_LeavesTheFloorInsideTheRange_WithNoGapsAbove</c> tests, since that pin asserts
    /// the BEHAVIOUR without naming the option and therefore holds on engines that never receive it.</para>
    /// </summary>
    public static string RefreshSliceSql(string view, bool force = false, bool withOptions = true)
    {
        var call = $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp";

        /* force is positional, so reaching options means stating it rather than defaulting to it. */
        return withOptions
            ? call + $", {(force ? "true" : "false")}, $3::jsonb)"
            : call + (force ? ", true)" : ")");
    }

    /// <summary>The refresh contract the resume story depends on. Newest-first is the ONLY option set: batch
    /// size and the batch cap stay at the defaults the issue's API research established are already right.</summary>
    public const string RefreshOptionsJson = "{\"refresh_newest_first\": true}";

    /// <summary>SQLSTATE <c>undefined_function</c> — what a pre-2.21 store answers to the 5-argument call
    /// because it has no <c>options</c> parameter. Branched on the STATE, never the message.</summary>
    public const string UndefinedFunctionSqlState = "42883";

    /// <summary>The rollup's oldest materialized bucket — the DATA-based convergence signal. A refresh CALL that
    /// returns without error proves nothing: a batch-cap stop is logged server-side and is completely silent to
    /// the client.</summary>
    public static string CoverageFloorSql(string view) => $"SELECT min(bucket) FROM collect.{view}";

    /* ─────────────────────────── the plan + preflight (pure) ─────────────────────────── */

    /// <summary>
    /// What one rollup needs, from its probe row. Returns a plan with <see cref="RollupBackfillPlan.IsComplete"/>
    /// when there is nothing to do — an empty raw table (a fresh store), or coverage that already reaches raw
    /// (which is what makes re-running this verb converge to a no-op instead of re-materializing forever).
    ///
    /// <para>The estimate is CALIBRATED from what the rollup has already materialized (bytes per bucket, scaled
    /// by the buckets to add) whenever there is a sample — which on the stores this exists for there always is,
    /// since their refresh policies have been materializing a trailing start-offset window all along. With no sample at
    /// all it falls back to a fraction of raw's own bytes over the same span and says so, so an operator can
    /// tell a measured number from a bounding one.</para>
    /// </summary>
    public static RollupBackfillPlan Plan(
        string view,
        DateTime? sourceOldestUtc,
        DateTime? coverageOldestUtc,
        long materializedBuckets,
        long materializedBytes,
        long rawBytes,
        TimeSpan bucketWidth)
    {
        if (sourceOldestUtc is not DateTime sourceOldest)
        {
            return RollupBackfillPlan.Complete(view, "raw holds no rows, so there is nothing to materialize");
        }

        /* Slices start at midnight so every window opens on a bucket boundary for both grains. */
        var from = sourceOldest.Date;
        var to = coverageOldestUtc ?? DateTime.SpecifyKind(DateTime.UtcNow, sourceOldest.Kind);

        if (coverageOldestUtc is DateTime coverage && coverage <= sourceOldest)
        {
            return RollupBackfillPlan.Complete(view, "coverage already reaches raw's oldest row");
        }

        /* And the range CLOSES on one too. A refresh window narrower than a single bucket is rejected outright
           — TimescaleDB raises 22023 "refresh window too small" — and the ragged tail of a day-wide slice is
           exactly that for a DAILY rollup, whose bucket IS a day. The end is a coverage floor or "now", so it
           is an arbitrary instant and lands mid-bucket most of the time; caught on the gated live leg as a
           slice failure that aborted the daily tier. Truncating instead of rounding up is deliberate: the live
           edge belongs to the refresh policy, and this verb is only ever trying to extend the OLD end. */
        var span = to - from;
        if (span > TimeSpan.Zero && bucketWidth > TimeSpan.Zero)
        {
            to = from + TimeSpan.FromTicks(span.Ticks / bucketWidth.Ticks * bucketWidth.Ticks);
            span = to - from;
        }

        if (span <= TimeSpan.Zero)
        {
            return RollupBackfillPlan.Complete(view, "coverage already reaches raw's oldest row");
        }

        var bucketsToAdd = (long)Math.Ceiling(span / bucketWidth);
        var slices = (long)Math.Ceiling(span / SliceWidth);

        /* SANITY CLAMP. min(collection_time) is data, and data can be garbage: one row with an epoch-era or
           otherwise absurd timestamp stretches the plan across the whole interval between it and now. Observed
           live — a single stray value produced a 739,825-slice plan spanning 3.7 days of pure planning CPU
           before anything was materialized. Refusing beats spanning: a timestamp that old is corruption worth
           NAMING so someone goes and looks at the row, not a history to back fill. */
        if (slices > MaxPlannedSlices)
        {
            return RollupBackfillPlan.Absurd(
                view,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"{sourceOldest:yyyy-MM-dd HH:mm} is the oldest timestamp in the source, which would plan {slices:N0} slices ({span.TotalDays:N0} days). That is past the {MaxPlannedSlices:N0}-slice ceiling and almost certainly a corrupt row rather than real history — find and fix that row, then re-run."));
        }

        long estimatedBytes;
        bool calibrated;
        if (materializedBuckets > 0 && materializedBytes > 0)
        {
            estimatedBytes = (long)Math.Ceiling((double)materializedBytes / materializedBuckets * bucketsToAdd);
            calibrated = true;
        }
        else
        {
            /* No sample to calibrate from. Bound it instead: the rollup collapses many per-sweep raw rows into
               one row per (dimensions, bucket), so it is materially smaller than raw for the same span — but
               "materially" is not a number we can measure here, and guessing LOW is the failure that fills a
               volume. UncalibratedFractionOfRaw is therefore an upper bound, not an expectation, and the caller
               reports the estimate as uncalibrated so the operator reads it as one. */
            estimatedBytes = (long)Math.Ceiling(rawBytes * UncalibratedFractionOfRaw);
            calibrated = false;
        }

        return new RollupBackfillPlan(view, from, to, bucketsToAdd, slices, estimatedBytes, calibrated, IsComplete: false, SkipReason: null);
    }

    /// <summary>
    /// The bounding fraction of raw's own bytes used when a rollup has materialized NOTHING to calibrate
    /// against. Deliberately generous: an over-estimate refuses a backfill that would in fact have fit (the
    /// operator grows the disk or re-runs after the policy has materialized a sample), while an under-estimate
    /// fills a production volume. Those two mistakes are not symmetric.
    /// </summary>
    public const double UncalibratedFractionOfRaw = 0.5;

    /// <summary>The most slices a single rollup's plan may span before it is treated as corruption rather than
    /// history. 3,660 slices is ten years at one day each — well past any real store's life, and far short of
    /// what one epoch-era row produces.</summary>
    public const long MaxPlannedSlices = 3_660;

    /// <summary>
    /// Is there room? Requires the estimate plus <see cref="SafetyFactor"/> headroom AND
    /// <see cref="ReserveBytes"/> left untouched. Pure, so the refusal is unit-testable without a full volume.
    /// </summary>
    public static bool HasRoom(long estimatedBytes, long freeBytes) =>
        freeBytes >= RequiredBytes(estimatedBytes);

    /// <summary>Free space this backfill requires: the estimate, scaled for error, plus the untouchable reserve.</summary>
    public static long RequiredBytes(long estimatedBytes) =>
        (long)Math.Ceiling(estimatedBytes * SafetyFactor) + ReserveBytes;

    /// <summary>How long the materialization is expected to take at the measured field throughput. Budgeting
    /// only.</summary>
    public static TimeSpan EstimatedDuration(long estimatedBytes) =>
        TimeSpan.FromSeconds(estimatedBytes / FieldThroughputBytesPerSecond);

    /* ─────────────────────────── the slice runner ─────────────────────────── */

    /// <summary>
    /// Refreshes ONE slice and returns the rollup's measured coverage floor afterwards, for progress output.
    ///
    /// <para><b>Deliberately no per-slice verdict.</b> The obvious check — "did the floor move to at or before
    /// this slice's start?" — is WRONG, and the gated live leg caught it: a slice's range can legitimately hold
    /// no source rows (raw's oldest row lands partway into the first slice, and any collection gap does the same
    /// mid-run), so the floor correctly stops short and the check reads a healthy slice as a failure. It fired
    /// on the very first slice of every run. A global <c>min(bucket)</c> simply cannot answer a question about
    /// one slice.</para>
    ///
    /// <para>Convergence is judged ONCE, at the end, against the only target that means anything — raw's oldest
    /// row (<see cref="RepairAsync"/>). That is the same shape #1762 settled on: re-probe coverage after
    /// refreshing and escalate only on a MEASURED shortfall.</para>
    /// </summary>
    public static async Task<DateTime?> RunSliceAsync(
        NpgsqlConnection connection, string view, DateTime fromUtc, DateTime toUtc,
        RefreshDisclosure disclosure, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        await RefreshAsync(connection, view, fromUtc, toUtc, force: false, disclosure, cancellationToken);
        return await ReadCoverageFloorAsync(connection, view, cancellationToken);
    }

    /// <summary>
    /// The escalation, run ONLY on a measured shortfall: a FORCED refresh over the whole planned range, which
    /// ignores the invalidation log and re-batches every bucket in it. Returns the floor afterwards, so the
    /// caller still judges the outcome from data.
    ///
    /// <para>This is what a plain refresh cannot do, and the reason a shortfall is not simply retried: a
    /// refresh CONSUMES invalidation records as it goes, so a pass cut short by a shutdown (or by a batch cap,
    /// which is logged server-side and completely silent to the client) leaves a region whose entries are
    /// already gone — and a later plain refresh no-ops straight over that hole while reporting success.</para>
    ///
    /// <para>Never speculative: it is strictly more work, and <c>force</c> only exists from TimescaleDB 2.18,
    /// so on an older bring-your-own store this raises 42883 — which the caller reports rather than crashing
    /// the run, and which never happens at all unless a shortfall was already measured.</para>
    /// </summary>
    public static async Task<DateTime?> RepairAsync(
        NpgsqlConnection connection, string view, DateTime fromUtc, DateTime toUtc,
        RefreshDisclosure disclosure, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        await RefreshAsync(connection, view, fromUtc, toUtc, force: true, disclosure, cancellationToken);
        return await ReadCoverageFloorAsync(connection, view, cancellationToken);
    }

    /// <summary>
    /// TimescaleDB serializes refreshes of the same aggregate and raises <see cref="ConcurrentRefreshSqlState"/>
    /// (<c>55P03 lock_not_available</c>) rather than waiting. This verb runs while the SERVICE IS UP, so the
    /// aggregate's own refresh policy — which fires roughly hourly over a trailing
    /// <see cref="TimescaleSupport.HourlyRefreshStartOffset"/> window — will
    /// eventually land on top of a slice. Caught live on the gated PostgreSQL 18.4 / TimescaleDB 2.28.1 leg:
    /// <c>EnsureContinuousAggregatesAsync</c> attaches the policy, the policy runs immediately, and the very
    /// next slice failed.
    ///
    /// <para>Retried rather than reported, because it is neither an error nor an ambiguous state: the policy's
    /// run is short and bounded, the collision says only "not now", and letting it abort the rollup would make
    /// a multi-hour backfill fail on a store doing exactly what it is supposed to be doing. Bounded and
    /// transient-only — every other SQLSTATE still propagates on the first occurrence, so a permission problem
    /// or a missing relation still fails fast instead of being retried into a timeout.</para>
    /// </summary>
    public const string ConcurrentRefreshSqlState = "55P03";

    /// <summary>How many times a slice waits out a concurrent policy refresh before giving up. Generous: the
    /// alternative to waiting is failing a backfill that was going to succeed.</summary>
    public const int ConcurrentRefreshAttempts = 10;

    private static readonly TimeSpan ConcurrentRefreshDelay = TimeSpan.FromSeconds(5);

    private static async Task RefreshAsync(
        NpgsqlConnection connection, string view, DateTime fromUtc, DateTime toUtc, bool force,
        RefreshDisclosure disclosure, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(disclosure);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                /* PreventInTransactionBlock: the CALL can never be wrapped in an explicit transaction. */
                using var refresh = new NpgsqlCommand(RefreshSliceSql(view, force, disclosure.OptionsSupported), connection) { CommandTimeout = SliceTimeoutSeconds };
                refresh.Parameters.AddWithValue(fromUtc);
                refresh.Parameters.AddWithValue(toUtc);
                if (disclosure.OptionsSupported)
                {
                    refresh.Parameters.AddWithValue(RefreshOptionsJson);
                }

                await refresh.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
            catch (PostgresException ex) when (disclosure.OptionsSupported && ex.SqlState == UndefinedFunctionSqlState)
            {
                /* Pre-2.21: no options parameter. Latch for the whole run so this costs ONE failed call, and
                   disclose, because from here on newest-first is an inherited default rather than a contract. */
                disclosure.OptionsUnavailable(
                    "this store's TimescaleDB predates the refresh 'options' parameter (added in 2.21), so " +
                    "refresh_newest_first cannot be set explicitly. The backfill continues and is still safe to " +
                    "interrupt, but the newest-first resume guarantee now rests on this engine's own default " +
                    "rather than on anything this tool asked for.");
            }
            catch (PostgresException ex)
                when (attempt < ConcurrentRefreshAttempts && ex.SqlState == ConcurrentRefreshSqlState)
            {
                await Task.Delay(ConcurrentRefreshDelay, cancellationToken);
            }
        }
    }

    /// <summary>
    /// The oldest instant a relation holds — used to bound a hierarchical rollup's PREFLIGHT estimate against
    /// where its source will end up, not merely where the source starts today.
    ///
    /// <para>The preflight has to total the whole job BEFORE any slice runs, but a daily's source is an hourly
    /// that this same run deepens first. Estimating the daily from the hourly's pre-backfill floor would
    /// under-count exactly the region the run then writes — the same "the printed number must bound the whole
    /// job" invariant as the rows-per-bucket under-estimate, and it fails on the store shape where disk is
    /// tightest.</para>
    /// </summary>
    public static async Task<DateTime?> OldestInstantAsync(
        NpgsqlConnection connection, string relation, string timeColumn, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using var command = new NpgsqlCommand($"SELECT min({timeColumn}) FROM collect.{relation}", connection)
        {
            CommandTimeout = ProbeTimeoutSeconds,
        };
        return await command.ExecuteScalarAsync(cancellationToken) is DateTime oldest ? oldest : null;
    }

    /// <summary>
    /// The floor a rollup's backfill will EVENTUALLY be aimed at, given that its source may itself be deepened
    /// earlier in the same run. The deeper of the two candidates wins, because the run converges to whichever
    /// the source ends up holding: the source's floor as it stands now, or the root raw table's oldest row,
    /// which is as far as backfilling that source can ever take it.
    ///
    /// <para>For an HOURLY this is a no-op — its source IS raw, so both candidates are the same instant. It
    /// matters only for the hierarchical dailies, and only in the direction that makes the preflight bound
    /// LARGER, which is the safe direction for a disk gate.</para>
    /// </summary>
    public static DateTime? EventualSourceFloor(DateTime? sourceOldestUtc, DateTime? rootRawOldestUtc)
    {
        if (sourceOldestUtc is null)
        {
            return rootRawOldestUtc;
        }

        if (rootRawOldestUtc is null)
        {
            return sourceOldestUtc;
        }

        return rootRawOldestUtc < sourceOldestUtc ? rootRawOldestUtc : sourceOldestUtc;
    }

    /// <summary>The rollup's measured coverage floor, or null when it holds nothing.</summary>
    public static async Task<DateTime?> ReadCoverageFloorAsync(
        NpgsqlConnection connection, string view, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using var probe = new NpgsqlCommand(CoverageFloorSql(view), connection) { CommandTimeout = ProbeTimeoutSeconds };
        return await probe.ExecuteScalarAsync(cancellationToken) is DateTime floor ? floor : null;
    }

    /// <summary>Reads one rollup's probe row (<see cref="ProbeSql"/>).</summary>
    public static async Task<RollupBackfillProbe> ProbeAsync(
        NpgsqlConnection connection, string view, string source, string sourceTimeColumn, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using var command = new NpgsqlCommand(ProbeSql(view, source, sourceTimeColumn), connection) { CommandTimeout = ProbeTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new RollupBackfillProbe(null, null, 0, 0);
        }

        return new RollupBackfillProbe(
            reader.IsDBNull(0) ? null : reader.GetDateTime(0),
            reader.IsDBNull(1) ? null : reader.GetDateTime(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
            reader.IsDBNull(3) ? 0 : reader.GetInt64(3));
    }

    /// <summary>Total bytes a raw hypertable occupies, for the uncalibrated estimate's bound. Zero when the
    /// relation is not a hypertable or the extension is absent — the caller then has no basis for an
    /// uncalibrated estimate and says so rather than inventing one.</summary>
    public static async Task<long> RawBytesAsync(
        NpgsqlConnection connection, string rawTable, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        try
        {
            using var command = new NpgsqlCommand($"SELECT hypertable_size('collect.{rawTable}'::regclass)", connection)
            {
                CommandTimeout = ProbeTimeoutSeconds,
            };
            return await command.ExecuteScalarAsync(cancellationToken) is long bytes ? bytes : 0;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return 0;
        }
    }

    /// <summary>The store's own data directory, so free space is measured on the volume that will actually
    /// grow. Asked of the STORE rather than derived from config, so it is right in bring-your-own mode too —
    /// and so a store on ANOTHER host reports a path this machine does not have, which the caller turns into a
    /// refusal instead of measuring the wrong volume. Null when the login may not read the setting.</summary>
    public static async Task<string?> DataDirectoryAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        try
        {
            using var command = new NpgsqlCommand("SELECT current_setting('data_directory')", connection)
            {
                CommandTimeout = ProbeTimeoutSeconds,
            };
            return await command.ExecuteScalarAsync(cancellationToken) as string;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// The bucket-aligned slices of [from, to), <b>NEWEST FIRST</b> — walking DOWN from the current coverage
    /// floor toward raw's oldest row.
    ///
    /// <para><b>The direction is a data-safety property, not a preference, and oldest-first was actively
    /// dangerous.</b> <c>min(bucket)</c> is this verb's resume point AND its completion verdict, and it is also
    /// what the #1680 arming gate reads to decide a raw purge is safe. Running oldest-first drove that floor to
    /// its FINAL value on the very first slice, which made every later slice invisible to all three: an
    /// interrupted run left holes in the un-run region, the next run re-planned from a floor that already
    /// looked complete and reported "nothing to do — DONE", and the arming gate then armed the 4-day raw purge
    /// over a window whose only remaining copy was the raw rows about to be dropped. Proven by execution on a
    /// live store: killed at slice 32 of 43, 47 of 264 buckets missing, re-run exited 0 claiming success.</para>
    ///
    /// <para>Descending inverts that into a guarantee. The floor can only reach raw's oldest row once the
    /// LAST, oldest slice has run, so <c>floor &lt;= raw_oldest</c> implies completeness BY CONSTRUCTION — in
    /// any interleaving, under any interruption. A run that stops early leaves the floor visibly above the
    /// target, which is a truthful SHORT rather than a false DONE, and the gate stays closed because it reads
    /// the same honest number. The slice-failure path lands in the same safe state for the same reason, where
    /// oldest-first would have reported success over a hole.</para>
    ///
    /// <para>Mid-slice interruption needs no extra bookkeeping, because a refresh commits per batch and
    /// <c>refresh_newest_first</c> is on: a killed slice leaves its NEWER portion materialized and the floor
    /// somewhere inside it, so re-planning from the measured floor makes the next run's top slice re-cover that
    /// remainder. Re-refreshing an already-materialized range is near-free, and the overlap is what closes the
    /// partial-slice seam.</para>
    /// </summary>
    public static IEnumerable<(DateTime FromUtc, DateTime ToUtc)> Slices(DateTime fromUtc, DateTime toUtc)
    {
        for (var end = toUtc; end > fromUtc;)
        {
            var start = end - SliceWidth;
            if (start < fromUtc)
            {
                start = fromUtc;
            }

            yield return (start, end);
            end = start;
        }
    }

    /// <summary>Per-slice refresh ceiling. A slice is one source chunk, so this is generous by design — it is a
    /// runaway guard, not an expectation.</summary>
    private const int SliceTimeoutSeconds = 60 * 60;

    private const int ProbeTimeoutSeconds = 300;
}

/// <summary>
/// Carries the run's refresh-option capability AND the sink that tells the operator when it is missing.
///
/// <para>Two jobs in one object because they are the same fact. <c>options</c> is 2.21+, so a pre-2.21 store
/// answers 42883 once; from then on the run must (a) stop re-attempting the 5-argument form, and (b) have
/// already SAID so. Latching without disclosing is degrade-and-be-silent — the operator reads a call site and
/// a test both asserting <c>refresh_newest_first</c> is guaranteed, on a store where it silently is not.</para>
///
/// <para><b>Deliberately a REQUIRED constructor argument and a REQUIRED parameter on the refresh entry
/// points.</b> An earlier cut made the sink a trailing optional <c>Action&lt;string&gt;?</c>; both production
/// call sites simply omitted it, the null-conditional invoke became a no-op, and a green suite plus green CI
/// plus a self-review all missed it, because nothing anywhere had to acknowledge it existed. Required, the
/// compiler forces every caller — tests included — to decide where the disclosure goes. Enforce with the
/// compiler, not with a test that has to be remembered.</para>
/// </summary>
public sealed class RefreshDisclosure
{
    private readonly Action<string> _sink;
    private bool _disclosed;

    public RefreshDisclosure(Action<string> sink) =>
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));

    /// <summary>False once the store has proved it has no <c>options</c> parameter. Latched for the whole run,
    /// so the failed call is paid ONCE rather than on every slice of a multi-hundred-slice backfill.</summary>
    public bool OptionsSupported { get; private set; } = true;

    /// <summary>Records that this store cannot take the option, and tells the operator — once, however many
    /// slices and rollups follow.</summary>
    public void OptionsUnavailable(string message)
    {
        OptionsSupported = false;

        if (_disclosed)
        {
            return;
        }

        _disclosed = true;
        _sink(message);
    }
}

/// <summary>One rollup to backfill: the view, the RAW table whose oldest row is the coverage target, the source
/// it converges to, whether it reads another rollup rather than raw (which orders the run), and its own bucket
/// width — a SEPARATE fact since #1849, when <c>query_store_stats_corrected_hourly</c> became the first
/// hierarchical rollup that is not a daily. See <see cref="RollupBackfill.Targets"/>.</summary>
public sealed record RollupBackfillTarget(
    string View, string RawTable, string Source, string SourceTimeColumn, bool IsHierarchical, TimeSpan BucketWidth);

/// <summary>One rollup's measured state (<see cref="RollupBackfill.ProbeSql"/>).</summary>
public sealed record RollupBackfillProbe(
    DateTime? SourceOldestUtc, DateTime? CoverageOldestUtc, long MaterializedBuckets, long MaterializedBytes);

/// <summary>
/// What one rollup's backfill will do: the window, its size in buckets and slices, and the disk it is estimated
/// to cost. <see cref="IsComplete"/> means there is nothing to do and <see cref="SkipReason"/> says why — which
/// is the normal outcome on a fresh store and on every re-run after a successful pass.
/// </summary>
public sealed record RollupBackfillPlan(
    string View,
    DateTime FromUtc,
    DateTime ToUtc,
    long BucketsToAdd,
    long Slices,
    long EstimatedBytes,
    bool Calibrated,
    bool IsComplete,
    string? SkipReason,
    string? Refusal = null)
{
    public static RollupBackfillPlan Complete(string view, string reason) =>
        new(view, default, default, 0, 0, 0, Calibrated: true, IsComplete: true, SkipReason: reason);

    /// <summary>
    /// A plan the store's own data makes nonsense of — today, a source timestamp so old that the span is
    /// corruption rather than history. Distinct from <see cref="Complete"/> on purpose: "nothing to do" is a
    /// success an operator can ignore, this is a REFUSAL that names a row someone has to go and look at, and
    /// collapsing the two would bury it in the [OK] lines.
    /// </summary>
    public static RollupBackfillPlan Absurd(string view, string refusal) =>
        new(view, default, default, 0, 0, 0, Calibrated: true, IsComplete: true, SkipReason: null, Refusal: refusal);

    /// <summary>Human size for the estimate, e.g. "12.4 GB".</summary>
    public string EstimatedSize => FormatBytes(EstimatedBytes);

    /// <summary>Bytes as a human-readable size. Binary units, one decimal, invariant — this goes into operator
    /// output and a refusal message, so it must not vary with the machine's locale.</summary>
    public static string FormatBytes(long bytes)
    {
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double value = bytes;
        var unit = 0;
        while (value >= 1024 && unit < units.Length - 1)
        {
            value /= 1024;
            unit++;
        }

        return string.Create(CultureInfo.InvariantCulture, $"{value:0.#} {units[unit]}");
    }
}
