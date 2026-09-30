/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The latest Query Store snapshot per interval, EVERY outcome (Regular, Aborted, Exception), kept beside
/// <see cref="QueryStoreIntervalLatest"/> (#3953, rung V144). Review D4R found that widening V143's own table to
/// carry every outcome and every column the three new reads need broke PLAN_REGRESSION (H1, H2, H4) and the
/// slicer (H4): PLAN_REGRESSION's near-full-table scan gets 2.6x wider rows for no benefit, and the slicer's
/// <c>COALESCE</c> predicate can't use either measured index. F1's follow-up measurement confirmed split (c): V143
/// untouched, plus this table, 8-9 days on <c>collection_time</c>/<c>first_execution_time</c>, no secondary index
/// (a window index took HOT to 0%, F1). This class is the write side only: the table, its own coverage claim and
/// pending replay table, mirroring <see cref="QueryStoreIntervalLatest"/>'s shape column for column so a later
/// gate can parametrize on the table rather than duplicate the decision (ruling issuecomment-5836972848, item 5).
/// The three reads and their gate are a later lane's; this class exposes no read-source decision.
///
/// <para><b>Raw never depends on it, exactly as V143.</b> The apply runs in the SAME raw COPY transaction as
/// V143's, behind its OWN savepoint (<see cref="SavepointName"/>), beside V143's
/// (<c>DarlingCollectorRunner.CopyBatchOnceAsync</c>). A fault here rolls back only to this savepoint: V143's
/// already-applied rows and claim in the same transaction are untouched, and raw still commits. The next apply for
/// the server replays this table's own pending rows independently of V143's.</para>
///
/// <para><b>The coverage claim.</b> For a server, every raw snapshot (any outcome) with <c>collection_time</c> at
/// or after <c>filled_since</c> is represented here by its interval's latest snapshot, except the batches in this
/// table's own pending table. The claim starts at the first batch THIS build writes — never backdated to a raw row
/// this build did not itself apply — and <c>applied_through</c> advances exactly as V143's does. The hourly gap
/// check reads raw for rows no apply of THIS table accounted for, so a raw batch that only an old writer's V143
/// upsert reached (this table never heard of it) restarts this table's claim above it, same as V143's own gap
/// check restarts V143's.</para>
/// </summary>
public sealed class QueryStoreIntervalWide
{
    /// <summary>The table: one row per Query Store interval identity, every outcome.</summary>
    public const string TableName = "query_store_interval_wide";

    /// <summary>Per server: <c>filled_since</c> and <c>applied_through</c>, this table's own claim.</summary>
    public const string CoverageTableName = "query_store_interval_wide_coverage";

    /// <summary>One row per raw batch whose apply to THIS table failed, replayed by the server's next apply.</summary>
    public const string PendingTableName = "query_store_interval_wide_pending";

    /// <summary>Same bound as <see cref="QueryStoreIntervalLatest.ApplyLockTimeoutSeconds"/>, for the same reason:
    /// a wait here holds raw's COPY transaction open.</summary>
    public const int ApplyLockTimeoutSeconds = 5;

    /// <summary>How many pending batches one apply replays, oldest first.</summary>
    public const int MaxReplaysPerApply = 16;

    /// <summary>How often the gap check runs per server.</summary>
    public static readonly TimeSpan GapCheckInterval = TimeSpan.FromHours(1);

    /// <summary>This table's own savepoint — distinct from <see cref="QueryStoreIntervalLatest.SavepointName"/> so
    /// the two applies, run one after the other in the same transaction, never share a rollback target.</summary>
    internal const string SavepointName = "qsiw_apply";

    /// <summary>
    /// The dedup's identity: V143's identity plus <c>execution_type_desc</c> (D4's design, ruled), because this
    /// table holds every outcome and two outcomes can share every other key. In the unique index's column order.
    /// </summary>
    public const string IdentityColumns =
        "server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc";

    /// <summary>The identity minus <c>server_id</c>: one batch is one server, so this is the batch's own key.</summary>
    public const string BatchIdentityColumns =
        "database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc";

    /// <summary>(c) The coverage row, created once per server per process before its first COPY. Same shape as
    /// <see cref="QueryStoreIntervalLatest.EnsureCoverageSql"/>: <c>filled_since</c> is the clock raised to the
    /// server's newest raw <c>collection_time</c>, so a backdated backfill slice's time is never used, and
    /// <c>applied_through</c> starts at the same value.</summary>
    public const string EnsureCoverageSql = @"
INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through)
SELECT
    $1,
    f.filled_since,
    f.filled_since
FROM
(
    SELECT
        GREATEST
        (
            $2,
            (
                SELECT
                    MAX(s.collection_time)
                FROM collect.query_store_stats AS s
                WHERE s.server_id = $1
                AND   s.collection_time >= $3
            )
        ) AS filled_since
) AS f
ON CONFLICT (server_id) DO NOTHING;";

    /// <summary>(g) The gap check's read: raw rows for the server newer than anything an apply of THIS table
    /// accounted for. Same shape as <see cref="QueryStoreIntervalLatest.GapCheckSql"/>.</summary>
    public const string GapCheckSql = @"
SELECT
    MAX(s.collection_time)
FROM collect.query_store_stats AS s
WHERE s.server_id = $1
AND   s.collection_time > $2
AND   s.collection_time >
      (
          SELECT
              c.applied_through
          FROM collect.query_store_interval_wide_coverage AS c
          WHERE c.server_id = $1
      );";

    /// <summary>This table's coverage row's <c>applied_through</c>, by primary key.</summary>
    public const string AppliedThroughSql = @"
SELECT
    c.applied_through
FROM collect.query_store_interval_wide_coverage AS c
WHERE c.server_id = $1;";

    /// <summary>Restarts the server's coverage claim above <c>$3</c>, a raw <c>collection_time</c> this table is
    /// known to lack. Never moves <c>filled_since</c> down.</summary>
    public const string ResetCoverageSql = @"
UPDATE collect.query_store_interval_wide_coverage
SET
    filled_since = GREATEST(filled_since, $2, $3 + INTERVAL '1 microsecond'),
    applied_through = GREATEST(applied_through, $3)
WHERE server_id = $1;";

    /// <summary>(a) The per-batch stamp, run first inside the savepoint so it takes this table's coverage row lock
    /// before anything else.</summary>
    public const string StampSql = @"
INSERT INTO collect.query_store_interval_wide_coverage AS c (server_id, filled_since, applied_through)
VALUES ($1, $3, $2)
ON CONFLICT (server_id) DO UPDATE
SET applied_through = EXCLUDED.applied_through
WHERE c.applied_through < EXCLUDED.applied_through;";

    /// <summary>(r) This server's pending batches for THIS table, oldest first, bounded.</summary>
    public const string PendingForServerSql = @"
SELECT
    p.collection_time,
    p.database_name
FROM collect.query_store_interval_wide_pending AS p
WHERE p.server_id = $1
ORDER BY
    p.collection_time,
    p.database_name
LIMIT $2;";

    /// <summary>(r) Removes one replayed pending batch.</summary>
    public const string DeletePendingSql = @"
DELETE FROM collect.query_store_interval_wide_pending
WHERE server_id = $1
AND   collection_time = $2
AND   database_name = $3;";

    /// <summary>(f) Records a batch whose apply to THIS table failed, one row per database, in the raw rows' own
    /// transaction.</summary>
    public const string RecordPendingSql = @"
INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at, failure)
SELECT
    $1,
    $2,
    d.database_name,
    $3,
    $4
FROM unnest($5::text[]) AS d (database_name)
ON CONFLICT (server_id, collection_time, database_name) DO NOTHING;";

    /// <summary>(f) Advances <c>applied_through</c> past a batch recorded as pending, so the gap check reads it as
    /// accounted for.</summary>
    public const string AdvanceAppliedThroughSql = @"
UPDATE collect.query_store_interval_wide_coverage
SET applied_through = $2
WHERE server_id = $1
AND   applied_through < $2;";

    /// <summary>
    /// (b) The upsert, one set-based statement per batch: this batch's rows (every outcome) read back from raw by
    /// <c>collection_time = $2</c>, one survivor per identity by the raw read's own tie-break, kept only where it
    /// is newer under <c>(collection_time DESC, execution_count DESC)</c> — the running-maximum guard, unchanged
    /// from V143's. Unlike V143's upsert, there is no <c>execution_type_desc = 'Regular'</c> filter: this table
    /// holds every outcome. <c>first_execution_time IS NOT NULL</c> stays (M2, ruled): the column is
    /// <c>NOT NULL</c> here too, and a Regular OR non-Regular row with a NULL value fails the same closed way V143
    /// does, below.
    /// </summary>
    public const string UpsertSql = @"
WITH
    batch_rows AS
(
    SELECT
        COUNT(*) AS raw_rows,
        COUNT(*) FILTER (WHERE s.first_execution_time IS NULL) AS null_first_execution_rows
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = $2
    AND   s.database_name = ANY ($3::text[])
),
    upserted AS
(
    INSERT INTO collect.query_store_interval_wide AS t
    (
        collection_time,
        server_id,
        database_name,
        query_id,
        plan_id,
        execution_type_desc,
        first_execution_time,
        last_execution_time,
        module_name,
        query_text,
        query_hash,
        execution_count,
        avg_duration_us,
        min_duration_us,
        max_duration_us,
        avg_cpu_time_us,
        min_cpu_time_us,
        max_cpu_time_us,
        avg_logical_io_reads,
        min_logical_io_reads,
        max_logical_io_reads,
        avg_logical_io_writes,
        min_logical_io_writes,
        max_logical_io_writes,
        avg_physical_io_reads,
        min_physical_io_reads,
        max_physical_io_reads,
        avg_clr_time_us,
        min_clr_time_us,
        max_clr_time_us,
        min_dop,
        max_dop,
        avg_query_max_used_memory,
        min_query_max_used_memory,
        max_query_max_used_memory,
        avg_rowcount,
        min_rowcount,
        max_rowcount,
        avg_num_physical_io_reads,
        min_num_physical_io_reads,
        max_num_physical_io_reads,
        avg_log_bytes_used,
        min_log_bytes_used,
        max_log_bytes_used,
        avg_tempdb_space_used,
        min_tempdb_space_used,
        max_tempdb_space_used,
        plan_type,
        plan_forcing_type,
        is_forced_plan,
        force_failure_count,
        last_force_failure_reason,
        compatibility_level,
        query_plan_hash,
        replica_role,
        runtime_stats_interval_id,
        interval_start_time_utc,
        interval_end_time_utc
    )
    SELECT DISTINCT ON (" + BatchIdentityColumns + @")
        s.collection_time,
        s.server_id,
        s.database_name,
        s.query_id,
        s.plan_id,
        s.execution_type_desc,
        s.first_execution_time,
        s.last_execution_time,
        s.module_name,
        s.query_text,
        s.query_hash,
        s.execution_count,
        s.avg_duration_us,
        s.min_duration_us,
        s.max_duration_us,
        s.avg_cpu_time_us,
        s.min_cpu_time_us,
        s.max_cpu_time_us,
        s.avg_logical_io_reads,
        s.min_logical_io_reads,
        s.max_logical_io_reads,
        s.avg_logical_io_writes,
        s.min_logical_io_writes,
        s.max_logical_io_writes,
        s.avg_physical_io_reads,
        s.min_physical_io_reads,
        s.max_physical_io_reads,
        s.avg_clr_time_us,
        s.min_clr_time_us,
        s.max_clr_time_us,
        s.min_dop,
        s.max_dop,
        s.avg_query_max_used_memory,
        s.min_query_max_used_memory,
        s.max_query_max_used_memory,
        s.avg_rowcount,
        s.min_rowcount,
        s.max_rowcount,
        s.avg_num_physical_io_reads,
        s.min_num_physical_io_reads,
        s.max_num_physical_io_reads,
        s.avg_log_bytes_used,
        s.min_log_bytes_used,
        s.max_log_bytes_used,
        s.avg_tempdb_space_used,
        s.min_tempdb_space_used,
        s.max_tempdb_space_used,
        s.plan_type,
        s.plan_forcing_type,
        s.is_forced_plan,
        s.force_failure_count,
        s.last_force_failure_reason,
        s.compatibility_level,
        s.query_plan_hash,
        s.replica_role,
        s.runtime_stats_interval_id,
        s.interval_start_time_utc,
        s.interval_end_time_utc
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = $2
    AND   s.database_name = ANY ($3::text[])
    AND   s.first_execution_time IS NOT NULL
    ORDER BY
        " + BatchIdentityColumns + @",
        s.execution_count DESC
    ON CONFLICT (" + IdentityColumns + @")
    DO UPDATE SET
        collection_time = EXCLUDED.collection_time,
        last_execution_time = EXCLUDED.last_execution_time,
        module_name = EXCLUDED.module_name,
        query_text = EXCLUDED.query_text,
        query_hash = EXCLUDED.query_hash,
        execution_count = EXCLUDED.execution_count,
        avg_duration_us = EXCLUDED.avg_duration_us,
        min_duration_us = EXCLUDED.min_duration_us,
        max_duration_us = EXCLUDED.max_duration_us,
        avg_cpu_time_us = EXCLUDED.avg_cpu_time_us,
        min_cpu_time_us = EXCLUDED.min_cpu_time_us,
        max_cpu_time_us = EXCLUDED.max_cpu_time_us,
        avg_logical_io_reads = EXCLUDED.avg_logical_io_reads,
        min_logical_io_reads = EXCLUDED.min_logical_io_reads,
        max_logical_io_reads = EXCLUDED.max_logical_io_reads,
        avg_logical_io_writes = EXCLUDED.avg_logical_io_writes,
        min_logical_io_writes = EXCLUDED.min_logical_io_writes,
        max_logical_io_writes = EXCLUDED.max_logical_io_writes,
        avg_physical_io_reads = EXCLUDED.avg_physical_io_reads,
        min_physical_io_reads = EXCLUDED.min_physical_io_reads,
        max_physical_io_reads = EXCLUDED.max_physical_io_reads,
        avg_clr_time_us = EXCLUDED.avg_clr_time_us,
        min_clr_time_us = EXCLUDED.min_clr_time_us,
        max_clr_time_us = EXCLUDED.max_clr_time_us,
        min_dop = EXCLUDED.min_dop,
        max_dop = EXCLUDED.max_dop,
        avg_query_max_used_memory = EXCLUDED.avg_query_max_used_memory,
        min_query_max_used_memory = EXCLUDED.min_query_max_used_memory,
        max_query_max_used_memory = EXCLUDED.max_query_max_used_memory,
        avg_rowcount = EXCLUDED.avg_rowcount,
        min_rowcount = EXCLUDED.min_rowcount,
        max_rowcount = EXCLUDED.max_rowcount,
        avg_num_physical_io_reads = EXCLUDED.avg_num_physical_io_reads,
        min_num_physical_io_reads = EXCLUDED.min_num_physical_io_reads,
        max_num_physical_io_reads = EXCLUDED.max_num_physical_io_reads,
        avg_log_bytes_used = EXCLUDED.avg_log_bytes_used,
        min_log_bytes_used = EXCLUDED.min_log_bytes_used,
        max_log_bytes_used = EXCLUDED.max_log_bytes_used,
        avg_tempdb_space_used = EXCLUDED.avg_tempdb_space_used,
        min_tempdb_space_used = EXCLUDED.min_tempdb_space_used,
        max_tempdb_space_used = EXCLUDED.max_tempdb_space_used,
        plan_type = EXCLUDED.plan_type,
        plan_forcing_type = EXCLUDED.plan_forcing_type,
        is_forced_plan = EXCLUDED.is_forced_plan,
        force_failure_count = EXCLUDED.force_failure_count,
        last_force_failure_reason = EXCLUDED.last_force_failure_reason,
        compatibility_level = EXCLUDED.compatibility_level,
        query_plan_hash = EXCLUDED.query_plan_hash,
        interval_start_time_utc = EXCLUDED.interval_start_time_utc,
        interval_end_time_utc = EXCLUDED.interval_end_time_utc
    WHERE (EXCLUDED.collection_time, EXCLUDED.execution_count) > (t.collection_time, t.execution_count)
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM upserted) AS applied_rows,
    b.raw_rows,
    b.null_first_execution_rows
FROM batch_rows AS b;";

    /* ---- the read-source decision (grid/MCP top reads, #3953, ruling issuecomment-5836972848) ------------ */
    /* This is a SEPARATE decision from QueryStoreIntervalLatest.ReadsTableAsync (V143's, which PLAN_REGRESSION
       alone uses): the two tables have independent coverage claims, and the reads gated here (the Queries grid,
       its MCP twin) have an upper bound and a per-read minimum window that PLAN_REGRESSION's
       always-open, always-14-day read does not. V143's shape is still the model for clauses 1-3; read it once,
       by offset, before touching this. */
    /* The clamp (ClampedStart) is the read's lower bound AT and ABOVE raw's chunk floor. Below that floor the
       table is the only record left, and ResolveReadAsync extends the bound down to
       ExactBelowFloorStart = max(window start, filled_since, table floor + one day) and no further: those are the
       three bounds under which the table provably holds what raw held before its purge.
       There is no per-server probe on this table: its only secondary index leads with first_execution_time. */

    /// <summary>
    /// This table's store-shape inputs for one server: its coverage row's <c>filled_since</c> and
    /// <c>applied_through</c> (both NULL when there is no coverage row), whether it has pending batches, and
    /// whether TimescaleDB's catalog views exist on this store. Same shape as
    /// <see cref="QueryStoreIntervalLatest.ReadSourceInputsSql"/> plus <c>applied_through</c>, which this gate's
    /// clause 4 needs and V143's PLAN_REGRESSION gate does not (PLAN_REGRESSION's read has no upper bound).
    /// </summary>
    public const string ReadSourceInputsSql = @"
SELECT
    c.filled_since,
    c.applied_through,
    EXISTS
    (
        SELECT
            1
        FROM collect.query_store_interval_wide_pending AS p
        WHERE p.server_id = $1
    ) AS has_pending,
    to_regclass('timescaledb_information.chunks') IS NOT NULL AS has_timescale
FROM (SELECT 1) AS one
LEFT JOIN collect.query_store_interval_wide_coverage AS c
  ON c.server_id = $1;";

    /// <summary>
    /// The floors, from TimescaleDB's catalog (metadata, never a scan), exactly as
    /// <see cref="QueryStoreIntervalLatest.ChunkFloorsSql"/>: raw's oldest chunk, and this table's when
    /// TimescaleDB's catalog carries it as a hypertable. It does not today — this table is engine-plain, per its
    /// migration's own comment — so the <c>table_is_hypertable</c> arm is defensive, matching V143's.
    /// </summary>
    public const string ChunkFloorsSql = @"
SELECT
    (
        SELECT
            MIN(ch.range_start) AT TIME ZONE 'UTC'
        FROM timescaledb_information.chunks AS ch
        WHERE ch.hypertable_schema = 'collect'
        AND   ch.hypertable_name = 'query_store_stats'
    ) AS raw_floor,
    EXISTS
    (
        SELECT
            1
        FROM timescaledb_information.hypertables AS h
        WHERE h.hypertable_schema = 'collect'
        AND   h.hypertable_name = 'query_store_interval_wide'
    ) AS table_is_hypertable,
    (
        SELECT
            MIN(ch.range_start) AT TIME ZONE 'UTC'
        FROM timescaledb_information.chunks AS ch
        WHERE ch.hypertable_schema = 'collect'
        AND   ch.hypertable_name = 'query_store_interval_wide'
    ) AS table_floor;";

    /// <summary>The table's floor for one server where the table is a plain heap (today, always): its oldest
    /// interval.</summary>
    public const string PlainTableFloorSql = @"
SELECT
    MIN(t.first_execution_time)
FROM collect.query_store_interval_wide AS t
WHERE t.server_id = $1;";

    /// <summary>
    /// Clause 6's probe (2026 field review of #4310 site 3): whether the window holds any pre-tier-2 row (no
    /// <c>runtime_stats_interval_id</c>, no reconstructable identity) for this server. The upsert's identity
    /// (<see cref="IdentityColumns"/>) does not carry <c>collection_time</c>, so a legacy row re-fetched across
    /// several cycles — the collector's normal behaviour for an open interval, before tier 2 assigned intervals
    /// their own identity — keeps only its LATEST snapshot in the table; every caller that places these rows at
    /// <c>collection_time</c> (both duration-trend arm 2's) needs every one of them, not the running maximum. A
    /// field store carries zero such rows once raw retention (days) has aged the post-upgrade window out, so
    /// this scan is a bounded, indexed (<c>server_id</c>, <c>collection_time</c>) read on the common path and
    /// costs nothing extra there; it is what lets clause 6 answer TRUE only on the rare upgraded-recently store
    /// this table cannot yet serve correctly. Bounded to the SAME range the table read would use
    /// (<see cref="ClampedStart"/> through <paramref name="windowEnd"/> in <see cref="ReadsTableAsync"/>), not
    /// the caller's raw windowStart, so a legacy row outside the served range cannot force a needless refusal.
    /// </summary>
    public const string HasLegacyRowSql = @"
SELECT EXISTS
(
    SELECT
        1
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time >= $2
    AND   s.collection_time <= $3
    AND   s.interval_start_time_utc IS NULL
);";

    /// <summary>
    /// The Queries grid's own minimum window (#3953 clause 5, ruling issuecomment-5836972848 item 5): below this
    /// the table's fixed per-decision round trips (two more queries plus a transaction) cost more than the read
    /// they would save, so the gate reads raw regardless of coverage. One constant per read — the MCP read (a
    /// later lane) sets its own, and may not share this value. Stays 12 hours (lane B4t, rig-d4, 15-day
    /// seed at a field store's rate, end-to-end through the viewer's grid read): median of 5 at the ruled
    /// 12-hour cell, table 1488.1 ms (spread 1118.9-1563.3) against raw 1725.6 ms (spread 1239.4-2512.5) — the
    /// table is at least as fast as raw there, so the ruling keeps this threshold.
    /// </summary>
    public static readonly TimeSpan GridWideMinWindow = TimeSpan.FromHours(12);

    /// <summary>
    /// How far below the window start clause 3 still allows the table (V143's clause 3 restated: "B is the
    /// window minus a day, and an interval spans at most a day"). One Query Store runtime-stats interval can
    /// span up to a day, so an interval that STARTED up to a day before the window can still have executions
    /// inside it. Restated here (rather than referencing <c>PgFactCollector.QueryPerf.PlanRegressionSkewMarginDays</c>)
    /// because the viewer does not reference the service assembly (#1661 / #2530).
    /// </summary>
    public static readonly TimeSpan IntervalSpanMargin = TimeSpan.FromDays(1);

    /// <summary>The slowest Query Store collection cadence (and the longest gap between two collections near the
    /// purge edge) for which a read may go below raw's floor.</summary>
    public static readonly TimeSpan MaxBelowFloorCadence = TimeSpan.FromMinutes(60);

    /// <summary>How far past <c>first_execution_time</c> a deleted interval's last snapshot can still land in raw:
    /// one interval length, plus the slowest allowed collection cadence, plus the collector's catch-up cap. The
    /// per-server reads of the table also use it as their <c>first_execution_time</c> floor
    /// (<see cref="PurgeEdgeMarginSql"/>, #4605).</summary>
    public static readonly TimeSpan PurgeEdgeMargin = IntervalSpanMargin + MaxBelowFloorCadence + WatermarkPolicy.MaxCatchup;

    /// <summary>
    /// <see cref="PurgeEdgeMargin"/> as a Postgres interval literal (rounded UP to whole minutes, so the SQL form
    /// can never be shorter than the margin), for the <c>first_execution_time &gt;= &lt;window start&gt; -
    /// PurgeEdgeMarginSql</c> floor the per-server reads of this table carry (#4605). Among the table's indexes are the
    /// unique key, which leads with <c>server_id</c> and holds <c>first_execution_time</c> as a key column, and
    /// <c>idx_query_store_interval_wide_first_exec</c>. A read that filters only by <c>collection_time</c> (or
    /// <c>interval_start_time_utc</c>) is served by neither, so a per-server read walked all of the server's rows;
    /// the floor filters the unique key's entries before the heap. The Custom Views route, for all servers or some,
    /// does not carry the floor (see <c>ComposeCompiler.BuildFactRelation</c>).
    /// <para><b>Why no row is lost.</b> The collector keeps only intervals with <c>end_time &gt; @cutoff_time</c>,
    /// and the cutoff is never more than <see cref="WatermarkPolicy.MaxCatchup"/> before the row's
    /// <c>collection_time</c> (<c>WatermarkPolicy.ClampCatchup</c>: <c>C - MaxCatchup</c> with no watermark).
    /// Backfill slices span at most <c>QueryStoreBackfillState.MaxSliceSpan</c> (no more than
    /// <see cref="WatermarkPolicy.MaxCatchup"/>) and stamp the slice ceiling as <c>collection_time</c>. An interval
    /// spans at most <see cref="IntervalSpanMargin"/>, and <c>first_execution_time</c> lies inside it. So every row
    /// has <c>first_execution_time &gt; collection_time - (IntervalSpanMargin + MaxCatchup)</c>, and this margin is
    /// that bound plus <see cref="MaxBelowFloorCadence"/> of slack. The upsert copies each row's
    /// <c>collection_time</c> from its raw snapshot (never <c>now()</c>), so the two stored values the collector
    /// compared are the two the read compares: clock skew adds nothing.</para>
    /// </summary>
    public static readonly string PurgeEdgeMarginSql =
        $"interval '{(long)Math.Ceiling(PurgeEdgeMargin.TotalMinutes)} minutes'";

    /// <summary>True when the server's effective <c>query_store</c> cadence (null: the default) is at most
    /// <see cref="MaxBelowFloorCadence"/> and the largest gap between successive collections near the purge edge is
    /// too. An unknown gap (no collection-log rows there) is not allowed.</summary>
    public static bool CadenceAllowsBelowFloor(int? effectiveQueryStoreCadenceMinutes, TimeSpan? maxObservedGap) =>
        TimeSpan.FromMinutes(CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(
            effectiveQueryStoreCadenceMinutes ?? CollectorScheduleDefaults.All["query_store"].FrequencyMinutes)) <= MaxBelowFloorCadence
        && maxObservedGap is TimeSpan g && g <= MaxBelowFloorCadence;

    /// <summary>The per-server and fleet-wide <c>query_store</c> frequency overrides. $1 server_id.</summary>
    public const string CadenceOverridesSql = @"
SELECT server_id, frequency_minutes
FROM config.config_collector_schedules
WHERE lower(collector_name) = 'query_store'
AND   (server_id = $1 OR server_id IS NULL)";

    /// <summary>The largest gap between successive successful (<c>status = 'SUCCESS'</c>) <c>query_store</c> collections whose time is in
    /// [$2, $3], as seconds. The two boundary instants join the log rows as virtual rows, so the lead-in gap (from
    /// $2 to the first row) and the tail gap (from the last row to $3) count; a range with no log row therefore
    /// reads as the whole span, which is not allowed. Only SUCCESS rows count: the sole Darling writer of
    /// <c>collect.collection_log</c> (DarlingObservability.cs:87, called with <c>collection_time = UtcNow</c> at
    /// insert, :271) never backdates a row, so a settled window's answer cannot change; SKIPPED is written only by
    /// Lite, which has no interval table. Bounded range on
    /// <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c>. $1 server_id.</summary>
    public static readonly string MaxCollectionGapSql = @"
SELECT MAX(EXTRACT(EPOCH FROM (t.collection_time - t.prev)))::float8
FROM (
    SELECT c.collection_time, LAG(c.collection_time) OVER (ORDER BY c.collection_time) AS prev
    FROM (
        SELECT $2::timestamp AS collection_time
        UNION ALL
        SELECT $3::timestamp
        UNION ALL
        SELECT l.collection_time
        FROM collect.collection_log AS l
        WHERE l.server_id = $1
        AND   l.collector_name = 'query_store'
        AND   l.status = 'SUCCESS'
        AND   l.collection_time >= $2
        AND   l.collection_time <= $3
    ) AS c
) AS t";

    /// <summary>
    /// The rule: read <c>query_store_interval_wide</c> for a grid/MCP top read if and only if all five of
    /// these hold, otherwise run today's raw statement unchanged (ruling issuecomment-5836972848; review D4R
    /// items H3, M1). Two further clauses are the caller's, checked outside this pure function because each
    /// needs its own round trip: the viewer's store must report schema version 145 or later (needs the
    /// viewer's own connection probe), and <see cref="ReadsTableAsync"/>'s clause 6 (a 2026 field review) —
    /// the window must hold no legacy row (<c>interval_start_time_utc IS NULL</c>, see
    /// <see cref="HasLegacyRowSql"/>) — checked once clauses 1-5 already say "table", so a store refused for
    /// any other reason never pays the probe.
    /// <list type="number">
    /// <item>Coverage exists (<paramref name="filledSince"/> is not null) and there is no pending batch.</item>
    /// <item><c>filledSince &lt;= max(R, S)</c> (<paramref name="rawFloor"/>, <paramref name="windowStart"/>):
    /// the table holds every snapshot raw's own read would, and may hold more (the ruled window extension).</item>
    /// <item><c>R &gt;= H</c> or <c>S - 1 day &gt;= H</c> (<paramref name="tableFloor"/>,
    /// <see cref="IntervalSpanMargin"/>): raw holds no history the table has dropped, or every interval the
    /// window needs starts inside the table. A NULL <paramref name="tableFloor"/> means the table holds nothing
    /// for the server, and clause 2 already refuses that case.</item>
    /// <item>A literal <paramref name="literalWindowEnd"/> (a custom range, MCP <c>as_of</c>) must be at or
    /// after <paramref name="appliedThrough"/>. NULL means an open end (a preset), which skips this clause
    /// entirely: the WPF preset's own end is the viewer's clock, not the store's, so comparing it against
    /// <paramref name="appliedThrough"/> would send a slow-clocked viewer to raw on every ordinary read (M1).</item>
    /// <item>The window (<paramref name="windowEnd"/> minus <paramref name="windowStart"/>) is at least
    /// <paramref name="minWindow"/> (<see cref="GridWideMinWindow"/> for the grid).</item>
    /// </list>
    /// A NULL <paramref name="rawFloor"/> is raw with no chunk floor (a plain store, whose raw keeps 30 days):
    /// minus infinity, which can only pick raw. Every input errs toward raw, never toward an under-read, same as
    /// V143's.
    /// </summary>
    public static bool UseTable(
        DateTime? filledSince,
        bool hasPending,
        DateTime? rawFloor,
        DateTime windowStart,
        DateTime windowEnd,
        DateTime? literalWindowEnd,
        DateTime appliedThrough,
        DateTime? tableFloor,
        TimeSpan minWindow)
    {
        if (filledSince is not DateTime f || hasPending)
        {
            return false;
        }

        var rawReadsFrom = rawFloor is DateTime r && r > windowStart ? r : windowStart;
        if (f > rawReadsFrom)
        {
            return false;
        }

        if (tableFloor is DateTime h)
        {
            var skewFloor = windowStart - IntervalSpanMargin;
            if (!((rawFloor is DateTime floor && floor >= h) || skewFloor >= h))
            {
                return false;
            }
        }

        if (literalWindowEnd is DateTime end && end < appliedThrough)
        {
            return false;
        }

        return windowEnd - windowStart >= minWindow;
    }

    /// <summary>The clamp (review D4R H3): the table read's own lower bound AT and ABOVE raw's floor,
    /// <c>max(windowStart, rawFloor)</c>; <see cref="ResolveReadAsync"/> extends it below the floor.
    /// Raw chunks drop whole, so bounding the table there returns exactly the raw read's own answer over the
    /// snapshots raw still holds, reaching further back only where raw has already dropped the chunk.</summary>
    public static DateTime ClampedStart(DateTime? rawFloor, DateTime windowStart) =>
        rawFloor is DateTime r && r > windowStart ? r : windowStart;

    /// <summary>
    /// Option D: what one <c>_wide</c> read may bind as its lower bound, and what it must disclose. At or above
    /// raw's chunk floor the bound is <see cref="ClampedStart"/>, unchanged: raw chunks drop whole, so the table
    /// there returns exactly the raw read's answer. Below the floor raw no longer holds anything to compare, and
    /// the table is the only record. It reads down to <see cref="ExactBelowFloorStart"/> and no further, because
    /// below that bound it cannot prove it holds what raw held before the purge.
    /// </summary>
    /// <param name="UseTable">The gate's answer (<see cref="UseTable"/> plus clause 6), unchanged.</param>
    /// <param name="ClampedStart">Today's clamp, <c>max(windowStart, rawFloor)</c>.</param>
    /// <param name="ReadStart">The bound the caller's table read binds as its lower bound: the below-floor start
    /// when one exists, else <see cref="ClampedStart"/>.</param>
    /// <param name="BelowFloorStart">Non-null only when the read reaches below raw's floor.</param>
    /// <param name="StartBound">Which bound set <see cref="ReadStart"/>; a surface names it when the read is
    /// truncated.</param>
    public readonly record struct WideReadPlan(
        bool UseTable,
        DateTime ClampedStart,
        DateTime ReadStart,
        DateTime? BelowFloorStart,
        WideStartBound StartBound = WideStartBound.Window)
    {
        /// <summary>Where the table's proven-complete history starts for this read: <see cref="ReadStart"/>
        /// whenever the table serves. NULL when the table does not serve (the gate could not decide), because the
        /// surface then reports the raw tier's own reach.</summary>
        public DateTime? EffectiveStart => UseTable ? ReadStart : null;
    }

    /// <summary>#4689: which bound set a <c>_wide</c> read's lower bound.</summary>
    public enum WideStartBound
    {
        /// <summary>The window's own start: nothing was cut.</summary>
        Window,
        /// <summary>The table's coverage claim (<c>filled_since</c>) starts later than the window.</summary>
        FilledSince,
        /// <summary>The 9-day purge edge plus the one-day interval-span margin starts later than the window.</summary>
        TablePurgeEdge,
        /// <summary>No exact span below raw's chunk floor: the read is clamped at the floor, as before #4689.</summary>
        RawFloor,
        /// <summary>The server's Query Store collection cadence, or a gap in its collection log near the purge edge, is
        /// over <see cref="MaxBelowFloorCadence"/>: the read is clamped at raw's floor.</summary>
        RawFloorSlowCadence,
        /// <summary>The table is younger than the gap check's window: the collection log does not yet cover the
        /// interval table's purge edge, so the read is clamped at raw's floor.</summary>
        RawFloorLogNotYetCovering,
    }

    /// <summary>#4689: the note a table-served Query Store read carries when the interval table started it later than
    /// the window. The MCP top-queries table route (one server) and Compose's panel (the servers in scope) both
    /// take their text from here. <paramref name="settingServer"/> names the server whose history set the common
    /// start of a many-server read; when null the reason says "these servers".</summary>
    public static string HistoryNote(DateTime effectiveStart, WideStartBound bound, bool manyServers, string? settingServer = null)
    {
        var scope = manyServers ? "the servers in scope" : "this server";
        var reason = bound switch
        {
            WideStartBound.FilledSince =>
                $"The interval table began keeping complete history for {(!manyServers ? "this server" : settingServer ?? "these servers")} at {effectiveStart:o}.",
            WideStartBound.TablePurgeEdge =>
                "The interval table keeps 9 days, and intervals that began before its purge edge are not read.",
            WideStartBound.RawFloorSlowCadence =>
                $"{(!manyServers ? "This server's" : settingServer is null ? "A server's" : settingServer + "'s")} Query Store collection cadence is over 60 minutes, or its collection log shows a longer gap, so older intervals are not read from the interval table.",
            WideStartBound.RawFloorLogNotYetCovering =>
                "The collection log does not yet cover the interval table's purge edge, so older intervals are not read from the interval table.",
            _ =>
                "The read is clamped at the raw tier's retention floor: nothing older than it can be shown exactly.",
        };
        return $"The window reaches further back than the Query Store history this store holds for {scope}. Nothing older than {effectiveStart:o} was read. Past the raw tier's retention, intervals are read from the per-interval table (kept 9 days), which holds exactly what raw held for them. "
            + reason;
    }

    /// <summary>#4689: the short parenthetical a table-served grid banner appends to its "Showing since" line, or
    /// null when the window's own start or raw's floor set the start.</summary>
    public static string? BannerReason(WideStartBound bound) => bound switch
    {
        WideStartBound.FilledSince => " (interval table complete from then)",
        WideStartBound.TablePurgeEdge => " (interval table keeps 9 days)",
        WideStartBound.RawFloorSlowCadence => " (slow Query Store cadence)",
        WideStartBound.RawFloorLogNotYetCovering => " (collection log not yet covering the purge edge)",
        _ => null,
    };

    /// <summary>The window, in days after the table floor's UTC day, the cached gap verdict covers. A superset of
    /// the two days the decision needs from any floor inside that day, so the cached maximum gap is never smaller
    /// than the exact one: a gap inside the decision's sub-window lies inside a gap of the superset series. The
    /// verdict can only clamp more, never less.</summary>
    internal const int GapCacheWindowDays = 3;

    /// <summary>A settle margin beyond <see cref="WatermarkPolicy.MaxCatchup"/>: a gap window counts as settled
    /// (no collection can still land inside it) only once its end is this much older than the catch-up cap.</summary>
    internal static readonly TimeSpan GapSettleMargin = TimeSpan.FromMinutes(5);

    /// <summary>True when the gap window of <paramref name="floorDay"/> ends at least
    /// <see cref="WatermarkPolicy.MaxCatchup"/> + <see cref="GapSettleMargin"/> before now.</summary>
    internal static bool GapWindowSettled(DateTime floorDay) =>
        floorDay.AddDays(GapCacheWindowDays) <= GapCacheClock() - WatermarkPolicy.MaxCatchup - GapSettleMargin;

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<(string StoreKey, int ServerId, DateTime FloorDay, int CadenceMinutes), TimeSpan?> GapCache = new();

    private static int _gapReads;

    /// <summary>The clock the settled-window rule reads; tests replace it.</summary>
    internal static Func<DateTime> GapCacheClock = static () => DateTime.UtcNow;

    private static readonly ConcurrentDictionary<string, int> GapReadsByStore = new();

    private static string GapStoreKey(NpgsqlConnection connection) => connection.Host + ":" + connection.Port + "/" + connection.Database;

    /// <summary>Actual executions of <see cref="MaxCollectionGapSql"/> against the connection's store since the last reset.</summary>
    internal static int GapReadsForStoreForTests(NpgsqlConnection connection) => GapReadsByStore.TryGetValue(GapStoreKey(connection), out var n) ? n : 0;

    /// <summary>Actual executions of <see cref="MaxCollectionGapSql"/> since the last reset.</summary>
    internal static int GapReadsForTests => Volatile.Read(ref _gapReads);

    /// <summary>Empties the gap-verdict cache and zeroes <see cref="GapReadsForTests"/>.</summary>
    internal static void ResetGapCacheForTests()
    {
        GapCache.Clear();
        Volatile.Write(ref _gapReads, 0);
        GapReadsByStore.Clear();
        GapCacheClock = static () => DateTime.UtcNow;
    }

    /// <summary>
    /// The largest query_store collection gap over [floor day, floor day + <see cref="GapCacheWindowDays"/>],
    /// cached per (store, server, floor day, cadence). The range sits 7-9 days back in compressed
    /// <c>collection_log</c> chunks, so each uncached read decompresses them. Caching is exact because the
    /// range is in the past and <c>collection_log</c> rows are only ever inserted with the wall-clock
    /// <c>collection_time</c> of the insert (DarlingObservability.InsertCollectionLogSql callers pass
    /// UtcNow), never backdated. Only a SETTLED verdict, lifted or clamped, is cached: the window's end
    /// (floor day + 3 d) must be at least <see cref="WatermarkPolicy.MaxCatchup"/> + <see cref="GapSettleMargin"/>
    /// in the past (<see cref="GapWindowSettled"/>), because a collection can still land in a window that is not.
    /// An unsettled verdict is recomputed on every decision. The
    /// window derives from the key's floor day, never from the raw floor, so a verdict cached for day D can
    /// never serve a floor in day D+1. One entry is kept per (store, server): storing a new floor day or
    /// cadence removes that server's older entries. A read that yields no value is not cached.
    /// </summary>
    internal static async Task<TimeSpan?> MaxCollectionGapCachedAsync(
        NpgsqlConnection connection, int serverId, DateTime tableFloor, int frequencyMinutes, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var floorDay = DateTime.SpecifyKind(tableFloor.Date, DateTimeKind.Unspecified);
        var storeKey = GapStoreKey(connection);
        var key = (storeKey, serverId, floorDay, frequencyMinutes);
        if (GapCache.TryGetValue(key, out var cached))
        {
            return cached;
        }

        TimeSpan? gap = null;
        await using (var gapCommand = new NpgsqlCommand(MaxCollectionGapSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            gapCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            gapCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floorDay });
            gapCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floorDay.AddDays(GapCacheWindowDays) });
            Interlocked.Increment(ref _gapReads);
            GapReadsByStore.AddOrUpdate(storeKey, 1, (_, n) => n + 1);
            if (await gapCommand.ExecuteScalarAsync(cancellationToken) is double seconds)
            {
                gap = TimeSpan.FromSeconds(seconds);
            }
        }

        if (gap is not null && GapWindowSettled(floorDay))
        {
            foreach (var stale in GapCache.Keys)
            {
                if (stale.StoreKey == storeKey && stale.ServerId == serverId)
                {
                    GapCache.TryRemove(stale, out _);
                }
            }

            GapCache[key] = gap;
        }

        return gap;
    }

    private enum BelowFloorVerdict
    {
        Allowed,
        SlowCadence,
        LogNotYetCovering,
    }

    private static async Task<BelowFloorVerdict> CadenceAllowsAsync(
        NpgsqlConnection connection, int serverId, DateTime tableFloor, int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken)
    {
        int? perServer = null;
        int? fleet = null;
        try
        {
            await using var cadence = new NpgsqlCommand(CadenceOverridesSql, connection) { CommandTimeout = commandTimeoutSeconds };
            cadence.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            await using var reader = await cadence.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var minutes = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1);
                if (reader.IsDBNull(0))
                {
                    fleet = minutes;
                }
                else
                {
                    perServer = minutes;
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The cadence is unknown, so reading below the floor is not allowed; the at-floor table read stands. */
            logger?.LogWarning(ex, "Query Store cadence probe failed for server {ServerId}; not reading below raw's floor", serverId);
            return BelowFloorVerdict.SlowCadence;
        }

        var frequency = CollectorScheduleDefaults.ResolveFrequencyMinutes("query_store", perServer, fleet);
        if (TimeSpan.FromMinutes(CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(frequency)) > MaxBelowFloorCadence)
        {
            return BelowFloorVerdict.SlowCadence;
        }

        /* A young table's gap window reaches into time the log has not settled over yet: its virtual end row would
           read as a long tail gap, so name the real reason before reading the gap. */
        if (!GapWindowSettled(DateTime.SpecifyKind(tableFloor.Date, DateTimeKind.Unspecified)))
        {
            return BelowFloorVerdict.LogNotYetCovering;
        }

        var gap = await MaxCollectionGapCachedAsync(connection, serverId, tableFloor, frequency, commandTimeoutSeconds, cancellationToken);

        return CadenceAllowsBelowFloor(frequency, gap) ? BelowFloorVerdict.Allowed : BelowFloorVerdict.SlowCadence;
    }

    /// <summary>
    /// The lowest instant below raw's floor from which the table provably equals what raw said before its purge,
    /// or NULL when the read cannot go below the floor (no floor, the floor at or below the window start, an
    /// empty table, or no exact span left). Three bounds, each a hazard:
    /// <list type="bullet">
    /// <item><paramref name="filledSince"/>: the coverage claim starts there and is never backdated, and a gap
    /// check restarts it above a hole (<see cref="ResetCoverageSql"/>).</item>
    /// <item><paramref name="tableFloor"/> + <see cref="IntervalSpanMargin"/>: the 9-day purge deletes row by
    /// row on <c>first_execution_time</c>, so a row whose interval began before the table floor is gone even when
    /// its <c>collection_time</c> is inside the window.</item>
    /// <item><paramref name="windowStart"/>: never read before the window asked for.</item>
    /// </list>
    /// </summary>
    public static (DateTime Start, WideStartBound Bound)? ExactBelowFloorStart(
        DateTime? rawFloor, DateTime windowStart, DateTime filledSince, DateTime? tableFloor)
    {
        if (rawFloor is not DateTime r || r <= windowStart || tableFloor is not DateTime h)
        {
            return null;
        }

        /* Ties prefer the coverage claim over the purge edge over the window. */
        var start = windowStart;
        var bound = WideStartBound.Window;
        var retentionSafe = h + PurgeEdgeMargin;
        if (retentionSafe >= start)
        {
            start = retentionSafe;
            bound = WideStartBound.TablePurgeEdge;
        }

        if (filledSince >= start)
        {
            start = filledSince;
            bound = WideStartBound.FilledSince;
        }

        return start < r ? (start, bound) : null;
    }

    /// <summary>
    /// <see cref="ResolveReadAsync"/> reduced to the gate's answer and the clamp (<see cref="ClampedStart"/>): the
    /// table read's lower bound at and above raw's chunk floor. A caller that reads the table below the floor uses
    /// <see cref="ResolveReadAsync"/> and binds <see cref="WideReadPlan.ReadStart"/> instead.
    /// </summary>
    public static async Task<(bool UseTable, DateTime ClampedStart)> ReadsTableAsync(
        NpgsqlConnection connection,
        int serverId,
        DateTime windowStart,
        DateTime windowEnd,
        DateTime? literalWindowEnd,
        TimeSpan minWindow,
        int commandTimeoutSeconds,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        var p = await ResolveReadAsync(
            connection, serverId, windowStart, windowEnd, literalWindowEnd, minWindow,
            commandTimeoutSeconds, logger, cancellationToken);
        return (p.UseTable, p.ClampedStart);
    }

    /// <summary>
    /// <see cref="UseTable"/> plus the store round trips it needs, the clamp (<see cref="ClampedStart"/>), and the
    /// lower bound the table read may bind (<see cref="WideReadPlan.ReadStart"/>), which reaches below raw's chunk
    /// floor down to <see cref="ExactBelowFloorStart"/> — all computed from the SAME <c>rawFloor</c> this decision
    /// read, on the SAME connection, so the decision and the bound cannot see different snapshots.
    /// </summary>
    public static async Task<WideReadPlan> ResolveReadAsync(
        NpgsqlConnection connection,
        int serverId,
        DateTime windowStart,
        DateTime windowEnd,
        DateTime? literalWindowEnd,
        TimeSpan minWindow,
        int commandTimeoutSeconds,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        /* The probes bind these as timestamp-without-time-zone, which Npgsql refuses for Kind=Utc; a caller that
           resolves its window from DateTime.UtcNow (the MCP tool) would otherwise fail every decision and read raw. */
        windowStart = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified);
        windowEnd = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified);
        literalWindowEnd = literalWindowEnd is DateTime lw ? DateTime.SpecifyKind(lw, DateTimeKind.Unspecified) : null;
        try
        {
            DateTime? filledSince;
            DateTime appliedThrough;
            bool hasPending;
            bool hasTimescale;
            await using (var inputs = new NpgsqlCommand(ReadSourceInputsSql, connection) { CommandTimeout = commandTimeoutSeconds })
            {
                inputs.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                await using var reader = await inputs.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                filledSince = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                appliedThrough = reader.IsDBNull(1) ? DateTime.MinValue : reader.GetDateTime(1);
                hasPending = reader.GetBoolean(2);
                hasTimescale = reader.GetBoolean(3);
            }

            if (filledSince is null || hasPending)
            {
                return new WideReadPlan(false, windowStart, windowStart, null);
            }

            /* Review D4R H1: clauses 4 and 5 need no table floor at all, so check them before the two
               remaining round trips (ChunkFloorsSql's metadata read and, worse, PlainTableFloorSql's
               unindexed server-wide scan). A short window or a literal end before appliedThrough can only
               ever land on "raw" (UseTable's own tail), so failing here saves both queries. */
            if (windowEnd - windowStart < minWindow || (literalWindowEnd is DateTime e && e < appliedThrough))
            {
                return new WideReadPlan(false, windowStart, windowStart, null);
            }

            DateTime? rawFloor = null;
            DateTime? tableFloor = null;
            var tableIsHypertable = false;
            if (hasTimescale)
            {
                await using var floors = new NpgsqlCommand(ChunkFloorsSql, connection) { CommandTimeout = commandTimeoutSeconds };
                await using var reader = await floors.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                rawFloor = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                tableIsHypertable = reader.GetBoolean(1);
                tableFloor = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
            }

            /* Clause 2 (filledSince <= max(rawFloor, windowStart)) needs only rawFloor, already in hand from
               ChunkFloorsSql's metadata read (or never set, on a non-Timescale store) — never the table floor.
               An upgraded store whose claim doesn't cover the window yet fails here, before PlainTableFloorSql's
               scan, instead of after it. */
            if (filledSince > ClampedStart(rawFloor, windowStart))
            {
                return new WideReadPlan(false, windowStart, windowStart, null);
            }

            if (!tableIsHypertable)
            {
                await using var plain = new NpgsqlCommand(PlainTableFloorSql, connection) { CommandTimeout = commandTimeoutSeconds };
                plain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                tableFloor = await plain.ExecuteScalarAsync(cancellationToken) as DateTime?;
            }

            var useTable = UseTable(filledSince, hasPending, rawFloor, windowStart, windowEnd, literalWindowEnd, appliedThrough, tableFloor, minWindow);
            var clampedStart = ClampedStart(rawFloor, windowStart);

            /* Clause 6 (2026 field review): only checked once clauses 1-5 already say "table" — a store
               refused for any other reason never pays this probe. See HasLegacyRowSql's remarks for why a
               legacy row (interval_start_time_utc IS NULL) makes the table's answer wrong, not merely stale:
               the upsert's identity carries no collection_time, so a legacy interval re-fetched across several
               cycles keeps only its latest snapshot, and every caller that places these rows at collection_time
               needs every one of them. */
            if (useTable)
            {
                await using var legacy = new NpgsqlCommand(HasLegacyRowSql, connection) { CommandTimeout = commandTimeoutSeconds };
                legacy.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                legacy.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = clampedStart });
                legacy.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = windowEnd });
                var hasLegacyRow = (bool)(await legacy.ExecuteScalarAsync(cancellationToken))!;
                if (hasLegacyRow)
                {
                    useTable = false;
                }
            }

            var below = useTable
                ? ExactBelowFloorStart(rawFloor, windowStart, filledSince.Value, tableFloor)
                : null;
            var clampReason = (WideStartBound?)null;
            if (below is not null && tableFloor is DateTime tf)
            {
                var verdict = await CadenceAllowsAsync(connection, serverId, tf, commandTimeoutSeconds, logger, cancellationToken);
                if (verdict != BelowFloorVerdict.Allowed)
                {
                    below = null;
                    clampReason = verdict == BelowFloorVerdict.LogNotYetCovering ? WideStartBound.RawFloorLogNotYetCovering : WideStartBound.RawFloorSlowCadence;
                }
            }

            var belowFloorStart = below?.Start;
            var readStart = belowFloorStart ?? clampedStart;
            var startBound = below?.Bound
                ?? (clampReason ?? (rawFloor is DateTime rf && rf > windowStart ? WideStartBound.RawFloor : WideStartBound.Window));

            logger?.LogDebug(
                "Query Store wide-table source for server {ServerId}: {Source} (coverage since {FilledSince:o}; applied through {AppliedThrough:o}; raw floor {RawFloor:o}; window {WindowStart:o}-{WindowEnd:o}; literal end {LiteralEnd:o}; table floor {TableFloor:o}; read start {ReadStart:o}; below-floor start {BelowFloorStart:o})",
                serverId, useTable ? "interval table" : "raw", filledSince, appliedThrough, rawFloor, windowStart, windowEnd, literalWindowEnd, tableFloor, readStart, belowFloorStart);
            return new WideReadPlan(useTable, clampedStart, readStart, belowFloorStart, useTable ? startBound : WideStartBound.Window);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "Query Store wide-table source decision failed for server {ServerId}; reading raw", serverId);
            return new WideReadPlan(false, windowStart, windowStart, null);
        }
    }

    private readonly ConcurrentDictionary<int, byte> _coverageEnsured = new();
    private readonly ConcurrentDictionary<int, DateTime> _gapCheckedAt = new();
    private readonly ILogger? _logger;
    private readonly Func<DateTime> _utcNow;

    /// <param name="logger">Where an apply fault's Error line goes.</param>
    /// <param name="utcNow">The service clock; injectable so a test can step it.</param>
    public QueryStoreIntervalWide(ILogger? logger = null, Func<DateTime>? utcNow = null)
    {
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>What one batch's apply did.</summary>
    public enum ApplyResult
    {
        /// <summary>The batch (and any replays) applied.</summary>
        Applied,

        /// <summary>The apply failed or was skipped; the batch is recorded as pending and raw commits.</summary>
        RecordedAsPending,
    }

    private DateTime Clock() => DateTime.SpecifyKind(_utcNow(), DateTimeKind.Unspecified);

    /// <summary>
    /// The two pre-transaction steps for a server, run on THIS table: autocommit statements on the batch's own
    /// connection, before its COPY. Returns false if either faulted, in which case the caller still COPYs the batch
    /// and passes <c>skipApply: true</c> to <see cref="ApplyBatchAsync"/>.
    /// </summary>
    public async Task<bool> PrepareServerAsync(
        NpgsqlConnection connection,
        int serverId,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        try
        {
            if (!_coverageEnsured.ContainsKey(serverId))
            {
                var clock = Clock();
                await using var ensure = new NpgsqlCommand(EnsureCoverageSql, connection) { CommandTimeout = commandTimeoutSeconds };
                ensure.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                ensure.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = clock });
                ensure.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = clock.AddDays(-1) });
                await ensure.ExecuteNonQueryAsync(cancellationToken);

                _coverageEnsured[serverId] = 0;
            }

            var now = _utcNow();
            if (!_gapCheckedAt.TryGetValue(serverId, out var checkedAt) || now - checkedAt >= GapCheckInterval)
            {
                await CheckGapAsync(connection, serverId, commandTimeoutSeconds, cancellationToken);

                _gapCheckedAt[serverId] = now;
            }

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogError(ex,
                "Query Store wide interval table (#3953): preparing server {ServerId} failed; its batches are recorded for replay",
                serverId);
            return false;
        }
    }

    /// <summary>
    /// Raw rows for the server that no apply of THIS table accounted for: an old writer that only knew V143's
    /// upsert, a rollback and re-upgrade, or a second, older process. The claim restarts above the newest such row.
    /// </summary>
    internal async Task CheckGapAsync(
        NpgsqlConnection connection,
        int serverId,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        object? appliedThrough;
        await using (var read = new NpgsqlCommand(AppliedThroughSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            appliedThrough = await read.ExecuteScalarAsync(cancellationToken);
        }

        if (appliedThrough is not DateTime bound)
        {
            return;
        }

        object? newestUnaccounted;
        await using (var gap = new NpgsqlCommand(GapCheckSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            gap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            gap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = bound });
            newestUnaccounted = await gap.ExecuteScalarAsync(cancellationToken);
        }

        if (newestUnaccounted is DateTime newest)
        {
            _logger?.LogWarning(
                "Query Store wide interval table (#3953): server {ServerId} has raw Query Store rows through {Newest:o} that no apply of this table wrote (an old writer that only knew V143, a build without this writer, or a second service process); its coverage restarts",
                serverId, newest);
            await ResetCoverageAsync(connection, null, serverId, newest, commandTimeoutSeconds, cancellationToken);
        }
    }

    private async Task ResetCoverageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        int serverId,
        DateTime lacking,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using var reset = new NpgsqlCommand(ResetCoverageSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        reset.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        reset.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Clock() });
        reset.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = lacking });
        await reset.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Applies one raw batch inside its COPY's transaction, behind <see cref="SavepointName"/> — this table's own
    /// savepoint, independent of V143's. Never throws for an apply fault: it rolls back to ITS savepoint only
    /// (V143's already-applied rows and claim, earlier in the same transaction, are untouched) and records the
    /// batch as pending, so raw and V143 still commit. Throws only if recording the pending row itself fails or on
    /// cancellation.
    /// </summary>
    public async Task<ApplyResult> ApplyBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        IReadOnlyList<string> databaseNames,
        bool skipApply,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(databaseNames);

        if (databaseNames.Count == 0)
        {
            return ApplyResult.Applied;
        }

        var names = new string[databaseNames.Count];
        for (var i = 0; i < names.Length; i++)
        {
            names[i] = databaseNames[i];
        }

        Exception? fault = null;
        if (!skipApply)
        {
            await transaction.SaveAsync(SavepointName, cancellationToken);
            try
            {
                await ExecuteAsync(connection, transaction,
                    "SET LOCAL lock_timeout = '" + ApplyLockTimeoutSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture) + "s';",
                    commandTimeoutSeconds, cancellationToken);

                await StampAsync(connection, transaction, serverId, collectionTime, commandTimeoutSeconds, cancellationToken);
                await ReplayPendingAsync(connection, transaction, serverId, commandTimeoutSeconds, cancellationToken);
                await UpsertAsync(connection, transaction, serverId, collectionTime, names, isReplay: false, commandTimeoutSeconds, cancellationToken);

                await transaction.ReleaseAsync(SavepointName, cancellationToken);
                return ApplyResult.Applied;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                fault = ex;
                await transaction.RollbackAsync(SavepointName, cancellationToken);
            }
        }

        await RecordPendingAsync(connection, transaction, serverId, collectionTime, names, fault, commandTimeoutSeconds, cancellationToken);

        _logger?.LogError(fault,
            "Query Store wide interval table (#3953): the batch for server {ServerId} at {CollectionTime:o} ({Databases}) stored its raw rows but missed this table; it is queued for replay",
            serverId, collectionTime, string.Join(", ", names));

        return ApplyResult.RecordedAsPending;
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string sql,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task StampAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using var stamp = new NpgsqlCommand(StampSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        stamp.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        stamp.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
        stamp.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Clock() });
        await stamp.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task ReplayPendingAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        var pending = new List<(DateTime CollectionTime, string DatabaseName)>();
        await using (var read = new NpgsqlCommand(PendingForServerSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxReplaysPerApply });
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                pending.Add((reader.GetDateTime(0), reader.GetString(1)));
            }
        }

        foreach (var (pendingTime, databaseName) in pending)
        {
            await UpsertAsync(connection, transaction, serverId, pendingTime, new[] { databaseName }, isReplay: true, commandTimeoutSeconds, cancellationToken);

            await using var delete = new NpgsqlCommand(DeletePendingSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = pendingTime });
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = databaseName });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task UpsertAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        string[] databaseNames,
        bool isReplay,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        long rawRows;
        long nullFirstExecutionRows;
        await using (var upsert = new NpgsqlCommand(UpsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
            upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databaseNames });
            await using var reader = await upsert.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            rawRows = reader.GetInt64(1);
            nullFirstExecutionRows = reader.GetInt64(2);
        }

        /* A replayed batch raw no longer holds was purged before it could apply: this table lacks it for good, so
           the claim restarts above it. */
        if (isReplay && rawRows == 0)
        {
            _logger?.LogWarning(
                "Query Store wide interval table (#3953): the pending batch for server {ServerId} at {CollectionTime:o} was purged from raw before it could replay; the server's coverage restarts above it",
                serverId, collectionTime);
            await ResetCoverageAsync(connection, transaction, serverId, collectionTime, commandTimeoutSeconds, cancellationToken);
            return;
        }

        /* The NULL seam fails closed, same as V143's (M2, ruled): this table cannot hold a row with no
           first_execution_time, of any outcome, and raw's own read keeps it, so the claim restarts above it rather
           than silently under-reading. */
        if (nullFirstExecutionRows > 0)
        {
            _logger?.LogWarning(
                "Query Store wide interval table (#3953): the batch for server {ServerId} at {CollectionTime:o} carried {Rows} row(s) with no first_execution_time, which this table cannot hold; the server's coverage restarts above it",
                serverId, collectionTime, nullFirstExecutionRows);
            await ResetCoverageAsync(connection, transaction, serverId, collectionTime, commandTimeoutSeconds, cancellationToken);
        }
    }

    private async Task RecordPendingAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        string[] databaseNames,
        Exception? fault,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using (var advance = new NpgsqlCommand(AdvanceAppliedThroughSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            advance.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            advance.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
            await advance.ExecuteNonQueryAsync(cancellationToken);
        }

        var failure = fault is null
            ? "not applied: preparing the server failed"
            : fault is PostgresException pg ? pg.SqlState + ": " + pg.MessageText : fault.GetType().Name + ": " + fault.Message;

        await using var record = new NpgsqlCommand(RecordPendingSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
        record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Clock() });
        record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = failure.Length > 2000 ? failure[..2000] : failure });
        record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databaseNames });
        await record.ExecuteNonQueryAsync(cancellationToken);
    }
}
