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
/// untouched, plus this table, 9-10 days on <c>collection_time</c>/<c>first_execution_time</c>, no secondary index
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
       ExactBelowFloorStart = max(window start, filled_since, table floor + PurgeEdgeMargin (26 h)) and no further: those are the
       three bounds under which the table provably holds what raw held before its purge.
       There is no per-server probe on this table: of its first two secondary btrees, idx_query_store_interval_wide_first_exec
       leads with first_execution_time, and the wide btree on (server_id, first_execution_time) (#4952,
       QueryStoreBackgroundIndexes.WideServerFirstExec) leads with server_id but is built in the background by the
       service, not by a migration, so a store can lack it. The per-server reads' bound on first_execution_time is
       served by that wide btree where it exists. The third secondary btree (#5807,
       QueryStoreBackgroundIndexes.WideDatabaseQueryFirstExec) serves only the one-query history read, and is built
       the same way. */

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
    /// Raw's floor, from TimescaleDB's catalog (metadata, never a scan), exactly as
    /// <see cref="QueryStoreIntervalLatest.RawChunkFloorSql"/>: raw's oldest chunk (NULL when raw is not a
    /// hypertable). This table's floor is never read from the catalog (#5541): the table is engine-plain, per its
    /// migration's own comment, and retention purges it with a row-capped DELETE (#5569) that leaves emptied chunks
    /// listed, so a catalog floor would sit far below the real one and move the rule toward the table.
    /// </summary>
    public const string RawChunkFloorSql = @"
SELECT
    MIN(ch.range_start) AT TIME ZONE 'UTC' AS raw_floor
FROM timescaledb_information.chunks AS ch
WHERE ch.hypertable_schema = 'collect'
AND   ch.hypertable_name = 'query_store_stats';";

    /// <summary>The table's floor <c>H</c> for one server: its oldest interval. Always this read (#5541): the purge
    /// deletes oldest first on <c>first_execution_time</c>, so every deleted row is below the remaining minimum.
    /// <b>Cost after a drain (#5581):</b> the table is partitioned by day since #5571, so this MIN runs over each
    /// partition's index. The dead entries are in the legacy table and the DEFAULT partition, the only places rows
    /// are deleted one by one (a whole expired day is dropped, indexes and all). Right after a large row delete
    /// the read makes a heap fetch for every dead entry vacuum has not removed and no read has marked yet (19.6 s
    /// and 854,535 fetches for one server on a large store, 15 ms on the re-run). The retention pass runs this
    /// constant once per server after such a delete (<c>QueryStoreIntervalFloorWarmUp</c>) so a user's first read
    /// does not pay it; change this statement and that warm-up follows, because it executes this constant.</summary>
    public const string PlainTableFloorSql = @"
SELECT
    MIN(t.first_execution_time)
FROM collect.query_store_interval_wide AS t
WHERE t.server_id = $1;";

    /// <summary>
    /// <see cref="PlainTableFloorSql"/> for many servers in ONE statement (#6807), and only for the servers that reach the gate's floor
    /// step: <c>$1</c> is the <c>int[]</c> of server ids, <c>$2</c> the clamp (<see cref="ClampedStart"/>) and <c>$3</c> the literal window
    /// end (NULL when there is none). The materialized <c>reach</c> keeps a server only when <see cref="ResolveReadAsync"/> would get past
    /// clauses 1, 2 and 5 for it (a coverage row, no pending batch, <c>filled_since</c> at or below the clamp, a literal end at or past
    /// <c>applied_through</c>), so a server that refuses ahead of the floor step is never scanned. Each kept id then gets its own LATERAL
    /// <c>MIN</c>, the same aggregate the one-server statement runs, and a server with no rows answers NULL as it does there. The plan is
    /// whatever plan the one-server statement gets on this store, per id. A fleet gate used to run the one-server statement once per
    /// server, 42 round trips at 17 to 20 ms each on a store with 42 servers.
    /// </summary>
    public const string PlainTableFloorsSql = @"
WITH
    reach AS MATERIALIZED
(
    SELECT DISTINCT
        ids.server_id
    FROM unnest($1::integer[]) AS ids(server_id)
    JOIN collect.query_store_interval_wide_coverage AS c
      ON c.server_id = ids.server_id
    WHERE c.filled_since <= $2::timestamp
    AND   ($3::timestamp IS NULL OR $3::timestamp >= c.applied_through)
    AND   NOT EXISTS
          (
              SELECT
                  1
              FROM collect.query_store_interval_wide_pending AS p
              WHERE p.server_id = ids.server_id
          )
)
SELECT
    r.server_id,
    f.floor
FROM reach AS r
CROSS JOIN LATERAL
(
    SELECT
        MIN(t.first_execution_time) AS floor
    FROM collect.query_store_interval_wide AS t
    WHERE t.server_id = r.server_id
) AS f;";

    /// <summary>
    /// The deadline of <see cref="PlainTableFloorsSql"/>, in seconds (#6807 review). Warm, the statement over 42 servers takes
    /// milliseconds. Right after a retention drain one server's floor read alone took 19.6 s and 38.9 s on large stores
    /// (<c>QueryStoreIntervalFloorWarmUp</c>), so a fleet statement can run for minutes there. It is given this short deadline and,
    /// when it passes it or faults, each server reads its own floor under the caller's full deadline, as before #6807. The walk it made
    /// before the deadline is not lost: the index entries it found dead are marked, so the one-server reads skip them.
    /// </summary>
    public const int TableFloorsPrefetchSeconds = 5;

    /// <summary>
    /// Clause 6's probe (2026 field review of #4310 site 3): whether the window holds any pre-tier-2 row (no
    /// <c>runtime_stats_interval_id</c>, no reconstructable identity) for this server. The upsert's identity
    /// (<see cref="IdentityColumns"/>) does not carry <c>collection_time</c>, so a legacy row re-fetched across
    /// several cycles — the collector's normal behaviour for an open interval, before tier 2 assigned intervals
    /// their own identity — keeps only its LATEST snapshot in the table; every caller that places these rows at
    /// <c>collection_time</c> (both duration-trend arm 2's) needs every one of them, not the running maximum. A
    /// field store carries zero such rows once raw retention (days) has aged the post-upgrade window out, so
    /// the answer is "none" on the common path, and it is what lets clause 6 answer TRUE only on the rare
    /// upgraded-recently store this table cannot yet serve correctly. <b>The cost (#4952):</b> with only the
    /// (<c>server_id</c>, <c>collection_time</c>) index this EXISTS finds nothing, so it would have to fetch every
    /// heap tuple the server's window touches to say no. The partial index
    /// <see cref="PgTableTuning.LegacyRowIndexName"/> (built on the start path) holds only the rows with no interval
    /// start, so for the uncompressed chunks the "no" is an index read of that near-empty tree; compressed
    /// chunks are still read by the columnar scan on their server_id segment index, which this index does not
    /// reach. It stays an exact read, not a cached answer, because the collector can still store a NULL start
    /// on a catalog join miss.
    /// <para><b>Opt-in (#6267 follow-up).</b> Only a caller that passes <c>LegacyRows.Refuse</c> to
    /// <see cref="ResolveReadAsync"/> or <see cref="ReadsTableAsync"/> pays this probe. Today that is the
    /// Viewer's duration trend (<c>ViewerDataService.GetQueryStoreDurationTrendAsync</c>) alone, because its arm 2 places a
    /// legacy row at every <c>collection_time</c> it was stored at, and the table kept one. Every other reader keeps ONE
    /// snapshot per interval identity, the newest, which is exactly what the table holds for a legacy row too (PostgreSQL
    /// matches NULL to NULL in the raw dedupe's <c>PARTITION BY</c>, and the table's identity matches them the same way), so
    /// its answer is the same with a legacy row in the window. Those readers are the Queries grid, the grid's regressions, the
    /// MCP top, history, plan-force precheck, aborts, regression and tag-rollup reads, the composed Query Store panels, and the
    /// analysis pass's plan-regression failure read. They skip the probe. The probe is cheap on uncompressed chunks and
    /// expensive on compressed ones, so the opt-in matters most to the analysis pass, which runs it for every server on
    /// every pass (a 4 to 4.5 s read on the largest store, and a cold full read of the compressed chunks after a restart).</para>
    /// Bounded to the SAME range the table read would use
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
    /// The window the by-Query-Store-top reads must span before the interval table is read at all (#3953's own threshold
    /// for the MCP top read, ruling issuecomment-5836972848 item 5): below this window the table's extra round trips (the
    /// gate's own reads plus a second transaction) cost more than they save, so the gate reads raw regardless of coverage.
    /// A read's own constant, not <see cref="GridWideMinWindow"/> (the grid's): the two reads may not share a threshold.
    /// Raised from 12 to 24 hours (lane B4t, rig-d4, 15-day seed at a field store's rate, end-to-end through the MCP top
    /// read): median of 5 at the ruled 12-hour cell, table 737.0 ms (spread 655.1-746.5) against raw 581.1 ms (spread
    /// 544.3-629.3), so the table was slower than raw there and the ruling moved this threshold to 24 hours.
    /// <para>The ONE value of it (#5971): the MCP top read (<c>DarlingDataReader.QueryStoreTopMinWindow</c>) and the by-tag
    /// rollup's Query Store section (<c>DarlingQueryTagRollupReader.QueryStoreMinWindow</c>) both read this field, so one
    /// retune cannot move one and leave the other. It is not the 48-hour duration-trend threshold, which was measured
    /// separately.</para>
    /// </summary>
    public static readonly TimeSpan TopMinWindow = TimeSpan.FromHours(24);

    /// <summary>
    /// How long one Query Store runtime-stats interval can span (V143's clause 3 restated: "B is the window minus
    /// a day, and an interval spans at most a day"), so an interval that STARTED up to a day before the window can
    /// still have executions inside it. Clause 3 of <see cref="UseTable"/> no longer uses it alone (#5541): it is
    /// one term of <see cref="PurgeEdgeMargin"/>. Restated here (rather than referencing <c>PgFactCollector.QueryPerf.PlanRegressionSkewMarginDays</c>)
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
    /// PurgeEdgeMarginSql</c> floor the per-server reads of this table carry (#4605). Among the table's btrees are the
    /// unique key, which leads with <c>server_id</c> and holds <c>first_execution_time</c> as a key column,
    /// <c>idx_query_store_interval_wide_first_exec</c>, and, where the service has built it, the wide btree on
    /// <c>(server_id, first_execution_time)</c> (#4952), which the service builds in the background rather than a
    /// migration. A read that filters only by <c>collection_time</c> (or <c>interval_start_time_utc</c>) is served by
    /// none of them, so a per-server read walked all of the server's rows; the floor filters the unique key's entries
    /// before the heap and, where the wide btree exists, is the lower end of the range that btree scans (the upper end is
    /// <see cref="FirstExecUpperSlackSql"/>, #5523). The Custom Views route, for all
    /// servers or some, does not carry the floor (see <c>ComposeCompiler.BuildFactRelation</c>).
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

    /// <summary>
    /// <see cref="PurgeEdgeMargin"/> with the interval term (<see cref="IntervalSpanMargin"/>, one day, the longest
    /// interval Query Store allows) replaced by the longest interval the server has actually had (#5882):
    /// <c>longestSpan + <see cref="MaxBelowFloorCadence"/> + <see cref="WatermarkPolicy.MaxCatchup"/></c>, the same
    /// argument <see cref="PurgeEdgeMarginSql"/>'s summary makes with a shorter span. At a 60-minute interval that is
    /// three hours instead of 26, so the read of a 24-hour window no longer takes yesterday's whole partition. Never
    /// longer than <see cref="PurgeEdgeMargin"/>, so a span above a day (not a Query Store setting) cannot widen the read.
    /// </summary>
    public static TimeSpan EdgeMarginFor(TimeSpan longestSpan)
    {
        var margin = longestSpan + MaxBelowFloorCadence + WatermarkPolicy.MaxCatchup;
        return margin > PurgeEdgeMargin ? PurgeEdgeMargin : margin;
    }

    /// <summary><see cref="EdgeMarginFor"/>'s answer as the interval literal <see cref="PurgeEdgeMarginSql"/> is, rounded UP
    /// to whole minutes. Equals <see cref="PurgeEdgeMarginSql"/> when the margin is the full <see cref="PurgeEdgeMargin"/>.</summary>
    public static string EdgeMarginSqlFor(TimeSpan margin) =>
        $"interval '{(long)Math.Ceiling(margin.TotalMinutes)} minutes'";

    /// <summary>#5882: <paramref name="sql"/> with every use of <see cref="PurgeEdgeMarginSql"/> replaced by
    /// <paramref name="marginSql"/> (<see cref="EdgeMarginSqlFor"/>). The margin is a literal in the statement text, not a
    /// parameter, so the planner sees the range it will scan and every caller that binds the statement by position is unchanged.</summary>
    public static string WithEdgeMargin(string sql, string marginSql) =>
        sql.Replace(PurgeEdgeMarginSql, marginSql, StringComparison.Ordinal);

    /// <summary>
    /// The longest positive <c>interval_length_minutes</c> the server's databases have reported (<c>v_query_store_health</c>, one
    /// row per database per capture) over [readStart - 2 days, end + 1 day], and how many positive-length captures that range
    /// holds, how many NULL or non-positive lengths it holds for a database that HAS wide rows (the collector writes 0 for a
    /// NULL length; a database with no wide rows cannot contribute to
    /// the read, so its non-positive row is ignored and does not pin the server at 26 hours), and the oldest positive-length
    /// capture in it. The lookback is a day of interval plus a day of
    /// capture cadence; the day past the end covers a change the last capture before the end did not see. The fifth column is
    /// true when a database the server has wide rows for has no capture in that range, so its interval length is unknown and the
    /// longest of the others bounds nothing for it. The databases come from a skip scan of <c>ux_query_store_interval_wide</c>
    /// (<c>(server_id, database_name, ...)</c>: a handful of index probes per database), not a <c>DISTINCT</c> over the read's range:
    /// that reads every heap row of the 26 hours the narrower floor exists to avoid (measured, #5815). The trade is a fallback,
    /// never an under-read: a database whose wide rows are all outside the read's range (dropped or taken offline within the wide
    /// table's ten days of retention) and has no capture in the range also returns <see cref="PurgeEdgeMargin"/>.
    /// </summary>
    private const string LongestIntervalSpanSql = """
        WITH RECURSIVE wide_databases AS
        (
            (
                SELECT w.database_name
                FROM query_store_interval_wide AS w
                WHERE w.server_id = $1
                ORDER BY w.database_name
                LIMIT 1
            )
            UNION ALL
            SELECT
                (
                    SELECT w.database_name
                    FROM query_store_interval_wide AS w
                    WHERE w.server_id = $1
                    AND   w.database_name > p.database_name
                    ORDER BY w.database_name
                    LIMIT 1
                )
            FROM wide_databases AS p
            WHERE p.database_name IS NOT NULL
        )
        SELECT
            MAX(h.interval_length_minutes) FILTER (WHERE h.interval_length_minutes > 0),
            COUNT(*) FILTER (WHERE h.interval_length_minutes > 0),
            COUNT(*) FILTER
            (
                WHERE (h.interval_length_minutes IS NULL OR h.interval_length_minutes <= 0)
                AND   d.database_name IS NOT NULL
            ),
            MIN(h.capture_time) FILTER (WHERE h.interval_length_minutes > 0),
            EXISTS
            (
                SELECT 1
                FROM wide_databases AS d2
                WHERE d2.database_name IS NOT NULL
                AND   NOT EXISTS
                (
                    SELECT 1
                    FROM v_query_store_health AS h2
                    WHERE h2.server_id = $1
                    AND   h2.database_name = d2.database_name
                    AND   h2.capture_time >= $2 - interval '2 days'
                    AND   h2.capture_time <= $3 + interval '1 day'
                )
            )
        FROM v_query_store_health AS h
        LEFT JOIN wide_databases AS d
          ON d.database_name = h.database_name
        WHERE h.server_id = $1
        AND   h.capture_time >= $2 - interval '2 days'
        AND   h.capture_time <= $3 + interval '1 day'
        """;

    /// <summary>
    /// The edge margin (<see cref="EdgeMarginFor"/>) for a read of <paramref name="readStart"/> through
    /// <paramref name="end"/> on one server (#5882), or <see cref="PurgeEdgeMargin"/> whenever the server's longest interval
    /// cannot be trusted: no positive-length capture in the range, a NULL or non-positive length for a database with wide rows, a database with wide rows and no
    /// capture in the health range (its length is unknown and may exceed the longest the others report), or no capture as old as a day before the
    /// read starts (an interval that began then, or one backfilled from before the monitor connected, may have run under a
    /// setting the captures never saw). Exact otherwise: every stored row has
    /// <c>first_execution_time &gt; collection_time - (span + MaxCatchup)</c> (<see cref="PurgeEdgeMarginSql"/>'s argument), and
    /// the longest length any database reported over the range bounds the span of every interval written in it. Run on the
    /// read's own connection and snapshot. A fault is the caller's, not swallowed here: inside the read's transaction a failed
    /// statement aborts it, so the caller's existing fault path (a raw read) is the only honest answer.
    /// </summary>
    public static async Task<TimeSpan> ResolveEdgeMarginAsync(
        NpgsqlConnection connection, int serverId, DateTime readStart, DateTime end, int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(LongestIntervalSpanSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(readStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)
            || reader.IsDBNull(0) || reader.IsDBNull(3)
            || reader.GetInt64(1) == 0 || reader.GetInt64(2) != 0
            || reader.GetBoolean(4))
        {
            return PurgeEdgeMargin;
        }

        if (reader.GetDateTime(3) > DateTime.SpecifyKind(readStart, DateTimeKind.Unspecified) - IntervalSpanMargin)
        {
            return PurgeEdgeMargin;
        }

        return EdgeMarginFor(TimeSpan.FromMinutes(reader.GetInt64(0)));
    }

    /// <summary>
    /// How far past a read's window end a row's <c>first_execution_time</c> may sit, for the upper bound the
    /// per-server reads placed by <c>collection_time</c> carry (#5523). A row is read by its <c>collection_time</c>,
    /// which the service stamps from its own clock, and <c>first_execution_time</c> is the first execution inside the
    /// interval by the MONITORED server's clock. So a row collected at or before the window end can hold a
    /// <c>first_execution_time</c> past it by two things: the monitored clock running AHEAD of the service's, and the
    /// length of the collection cycle (the cycle stamps <c>collection_time</c> once, at its start, and a later database
    /// of the cycle is read minutes after that stamp, up to the collector's per-database budget each). Twelve hours
    /// covers a server whose clock is set to local time in almost any zone, and a long cycle. The trade: a row from a
    /// server whose clock runs more than this ahead of the service can drop from a closed window. A clock running
    /// BEHIND is not what this bound is about (<c>WatermarkPolicy.ClampCatchup</c> only raises a watermark that is too
    /// old, so it is a clock behind that the collector cannot tolerate). The slack only sets where the index range
    /// ends, so it costs a few more hours of the server's rows read and turns a range that ran to now into a bounded
    /// one. <see cref="QueryStoreTopDaily.SkewSlack"/> is one hour, because the daily summary is documented as
    /// approximate; these reads were exact, so theirs is no shorter.
    /// </summary>
    public static readonly TimeSpan FirstExecUpperSlack = TimeSpan.FromHours(12);

    /// <summary>
    /// <see cref="FirstExecUpperSlack"/> as a Postgres interval literal (rounded UP to whole minutes), for the
    /// <c>first_execution_time &lt;= &lt;window end&gt; + FirstExecUpperSlackSql</c> bound (#5523). With the floor
    /// (<see cref="PurgeEdgeMarginSql"/>) it makes the <c>first_execution_time</c> range, the range
    /// <c>ix_query_store_interval_wide_server_first_exec</c> (<c>(server_id, first_execution_time)</c>, #4952) scans,
    /// finite at both ends: the floor alone ran from the window start to NOW, so a read of a window that ended days
    /// ago, or the partial first or last day of a long one, walked every row of the server newer than the window.
    /// </summary>
    public static readonly string FirstExecUpperSlackSql =
        $"interval '{(long)Math.Ceiling(FirstExecUpperSlack.TotalMinutes)} minutes'";

    /// <summary>
    /// The slack either side of a window placed by <c>interval_start_time_utc</c> (#5523, the duration trend's arm 1).
    /// Both columns are the monitored server's own clock, so unlike <see cref="FirstExecUpperSlack"/> against
    /// <c>collection_time</c> no cross-clock skew or collection cycle is involved, and an hour is enough. The floor
    /// <c>first_execution_time &gt;= &lt;window start&gt; - </c> this slack holds because Microsoft documents
    /// <c>sys.query_store_runtime_stats.first_execution_time</c> as: "First execution time for the query plan within the
    /// aggregation interval. This is the end time of the query execution." The value is an end time inside the
    /// interval, so it is never before the interval's start.
    /// </summary>
    public static readonly TimeSpan IntervalStartSlack = TimeSpan.FromHours(1);

    /// <summary><see cref="IntervalStartSlack"/> as a Postgres interval literal (whole minutes, rounded up), the
    /// <c>first_execution_time &gt;= &lt;window start&gt; - IntervalStartSlackSql</c> floor of a read placed by
    /// <c>interval_start_time_utc</c> (#5523).</summary>
    public static readonly string IntervalStartSlackSql =
        $"interval '{(long)Math.Ceiling(IntervalStartSlack.TotalMinutes)} minutes'";

    /// <summary>
    /// How far a row's <c>first_execution_time</c> may sit from its own <c>interval_start_time_utc</c> and still
    /// be read by a window placed on that column (#5523): <see cref="IntervalSpanMargin"/> after it (an interval spans
    /// at most a day and <c>first_execution_time</c> lies inside it) and <see cref="IntervalStartSlack"/> as slack.
    /// </summary>
    public static readonly TimeSpan IntervalStartFirstExecMargin = IntervalSpanMargin + IntervalStartSlack;

    /// <summary><see cref="IntervalStartFirstExecMargin"/> as a Postgres interval literal (whole minutes, rounded up),
    /// the <c>first_execution_time &lt;= &lt;window end&gt; + IntervalStartFirstExecMarginSql</c> bound of a read placed
    /// by <c>interval_start_time_utc</c> (#5523).</summary>
    public static readonly string IntervalStartFirstExecMarginSql =
        $"interval '{(long)Math.Ceiling(IntervalStartFirstExecMargin.TotalMinutes)} minutes'";

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
    /// viewer's own connection probe), and clause 6 (a 2026 field review) — the window must hold no legacy row
    /// (<c>interval_start_time_utc IS NULL</c>, see <see cref="HasLegacyRowSql"/>) — checked once clauses 1-5 already
    /// say "table", so a store refused for any other reason never pays the probe. Clause 6 runs only for a caller that
    /// passes <c>LegacyRows.Refuse</c> to <see cref="ResolveReadAsync"/> or <see cref="ReadsTableAsync"/>: the duration trend,
    /// whose arm 2 needs every snapshot of a legacy row. The other readers keep the newest snapshot per interval, which the
    /// table holds for a legacy row as well, so they do not ask for it.
    /// <list type="number">
    /// <item>Coverage exists (<paramref name="filledSince"/> is not null) and there is no pending batch.</item>
    /// <item><c>filledSince &lt;= max(R, S)</c> (<paramref name="rawFloor"/>, <paramref name="windowStart"/>):
    /// the table holds every snapshot raw's own read would, and may hold more (the ruled window extension).</item>
    /// <item><c>R &gt;= H + <see cref="PurgeEdgeMargin"/></c> or <c>S - <see cref="PurgeEdgeMargin"/> &gt;= H</c>
    /// (<paramref name="tableFloor"/>): raw holds no snapshot of an interval the table has dropped, or every
    /// interval the raw read can count for the window starts inside the table. This read counts a snapshot by
    /// <c>collection_time</c> alone (no <c>last_execution_time</c> filter, unlike the Latest rule), so a snapshot
    /// collected at or after <c>S</c> can belong to an interval that began down to <c>S - PurgeEdgeMargin</c>, which
    /// is why the second arm uses the whole margin and not one interval length (#5541). The margin is there because the two purge on different
    /// columns (#5541): raw drops a snapshot by <c>collection_time</c>, the table drops an interval by
    /// <c>first_execution_time</c>, and a snapshot lands in raw up to <see cref="PurgeEdgeMargin"/> after its interval
    /// began. With <c>R</c> only at or above <c>H</c>, raw can still hold a snapshot of an interval that began below
    /// <c>H</c> and that the table dropped. A NULL <paramref name="tableFloor"/> means the table holds nothing
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
            var edgeFloor = windowStart - PurgeEdgeMargin;
            if (!((rawFloor is DateTime floor && floor >= h + PurgeEdgeMargin) || edgeFloor >= h))
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
    /// <param name="DecisionFailed">True only when the source decision itself faulted and the plan is the
    /// raw fallback; a legitimate "the gate says raw" leaves it false, so a caller can tell the two apart.</param>
    public readonly record struct WideReadPlan(
        bool UseTable,
        DateTime ClampedStart,
        DateTime ReadStart,
        DateTime? BelowFloorStart,
        WideStartBound StartBound = WideStartBound.Window,
        bool DecisionFailed = false)
    {
        /// <summary>Where the table's proven-complete history starts for this read: <see cref="ReadStart"/>
        /// whenever the table serves. NULL when the table does not serve (the gate could not decide), because the
        /// surface then reports the raw tier's own reach.</summary>
        public DateTime? EffectiveStart => UseTable ? ReadStart : null;
    }

    /// <summary>
    /// Whether the gate may route a window holding a legacy row (<c>interval_start_time_utc IS NULL</c>) to the table.
    /// A required argument of <see cref="ResolveReadAsync"/> and <see cref="ReadsTableAsync"/>, so a new reader cannot
    /// reach the cheap path by leaving it out. See <see cref="HasLegacyRowSql"/> for which readers need which.
    /// </summary>
    public enum LegacyRows
    {
        /// <summary>The reader keeps the newest snapshot per interval, which the table holds for a legacy row too. Clause 6 is skipped.</summary>
        Allow,
        /// <summary>The reader places a row at every <c>collection_time</c> it was stored at, which the table cannot give back. Clause 6 refuses the table when the window holds a legacy row.</summary>
        Refuse,
    }

    /// <summary>#4689: which bound set a <c>_wide</c> read's lower bound.</summary>
    public enum WideStartBound
    {
        /// <summary>The window's own start: nothing was cut.</summary>
        Window,
        /// <summary>The table's coverage claim (<c>filled_since</c>) starts later than the window.</summary>
        FilledSince,
        /// <summary>The 10-day purge edge plus the one-day interval-span margin starts later than the window.</summary>
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
    /// start of a many-server read; when null the reason says "these servers".
    ///
    /// <para>#4966: <paramref name="effectiveStart"/> is the start as its caller already prints it, in the same text
    /// as its <c>effective_start</c> field (<c>McpHelpers.FormatEffectiveStart</c>: UTC, with the Z), and the note
    /// names it as given. The page finds the instant in the sentence by the field's exact text to show it in the
    /// browser's zone, so a note that spelled the instant on its own (a plain "o" of the store's naive floor has no
    /// Z) was drawn in bare UTC above a grid of local times.</para></summary>
    public static string HistoryNote(string effectiveStart, WideStartBound bound, bool manyServers, string? settingServer = null)
    {
        var scope = manyServers ? "the servers in scope" : "this server";
        var reason = bound switch
        {
            WideStartBound.FilledSince =>
                $"The interval table began keeping complete history for {(!manyServers ? "this server" : settingServer ?? "these servers")} at {effectiveStart}.",
            WideStartBound.TablePurgeEdge =>
                "The interval table keeps 10 days, and intervals that began before its purge edge are not read.",
            WideStartBound.RawFloorSlowCadence =>
                $"{(!manyServers ? "This server's" : settingServer is null ? "A server's" : settingServer + "'s")} Query Store collection cadence is over 60 minutes, or its collection log shows a longer gap, so older intervals are not read from the interval table.",
            WideStartBound.RawFloorLogNotYetCovering =>
                "The collection log does not yet cover the interval table's purge edge, so older intervals are not read from the interval table.",
            _ =>
                "The read is clamped at the raw tier's retention floor: nothing older than it can be shown exactly.",
        };
        return $"The window reaches further back than the Query Store history this store holds for {scope}. Nothing older than {effectiveStart} was read. Past the raw tier's retention, intervals are read from the per-interval table (kept 10 days), which holds exactly what raw held for them. "
            + reason;
    }

    /// <summary>#4689: the short parenthetical a table-served grid banner appends to its "Showing since" line, or
    /// null when the window's own start or raw's floor set the start.</summary>
    public static string? BannerReason(WideStartBound bound) => bound switch
    {
        WideStartBound.FilledSince => " (interval table complete from then)",
        WideStartBound.TablePurgeEdge => " (interval table keeps 10 days)",
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

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<(string StoreKey, int ServerId, string Table, DateTime FloorDay, int CadenceMinutes), TimeSpan?> GapCache = new();

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
    /// cached per (store, server, floor day, cadence). The range sits 8-10 days back in compressed
    /// <c>collection_log</c> chunks, so each uncached read decompresses them. Caching is exact because the
    /// range is in the past and <c>collection_log</c> rows are only ever inserted with the wall-clock
    /// <c>collection_time</c> of the insert (DarlingObservability.InsertCollectionLogSql callers pass
    /// UtcNow), never backdated. Only a SETTLED verdict, lifted or clamped, is cached: the window's end
    /// (floor day + 3 d) must be at least <see cref="WatermarkPolicy.MaxCatchup"/> + <see cref="GapSettleMargin"/>
    /// in the past (<see cref="GapWindowSettled"/>), because a collection can still land in a window that is not.
    /// An unsettled verdict is recomputed on every decision. The
    /// window derives from the key's floor day, never from the raw floor, so a verdict cached for day D can
    /// never serve a floor in day D+1. One entry is kept per (store, server, table): storing a new floor day or
    /// cadence removes that server's older entries for the same table. A read that yields no value is not cached.
    /// <para>#6750: <paramref name="table"/> is the table whose floor this is. The regressions read checks the cadence at two
    /// floors, the wide table's and <c>query_store_interval_latest</c>'s, so a key without the table would make the two floors
    /// evict each other's entry on every call.</para>
    /// </summary>
    internal static async Task<TimeSpan?> MaxCollectionGapCachedAsync(
        NpgsqlConnection connection, int serverId, DateTime tableFloor, int frequencyMinutes, int commandTimeoutSeconds, CancellationToken cancellationToken,
        string table = TableName)
    {
        var floorDay = DateTime.SpecifyKind(tableFloor.Date, DateTimeKind.Unspecified);
        var storeKey = GapStoreKey(connection);
        var key = (storeKey, serverId, table, floorDay, frequencyMinutes);
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
                if (stale.StoreKey == storeKey && stale.ServerId == serverId && stale.Table == table)
                {
                    GapCache.TryRemove(stale, out _);
                }
            }

            GapCache[key] = gap;
        }

        return gap;
    }

    internal enum BelowFloorVerdict
    {
        Allowed,
        SlowCadence,
        LogNotYetCovering,
    }

    /// <summary>The cadence and gap verdict for reading below raw's floor from a table whose floor is <paramref name="tableFloor"/>.
    /// <paramref name="table"/> (#6750) keys the gap cache, so the wide table's and latest's verdicts are cached apart.</summary>
    internal static async Task<BelowFloorVerdict> CadenceAllowsAsync(
        NpgsqlConnection connection, int serverId, DateTime tableFloor, int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken,
        string table = TableName)
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

        var gap = await MaxCollectionGapCachedAsync(connection, serverId, tableFloor, frequency, commandTimeoutSeconds, cancellationToken, table);

        return CadenceAllowsBelowFloor(frequency, gap) ? BelowFloorVerdict.Allowed : BelowFloorVerdict.SlowCadence;
    }

    /// <summary>
    /// The lowest instant below raw's floor from which the table provably equals what raw said before its purge,
    /// or NULL when the read cannot go below the floor (no floor, the floor at or below the window start, an
    /// empty table, or no exact span left). Three bounds, each a hazard:
    /// <list type="bullet">
    /// <item><paramref name="filledSince"/>: the coverage claim starts there and is never backdated, and a gap
    /// check restarts it above a hole (<see cref="ResetCoverageSql"/>).</item>
    /// <item><paramref name="tableFloor"/> + <see cref="PurgeEdgeMargin"/>: the 10-day purge deletes row by
    /// row on <c>first_execution_time</c>, so a row whose interval began before the table floor is gone even when
    /// its <c>collection_time</c> is inside the window.</item>
    /// <item><paramref name="windowStart"/>: never read before the window asked for.</item>
    /// </list>
    /// <para>#6750: pure and table-agnostic. The regressions read also runs it for <c>query_store_interval_latest</c>, with that
    /// table's coverage claim and floor (<see cref="QueryStoreRegressionBaseline.ResolveSpansAsync"/>).</para>
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
    /// <paramref name="legacyRows"/> is required: <see cref="LegacyRows.Refuse"/> turns clause 6 on (see <see cref="ResolveReadAsync"/>).
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
        LegacyRows legacyRows,
        CancellationToken cancellationToken)
    {
        var p = await ResolveReadAsync(
            connection, serverId, windowStart, windowEnd, literalWindowEnd, minWindow,
            commandTimeoutSeconds, logger, legacyRows, cancellationToken);
        return (p.UseTable, p.ClampedStart);
    }

    /// <summary>
    /// The inputs <see cref="ResolveReadAsync"/> reads that do not depend on the server: the raw floor of
    /// <see cref="RawChunkFloorSql"/>, which takes no parameter and reads TimescaleDB's catalog for the whole store.
    /// (<see cref="ReadSourceInputsSql"/>'s coverage and pending columns, <see cref="PlainTableFloorSql"/>,
    /// <see cref="HasLegacyRowSql"/> and the cadence probe all filter on the server, so they stay per server.)
    /// A caller that resolves many servers for one window passes one <see cref="StoreWideInputsCache"/> to each call.
    /// </summary>
    public sealed record StoreWideInputs(DateTime? RawFloor);

    /// <summary>
    /// A per-check holder for <see cref="StoreWideInputs"/>. <see cref="ResolveReadAsync"/> fills it the first
    /// time a server reaches the floors step and reuses it for every later server, so the raw floor is read at
    /// most once per check and not at all when every server refuses before that step.
    /// <para>#6807: a caller that names its servers with <see cref="ReadTableFloorsTogether"/> also gets their table floors in one
    /// statement (<see cref="PlainTableFloorsSql"/>), read at that same first arrival and only for the named servers that reach the
    /// floor step. So the table floors too are read not at all when every server refuses before that step. A server the statement did
    /// not answer, or every server when it failed or passed <see cref="TableFloorsPrefetchSeconds"/>, reads its own floor
    /// (<see cref="PlainTableFloorSql"/>) as before. Every server in one check must be resolved for the same window.</para>
    /// </summary>
    public sealed class StoreWideInputsCache
    {
        internal StoreWideInputs? Value;

        /// <summary>The table floors <see cref="PlainTableFloorsSql"/> answered; null until it ran, and after it failed.</summary>
        internal Dictionary<int, DateTime?>? TableFloors;

        /// <summary>A test's replacement for <see cref="PlainTableFloorsSql"/>, one that faults or sleeps, to drive the fallback. Scoped to
        /// the test's own async flow, so no other caller ever sees it.</summary>
        internal static readonly AsyncLocal<string?> TableFloorsSqlForTests = new();

        private int[]? _floorServers;
        private bool _inTransaction;
        private bool _floorsTried;

        /// <summary>
        /// Asks for the table floors of <paramref name="serverIds"/> to be read in one statement when the first of them reaches the
        /// floor step. Reads nothing now. <paramref name="inTransaction"/> says the connection is inside an explicit transaction: the
        /// statement then runs under a SAVEPOINT, so its failure rolls back only itself, and the transaction's snapshot and every later
        /// statement on it are untouched.
        /// </summary>
        public void ReadTableFloorsTogether(IEnumerable<int> serverIds, bool inTransaction)
        {
            ArgumentNullException.ThrowIfNull(serverIds);
            _floorServers = serverIds.Distinct().ToArray();
            _inTransaction = inTransaction;
        }

        /// <summary>Runs <see cref="PlainTableFloorsSql"/> once, at the first server to reach the floor step. A fault leaves
        /// <see cref="TableFloors"/> null; cancellation propagates.</summary>
        internal async Task EnsureTableFloorsAsync(
            NpgsqlConnection connection, DateTime clampedStart, DateTime? literalWindowEnd, int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken)
        {
            if (_floorsTried || _floorServers is not { Length: > 0 } ids)
            {
                return;
            }

            _floorsTried = true;
            if (_inTransaction)
            {
                await ExecuteAsync(connection, TableFloorsSavepointSql, commandTimeoutSeconds, cancellationToken);
            }

            try
            {
                var floors = new Dictionary<int, DateTime?>(ids.Length);
                await using (var command = new NpgsqlCommand(TableFloorsSqlForTests.Value ?? PlainTableFloorsSql, connection) { CommandTimeout = Math.Min(TableFloorsPrefetchSeconds, commandTimeoutSeconds) })
                {
                    command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = ids });
                    command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(clampedStart, DateTimeKind.Unspecified) });
                    command.Parameters.Add(new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Timestamp,
                        Value = literalWindowEnd is DateTime e ? DateTime.SpecifyKind(e, DateTimeKind.Unspecified) : DBNull.Value,
                    });
                    await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        floors[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
                    }
                }

                if (_inTransaction)
                {
                    await ExecuteAsync(connection, TableFloorsReleaseSql, commandTimeoutSeconds, cancellationToken);
                }

                TableFloors = floors;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                TableFloors = null;
                logger?.LogInformation("Query Store table floors for {Servers} servers in one statement failed ({Failure}); each server reads its own", ids.Length, ex.Message);
                if (_inTransaction)
                {
                    /* Undo the failed statement only. If this fails too, the connection is gone, and each server's own statements fail
                       and refuse, as they would have on that connection without the prefetch. */
                    try
                    {
                        await ExecuteAsync(connection, TableFloorsRollbackSql, commandTimeoutSeconds, cancellationToken);
                    }
                    catch (Exception rollback) when (rollback is not OperationCanceledException)
                    {
                        logger?.LogWarning("Query Store table floors: the rollback to the savepoint failed ({Failure})", rollback.Message);
                    }
                }
            }
        }

        private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, int commandTimeoutSeconds, CancellationToken cancellationToken)
        {
            await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeoutSeconds };
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>#6807: the savepoint the fleet floor statement runs under inside an explicit transaction.</summary>
    private const string TableFloorsSavepointSql = "SAVEPOINT query_store_table_floors";

    /// <summary>#6807: drops the savepoint once the fleet floor statement answered.</summary>
    private const string TableFloorsReleaseSql = "RELEASE SAVEPOINT query_store_table_floors";

    /// <summary>#6807: undoes a failed fleet floor statement, and only it, so the transaction and its snapshot carry on.</summary>
    private const string TableFloorsRollbackSql = "ROLLBACK TO SAVEPOINT query_store_table_floors; RELEASE SAVEPOINT query_store_table_floors";

    /// <summary>
    /// <see cref="UseTable"/> plus the store round trips it needs, the clamp (<see cref="ClampedStart"/>), and the
    /// lower bound the table read may bind (<see cref="WideReadPlan.ReadStart"/>), which reaches below raw's chunk
    /// floor down to <see cref="ExactBelowFloorStart"/> — all computed from the SAME <c>rawFloor</c> this decision
    /// read, on the SAME connection, so the decision and the bound cannot see different snapshots.
    /// <para><paramref name="legacyRows"/> has no default, so a new reader must choose. <see cref="LegacyRows.Refuse"/> runs
    /// clause 6, <see cref="HasLegacyRowSql"/>, and refuses the table when the window holds a legacy row. Choose it when the
    /// read would be WRONG with one: a reader that places a row at every <c>collection_time</c> it was stored at (the
    /// Viewer's duration trend). A reader that keeps the newest snapshot per interval gets the same answer from the table,
    /// so it passes <see cref="LegacyRows.Allow"/> and never pays the probe, which on compressed chunks decompresses every
    /// batch of the server to prove there are none.</para>
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
        LegacyRows legacyRows,
        CancellationToken cancellationToken,
        StoreWideInputsCache? storeWide = null)
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
               remaining round trips (RawChunkFloorSql's metadata read and, worse, PlainTableFloorSql's
               unindexed server-wide scan). A short window or a literal end before appliedThrough can only
               ever land on "raw" (UseTable's own tail), so failing here saves both queries. */
            if (windowEnd - windowStart < minWindow || (literalWindowEnd is DateTime e && e < appliedThrough))
            {
                return new WideReadPlan(false, windowStart, windowStart, null);
            }

            DateTime? rawFloor = null;
            if (hasTimescale && storeWide?.Value is StoreWideInputs cached)
            {
                rawFloor = cached.RawFloor;
            }
            else if (hasTimescale)
            {
                await using var floors = new NpgsqlCommand(RawChunkFloorSql, connection) { CommandTimeout = commandTimeoutSeconds };
                rawFloor = await floors.ExecuteScalarAsync(cancellationToken) as DateTime?;
                if (storeWide is not null)
                {
                    storeWide.Value = new StoreWideInputs(rawFloor);
                }
            }

            /* Clause 2 (filledSince <= max(rawFloor, windowStart)) needs only rawFloor, already in hand from
               RawChunkFloorSql's metadata read (or never set, on a non-Timescale store) — never the table floor.
               An upgraded store whose claim doesn't cover the window yet fails here, before PlainTableFloorSql's
               scan, instead of after it. */
            if (filledSince > ClampedStart(rawFloor, windowStart))
            {
                return new WideReadPlan(false, windowStart, windowStart, null);
            }

            DateTime? tableFloor;
            if (storeWide is not null)
            {
                await storeWide.EnsureTableFloorsAsync(connection, ClampedStart(rawFloor, windowStart), literalWindowEnd, commandTimeoutSeconds, logger, cancellationToken);
            }

            if (storeWide?.TableFloors is { } prefetched && prefetched.TryGetValue(serverId, out var prefetchedFloor))
            {
                tableFloor = prefetchedFloor;
            }
            else
            {
                await using var plain = new NpgsqlCommand(PlainTableFloorSql, connection) { CommandTimeout = commandTimeoutSeconds };
                plain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                tableFloor = await plain.ExecuteScalarAsync(cancellationToken) as DateTime?;
            }

            var useTable = UseTable(filledSince, hasPending, rawFloor, windowStart, windowEnd, literalWindowEnd, appliedThrough, tableFloor, minWindow);
            var clampedStart = ClampedStart(rawFloor, windowStart);

            /* Clause 6 (2026 field review): only checked once clauses 1-5 already say "table" — a store
               refused for any other reason never pays this probe — and only for a caller that asked
               (LegacyRows.Refuse). See HasLegacyRowSql's remarks for why a legacy row
               (interval_start_time_utc IS NULL) makes the table's answer wrong for ONE kind of reader, not merely stale:
               the upsert's identity carries no collection_time, so a legacy interval re-fetched across several
               cycles keeps only its latest snapshot, and a caller that places these rows at collection_time
               needs every one of them. A caller that keeps the newest snapshot per interval reads the same rows
               either way and skips the probe. */
            if (useTable && legacyRows == LegacyRows.Refuse)
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
            return new WideReadPlan(false, windowStart, windowStart, null, DecisionFailed: true);
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
