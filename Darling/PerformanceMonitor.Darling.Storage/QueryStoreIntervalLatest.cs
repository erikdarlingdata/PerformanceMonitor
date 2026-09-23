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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The latest Query Store snapshot per interval, kept as raw <c>query_store_stats</c> is written (#3953, rung V140).
/// PLAN_REGRESSION and its drill-down used to deduplicate the server's whole raw slice on every pass (a sort at the
/// interval grain, 20-27 s on a busy store). This table holds that dedup's answer, one row per Regular interval
/// identity, maintained by one set-based upsert per raw COPY batch.
///
/// <para><b>Raw never depends on it.</b> The apply runs in the raw COPY's transaction behind a savepoint with a
/// <c>lock_timeout</c>. Any apply fault rolls back to the savepoint, records the batch in the pending table in the
/// same transaction, and lets raw commit. The next apply for the server replays the pending rows, and a server with
/// any pending row reads raw. Only a failure to record the pending row itself aborts the batch: a store that cannot
/// insert one row into an empty table is failing raw's COPY anyway.</para>
///
/// <para><b>Nothing inside the write transaction reads raw except with <c>collection_time = $n</c>.</b> Raw's
/// <c>drop_chunks</c> needs an AccessExclusiveLock on the chunks it drops, and an unbounded read would expand, and
/// lock, every chunk (#2143, #1815). The two reads that need a wider bound, the coverage row's creation and the gap
/// check, run before the transaction as autocommit statements, with their bounds bound as bare parameters so chunk
/// exclusion happens at plan time (#2387).</para>
///
/// <para><b>The coverage claim.</b> For a server, every Regular raw snapshot with <c>collection_time</c> at or after
/// <c>filled_since</c> is represented here by its interval's latest snapshot, except the batches in the pending
/// table. The reader uses the table only where that claim covers everything the raw read would read.</para>
/// </summary>
public sealed class QueryStoreIntervalLatest
{
    /// <summary>The table: one row per Regular Query Store interval identity.</summary>
    public const string TableName = "query_store_interval_latest";

    /// <summary>Per server: <c>filled_since</c> and <c>applied_through</c>.</summary>
    public const string CoverageTableName = "query_store_interval_latest_coverage";

    /// <summary>One row per raw batch whose apply failed, replayed by the server's next apply.</summary>
    public const string PendingTableName = "query_store_interval_latest_pending";

    /// <summary>
    /// How long an apply waits on a lock before it gives up and records the batch as pending. Short on purpose: a
    /// wait here holds raw's COPY transaction open, and the apply's normal cost is tens of milliseconds.
    /// </summary>
    public const int ApplyLockTimeoutSeconds = 5;

    /// <summary>How many pending batches one apply replays, oldest first. Bounds a recovering server's first apply.</summary>
    public const int MaxReplaysPerApply = 16;

    /// <summary>How often the gap check runs per server (review finding 5: hourly, not once per process).</summary>
    public static readonly TimeSpan GapCheckInterval = TimeSpan.FromHours(1);

    /// <summary>The savepoint the apply runs behind.</summary>
    internal const string SavepointName = "qsil_apply";

    /// <summary>
    /// The dedup's identity, in the unique index's column order: the raw read's <c>GROUP BY</c> plus
    /// <c>server_id</c>. The <c>ON CONFLICT</c> target, and (minus <c>server_id</c>) the <c>DISTINCT ON</c> key, are
    /// both rendered from this, so they cannot drift from each other or from the index.
    /// </summary>
    public const string IdentityColumns =
        "server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time";

    /// <summary>The identity minus <c>server_id</c>: one batch is one server, so this is the batch's own key.</summary>
    public const string BatchIdentityColumns =
        "database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time";

    /// <summary>
    /// (c) The coverage row, created once per server per process before its first COPY. <c>filled_since</c> is the
    /// clock raised to the server's newest raw <c>collection_time</c>, so it sorts after every row an earlier build
    /// wrote, even across a backward clock step; a backdated backfill slice's time is never used. <c>$3</c> is
    /// <c>$2 - 1 day</c> as a bare parameter: any row newer than the clock is inside that bound, so the answer equals
    /// the unbounded form and the plan excludes all but the newest one or two chunks.
    ///
    /// <para><c>applied_through</c> starts at the same value: every raw row that exists when the row is created sorts
    /// at or below it, so the gap check reads none of them as unaccounted, and only a row written after this point
    /// without an apply can.</para>
    /// </summary>
    public const string EnsureCoverageSql = @"
INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through)
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

    /// <summary>
    /// (g) The gap check's read: raw rows for the server newer than anything a writer build accounted for. <c>$2</c>
    /// is the <c>applied_through</c> just read, bound bare for plan-time exclusion; the subquery re-reads it in this
    /// statement's own snapshot. A raw batch and its stamp commit together, so a concurrent commit cannot look like a
    /// gap. NULL means no gap.
    /// </summary>
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
          FROM collect.query_store_interval_latest_coverage AS c
          WHERE c.server_id = $1
      );";

    /// <summary>The coverage row's <c>applied_through</c>, by primary key, for <see cref="GapCheckSql"/>'s bound.</summary>
    public const string AppliedThroughSql = @"
SELECT
    c.applied_through
FROM collect.query_store_interval_latest_coverage AS c
WHERE c.server_id = $1;";

    /// <summary>
    /// Restarts the server's coverage claim above <c>$3</c>, a raw <c>collection_time</c> the table is known to lack
    /// (a gap, a purged pending batch, or a Regular row the writer had to drop). <c>$2</c> is the clock. It never
    /// moves <c>filled_since</c> down.
    /// </summary>
    public const string ResetCoverageSql = @"
UPDATE collect.query_store_interval_latest_coverage
SET
    filled_since = GREATEST(filled_since, $2, $3 + INTERVAL '1 microsecond'),
    applied_through = GREATEST(applied_through, $3)
WHERE server_id = $1;";

    /// <summary>
    /// (a) The per-batch stamp. A <c>VALUES</c> upsert that reads no raw; the insert arm only fires if (c)'s row has
    /// vanished. It runs first inside the savepoint, so it takes the server's coverage row lock before anything else:
    /// every apply for a server takes the same lock order and serializes here for milliseconds.
    /// </summary>
    public const string StampSql = @"
INSERT INTO collect.query_store_interval_latest_coverage AS c (server_id, filled_since, applied_through)
VALUES ($1, $3, $2)
ON CONFLICT (server_id) DO UPDATE
SET applied_through = EXCLUDED.applied_through
WHERE c.applied_through < EXCLUDED.applied_through;";

    /// <summary>(r) This server's pending batches, oldest first, bounded.</summary>
    public const string PendingForServerSql = @"
SELECT
    p.collection_time,
    p.database_name
FROM collect.query_store_interval_latest_pending AS p
WHERE p.server_id = $1
ORDER BY
    p.collection_time,
    p.database_name
LIMIT $2;";

    /// <summary>(r) Removes one replayed pending batch.</summary>
    public const string DeletePendingSql = @"
DELETE FROM collect.query_store_interval_latest_pending
WHERE server_id = $1
AND   collection_time = $2
AND   database_name = $3;";

    /// <summary>
    /// (f) Records a batch whose apply failed, one row per database, in the raw rows' own transaction. <c>$3</c> is
    /// the clock, <c>$4</c> the fault's text.
    /// </summary>
    public const string RecordPendingSql = @"
INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure)
SELECT
    $1,
    $2,
    d.database_name,
    $3,
    $4
FROM unnest($5::text[]) AS d (database_name)
ON CONFLICT (server_id, collection_time, database_name) DO NOTHING;";

    /// <summary>
    /// (f) Advances <c>applied_through</c> past a batch recorded as pending, so the gap check reads it as accounted
    /// for. An UPDATE, not the stamp's upsert: with no coverage row the server reads raw anyway, and (c) will create
    /// the row later with a <c>filled_since</c> above this batch.
    /// </summary>
    public const string AdvanceAppliedThroughSql = @"
UPDATE collect.query_store_interval_latest_coverage
SET applied_through = $2
WHERE server_id = $1
AND   applied_through < $2;";

    /// <summary>
    /// (b) The upsert, one set-based statement per batch: this batch's Regular rows read back from raw by
    /// <c>collection_time = $2</c> (the pages the COPY just wrote), one survivor per identity by the raw read's own
    /// tie-break, then kept only where it is newer under the raw read's total order
    /// <c>(collection_time DESC, execution_count DESC)</c>. The stored row is therefore a running maximum under exactly
    /// the order the raw dedup maximizes, whatever order batches commit or replay in.
    ///
    /// <para>The <c>DISTINCT ON</c> is load-bearing: <c>ON CONFLICT DO UPDATE</c> refuses to touch one row twice in one
    /// statement, and a batch can carry two rows with one identity (two replica groups sharing a role name). It sorts
    /// one batch, at most <c>MaxRowsPerDatabase</c> rows; #2827's "never DISTINCT ON" is about the multi-million-row
    /// read and is not reopened here.</para>
    ///
    /// <para>It returns the upsert's row count, the batch's raw row count (zero means raw no longer holds it, which a
    /// replay treats as purged), and the batch's Regular rows with a NULL <c>first_execution_time</c>, which the table
    /// cannot hold and raw's <c>GROUP BY</c> keeps, so any of them resets coverage rather than under-read silently.</para>
    /// </summary>
    public const string UpsertSql = @"
WITH
    batch_rows AS
(
    SELECT
        COUNT(*) AS raw_rows,
        COUNT(*) FILTER (WHERE s.execution_type_desc = 'Regular' AND s.first_execution_time IS NULL) AS null_first_execution_rows
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = $2
    AND   s.database_name = ANY ($3::text[])
),
    upserted AS
(
    INSERT INTO collect.query_store_interval_latest AS t
    (
        server_id,
        database_name,
        query_id,
        plan_id,
        replica_role,
        runtime_stats_interval_id,
        first_execution_time,
        collection_time,
        query_plan_hash,
        query_hash,
        execution_count,
        avg_cpu_time_us,
        avg_duration_us,
        last_execution_time,
        is_forced_plan,
        force_failure_count,
        query_text
    )
    SELECT DISTINCT ON (" + BatchIdentityColumns + @")
        s.server_id,
        s.database_name,
        s.query_id,
        s.plan_id,
        s.replica_role,
        s.runtime_stats_interval_id,
        s.first_execution_time,
        s.collection_time,
        s.query_plan_hash,
        s.query_hash,
        s.execution_count,
        s.avg_cpu_time_us,
        s.avg_duration_us,
        s.last_execution_time,
        s.is_forced_plan,
        s.force_failure_count,
        s.query_text
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = $2
    AND   s.database_name = ANY ($3::text[])
    AND   s.execution_type_desc = 'Regular'
    AND   s.first_execution_time IS NOT NULL
    ORDER BY
        " + BatchIdentityColumns + @",
        s.execution_count DESC
    ON CONFLICT (" + IdentityColumns + @")
    DO UPDATE SET
        collection_time = EXCLUDED.collection_time,
        query_plan_hash = EXCLUDED.query_plan_hash,
        query_hash = EXCLUDED.query_hash,
        execution_count = EXCLUDED.execution_count,
        avg_cpu_time_us = EXCLUDED.avg_cpu_time_us,
        avg_duration_us = EXCLUDED.avg_duration_us,
        last_execution_time = EXCLUDED.last_execution_time,
        is_forced_plan = EXCLUDED.is_forced_plan,
        force_failure_count = EXCLUDED.force_failure_count,
        query_text = EXCLUDED.query_text
    WHERE (EXCLUDED.collection_time, EXCLUDED.execution_count) > (t.collection_time, t.execution_count)
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM upserted) AS applied_rows,
    b.raw_rows,
    b.null_first_execution_rows
FROM batch_rows AS b;";

    /* ---- the read-source decision (6.2 of the design) ---------------------------------------------------- */

    /// <summary>
    /// The decision's store-shape inputs for one server: its coverage row's <c>filled_since</c> (NULL when none),
    /// whether it has pending batches, and whether TimescaleDB's catalog views exist on this store.
    /// </summary>
    public const string ReadSourceInputsSql = @"
SELECT
    c.filled_since,
    EXISTS
    (
        SELECT
            1
        FROM collect.query_store_interval_latest_pending AS p
        WHERE p.server_id = $1
    ) AS has_pending,
    to_regclass('timescaledb_information.chunks') IS NOT NULL AS has_timescale
FROM (SELECT 1) AS one
LEFT JOIN collect.query_store_interval_latest_coverage AS c
  ON c.server_id = $1;";

    /// <summary>
    /// The floors, from TimescaleDB's catalog (metadata, never a scan): raw's oldest chunk (NULL when raw is not a
    /// hypertable, which the rule reads as "raw holds everything"), and the table's, when the table is one. A
    /// chunk's <c>range_start</c> is at or below its oldest row, so both err toward raw. <c>AT TIME ZONE 'UTC'</c>
    /// collapses the catalog's <c>timestamptz</c> to the store's naive-UTC discipline, as DarlingRetention does.
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
        AND   h.hypertable_name = 'query_store_interval_latest'
    ) AS table_is_hypertable,
    (
        SELECT
            MIN(ch.range_start) AT TIME ZONE 'UTC'
        FROM timescaledb_information.chunks AS ch
        WHERE ch.hypertable_schema = 'collect'
        AND   ch.hypertable_name = 'query_store_interval_latest'
    ) AS table_floor;";

    /// <summary>The table's floor for one server where the table is a plain heap: its oldest interval.</summary>
    public const string PlainTableFloorSql = @"
SELECT
    MIN(t.first_execution_time)
FROM collect.query_store_interval_latest AS t
WHERE t.server_id = $1;";

    /// <summary>
    /// The rule: read the table if and only if all four hold, otherwise run the shipped raw SQL unchanged.
    /// <list type="number">
    /// <item><c>F</c> exists: this build has claimed coverage for the server.</item>
    /// <item><c>F &lt;= max(R, B)</c>: the table holds every snapshot the raw read would read (it reads
    /// <c>collection_time &gt;= max(R, B)</c>), and may hold more, which is the ruled window extension.</item>
    /// <item><c>R &gt;= H</c> or <c>B &gt;= H</c>: raw holds no history the table has dropped, or every interval the
    /// window needs starts inside the table (<c>B</c> is the window minus a day, and an interval spans at most a
    /// day). A NULL <c>H</c> means the table holds nothing for the server, and then clause 2 already says raw holds
    /// nothing in the window either.</item>
    /// <item>No pending batch: a batch that stored raw but missed the table is not in the claim.</item>
    /// </list>
    /// A NULL <c>R</c> is raw with no chunk floor (a plain store, whose raw keeps 30 days): minus infinity, which can
    /// only pick raw. Every input errs toward raw, never toward an under-read.
    /// </summary>
    public static bool UseTable(DateTime? filledSince, bool hasPending, DateTime? rawFloor, DateTime rawBound, DateTime? tableFloor)
    {
        if (filledSince is not DateTime f || hasPending)
        {
            return false;
        }

        var rawReadsFrom = rawFloor is DateTime r && r > rawBound ? r : rawBound;
        if (f > rawReadsFrom)
        {
            return false;
        }

        if (tableFloor is not DateTime h)
        {
            return true;
        }

        return (rawFloor is DateTime floor && floor >= h) || rawBound >= h;
    }

    /// <summary>
    /// Decides the PLAN_REGRESSION source for one server and pass. <paramref name="rawBound"/> is the raw read's own
    /// <c>collection_time</c> bound (<c>$3</c>). Any fault picks raw: the raw SQL is the shipped read, so the worst a
    /// broken decision can cost is today's behaviour.
    /// </summary>
    public static async Task<bool> ReadsTableAsync(
        NpgsqlConnection connection,
        int serverId,
        DateTime rawBound,
        int commandTimeoutSeconds,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        try
        {
            DateTime? filledSince;
            bool hasPending;
            bool hasTimescale;
            await using (var inputs = new NpgsqlCommand(ReadSourceInputsSql, connection) { CommandTimeout = commandTimeoutSeconds })
            {
                inputs.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                await using var reader = await inputs.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                filledSince = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                hasPending = reader.GetBoolean(1);
                hasTimescale = reader.GetBoolean(2);
            }

            if (filledSince is null || hasPending)
            {
                return false;
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

            if (!tableIsHypertable)
            {
                await using var plain = new NpgsqlCommand(PlainTableFloorSql, connection) { CommandTimeout = commandTimeoutSeconds };
                plain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                tableFloor = await plain.ExecuteScalarAsync(cancellationToken) as DateTime?;
            }

            var useTable = UseTable(filledSince, hasPending, rawFloor, rawBound, tableFloor);
            logger?.LogDebug(
                "PLAN_REGRESSION source for server {ServerId}: {Source} (coverage since {FilledSince:o}; raw floor {RawFloor:o}; raw bound {RawBound:o}; table floor {TableFloor:o})",
                serverId, useTable ? "interval table" : "raw", filledSince, rawFloor, rawBound, tableFloor);
            return useTable;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "PLAN_REGRESSION source decision failed for server {ServerId}; reading raw", serverId);
            return false;
        }
    }

    private readonly ConcurrentDictionary<int, byte> _coverageEnsured = new();
    private readonly ConcurrentDictionary<int, DateTime> _gapCheckedAt = new();
    private readonly ILogger? _logger;
    private readonly Func<DateTime> _utcNow;

    /// <param name="logger">Where an apply fault's Error line goes.</param>
    /// <param name="utcNow">The service clock; injectable so a test can step it.</param>
    public QueryStoreIntervalLatest(ILogger? logger = null, Func<DateTime>? utcNow = null)
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
    /// The two pre-transaction steps for a server, (c) and (g): autocommit statements on the batch's own connection,
    /// run before its COPY. Returns false if either faulted, in which case the caller still COPYs the batch and passes
    /// <c>skipApply: true</c> to <see cref="ApplyBatchAsync"/>, which records it as pending: neither step can block
    /// raw, and a server without a verified coverage row reads raw.
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

                /* Set only after the statement returned, so a failed creation re-runs on the next batch. */
                _coverageEnsured[serverId] = 0;
            }

            var now = _utcNow();
            if (!_gapCheckedAt.TryGetValue(serverId, out var checkedAt) || now - checkedAt >= GapCheckInterval)
            {
                await CheckGapAsync(connection, serverId, commandTimeoutSeconds, cancellationToken);

                /* Set only after the check (and any reset) returned: a failed check re-runs on the next batch. */
                _gapCheckedAt[serverId] = now;
            }

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogError(ex,
                "Query Store interval table (#3953): preparing server {ServerId} failed; its batches are recorded for replay and PLAN_REGRESSION reads raw for it until they apply",
                serverId);
            return false;
        }
    }

    /// <summary>
    /// (g) Raw rows for the server that no apply accounted for mean a build without the writer ran against this store
    /// (a rollback and re-upgrade) or a second, older process is collecting the same server. The claim restarts above
    /// the newest such row.
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
                "Query Store interval table (#3953): server {ServerId} has raw Query Store rows through {Newest:o} that no apply wrote (a build without the writer, or a second service process, collected it); its coverage restarts, and PLAN_REGRESSION reads raw for it until raw's floor passes the new start",
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
    /// Applies one raw batch inside its COPY's transaction, behind <see cref="SavepointName"/>: (a) the stamp, (r) up to
    /// <see cref="MaxReplaysPerApply"/> pending replays, (b) the upsert. Never throws for an apply fault: it rolls back
    /// to the savepoint and records the batch as pending, so raw commits. Throws only if recording the pending row
    /// itself fails (the batch then rolls back entirely, the last resort) or on cancellation.
    /// </summary>
    /// <param name="collectionTime">The SAME naive value the COPY wrote, so <c>collection_time = $2</c> finds the batch.</param>
    /// <param name="databaseNames">The batch's distinct database names (normally exactly one).</param>
    /// <param name="skipApply">True when <see cref="PrepareServerAsync"/> faulted: record the batch without applying it.</param>
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
            "Query Store interval table (#3953): the batch for server {ServerId} at {CollectionTime:o} ({Databases}) stored its raw rows but missed the interval table; it is queued for replay, and PLAN_REGRESSION reads raw for this server until it applies",
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

        /* A replayed batch raw no longer holds was purged before it could apply: the table lacks it for good, so the
           claim restarts above it. A live batch always finds its own rows. */
        if (isReplay && rawRows == 0)
        {
            _logger?.LogWarning(
                "Query Store interval table (#3953): the pending batch for server {ServerId} at {CollectionTime:o} was purged from raw before it could replay; the server's coverage restarts above it",
                serverId, collectionTime);
            await ResetCoverageAsync(connection, transaction, serverId, collectionTime, commandTimeoutSeconds, cancellationToken);
            return;
        }

        /* The NULL seam fails closed: the table cannot hold these rows and raw's GROUP BY keeps them. */
        if (nullFirstExecutionRows > 0)
        {
            _logger?.LogWarning(
                "Query Store interval table (#3953): the batch for server {ServerId} at {CollectionTime:o} carried {Rows} Regular row(s) with no first_execution_time, which the table cannot hold; the server's coverage restarts above it",
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
        /* Coverage first, then the pending rows: the same lock order as the apply, so a concurrent apply for this
           server and this record cannot deadlock. */
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
