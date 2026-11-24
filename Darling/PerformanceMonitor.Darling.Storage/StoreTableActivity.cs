/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #9413: what the monitoring store's OWN tables did each hour, as deltas. <c>get_store_metrics</c> kept each table's size and
/// row count, and <c>get_store_table_health</c> reads live counters that run from the last statistics reset, so neither could say
/// which table an hourly pass wrote or read, or when that started.
///
/// <para><b>Shape (V308).</b> <c>collect.store_table_activity</c> holds one row per store table per hour that table was active:
/// the tuples inserted, updated (and how many of those HOT) and deleted, heap and index blocks read and hit, sequential and index
/// scans, vacuums and analyzes, and how many of the table's relations (chunks, compressed chunks, partitions) were scanned in
/// the hour. The values are the hour's DIFFERENCES, not raw counters, which is the one place this table departs from the
/// <c>store_metrics</c> convention (raw counters, the read differences them). It cannot follow it: a table's raw counters are
/// the sum over its chunks, and retention drops chunks every day, so a rolled-up raw sum falls whenever a chunk goes and a reader
/// cannot tell that from a statistics reset. The difference has to be taken per relation, before the roll-up.</para>
///
/// <para><b>Baseline.</b> <c>collect.store_table_activity_baseline</c> holds the last capture's counters per relation (the
/// relation's oid is the key) and the database's statistics reset time it was read at, so a restart of the service costs nothing: the previous capture is on disk. It holds only relations
/// with a non-zero counter. A relation it does not hold has a baseline of zero, which is the same thing, so a chunk created in
/// the hour counts its whole life as the hour's activity. The first capture on a store has no baseline and writes none of the
/// activity rows: its counters began at the last statistics reset, which is not an hour. When the database's statistics reset time
/// differs from the one stored with the baseline, a reset (<c>pg_stat_reset()</c>, or the discard of every statistic after a crash) ran between the
/// two captures, and every counter of that capture is counted from zero: the value is everything counted since the reset, which is inside the
/// interval, even where the counter has already climbed back above its old baseline. A counter below its baseline with no change of reset time
/// means one table's counters were reset alone, and its new value is taken as the difference the same way. The checkpointer rows store NULL there
/// because they keep raw counters. Each row carries <c>interval_seconds</c>, the time since the previous capture, so a skipped
/// tick shows as a longer interval and not as a busy hour.</para>
///
/// <para><b>Roll-up.</b> A plain chunk, and a leaf of a natively partitioned table, goes to its inheritance parent. A compressed
/// chunk's relation is in no inheritance chain from 2.29 on (and in the wrong one before), so it goes through
/// <c>compression_settings.compress_relid</c> to its chunk and from there to the hypertable: the join the sizing statements
/// already use, which holds on every TimescaleDB version the store runs. A continuous aggregate's materialization is named by its
/// view, as <c>store_metrics</c> names it. A relation that is none of these is its own row. Only relations that were active in the
/// hour are rolled up, so the cost follows the activity and not the chunk count.</para>
///
/// <para><b>Retention.</b> The capture's own DELETE of rows past <see cref="StoreSelfMetrics.RetentionDays"/>, the same bound as
/// <c>store_metrics</c>. Both tables are plain tables outside the collector catalog, so nothing converts them to hypertables and
/// the retention purge never reaches them. The baseline is one capture's worth and is replaced each time.</para>
/// </summary>
public static class StoreTableActivity
{
    /// <summary>The store schema version that adds the two tables. One constant so a renumber, when another rung lands first, is one edit.</summary>
    public const int Version = 308;

    /// <summary>The rung (V308): two empty plain tables, no data moves. An older build never reads them.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.store_table_activity
(
    metric_time timestamp(6) without time zone NOT NULL,
    interval_seconds integer NOT NULL,
    table_schema text NOT NULL,
    table_name text NOT NULL,
    table_kind text NOT NULL,
    relations_active integer NOT NULL,
    relations_scanned integer NOT NULL,
    seq_scans bigint NOT NULL,
    idx_scans bigint NOT NULL,
    tup_inserted bigint NOT NULL,
    tup_updated bigint NOT NULL,
    tup_hot_updated bigint NOT NULL,
    tup_deleted bigint NOT NULL,
    heap_blks_read bigint NOT NULL,
    heap_blks_hit bigint NOT NULL,
    idx_blks_read bigint NOT NULL,
    idx_blks_hit bigint NOT NULL,
    vacuums bigint NOT NULL,
    analyzes bigint NOT NULL,
    CONSTRAINT pk_store_table_activity PRIMARY KEY (metric_time, table_schema, table_name)
);

CREATE TABLE IF NOT EXISTS collect.store_table_activity_baseline
(
    rel_oid bigint NOT NULL,
    captured_at timestamp(6) without time zone NOT NULL,
    stats_reset timestamp(6) without time zone NULL,
    seq_scans bigint NOT NULL,
    idx_scans bigint NOT NULL,
    tup_inserted bigint NOT NULL,
    tup_updated bigint NOT NULL,
    tup_hot_updated bigint NOT NULL,
    tup_deleted bigint NOT NULL,
    heap_blks_read bigint NOT NULL,
    heap_blks_hit bigint NOT NULL,
    idx_blks_read bigint NOT NULL,
    idx_blks_hit bigint NOT NULL,
    vacuums bigint NOT NULL,
    analyzes bigint NOT NULL,
    CONSTRAINT pk_store_table_activity_baseline PRIMARY KEY (rel_oid)
);";

    /// <summary>The <c>table_kind</c> of a hypertable (its chunks and compressed chunks rolled up).</summary>
    public const string HypertableKind = "hypertable";

    /// <summary>The <c>table_kind</c> of a continuous aggregate (its materialization hypertable rolled up, named by the view).</summary>
    public const string ContinuousAggregateKind = "continuous_aggregate";

    /// <summary>The <c>table_kind</c> of every other table, partitioned parents included (their leaves rolled up).</summary>
    public const string TableKind = "table";

    /// <summary>The counters, as (column, expression over <c>pg_stat_user_tables</c> as <c>s</c> and <c>pg_statio_user_tables</c> as <c>io</c>). The index
    /// columns are NULL on a table with no index, so they are coalesced: NULL is a zero here. <c>pg_statio_user_tables.idx_blks_*</c> is the sum
    /// over the table's indexes, the same figure <c>pg_statio_user_indexes</c> gives per index.</summary>
    private static readonly (string Column, string Source)[] Counters =
    {
        ("seq_scans", "s.seq_scan"),
        ("idx_scans", "coalesce(s.idx_scan, 0)"),
        ("tup_inserted", "s.n_tup_ins"),
        ("tup_updated", "s.n_tup_upd"),
        ("tup_hot_updated", "s.n_tup_hot_upd"),
        ("tup_deleted", "s.n_tup_del"),
        ("heap_blks_read", "coalesce(io.heap_blks_read, 0)"),
        ("heap_blks_hit", "coalesce(io.heap_blks_hit, 0)"),
        ("idx_blks_read", "coalesce(io.idx_blks_read, 0)"),
        ("idx_blks_hit", "coalesce(io.idx_blks_hit, 0)"),
        ("vacuums", "s.vacuum_count + s.autovacuum_count"),
        ("analyzes", "s.analyze_count + s.autoanalyze_count"),
    };

    private static readonly string ColumnList = string.Join(", ", Counters.Select(c => c.Column));

    private static readonly string CurrentCountersSql =
        "SELECT s.relid::bigint AS rel_oid, " + string.Join(", ", Counters.Select(c => c.Source + " AS " + c.Column))
        + " FROM pg_stat_user_tables s JOIN pg_statio_user_tables io ON io.relid = s.relid";

    /// <summary>The database's statistics reset time (naive UTC), read in the same snapshot as the counters. NULL when the database's statistics were never
    /// reset. <c>pg_stat_reset()</c> moves it, and so does the discard of every statistic after a crash. A clean restart keeps both the counters and this
    /// value, which is why the postmaster start time is not the identity: it would call every clean restart a reset and report the table's whole life as one hour.</summary>
    private const string StatsResetSql = "(SELECT d.stats_reset AT TIME ZONE 'UTC' FROM pg_stat_database d WHERE d.datname = current_database())";

    private static string Sum(string alias) => string.Join(" + ", Counters.Select(c => alias + "." + c.Column));

    /// <summary>
    /// One relation's difference per counter. When the database's statistics reset time differs from the one stored with the baseline, every counter
    /// is counted from zero (the difference is the current value), whatever the baseline says: a busy table's counter can climb back above its old
    /// baseline before the next capture. Otherwise a counter at or above its baseline is the difference, and a counter below it ran from a reset of
    /// that table alone inside the interval, so its whole value is the difference. No baseline row is a baseline of zero.
    /// </summary>
    private static readonly string DeltaColumnsSql = string.Join(", ", Counters.Select(c =>
        "CASE WHEN rs.seen THEN c." + c.Column + " WHEN c." + c.Column + " >= coalesce(p." + c.Column + ", 0) THEN c." + c.Column + " - coalesce(p." + c.Column + ", 0) ELSE c." + c.Column + " END AS " + c.Column));

    private static readonly string RolledColumnsSql = string.Join(", ", Counters.Select(c => "sum(m." + c.Column + ")::bigint AS " + c.Column));

    private static readonly string FinalColumnsSql = string.Join(", ", Counters.Select(c => "r." + c.Column));

    /// <summary>
    /// The activity rows for the hour. $1 is the capture's metric_time (naive UTC). With no baseline the statement writes nothing (the first
    /// capture, or a store whose last baseline was lost). The TimescaleDB form also maps a compressed chunk's relation to its chunk and
    /// names a continuous aggregate by its view. <paramref name="timescale"/> is the worker's cached TimescaleDB detection.
    /// </summary>
    public static string ActivityInsertSql(bool timescale)
    {
        var rootJoins = timescale
            ? @"LEFT JOIN _timescaledb_catalog.compression_settings cs ON cs.compress_relid::oid = a.rel_oid::oid
    LEFT JOIN pg_inherits ih ON ih.inhrelid = coalesce(cs.relid::oid, a.rel_oid::oid)"
            : "LEFT JOIN pg_inherits ih ON ih.inhrelid = a.rel_oid::oid";
        var rootOid = timescale
            ? "coalesce(ih.inhparent::bigint, cs.relid::oid::bigint, a.rel_oid)"
            : "coalesce(ih.inhparent::bigint, a.rel_oid)";
        var nameJoins = timescale
            ? @"LEFT JOIN timescaledb_information.hypertables h ON h.hypertable_schema = n.nspname AND h.hypertable_name = c.relname
LEFT JOIN timescaledb_information.continuous_aggregates ca ON ca.materialization_hypertable_schema = n.nspname AND ca.materialization_hypertable_name = c.relname"
            : "";
        var schemaName = timescale ? "coalesce(ca.view_schema, n.nspname)" : "n.nspname";
        var tableName = timescale ? "coalesce(ca.view_name, c.relname)" : "c.relname";
        var kind = timescale
            ? "CASE WHEN ca.view_name IS NOT NULL THEN '" + ContinuousAggregateKind + "' WHEN h.hypertable_name IS NOT NULL THEN '" + HypertableKind + "' ELSE '" + TableKind + "' END"
            : "'" + TableKind + "'";

        return @"
INSERT INTO collect.store_table_activity
    (metric_time, interval_seconds, table_schema, table_name, table_kind, relations_active, relations_scanned, " + ColumnList + @")
WITH cur AS MATERIALIZED (" + CurrentCountersSql + @"),
base AS MATERIALIZED (SELECT * FROM collect.store_table_activity_baseline),
ident AS (SELECT " + StatsResetSql + @" AS stats_reset),
reset AS (SELECT coalesce(bool_or(b.stats_reset IS DISTINCT FROM i.stats_reset), false) AS seen FROM base b CROSS JOIN ident i),
span AS (SELECT greatest(extract(epoch FROM ($1::timestamp - max(captured_at))), 0)::integer AS seconds FROM base),
delta AS (
    SELECT c.rel_oid, " + DeltaColumnsSql + @"
    FROM cur c
    CROSS JOIN reset rs
    LEFT JOIN base p ON p.rel_oid = c.rel_oid
    WHERE EXISTS (SELECT 1 FROM base)
),
active AS (SELECT a.* FROM delta a WHERE " + Sum("a") + @" > 0),
mapped AS (
    SELECT a.*, " + rootOid + @" AS root_oid
    FROM active a
    " + rootJoins + @"
),
rolled AS (
    SELECT m.root_oid,
           count(*)::integer AS relations_active,
           (count(*) FILTER (WHERE m.seq_scans + m.idx_scans > 0))::integer AS relations_scanned,
           " + RolledColumnsSql + @"
    FROM mapped m
    GROUP BY m.root_oid
)
SELECT $1::timestamp, span.seconds, " + schemaName + ", " + tableName + ", " + kind + @", r.relations_active, r.relations_scanned, " + FinalColumnsSql + @"
FROM rolled r
CROSS JOIN span
JOIN pg_class c ON c.oid = r.root_oid::oid
JOIN pg_namespace n ON n.oid = c.relnamespace
" + nameJoins;
    }

    /// <summary>Clears the baseline. Runs in the capture's transaction, before <see cref="BaselineInsertSql"/>.</summary>
    public const string BaselineDeleteSql = "DELETE FROM collect.store_table_activity_baseline";

    /// <summary>The new baseline: every relation with a non-zero counter, stamped $1 (naive UTC).</summary>
    public static readonly string BaselineInsertSql =
        "INSERT INTO collect.store_table_activity_baseline (rel_oid, captured_at, stats_reset, " + ColumnList + ") "
        + "SELECT c.rel_oid, $1::timestamp, " + StatsResetSql + ", " + string.Join(", ", Counters.Select(x => "c." + x.Column))
        + " FROM (" + CurrentCountersSql + ") c WHERE " + Sum("c") + " > 0";

    /// <summary>The newest capture's time (naive UTC), or NULL when no capture has ever stored a baseline. One row.</summary>
    public const string LastCaptureSql = "SELECT max(captured_at) FROM collect.store_table_activity_baseline";

    /// <summary>The retention DELETE. $1 is the cutoff (naive UTC).</summary>
    public const string RetentionDeleteSql = "DELETE FROM collect.store_table_activity WHERE metric_time < $1";

    /// <summary>
    /// Takes one capture: the hour's differences into <c>store_table_activity</c>, then the baseline replaced with the counters it was
    /// taken from, in ONE repeatable-read transaction so the baseline is exactly what the differences were measured against (a relation
    /// created while the capture ran is in both or in neither). Retention follows outside the transaction. Returns the activity rows written.
    ///
    /// <para>Never fails the tick: any error other than a cancellation is logged and the hour has no rows. The next capture then reads
    /// the older baseline, so the interval it carries is longer and no activity is lost, only its hour boundary.</para>
    /// </summary>
    public static async Task<int> CaptureAsync(
        NpgsqlConnection connection,
        bool timescaleAvailable,
        DateTime utcNow,
        ILogger? logger,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var metricTime = DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified);
        try
        {
            int written;
            await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken))
            {
                await using (var activity = new NpgsqlCommand(ActivityInsertSql(timescaleAvailable), connection, transaction) { CommandTimeout = commandTimeoutSeconds })
                {
                    activity.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, metricTime);
                    written = await activity.ExecuteNonQueryAsync(cancellationToken);
                }

                await using (var clear = new NpgsqlCommand(BaselineDeleteSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
                {
                    await clear.ExecuteNonQueryAsync(cancellationToken);
                }

                await using (var baseline = new NpgsqlCommand(BaselineInsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
                {
                    baseline.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, metricTime);
                    await baseline.ExecuteNonQueryAsync(cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }

            await using var retention = new NpgsqlCommand(RetentionDeleteSql, connection) { CommandTimeout = commandTimeoutSeconds };
            retention.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, metricTime.AddDays(-StoreSelfMetrics.RetentionDays));
            await retention.ExecuteNonQueryAsync(cancellationToken);

            logger?.LogDebug("Store table activity capture wrote {Rows} rows at {MetricTime}", written, metricTime);
            return written;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "Store table activity capture failed, so this hour has no per-table activity rows");
            return 0;
        }
    }
}
