/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-collection totals of <c>collect.procedure_stats</c> (#5638, V176): one row per (server, collection, database)
/// holding what the raw procedure duration trend sums from every row of that collection. Since #5463 a collection stores
/// about 560 to 1,060 rows per server, and the trend's per-collection CTE read all of them (about 60 K heap pages for one
/// 24 h read). The collector now writes the totals in the same transaction as the rows (<see cref="ProcedureStatsTotalsWriter"/>)
/// and the raw trend's per-collection CTE (<c>DurationTrendRouting.BuildBucketedRawTrendSql</c>) reads them in place of the rows. This class is the storage half: the table, the upsert
/// and the retention predicate.
///
/// <para><b>Grain and measures.</b> <c>(server_id, collection_time, database_name)</c>. Per database because the Viewer's
/// database filter sums only the chosen databases of a collection. Every measure is the aggregate the trend's CTE takes over
/// the same rows, with the same column types and no unit conversion (PostgreSQL keeps the microseconds, so every value is
/// exact): <c>total_elapsed_time</c> = SUM(delta_elapsed_time), <c>total_execution_count</c> = SUM(delta_execution_count),
/// <c>row_count</c> = COUNT(*) (the filtered trend's <c>matched_rows</c>), <c>max_sample_interval_seconds</c> =
/// MAX(sample_interval_seconds). The two sums and the max are NULL when every row's value is NULL, as SUM and MAX are over the
/// rows, so the three-state interval contract (#3540: 0 = restart, NULL = unknown, positive = measured) reads the same from here.</para>
///
/// <para><b>NULL database_name.</b> <c>procedure_stats.database_name</c> is a nullable <c>text</c>, so the key is a unique
/// index <c>NULLS NOT DISTINCT</c> (the form V170 uses; PostgreSQL 15 is the supported floor): the NULL database is one group, as
/// GROUP BY makes it, and the writer's conflict target matches that index. The table keeps the NULL group exactly as the raw rows
/// have it, so a database filter (<c>database_name = ANY(...)</c>) treats it the same here as on the rows.</para>
///
/// <para><b>No backfill.</b> The table is empty when created. A collection written before the rung has no totals row, and the
/// read keeps reading the rows for it (stitched at the server's floor, <c>MIN(collection_time)</c> of this table).</para>
///
/// <para><b>A plain table, on both store kinds.</b> About one row per database per collection against a thousand raw rows, so
/// the table is a small fraction of <c>procedure_stats</c>. Like the hour ledger (V164) and the PostgreSQL I/O rollup (V170) it is
/// not a collector, is absent from <c>CollectorCatalog.All</c> and is not a hypertable. It sits in the <c>collect</c> schema, so the
/// blanket SELECT and the owner's default privileges cover it, and the collector writes as the owner. Retention is
/// <see cref="PrunePredicate"/>.</para>
/// </summary>
public static class ProcedureStatsTotals
{
    /// <summary>The store schema version of the rung that creates the table (V176). A store below it has no table.</summary>
    public const int RungVersion = 176;

    /// <summary>The totals table.</summary>
    public const string Table = "collect.procedure_stats_totals";

    /// <summary>The unique key's index name.</summary>
    public const string KeyIndexName = "ux_procedure_stats_totals";

    /// <summary>
    /// The table, its unique key and the time index, as the V176 rung runs them. Idempotent, and empty when created. Editing this text
    /// edits a shipped rung: add a new rung instead (<c>ProcedureStatsTotalsTests</c> pins the shape).
    /// </summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS collect.procedure_stats_totals
(
    server_id integer NOT NULL,
    collection_time timestamp NOT NULL,
    database_name text,
    total_elapsed_time bigint,
    total_execution_count bigint,
    row_count bigint NOT NULL,
    max_sample_interval_seconds integer
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_procedure_stats_totals ON collect.procedure_stats_totals (server_id, collection_time, database_name) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS ix_procedure_stats_totals_time ON collect.procedure_stats_totals (collection_time);
""";

    /// <summary>
    /// Writes the totals of one collection: <c>$1</c> is the server id and <c>$2</c> the collection's <c>collection_time</c> (naive UTC
    /// <c>timestamp</c>). It READS BACK the rows the store holds for that collection and upserts one row per database, so it is exact
    /// and idempotent when one collection spans several batches or a batch is attempted again: the second run recomputes the same
    /// values from the rows and never adds to them. The sums are cast to <c>bigint</c> (SUM of a bigint is numeric); a sum past bigint
    /// would fail the statement, which is 292,000 years of elapsed time in one collection. Run it on the COPY's own transaction, after
    /// the COPY and before the commit.
    /// </summary>
    public const string UpsertSql = """
INSERT INTO collect.procedure_stats_totals AS t
    (server_id, collection_time, database_name, total_elapsed_time, total_execution_count, row_count, max_sample_interval_seconds)
SELECT
    server_id,
    collection_time,
    database_name,
    SUM(delta_elapsed_time)::bigint,
    SUM(delta_execution_count)::bigint,
    COUNT(*),
    MAX(sample_interval_seconds)
FROM collect.procedure_stats
WHERE server_id = $1::integer
AND   collection_time = $2::timestamp
GROUP BY server_id, collection_time, database_name
ON CONFLICT (server_id, collection_time, database_name) DO UPDATE SET
    total_elapsed_time = EXCLUDED.total_elapsed_time,
    total_execution_count = EXCLUDED.total_execution_count,
    row_count = EXCLUDED.row_count,
    max_sample_interval_seconds = EXCLUDED.max_sample_interval_seconds;
""";

    /// <summary>
    /// The extra predicate the retention sweep adds to its one-day-slice DELETE (<c>DarlingRetention.TimeSlicedDeleteSql</c>; <c>$1</c>
    /// is the cutoff): a totals row is never purged before the <c>procedure_stats</c> rows of the same age. The cutoff is the raw
    /// collector's own retention, so on a plain store the two purge together. On TimescaleDB raw chunks leave through a gated purge that
    /// can hold them past the horizon, so the predicate also keeps every totals row at or after the oldest raw row. With no raw rows left
    /// the oldest is NULL and the cutoff alone decides.
    /// </summary>
    public const string PrunePredicate =
        "collection_time < COALESCE((SELECT min(collection_time) FROM collect.procedure_stats), $1)";
}
