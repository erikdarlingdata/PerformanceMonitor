/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

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
    ///
    /// <para><b>Limited to the batch's databases (review of #5706).</b> <c>$3</c> is the <c>text[]</c> of the non-NULL database names the
    /// batch wrote and <c>$4</c> says whether the batch wrote a NULL database; a NULL <c>$3</c> reads the whole collection. On Azure SQL
    /// Database the collector writes one batch per database at one <c>collection_time</c>, and a read-back of the whole collection in
    /// each made the work grow with the square of the database count. Totals are per database and a batch holds whole databases (the
    /// read-back still counts rows of earlier batches of the same database), so the result is exact. The NULL database matches only
    /// through <c>$4</c>, as <c>= ANY</c> never matches a NULL.</para>
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
AND   ($3::text[] IS NULL OR database_name = ANY($3::text[]) OR ($4::boolean AND database_name IS NULL))
GROUP BY server_id, collection_time, database_name
ON CONFLICT (server_id, collection_time, database_name) DO UPDATE SET
    total_elapsed_time = EXCLUDED.total_elapsed_time,
    total_execution_count = EXCLUDED.total_execution_count,
    row_count = EXCLUDED.row_count,
    max_sample_interval_seconds = EXCLUDED.max_sample_interval_seconds;
""";

    /// <summary>
    /// The gap probe, run on the COPY's transaction before anything else of the gap fill (round 2 review of #5706, L1): <c>$1</c> is
    /// the server id and <c>$2</c> the batch's <c>collection_time</c>. One row: the connection's current <c>statement_timeout</c>
    /// (to restore), the anchor (the server's newest totals row at or before the batch, NULL when it has none), and whether a gap
    /// exists (a <c>procedure_stats</c> row of the server strictly after the anchor and strictly before the batch). An <c>EXISTS</c>
    /// on the <c>(server_id, collection_time)</c> index stops at its first row, so it cannot be the "big gap" failure the savepoint
    /// guards against, and the normal batch pays this one statement and nothing else: no savepoint, no timeout change, no gap
    /// statement. <b>The anchor is <c>&lt;= $2</c></b> (round 2, M1): on Azure SQL Database every per-database batch of a cycle
    /// shares one <c>collection_time</c>, so a batch whose gap fill failed has still committed a totals row at that time, and the
    /// next batch of the cycle must see it as its anchor (its range is then empty) instead of trying the same gap again. A
    /// re-attempt of a batch that rolled back has no committed totals at that time, so it still tries the gap.
    /// </summary>
    public const string GapProbeSql = """
SELECT
    current_setting('statement_timeout'),
    a.anchor,
    a.anchor IS NOT NULL
    AND EXISTS
    (
        SELECT 1
        FROM collect.procedure_stats AS s
        WHERE s.server_id = $1::integer
        AND   s.collection_time > a.anchor
        AND   s.collection_time < $2::timestamp
    )
FROM
(
    SELECT max(p.collection_time) AS anchor
    FROM collect.procedure_stats_totals AS p
    WHERE p.server_id = $1::integer
    AND   p.collection_time <= $2::timestamp
) AS a;
""";

    /// <summary>
    /// Totals the collections a pre-V176 binary wrote after the floor (review of #5706): <c>$1</c> is the server id, <c>$2</c> the
    /// batch's <c>collection_time</c> and <c>$3</c> the anchor <see cref="GapProbeSql"/> read. <c>PgMigrations</c> lets an older
    /// binary write to a V176 store (a rollback), and the raw trend reads <c>procedure_stats</c> only below the server's floor, so a
    /// collection that has rows, no totals and sits above the floor would vanish from the trend. This totals every collection of the
    /// server that has rows strictly after the anchor and strictly before the batch. The writer runs it only when the probe found
    /// such a row, so it is never the normal path. <b>The anchor is a parameter, not a subquery (round 2, L2):</b> both bounds are
    /// then constants at plan time, which lets TimescaleDB exclude every chunk outside <c>(anchor, batch)</c>, compressed ones
    /// included, instead of keeping each chunk older than the batch. Same upsert target and recompute as <see cref="UpsertSql"/>, so
    /// it is idempotent and exact.
    /// </summary>
    public const string GapFillSql = """
INSERT INTO collect.procedure_stats_totals AS t
    (server_id, collection_time, database_name, total_elapsed_time, total_execution_count, row_count, max_sample_interval_seconds)
SELECT
    s.server_id,
    s.collection_time,
    s.database_name,
    SUM(s.delta_elapsed_time)::bigint,
    SUM(s.delta_execution_count)::bigint,
    COUNT(*),
    MAX(s.sample_interval_seconds)
FROM collect.procedure_stats AS s
WHERE s.server_id = $1::integer
AND   s.collection_time < $2::timestamp
AND   s.collection_time > $3::timestamp
GROUP BY s.server_id, s.collection_time, s.database_name
ON CONFLICT (server_id, collection_time, database_name) DO UPDATE SET
    total_elapsed_time = EXCLUDED.total_elapsed_time,
    total_execution_count = EXCLUDED.total_execution_count,
    row_count = EXCLUDED.row_count,
    max_sample_interval_seconds = EXCLUDED.max_sample_interval_seconds;
""";

    /// <summary>
    /// The extra predicate the retention sweep adds to its one-day-slice DELETE (<c>DarlingRetention.TimeSlicedDeleteSql</c>; <c>$1</c>
    /// is the cutoff, <c>$2</c> the raw floor): a totals row is never purged before the <c>procedure_stats</c> rows of the same age. The
    /// cutoff is the raw collector's own retention, so on a plain store the two purge together. On TimescaleDB raw chunks leave through a
    /// gated purge that can hold them past the horizon, so the predicate also keeps every totals row at or after the oldest raw row. With
    /// no raw rows left the floor is NULL and the cutoff alone decides.
    ///
    /// <para><b>The floor is a parameter, measured once (review of #5706, #1815).</b> The first form was a
    /// <c>(SELECT min(collection_time) FROM collect.procedure_stats)</c> inside the predicate, which the slice statement expands three
    /// times: three unbounded scans of the whole raw table per DELETE, and on compressed chunks (no btree) the probe that timed out in
    /// the field for the digest floor. <see cref="MeasureRawFloorAsync"/> reads it once before the DELETE.</para>
    /// </summary>
    public const string PrunePredicate = "collection_time < COALESCE($2::timestamp, $1)";

    /// <summary>
    /// The plain-store floor (round 2 review of #5706, L3): the oldest <c>procedure_stats</c> row of any server that has totals rows,
    /// as the minimum over those servers of each server's own <c>ORDER BY collection_time LIMIT 1</c>. The table's time index is
    /// <c>(server_id, collection_time)</c>, so a global <c>min(collection_time)</c> cannot read an index edge: nothing leads with
    /// <c>collection_time</c> but two partial indexes, and the planner scans the whole table or index once per sweep. Each server's
    /// own probe is one descent of the server-leading index. The servers come from a loose index scan of the totals key (the
    /// recursive step is one index descent per server), so the statement is an index probe per server and never a scan. A server
    /// without totals rows is left out on purpose: it has no totals to protect, and the trend stitch takes its floor per server. The
    /// minimum across servers is never later than any one server's own floor, so the guard can only keep more totals.
    /// </summary>
    public const string PlainRawFloorSql = """
WITH RECURSIVE servers AS
(
    (
        SELECT t.server_id
        FROM collect.procedure_stats_totals AS t
        ORDER BY t.server_id
        LIMIT 1
    )
    UNION ALL
    SELECT
    (
        SELECT t.server_id
        FROM collect.procedure_stats_totals AS t
        WHERE t.server_id > servers.server_id
        ORDER BY t.server_id
        LIMIT 1
    )
    FROM servers
    WHERE servers.server_id IS NOT NULL
)
SELECT min(oldest.collection_time)
FROM servers
CROSS JOIN LATERAL
(
    SELECT p.collection_time
    FROM collect.procedure_stats AS p
    WHERE p.server_id = servers.server_id
    ORDER BY p.collection_time
    LIMIT 1
) AS oldest
WHERE servers.server_id IS NOT NULL;
""";

    /// <summary>
    /// The oldest instant <c>collect.procedure_stats</c> can still hold a row at, the <c>$2</c> of <see cref="PrunePredicate"/>; NULL when
    /// the table holds no row of a server that has totals. On a hypertable it is the earliest chunk's <c>range_start</c> from the chunk
    /// catalog (one metadata row, immune to compression, as the digest floor of #1815 reads it); every surviving row sits at or after
    /// its chunk's start, so the floor is never later than the true minimum and can only keep more totals, never delete too many. On a
    /// plain table, or a hypertable with no chunks, it is <see cref="PlainRawFloorSql"/>: one index probe per server with totals, not
    /// a scan.
    /// </summary>
    public static async Task<DateTime?> MeasureRawFloorAsync(
        NpgsqlConnection connection,
        bool timescaleAvailable,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        if (timescaleAvailable)
        {
            /* AT TIME ZONE 'UTC' collapses the catalog's timestamptz to the naive-UTC discipline every stored timestamp follows. */
            await using var chunkProbe = new NpgsqlCommand(
                "SELECT range_start AT TIME ZONE 'UTC' FROM timescaledb_information.chunks " +
                "WHERE hypertable_schema = 'collect' AND hypertable_name = 'procedure_stats' " +
                "ORDER BY range_start LIMIT 1", connection)
            { CommandTimeout = commandTimeoutSeconds };
            if (await chunkProbe.ExecuteScalarAsync(cancellationToken) is DateTime chunkFloor)
            {
                return chunkFloor;
            }
        }

        await using var probe = new NpgsqlCommand(PlainRawFloorSql, connection)
        { CommandTimeout = commandTimeoutSeconds };
        return await probe.ExecuteScalarAsync(cancellationToken) as DateTime?;
    }
}
