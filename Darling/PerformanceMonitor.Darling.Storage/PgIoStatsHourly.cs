/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// SQL for the hourly rollup of <c>collect.pg_io_stats</c> (#5495, V170): per (server, hour, backend_type, object_type,
/// context), the window differences <c>get_pg_io_stats</c> would sum over that hour, so a long window reads whole hours from
/// a few thousand rollup rows plus its two raw edges instead of differencing every raw row (7 days: 907k rows, 4 to 6 s on the rig).
/// The text, the stitch proof and the measurements are in the PR for #5495; the storage half is here, the builder and
/// the read's fallback are <see cref="PgIoStatsHourlyBuilder"/> and <see cref="DarlingPgIoReader"/>.
///
/// <para><b>Why not a continuous aggregate (#5329/#5472).</b> The counters are cumulative and the read sums
/// <c>GREATEST(x - LAG(x), 0)</c> per combination; a continuous aggregate cannot hold a window function, so the rollup is a
/// plain table filled by the hourly maintenance tick, like V168's builder (#5448).</para>
///
/// <para><b>What a rollup row holds, so the stitch is exact.</b> <c>i_*</c> is the sum of the differences between rows
/// inside the hour. <c>b_*</c> is the difference of the hour's FIRST row against the combination's previous row (the
/// last row of an earlier hour), with that row's <c>prev_ct</c>: the read adds <c>b_*</c> only when <c>prev_ct</c> is inside
/// its window, which is exactly when the old per-window <c>LAG</c> would have seen that row. <c>l_*</c> is the hour's last row,
/// the previous row for the first raw row of the tail edge. A counter reset or a crash restart is a negative difference
/// that <c>GREATEST(..., 0)</c> flattens to 0 in all three, as in the raw read.</para>
///
/// <para><b>Plain tables, built by the service.</b> The service owns every write. The <c>collect</c> schema's blanket SELECT
/// covers both tables; no GRANT and no provisioning change. No Lite twin: Lite has no PostgreSQL targets.</para>
/// </summary>
public static class PgIoStatsHourly
{
    /// <summary>The store schema version of the rung that creates the tables (V170). A store below it has neither table.</summary>
    public const int RungVersion = 170;

    public const string Table = "collect.pg_io_stats_hourly";
    public const string StateTable = "collect.pg_io_stats_hourly_state";

    /// <summary>Both tables, the unique key and the seed index, as the V170 rung runs them. Idempotent. The tables are empty when created.</summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS collect.pg_io_stats_hourly
(
    server_id integer NOT NULL,
    hour_start timestamp NOT NULL,
    backend_type text, object_type text, context text,
    row_count bigint NOT NULL,
    last_ct timestamp NOT NULL,
    prev_ct timestamp,
    stats_reset timestamp,
    op_bytes bigint,
    writes_tracked boolean NOT NULL,
    byte_counters_tracked boolean NOT NULL,
    i_reads numeric, i_read_time_ms double precision, i_hits numeric, i_extends numeric, i_extend_time_ms double precision,
    i_evictions numeric, i_reuses numeric, i_writes numeric, i_write_time_ms double precision,
    i_read_bytes numeric, i_write_bytes numeric, i_extend_bytes numeric,
    b_reads numeric, b_read_time_ms double precision, b_hits numeric, b_extends numeric, b_extend_time_ms double precision,
    b_evictions numeric, b_reuses numeric, b_writes numeric, b_write_time_ms double precision,
    b_read_bytes numeric, b_write_bytes numeric, b_extend_bytes numeric,
    l_reads bigint, l_read_time_ms double precision, l_hits bigint, l_extends bigint, l_extend_time_ms double precision,
    l_evictions bigint, l_reuses bigint, l_writes bigint, l_write_time_ms double precision,
    l_read_bytes numeric(28,0), l_write_bytes numeric(28,0), l_extend_bytes numeric(28,0)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_pg_io_stats_hourly ON collect.pg_io_stats_hourly (server_id, hour_start, backend_type, object_type, context) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS ix_pg_io_stats_hourly_combo ON collect.pg_io_stats_hourly (server_id, (ARRAY[backend_type, object_type, context]), hour_start DESC);
CREATE TABLE IF NOT EXISTS collect.pg_io_stats_hourly_state (server_id integer PRIMARY KEY, first_hour timestamp NOT NULL, built_through timestamp NOT NULL);
""";

    /// <summary>
    /// Builds one (server, hour): $1 server_id, $2 the hour start (naive UTC timestamp); returns the rows inserted. The caller
    /// deletes the hour's rows first, in the same transaction. Seeds each combination's difference from the newest EARLIER rollup row
    /// (its <c>l_*</c>), so hours must be built in order from <c>first_hour</c>. The seed is one index probe per combination
    /// of the hour being built (<c>ORDER BY hour_start DESC LIMIT 1</c> on <c>ix_pg_io_stats_hourly_combo</c>), not a sort of every earlier
    /// rollup row of the server: that sort made the first fill quadratic in the hours already built.
    ///
    /// <para><b>Why an array compare.</b> The three key columns are nullable, and a NULL must match a NULL (as <c>IS NOT DISTINCT FROM</c>
    /// did), which no index can serve: a combination new to the server, or absent for many hours, scanned back through all the server's
    /// earlier rollup rows. Array equality treats NULL elements as equal and is btree-indexable, so the probe compares
    /// <c>ARRAY[backend_type, object_type, context]</c> and the index holds that same expression ahead of <c>hour_start</c>. The match
    /// and the row chosen are the same: the unique index is NULLS NOT DISTINCT, so each hour has one row per combination.</para>
    /// </summary>
    public const string BuildHourSql = """
        WITH hr AS (
            SELECT backend_type, object_type, context, collection_time AS ct, false AS is_seed,
                   reads, read_time_ms, hits, extends, extend_time_ms, evictions, reuses, writes, write_time_ms,
                   read_bytes, write_bytes, extend_bytes, stats_reset, op_bytes
            FROM collect.pg_io_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $2 + interval '1 hour'
        ),
        seed AS (
            SELECT s.backend_type, s.object_type, s.context, s.last_ct AS ct, true AS is_seed,
                   s.l_reads AS reads, s.l_read_time_ms AS read_time_ms, s.l_hits AS hits, s.l_extends AS extends, s.l_extend_time_ms AS extend_time_ms,
                   s.l_evictions AS evictions, s.l_reuses AS reuses, s.l_writes AS writes, s.l_write_time_ms AS write_time_ms,
                   s.l_read_bytes AS read_bytes, s.l_write_bytes AS write_bytes, s.l_extend_bytes AS extend_bytes,
                   NULL::timestamp AS stats_reset, NULL::bigint AS op_bytes
            FROM (SELECT DISTINCT backend_type, object_type, context FROM hr) AS cmb
            CROSS JOIN LATERAL (
                SELECT * FROM collect.pg_io_stats_hourly AS p
                WHERE p.server_id = $1 AND p.hour_start < $2
                AND   ARRAY[p.backend_type, p.object_type, p.context] = ARRAY[cmb.backend_type, cmb.object_type, cmb.context]
                ORDER BY p.hour_start DESC
                LIMIT 1
            ) AS s
        ),
        u AS (SELECT * FROM seed UNION ALL SELECT * FROM hr),
        d AS (
            SELECT u.*,
                   LAG(ct) OVER series AS prev_ct,
                   count(*) FILTER (WHERE NOT is_seed) OVER (series ROWS UNBOUNDED PRECEDING) AS rn,
                   GREATEST(reads - LAG(reads) OVER series, 0) AS d_reads,
                   GREATEST(read_time_ms - LAG(read_time_ms) OVER series, 0) AS d_read_time_ms,
                   GREATEST(hits - LAG(hits) OVER series, 0) AS d_hits,
                   GREATEST(extends - LAG(extends) OVER series, 0) AS d_extends,
                   GREATEST(extend_time_ms - LAG(extend_time_ms) OVER series, 0) AS d_extend_time_ms,
                   GREATEST(evictions - LAG(evictions) OVER series, 0) AS d_evictions,
                   GREATEST(reuses - LAG(reuses) OVER series, 0) AS d_reuses,
                   GREATEST(writes - LAG(writes) OVER series, 0) AS d_writes,
                   GREATEST(write_time_ms - LAG(write_time_ms) OVER series, 0) AS d_write_time_ms,
                   GREATEST(read_bytes - LAG(read_bytes) OVER series, 0) AS d_read_bytes,
                   GREATEST(write_bytes - LAG(write_bytes) OVER series, 0) AS d_write_bytes,
                   GREATEST(extend_bytes - LAG(extend_bytes) OVER series, 0) AS d_extend_bytes
            FROM u
            WINDOW series AS (PARTITION BY backend_type, object_type, context ORDER BY ct)
        ),
        a AS (
            SELECT backend_type, object_type, context,
                   count(*) AS row_count, max(ct) AS last_ct,
                   max(prev_ct) FILTER (WHERE rn = 1) AS prev_ct,
                   max(stats_reset) AS stats_reset, max(op_bytes) AS op_bytes,
                   bool_or(writes IS NOT NULL) AS writes_tracked, bool_or(read_bytes IS NOT NULL) AS byte_counters_tracked,
                   sum(d_reads) FILTER (WHERE rn > 1) AS i_reads, sum(d_read_time_ms) FILTER (WHERE rn > 1) AS i_read_time_ms,
                   sum(d_hits) FILTER (WHERE rn > 1) AS i_hits, sum(d_extends) FILTER (WHERE rn > 1) AS i_extends,
                   sum(d_extend_time_ms) FILTER (WHERE rn > 1) AS i_extend_time_ms, sum(d_evictions) FILTER (WHERE rn > 1) AS i_evictions,
                   sum(d_reuses) FILTER (WHERE rn > 1) AS i_reuses, sum(d_writes) FILTER (WHERE rn > 1) AS i_writes,
                   sum(d_write_time_ms) FILTER (WHERE rn > 1) AS i_write_time_ms, sum(d_read_bytes) FILTER (WHERE rn > 1) AS i_read_bytes,
                   sum(d_write_bytes) FILTER (WHERE rn > 1) AS i_write_bytes, sum(d_extend_bytes) FILTER (WHERE rn > 1) AS i_extend_bytes,
                   sum(d_reads) FILTER (WHERE rn = 1) AS b_reads, sum(d_read_time_ms) FILTER (WHERE rn = 1) AS b_read_time_ms,
                   sum(d_hits) FILTER (WHERE rn = 1) AS b_hits, sum(d_extends) FILTER (WHERE rn = 1) AS b_extends,
                   sum(d_extend_time_ms) FILTER (WHERE rn = 1) AS b_extend_time_ms, sum(d_evictions) FILTER (WHERE rn = 1) AS b_evictions,
                   sum(d_reuses) FILTER (WHERE rn = 1) AS b_reuses, sum(d_writes) FILTER (WHERE rn = 1) AS b_writes,
                   sum(d_write_time_ms) FILTER (WHERE rn = 1) AS b_write_time_ms, sum(d_read_bytes) FILTER (WHERE rn = 1) AS b_read_bytes,
                   sum(d_write_bytes) FILTER (WHERE rn = 1) AS b_write_bytes, sum(d_extend_bytes) FILTER (WHERE rn = 1) AS b_extend_bytes
            FROM d WHERE NOT is_seed
            GROUP BY backend_type, object_type, context
        ),
        lst AS (
            SELECT DISTINCT ON (backend_type, object_type, context) backend_type, object_type, context,
                   reads, read_time_ms, hits, extends, extend_time_ms, evictions, reuses, writes, write_time_ms,
                   read_bytes, write_bytes, extend_bytes
            FROM hr ORDER BY backend_type, object_type, context, ct DESC
        ),
        ins AS (
            INSERT INTO collect.pg_io_stats_hourly
            SELECT $1, $2, a.backend_type, a.object_type, a.context, a.row_count, a.last_ct, a.prev_ct, a.stats_reset, a.op_bytes,
                   a.writes_tracked, a.byte_counters_tracked,
                   a.i_reads, a.i_read_time_ms, a.i_hits, a.i_extends, a.i_extend_time_ms, a.i_evictions, a.i_reuses, a.i_writes, a.i_write_time_ms,
                   a.i_read_bytes, a.i_write_bytes, a.i_extend_bytes,
                   a.b_reads, a.b_read_time_ms, a.b_hits, a.b_extends, a.b_extend_time_ms, a.b_evictions, a.b_reuses, a.b_writes, a.b_write_time_ms,
                   a.b_read_bytes, a.b_write_bytes, a.b_extend_bytes,
                   l.reads, l.read_time_ms, l.hits, l.extends, l.extend_time_ms, l.evictions, l.reuses, l.writes, l.write_time_ms,
                   l.read_bytes, l.write_bytes, l.extend_bytes
            FROM a JOIN lst l ON l.backend_type IS NOT DISTINCT FROM a.backend_type AND l.object_type IS NOT DISTINCT FROM a.object_type
                             AND l.context IS NOT DISTINCT FROM a.context
            RETURNING 1)
        SELECT count(*) FROM ins;
""";

    /// <summary>
    /// The count guard: $1 server_id, $2/$3 the window. Returns (h1, h2), the whole-hour span the rollup may serve, or no row.
    /// h1 is the window start rounded up, h2 the end rounded down and capped at the built watermark; the span must be 6 hours or more,
    /// h1 must be after the server's first built hour (earlier, the seed of the first rollup rows is unknown), and every hour's raw row
    /// count must equal the rollup's <c>sum(row_count)</c>, for the hours in [h1, h2) AND for the hour just before h1. A late row
    /// older than the builder's rebuild span, an unbuilt hour or a purged hour all make a count differ.
    ///
    /// <para><b>The hour before h1.</b> The first rollup hour's boundary difference <c>b_*</c> was taken against the newest row
    /// before it as of the build, and the stitch adds it only when that row is inside the window, so that row is in the window's head
    /// edge and in the hour just before h1. A row that arrived there after the build is differenced by the raw head edge AND still
    /// missing from the stale boundary difference, which is a double count; the count of that hour catches it.</para>
    ///
    /// <para><b>Bounds the planner can use.</b> The raw count is bounded on the bound parameters ($2 less the extra hour, and $3),
    /// besides the exact hours: the exact bounds come out of a CTE column and cannot exclude a chunk of the hypertable, so without
    /// the parameter bounds the count read every compressed chunk of the store at any window length. The rollup table is a plain
    /// table whose unique index leads with (server_id, hour_start), so its range is already an index range.</para>
    /// </summary>
    public const string GuardSql = """
        WITH st AS (
            SELECT first_hour, built_through FROM pg_io_stats_hourly_state WHERE server_id = $1
        ),
        b AS (
            SELECT CASE WHEN date_trunc('hour', $2::timestamp) = $2::timestamp THEN $2::timestamp
                        ELSE date_trunc('hour', $2::timestamp) + interval '1 hour' END AS h1,
                   least(date_trunc('hour', $3::timestamp), st.built_through) AS h2,
                   st.first_hour
            FROM st
        ),
        ok AS (
            SELECT h1, h2 FROM b WHERE h2 - h1 >= interval '6 hours' AND h1 > first_hour
        ),
        l AS (
            SELECT date_trunc('hour', t.collection_time) AS hr, count(*) AS n
            FROM ok, pg_io_stats AS t
            WHERE t.server_id = $1
            AND   t.collection_time >= $2::timestamp - interval '1 hour' AND t.collection_time < $3::timestamp
            AND   t.collection_time >= ok.h1 - interval '1 hour' AND t.collection_time < ok.h2
            GROUP BY 1
        ),
        c AS (
            SELECT r.hour_start AS hr, sum(r.row_count) AS n
            FROM ok, pg_io_stats_hourly AS r
            WHERE r.server_id = $1 AND r.hour_start >= ok.h1 - interval '1 hour' AND r.hour_start < ok.h2
            GROUP BY 1
        )
        SELECT ok.h1, ok.h2
        FROM ok
        WHERE NOT EXISTS (SELECT 1 FROM l FULL JOIN c USING (hr) WHERE l.n IS DISTINCT FROM c.n)
""";

    /// <summary>
    /// The stitched read (same output columns as <see cref="DarlingPgIoReader.PgIoSql"/>): $1 server_id, $2/$3 the window,
    /// $4 row cap, $5/$6 the (h1, h2) the guard returned. Raw rows of the two edges are differenced as before; each rollup hour is
    /// one pseudo-row at its last row's time and values, carrying the hour's in-hour sum plus its boundary difference when the previous row is in the window.
    /// </summary>
    public const string StitchedReadSql = """
        WITH seg AS (
            SELECT collection_time AS ct, backend_type, object_type, context, false AS rolled,
                   reads, read_time_ms, hits, extends, extend_time_ms, evictions, reuses, writes, write_time_ms,
                   read_bytes, write_bytes, extend_bytes, stats_reset, op_bytes,
                   (writes IS NOT NULL) AS writes_tracked, (read_bytes IS NOT NULL) AS byte_counters_tracked,
                   NULL::timestamp AS prev_ct,
                   NULL::numeric AS i_reads, NULL::double precision AS i_read_time_ms, NULL::numeric AS i_hits, NULL::numeric AS i_extends,
                   NULL::double precision AS i_extend_time_ms, NULL::numeric AS i_evictions, NULL::numeric AS i_reuses, NULL::numeric AS i_writes,
                   NULL::double precision AS i_write_time_ms, NULL::numeric AS i_read_bytes, NULL::numeric AS i_write_bytes, NULL::numeric AS i_extend_bytes,
                   NULL::numeric AS b_reads, NULL::double precision AS b_read_time_ms, NULL::numeric AS b_hits, NULL::numeric AS b_extends,
                   NULL::double precision AS b_extend_time_ms, NULL::numeric AS b_evictions, NULL::numeric AS b_reuses, NULL::numeric AS b_writes,
                   NULL::double precision AS b_write_time_ms, NULL::numeric AS b_read_bytes, NULL::numeric AS b_write_bytes, NULL::numeric AS b_extend_bytes
            FROM pg_io_stats
            WHERE server_id = $1
            AND   ((collection_time >= $2 AND collection_time < $5) OR (collection_time >= $6 AND collection_time <= $3))
            UNION ALL
            SELECT last_ct, backend_type, object_type, context, true,
                   l_reads, l_read_time_ms, l_hits, l_extends, l_extend_time_ms, l_evictions, l_reuses, l_writes, l_write_time_ms,
                   l_read_bytes, l_write_bytes, l_extend_bytes, stats_reset, op_bytes, writes_tracked, byte_counters_tracked,
                   prev_ct,
                   i_reads, i_read_time_ms, i_hits, i_extends, i_extend_time_ms, i_evictions, i_reuses, i_writes, i_write_time_ms,
                   i_read_bytes, i_write_bytes, i_extend_bytes,
                   b_reads, b_read_time_ms, b_hits, b_extends, b_extend_time_ms, b_evictions, b_reuses, b_writes, b_write_time_ms,
                   b_read_bytes, b_write_bytes, b_extend_bytes
            FROM pg_io_stats_hourly
            WHERE server_id = $1 AND hour_start >= $5 AND hour_start < $6
        ),
        differenced AS (
            SELECT
                backend_type, object_type, context, stats_reset, op_bytes, writes_tracked, byte_counters_tracked,
                CASE WHEN rolled THEN coalesce(i_reads, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_reads, 0) ELSE 0 END ELSE GREATEST(reads - LAG(reads) OVER series, 0) END AS d_reads,
                CASE WHEN rolled THEN coalesce(i_read_time_ms, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_read_time_ms, 0) ELSE 0 END ELSE GREATEST(read_time_ms - LAG(read_time_ms) OVER series, 0) END AS d_read_time_ms,
                CASE WHEN rolled THEN coalesce(i_hits, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_hits, 0) ELSE 0 END ELSE GREATEST(hits - LAG(hits) OVER series, 0) END AS d_hits,
                CASE WHEN rolled THEN coalesce(i_extends, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_extends, 0) ELSE 0 END ELSE GREATEST(extends - LAG(extends) OVER series, 0) END AS d_extends,
                CASE WHEN rolled THEN coalesce(i_extend_time_ms, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_extend_time_ms, 0) ELSE 0 END ELSE GREATEST(extend_time_ms - LAG(extend_time_ms) OVER series, 0) END AS d_extend_time_ms,
                CASE WHEN rolled THEN coalesce(i_evictions, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_evictions, 0) ELSE 0 END ELSE GREATEST(evictions - LAG(evictions) OVER series, 0) END AS d_evictions,
                CASE WHEN rolled THEN coalesce(i_reuses, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_reuses, 0) ELSE 0 END ELSE GREATEST(reuses - LAG(reuses) OVER series, 0) END AS d_reuses,
                CASE WHEN rolled THEN coalesce(i_writes, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_writes, 0) ELSE 0 END ELSE GREATEST(writes - LAG(writes) OVER series, 0) END AS d_writes,
                CASE WHEN rolled THEN coalesce(i_write_time_ms, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_write_time_ms, 0) ELSE 0 END ELSE GREATEST(write_time_ms - LAG(write_time_ms) OVER series, 0) END AS d_write_time_ms,
                CASE WHEN rolled THEN coalesce(i_read_bytes, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_read_bytes, 0) ELSE 0 END ELSE GREATEST(read_bytes - LAG(read_bytes) OVER series, 0) END AS d_read_bytes,
                CASE WHEN rolled THEN coalesce(i_write_bytes, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_write_bytes, 0) ELSE 0 END ELSE GREATEST(write_bytes - LAG(write_bytes) OVER series, 0) END AS d_write_bytes,
                CASE WHEN rolled THEN coalesce(i_extend_bytes, 0) + CASE WHEN prev_ct >= $2 THEN coalesce(b_extend_bytes, 0) ELSE 0 END ELSE GREATEST(extend_bytes - LAG(extend_bytes) OVER series, 0) END AS d_extend_bytes
            FROM seg
            WINDOW series AS (
                PARTITION BY backend_type, object_type, context
                ORDER BY ct
            )
        )
        SELECT
            backend_type,
            object_type,
            context,
            CAST(coalesce(SUM(d_reads), 0) AS bigint)      AS reads,
            coalesce(SUM(d_read_time_ms), 0)               AS read_time_ms,
            CAST(coalesce(SUM(d_hits), 0) AS bigint)       AS hits,
            CAST(coalesce(SUM(d_extends), 0) AS bigint)    AS extends,
            coalesce(SUM(d_extend_time_ms), 0)             AS extend_time_ms,
            CAST(coalesce(SUM(d_evictions), 0) AS bigint)  AS evictions,
            CAST(coalesce(SUM(d_reuses), 0) AS bigint)     AS reuses,
            CAST(coalesce(SUM(d_writes), 0) AS bigint)     AS writes,
            coalesce(SUM(d_write_time_ms), 0)              AS write_time_ms,
            CAST(coalesce(MAX(op_bytes), 0) AS bigint)     AS op_bytes,
            bool_or(writes_tracked)                        AS write_counters_tracked,
            MAX(stats_reset)                               AS stats_reset,
            coalesce(SUM(d_read_bytes), 0)                 AS read_bytes,
            coalesce(SUM(d_write_bytes), 0)                AS write_bytes,
            coalesce(SUM(d_extend_bytes), 0)               AS extend_bytes,
            bool_or(byte_counters_tracked)                 AS byte_counters_tracked,
            CAST(SUM(coalesce(SUM(d_reads), 0)) OVER () AS bigint) AS window_total_reads,
            SUM(coalesce(SUM(d_read_time_ms), 0)) OVER ()          AS window_total_read_time_ms
        FROM differenced
        GROUP BY backend_type, object_type, context
        HAVING coalesce(SUM(d_reads), 0) + coalesce(SUM(d_writes), 0)
             + coalesce(SUM(d_extends), 0) + coalesce(SUM(d_hits), 0) > 0
        ORDER BY coalesce(SUM(d_read_time_ms), 0) DESC, coalesce(SUM(d_reads), 0) DESC
        LIMIT $4
""";
}
