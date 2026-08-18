/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Where the query-text fetch lands statement text (#2150): one row per
/// <c>(server_id, database_name, query_id)</c>, with the text stored directly.
///
/// <para><b>Why this table exists.</b> The runtime-stats payload used to carry <c>query_sql_text</c>
/// inside a <c>TOP ... WITH TIES ... ORDER BY last_execution_time</c> projection, and a Top-N Sort carries
/// every output column through the sort while reading ALL of its input before emitting row one — so
/// choosing the rows to ship materialized <c>nvarchar(max)</c> text for the entire qualifying set.
/// Measured with #2210's plan XML already gone and that column as the only difference: time-to-first-row
/// 4.67s against 0.45s at 1,505 rows, 5.02s against 0.57s at 4,037.</para>
///
/// <para><b>Deliberately NOT modeled on <see cref="QueryStorePlanMap"/>, and this is the design choice
/// worth understanding before extending it.</b> The plan side is a MAP into a content-addressed
/// <c>query_plan_dim</c>, with a digest per row and a liveness interlock so the dimension GC cannot delete
/// content that live facts still reference. That machinery is bought by plan XML being enormous and
/// heavily duplicated across plans. Query text is neither: Query Store has already de-duplicated it, one
/// row per distinct statement per database, so there is nothing left to squeeze — and storing it inline
/// removes the interlock entirely. That matters because the interlock's failure mode is text that is
/// silently missing, which no reader can distinguish from a statement that never had text.</para>
///
/// <para>PostgreSQL TOASTs the column transparently, the same property that made storing plan text
/// acceptable on this store in the first place.</para>
/// </summary>
public static class QueryStoreTextStore
{
    public const string TableName = "collect.query_store_text";

    /// <summary>
    /// The liveness column. This table is a keyed store rather than a time series — it has a PRIMARY KEY
    /// and no time dimension to partition on — so it is pruned on this column rather than by
    /// <c>drop_chunks</c>, exactly as <see cref="QueryStorePlanMap"/> is.
    /// </summary>
    public const string LastSeenColumn = "last_seen";

    /// <summary>
    /// Days ADDED to the widest fact retention before text is eligible to go.
    ///
    /// <para>Added rather than subtracted, and the direction is the whole point: text must outlive the rows
    /// that reference it. Retiring it early leaves facts whose statement reads as absent — a query with no
    /// text at all, which is worse than a stale one because nothing distinguishes it from a statement that
    /// never had text. The cost of being late is a few rows of text nobody reads.</para>
    /// </summary>
    public const int PruneMarginDays = 2;

    public const string CreateTableSql = @"CREATE TABLE IF NOT EXISTS collect.query_store_text (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    query_id bigint NOT NULL,
    query_sql_text text,
    last_seen timestamp NOT NULL,
    PRIMARY KEY (server_id, database_name, query_id)
);
CREATE INDEX IF NOT EXISTS idx_query_store_text_last_seen
    ON collect.query_store_text(last_seen);";

    /// <summary>
    /// Records what a text fetch landed.
    ///
    /// <para><b>The conflict arm overwrites the TEXT, not just the stamp</b>, and that is load-bearing
    /// rather than defensive. <c>query_id</c> is unique within a database only until Query Store is reset:
    /// a reset renumbers from the start, so id 5 afterwards is a DIFFERENT statement than id 5 before. The
    /// <see cref="TouchAndProbeSql"/> hash comparison is what brings us back to re-read it (#2312 — the
    /// retired watermark's daily expiry used to, eventually), and this is where the corrected text has to
    /// land. Touching only <c>last_seen</c> would leave the old statement's text attached to the new id
    /// forever, which reads as a plausible wrong answer rather than as missing data.</para>
    ///
    /// <para><c>ORDER BY</c> on the conflict key because concurrent batches that touch overlapping keys in
    /// different orders deadlock (#1801) — the same reason the plan map's upsert carries one. The
    /// <c>WHERE EXCLUDED.last_seen &gt;=</c> guard keeps the stamp monotonic so an out-of-order write
    /// cannot age a row backwards into the prune's reach. <c>query_hash</c> takes EXCLUDED when the fetch
    /// carried one and keeps the stored value otherwise — never replace knowledge with absence.</para>
    /// </summary>
    public const string UpsertSql = @"INSERT INTO collect.query_store_text
    (server_id, database_name, query_id, query_sql_text, query_hash, last_seen)
SELECT server_id, database_name, query_id, query_sql_text, query_hash, stamped
FROM unnest($1::integer[], $2::text[], $3::bigint[], $4::text[], $5::text[], $6::timestamp[])
     AS batch(server_id, database_name, query_id, query_sql_text, query_hash, stamped)
ORDER BY server_id, database_name, query_id
ON CONFLICT (server_id, database_name, query_id) DO UPDATE SET
    query_sql_text = EXCLUDED.query_sql_text,
    query_hash = COALESCE(EXCLUDED.query_hash, query_store_text.query_hash),
    last_seen = EXCLUDED.last_seen
WHERE EXCLUDED.last_seen >= query_store_text.last_seen";

    /// <summary>
    /// The text side's liveness touch and missing-set probe (#2312), the single-table sibling of
    /// <see cref="QueryStorePlanMap.TouchAndProbeSql"/>: refresh <c>last_seen</c> for every statement the
    /// cycle's batch references (hourly-guarded, same write-amplification argument), adopt the batch's
    /// <c>query_hash</c> where the stored one is NULL (legacy rows from before the column existed), and
    /// return per batch row whether the store already holds the text and whether the stored hash still
    /// matches the live one. <c>hash_stale</c> is the Query Store RESET detector: ids renumber, so id 5
    /// carrying a different hash means it now names a different statement and its text must be refetched —
    /// per-id, within one cycle, where the retired watermark design re-walked the whole catalog daily to
    /// eventually notice.
    /// </summary>
    public const string TouchAndProbeSql = @"WITH touched AS (
    SELECT t.server_id, t.database_name, t.query_id, batch.query_hash AS live_hash
    FROM collect.query_store_text AS t
    JOIN unnest($1::integer[], $2::text[], $3::bigint[], $4::text[])
         AS batch(server_id, database_name, query_id, query_hash)
      ON  batch.server_id = t.server_id
      AND batch.database_name = t.database_name
      AND batch.query_id = t.query_id
    WHERE t.last_seen < $5::timestamp - interval '1 hour'
    ORDER BY t.server_id, t.database_name, t.query_id
),
text_touch AS (
    UPDATE collect.query_store_text AS t
    SET last_seen = $5::timestamp,
        query_hash = COALESCE(t.query_hash, x.live_hash)
    FROM touched AS x
    WHERE t.server_id = x.server_id
      AND t.database_name = x.database_name
      AND t.query_id = x.query_id
    RETURNING t.query_id
)
SELECT
    batch.server_id,
    batch.database_name,
    batch.query_id,
    (t.query_id IS NOT NULL) AS resolved,
    (t.query_id IS NOT NULL AND t.query_hash IS NOT NULL AND batch.query_hash IS NOT NULL
        AND t.query_hash <> batch.query_hash) AS hash_stale
FROM unnest($1::integer[], $2::text[], $3::bigint[], $4::text[])
     AS batch(server_id, database_name, query_id, query_hash)
LEFT JOIN collect.query_store_text AS t
       ON  t.server_id = batch.server_id
       AND t.database_name = batch.database_name
       AND t.query_id = batch.query_id
ORDER BY batch.server_id, batch.database_name, batch.query_id";

    /// <summary>
    /// Retires text whose facts have all aged out, bounded to roughly one chunk-width of the oldest rows
    /// per call so a single sweep cannot take an unbounded row lock — the same shape and the same reason as
    /// <see cref="QueryStorePlanMap.PruneSql"/>.
    ///
    /// <para>Safe to run against live data because <c>last_seen</c> is refreshed by every pass that
    /// re-observes a statement: a row can only fall behind the cutoff once nothing has referenced it for
    /// the retention window, and re-fetching text for a statement that comes back is one row through a
    /// watermark that has already expired.</para>
    /// </summary>
    public static string PruneSql(int chunkIntervalDays) =>
        "DELETE FROM collect.query_store_text WHERE " + LastSeenColumn + " < $1" +
        " AND " + LastSeenColumn + " >= (SELECT min(" + LastSeenColumn + ") FROM collect.query_store_text WHERE " +
        LastSeenColumn + " < $1)" +
        " AND " + LastSeenColumn + " < (SELECT min(" + LastSeenColumn + ") FROM collect.query_store_text WHERE " +
        LastSeenColumn + " < $1) + INTERVAL '" +
        chunkIntervalDays.ToString(CultureInfo.InvariantCulture) + " days'";
}
