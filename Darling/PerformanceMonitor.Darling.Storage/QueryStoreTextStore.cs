/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System.Collections.Generic;

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
    /// The text row's own liveness column. This table is a keyed store rather than a time series — it has a PRIMARY KEY
    /// and no time dimension to partition on — so it is pruned on liveness rather than by <c>drop_chunks</c>, exactly
    /// as <see cref="QueryStorePlanMap"/> is. The stamp the touch keeps current is
    /// <see cref="QueryStoreTextSeen"/>'s; this column moves on every upsert and once per
    /// <see cref="QueryStoreTextSeen.TextRowRefreshHours"/> hours on a touch, and a text's effective stamp is the later
    /// of the two.
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

    /// <summary>
    /// The collectors whose fact rows carry a <c>query_id</c> this table resolves to text. The prune horizon is the widest
    /// effective retention among them (<c>DarlingRetention.WidestEffectiveRetentionDays</c>), so a retention raised on any
    /// one of them keeps the text its facts point at for as long as those facts live.
    /// </summary>
    public static readonly IReadOnlyList<string> ReferencingCollectors = new[] { "query_store", "query_store_forced_plans", "query_store_hints" };

    /* This const mirrors the IMMUTABLE V74 migration rung and stays byte-frozen with it. The live table differs
       in two ways. V149 (#4250) drops the last_seen btree index and sets fillfactor 90, so an update of last_seen
       changes no indexed column and stays heap-only where the page has room. V281 (#9043) keeps the liveness stamp
       the touch moves every cycle in collect.query_store_text_seen (QueryStoreTextSeen), because the pages of a busy
       store are full: an update that is not heap-only writes a new copy of the whole text row and every trigram of
       it into the GIN index. This table's own last_seen is written by the upsert and by RefreshTextRowSql below,
       and the prune reads the seen table (QueryStoreTextSeen.PruneSql), so nothing scans this heap for last_seen. */
    public const string CreateTableSql = @"CREATE TABLE IF NOT EXISTS collect.query_store_text (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    query_id bigint NOT NULL,
    query_sql_text text,
    last_seen timestamp NOT NULL,
    PRIMARY KEY (server_id, database_name, query_id)
);";

    /// <summary>
    /// Records what a text fetch landed.
    ///
    /// <para><b>The conflict arm overwrites the TEXT, not just the stamp</b>, and that is load-bearing
    /// rather than defensive. <c>query_id</c> is unique within a database only until Query Store is reset:
    /// a reset renumbers from the start, so id 5 afterwards is a DIFFERENT statement than id 5 before. The
    /// <see cref="ProbeSql"/> hash comparison is what brings us back to re-read it (#2312 — the
    /// retired watermark's daily expiry used to, eventually), and this is where the corrected text has to
    /// land. Touching only <c>last_seen</c> would leave the old statement's text attached to the new id
    /// forever, which reads as a plausible wrong answer rather than as missing data.</para>
    ///
    /// <para><c>ORDER BY</c> on the conflict key because concurrent batches that touch overlapping keys in
    /// different orders deadlock (#1801) — the same reason the plan map's upsert carries one. The
    /// <c>WHERE EXCLUDED.last_seen &gt;=</c> guard keeps the stamp monotonic so an out-of-order write
    /// cannot age a row backwards into the prune's reach. <c>query_hash</c> takes EXCLUDED when the fetch
    /// carried one and keeps the stored value otherwise — never replace knowledge with absence.</para>
    ///
    /// <para>The seen row of each inserted text row is written by the table's statement trigger
    /// (<see cref="QueryStoreTextSeen.TriggerSql"/>) after this statement's rows, so the text rows are locked
    /// first and the seen rows second, both in key order.</para>
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
    /// The first statement of the text side's touch (#2312, #9043): refreshes the text row's own <c>last_seen</c> for
    /// every statement the cycle's batch references whose stamp is more than
    /// <see cref="QueryStoreTextSeen.TextRowRefreshInterval"/> behind, and adopts the batch's <c>query_hash</c> where the
    /// stored one is NULL (rows stored without a hash).
    ///
    /// <para>The refresh exists for a rollback: a build that prunes on this column must find every live text inside its
    /// cutoff (see <see cref="QueryStoreTextSeen.TextRowRefreshHours"/> for the derivation). A text whose hash is stored
    /// and whose stamp is inside the interval is not written at all, which is every referenced text on most cycles.</para>
    ///
    /// <para>It runs before <see cref="TouchSeenSql"/> in one transaction
    /// (<see cref="QueryStoreFetchProbe.TouchAndProbeTextsAsync"/>), so the touch locks text rows first and seen rows
    /// second. Two statements, not two data-modifying CTEs of one, because PostgreSQL does not define the order those
    /// run in.</para>
    /// </summary>
    public static readonly string RefreshTextRowSql = @"WITH stale AS (
    SELECT t.server_id, t.database_name, t.query_id, batch.query_hash AS live_hash
    FROM collect.query_store_text AS t
    JOIN unnest($1::integer[], $2::text[], $3::bigint[], $4::text[])
         AS batch(server_id, database_name, query_id, query_hash)
      ON  batch.server_id = t.server_id
      AND batch.database_name = t.database_name
      AND batch.query_id = t.query_id
    WHERE t.last_seen < $5::timestamp - " + QueryStoreTextSeen.TextRowRefreshInterval + @"
       OR (t.query_hash IS NULL AND batch.query_hash IS NOT NULL)
    ORDER BY t.server_id, t.database_name, t.query_id
)
UPDATE collect.query_store_text AS t
SET last_seen = GREATEST(t.last_seen, $5::timestamp),
    query_hash = COALESCE(t.query_hash, x.live_hash)
FROM stale AS x
WHERE t.server_id = x.server_id
  AND t.database_name = x.database_name
  AND t.query_id = x.query_id";

    /// <summary>
    /// The second statement of the touch: the liveness stamp itself, on <see cref="QueryStoreTextSeen"/>. For every
    /// referenced statement the store holds text for, it writes <c>last_seen</c> when the seen row is more than
    /// <see cref="QueryStoreLivenessTouchGuard.GuardInterval"/> behind, and inserts the seen row when there is none (a
    /// text row written while the insert trigger was off).
    ///
    /// <para>The guard is the same width the plan side's two sites carry and written in exactly one place, for the same
    /// write-amplification argument: the horizons are multi-day, so one update per row per window is enough freshness.
    /// The width is derived from the SMALLER of the two prune margins — the text prune's
    /// <see cref="PruneMarginDays"/> is the wider of them, so the shared width is conservative here rather than merely
    /// adequate.</para>
    ///
    /// <para>The guard sits twice. In the select, because <c>ON CONFLICT DO UPDATE</c> locks every conflicting row
    /// whether or not its <c>WHERE</c> passes, and a row lock is a page write: filtering first means a seen row inside
    /// the guard is not touched at all. And on the conflict arm, for a row another session stamped between this
    /// statement's read and its write.</para>
    /// </summary>
    public static readonly string TouchSeenSql = @"INSERT INTO collect.query_store_text_seen AS s
    (server_id, database_name, query_id, last_seen)
SELECT t.server_id, t.database_name, t.query_id, $5::timestamp
FROM unnest($1::integer[], $2::text[], $3::bigint[], $4::text[])
     AS batch(server_id, database_name, query_id, query_hash)
JOIN collect.query_store_text AS t
  ON  t.server_id = batch.server_id
  AND t.database_name = batch.database_name
  AND t.query_id = batch.query_id
LEFT JOIN collect.query_store_text_seen AS cur
  ON  cur.server_id = batch.server_id
  AND cur.database_name = batch.database_name
  AND cur.query_id = batch.query_id
WHERE cur.query_id IS NULL
   OR cur.last_seen < $5::timestamp - " + QueryStoreLivenessTouchGuard.GuardInterval + @"
ORDER BY t.server_id, t.database_name, t.query_id
ON CONFLICT (server_id, database_name, query_id) DO UPDATE
SET last_seen = EXCLUDED.last_seen
WHERE s.last_seen < $5::timestamp - " + QueryStoreLivenessTouchGuard.GuardInterval;

    /// <summary>
    /// The third statement: the missing-set probe (#2312), the single-table sibling of
    /// <see cref="QueryStorePlanMap.TouchAndProbeSql"/>'s verdict. Per batch row, whether the store already holds the
    /// text and whether the stored hash still matches the live one. <c>hash_stale</c> is the Query Store RESET detector:
    /// ids renumber, so id 5 carrying a different hash means it now names a different statement and its text must be
    /// refetched, per id and within one cycle. A hash <see cref="RefreshTextRowSql"/> adopted in this pass equals the
    /// live one, so it reads as current.
    /// </summary>
    public const string ProbeSql = @"SELECT
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

    /* The prune retires text whose facts have all aged out, and it is QueryStoreTextSeen.PruneSql: one sequential scan
       of the narrow seen table, a primary-key probe of the text row for each seen row behind the cutoff, and a
       ctid delete, row-capped at DarlingRetention.LivenessTouchedTablePruneRowCap with no ORDER BY (see
       DarlingRetention.UnorderedRowCappedDeleteSql for why a fixed cutoff needs none). The cap's sizing comes from
       the field's last_seen age histogram: this table's rows retire up to ~32 days old, ~150k-220k rows/day.

       Safe to run against live data because a text is retired only when BOTH stamps are behind the cutoff, and
       every pass that re-observes a statement refreshes the seen stamp, and the text row's own once it trails by
       QueryStoreTextSeen.TextRowRefreshHours: a row can only fall behind the cutoff once nothing has referenced it
       for the retention window, and re-fetching text for a statement that comes back is one row through the probe. */
}
