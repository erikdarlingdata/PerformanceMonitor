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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// <c>collect.oversized_plan_backlog</c> — the store side of #3392: which cached plans
/// <see cref="QueryPlanXmlCaptureLimits"/> declined to capture, and the content once a later
/// out-of-band fetch has retrieved it.
///
/// <para><b>Why a table and not a retry flag.</b> The cap is a per-row CONTENT decision, so the same plan
/// ships NULL every cycle it recurs — nothing about the next cycle makes it smaller. And a capped row
/// produces no plan-dimension row at all, so there is no content, no digest and nothing in the store to
/// point at. The only way back is the cache coordinates, which is what these rows hold.</para>
///
/// <para><b>Identity is the row identity, not the plan's.</b> The key is
/// <c>(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset)</c>
/// — the full <c>sys.dm_exec_query_stats</c> row identity, for the reason that collector's own delta key
/// records: keying on <c>plan_handle</c> alone cross-contaminates multi-statement plans, where one handle
/// carries many statements at different offsets. A re-sighting touches <c>last_seen_at</c>; it does not
/// insert a second row and does not re-fetch content already held.</para>
///
/// <para><b>Growth is bounded twice: by the horizon, and by what the sweep can reach.</b> Retention prunes on
/// <c>last_seen_at</c> (<c>DarlingRetention</c>), so a plan that stops recurring leaves with the fact rows
/// it explained rather than accumulating forever. That horizon alone does not hold the table down, because
/// arrival is not uniformly a trickle. One production store class presents a handful of long-lived cache
/// residents per server: 13 plans over the cap at once on its outlier server, compile ages from 13.6 hours
/// to 144.6 days. Another re-keys the same logical queries under fresh <c>plan_handle</c> and offsets as the
/// plan cache churns, and those rows arrive many times faster than the sweep's claim
/// (<c>OversizedPlanBacklogSweep.MaxPlansPerServerPerTick</c>) drains them. So each pass first drops, unfetched,
/// the never-tried rows no claim is going to serve (#8567): every row last sighted before the newest sighting
/// of its query (<see cref="SupersedeSql"/>), then the least recently sighted rows past the pending cap
/// (<see cref="DropOverCapSql"/>). A dropped row is deleted by the statement that drops it, which adds it to that hour's
/// count in <c>collect.oversized_plan_backlog_drops</c> (<see cref="CreateDropCountsTableSql"/>, #8691), so the read counts the
/// day's drops from a few rows per server instead of holding millions of rows for a day to count them.</para>
///
/// <para><b>Two indexes: the primary key, and one for the purge.</b> The key's leading column is
/// <c>server_id</c>, which is the leading predicate of every access path — the sweep's trim and claim, the
/// read-surface fallback and the sighting upsert — so each one is served or is a scan of one server's rows, which
/// the trim holds to the pending cap plus what the sweep itself stamped inside the horizon.
/// <c>idx_oversized_plan_backlog_dropped</c> (V258) is on <c>(server_id, expired_at)</c> and partial to the rows
/// dropped unfetched, so <see cref="PurgeDroppedSql"/> reads the rows it deletes and not the server's every row. Since #8691 no
/// build of this one leaves such a row: the index serves the purge of the rows an older build dropped and held.
/// It names no column a re-sighting changes on a waiting row, so the upsert stays a heap-only update. An index on
/// <c>last_seen_at</c> would end that for every re-sighting, which is why retention's daily DELETE has none. An
/// index chosen for an ORDER BY nobody has measured would be a guess; the ORDER BYs below name the index to add if
/// it ever stops being one.</para>
///
/// <para><b>Not a collector table.</b> It is absent from <see cref="CollectorCatalog"/>, so
/// <see cref="TimescaleSupport"/>'s catalog-driven hypertable conversion and
/// <c>DarlingRetention</c>'s catalog purge never reach it — the <c>collect.store_metrics</c> precedent. It
/// needs neither chunks nor compression, and a hypertable could not carry this primary key anyway (a
/// hypertable's unique constraint must include the partitioning column).</para>
/// </summary>
public static class OversizedPlanBacklog
{
    /// <summary>
    /// The command deadline for this table's own store statements — its own constant rather than
    /// <see cref="StorageCommandDeadlines.McpReadSeconds"/>, whose regime is an interactive MCP read, and
    /// rather than an inherited Npgsql default, which is the defect class that constant exists to close.
    ///
    /// <para>The outcome statements and the sighting upsert are primary-key-matched single rows, so 30 s is
    /// orders of magnitude above the work. The one of those with real work is the capture UPDATE, which writes
    /// the fetched plan XML — megabytes of TOAST for the tail this exists to reach — and nothing encloses it:
    /// the sweep's per-plan budget bounds the TARGET fetch, and abandoning the store write afterwards would
    /// discard content already paid for. So the deadline is set low enough that a stalled write is reported
    /// rather than held, and high enough that the largest plan measured on the fleet (8.7 MB) is not close to
    /// it. The claim and the trim statements read one server's rows and the trim writes at most
    /// <c>OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick</c> narrow rows a statement, which is what
    /// keeps them under the same deadline on a server whose backlog is far over the cap.</para>
    /// </summary>
    public const int CommandTimeoutSeconds = 30;

    /// <summary>The table, schema-qualified — the migrate session's <c>search_path</c> puts <c>collect</c>
    /// first, but every site here is explicit so a read from a differently-configured session cannot land
    /// somewhere else.</summary>
    public const string TableName = "collect.oversized_plan_backlog";

    /// <summary>
    /// The two <c>collector_name</c> values this table ever holds — the collectors that capture cached-plan
    /// XML and therefore have a cap to exceed. Named rather than left implicit so the read-surface fallbacks
    /// can filter on the right one without a literal per call site, and so a pin can check the set against
    /// the collectors that actually describe observations.
    /// </summary>
    public const string QueryStatsCollectorName = "query_stats";

    /// <summary>See <see cref="QueryStatsCollectorName"/>.</summary>
    public const string ProcedureStatsCollectorName = "procedure_stats";

    /// <summary>
    /// The DDL, owned here rather than written into the migration rung as a second copy: <c>PgMigrations</c>
    /// concatenates this constant into its rung, the V38 idiom, so the shape the store gets and the shape the
    /// statements below address cannot drift.
    ///
    /// <para><c>attempt_count</c> and <c>last_attempt_at</c> are not bookkeeping. Ranking the claim by
    /// <c>last_attempt_at</c> makes the tried rows a round-robin: a row that was just tried goes to the back of
    /// the tried rows, and the tried rows hold at most <see cref="TriedRowsPerClaim"/> of a claim's slots, so
    /// one plan that can never be fetched cannot keep every never-tried row out (it does keep a slot's turn,
    /// every <c>ceil(tried rows / share)</c> passes, for as long as it fails: a failure is not counted). The
    /// count is the sweep's retirement limit: it grows only when a plan that had the whole judging budget to
    /// itself overran it (#5367 review, A-M1), so a connect failure or an expiry does not move it.</para>
    /// </summary>
    public const string CreateTableSql = @"
CREATE TABLE IF NOT EXISTS collect.oversized_plan_backlog
(
    server_id integer NOT NULL,
    collector_name text NOT NULL,
    plan_handle text NOT NULL,
    sql_handle text NOT NULL,
    statement_start_offset integer NOT NULL,
    statement_end_offset integer NOT NULL,
    database_name text,
    query_hash text,
    observed_bytes bigint NOT NULL,
    first_seen_at timestamp NOT NULL,
    last_seen_at timestamp NOT NULL,
    plan_xml text,
    captured_at timestamp,
    expired_at timestamp,
    last_attempt_at timestamp,
    attempt_count integer NOT NULL DEFAULT 0,
    CONSTRAINT pk_oversized_plan_backlog PRIMARY KEY
    (
        server_id,
        collector_name,
        plan_handle,
        sql_handle,
        statement_start_offset,
        statement_end_offset
    )
);";

    /// <summary>#8691: the per-hour drop counts, schema-qualified for the reason <see cref="TableName"/> is.</summary>
    public const string DropCountsTableName = "collect.oversized_plan_backlog_drops";

    /// <summary>
    /// #8691: the DDL of the drop counts, owned here for the reason <see cref="CreateTableSql"/> is: the V264 rung concatenates it.
    ///
    /// <para>One row per server, collector and UTC hour. <c>rows_dropped</c> is how many rows the trim dropped in that hour, and
    /// <c>rows_first_seen</c> is how many of the dropped rows were first sighted in that hour, which is what keeps a row the trim
    /// deleted in the read's <c>rows_added_last_day</c>. The trim writes both in the statement that deletes the rows
    /// (<see cref="SupersedeSql"/>, <see cref="DropOverCapSql"/>), and <see cref="PurgeDropCountsSql"/> removes the hours before the
    /// day the read counts, so the table holds about a day of hours per server and collector. The primary key serves the read's
    /// sums and the purge.</para>
    ///
    /// <para>The rows the trim used to keep for that day were 90 percent of the backlog table on a field store (5.17 million of 5.75
    /// million), and every read of the backlog scanned them only to count them.</para>
    /// </summary>
    public const string CreateDropCountsTableSql = @"
CREATE TABLE IF NOT EXISTS collect.oversized_plan_backlog_drops
(
    server_id integer NOT NULL,
    collector_name text NOT NULL,
    hour_start timestamp NOT NULL,
    rows_dropped bigint NOT NULL DEFAULT 0,
    rows_first_seen bigint NOT NULL DEFAULT 0,
    CONSTRAINT pk_oversized_plan_backlog_drops PRIMARY KEY
    (
        server_id,
        collector_name,
        hour_start
    )
);";

    /// <summary>
    /// #8789: the <c>collector_name</c> under which <see cref="RecordSightingsSql"/> keeps, in <c>collect.collector_state</c>, how many
    /// new rows its latest cycle left out (state key: the collector, state value: the number). No migration: the table is the
    /// store's small per-server state, and a row here is one short number.
    /// </summary>
    public const string LeftOutStateOwner = "oversized_plan_backlog";

    /// <summary>
    /// #8789: the sweep's once-per-pass step that takes what <see cref="RecordSightingsSql"/> recorded as left out. $1 is the server
    /// and $2 the pass's clock. It deletes the server's numbers (so the next cycle starts a new one, and the pass counts what it
    /// took once), adds each to the hour of $2 in <c>collect.oversized_plan_backlog_drops</c> as <c>rows_dropped</c> and
    /// <c>rows_first_seen</c>, the two columns the trim's own counting fills, and returns the total. One statement, so a number is
    /// never deleted without being counted. A row the trim would have dropped in this pass is counted here as the trim counted it,
    /// once per pass.
    /// </summary>
    public const string TakeLeftOutSql = @"
WITH taken AS
(
    DELETE FROM collect.collector_state
    WHERE server_id = $1
    AND   collector_name = '" + LeftOutStateOwner + @"'
    RETURNING
        state_key,
        state_value::bigint AS n
),
counted AS
(
    INSERT INTO collect.oversized_plan_backlog_drops AS d
    (server_id, collector_name, hour_start, rows_dropped, rows_first_seen)
    SELECT
        $1,
        t.state_key,
        date_trunc('hour', $2::timestamp),
        t.n,
        t.n
    FROM taken AS t
    WHERE t.n > 0
    ON CONFLICT ON CONSTRAINT pk_oversized_plan_backlog_drops DO UPDATE SET
        rows_dropped = d.rows_dropped + EXCLUDED.rows_dropped,
        rows_first_seen = d.rows_first_seen + EXCLUDED.rows_first_seen
)
SELECT COALESCE(sum(t.n), 0)::integer
FROM taken AS t;";

    /// <summary>
    /// The primary key as a bound predicate, written once so the three statements that stamp one row's
    /// outcome address exactly the row the claim handed back.
    /// </summary>
    private const string KeyPredicate = @"
WHERE server_id = $1
AND   collector_name = $2
AND   plan_handle = $3
AND   sql_handle = $4
AND   statement_start_offset = $5
AND   statement_end_offset = $6";

    /// <summary>
    /// The order the pending cap ranks rows in, written once for <see cref="DropOverCapSql"/> and for the cap's simulation in
    /// <see cref="RecordSightingsSql"/>, so the two cannot drift. Most recently sighted first. #8789: among rows sighted at the same
    /// instant, the one that has waited longest (the oldest <c>first_seen_at</c>) ranks first. Rows tied on <c>last_seen_at</c> were all seen
    /// in the same pass, so which of them survives is arbitrary, and ranking the newest first made the survivors the ones the table
    /// deleted last time: a row the cap dropped came back on the next sighting with a fresh <c>first_seen_at</c>, ranked above the
    /// rows that stayed, and pushed one of them out. Every pass then deleted and inserted the same number of rows and reset their
    /// waiting time. Ranking the longest wait first lets a row that is in the table keep its place.
    /// </summary>
    private const string PendingRankOrder = @"last_seen_at DESC,
                first_seen_at ASC,
                observed_bytes DESC,
                collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset";

    /// <summary>
    /// The key of an incoming row of <see cref="RecordSightingsSql"/> (alias <c>i</c>) matched to the same key on the alias
    /// <c>{0}</c>, written once.
    /// </summary>
    private const string SameKeyAs = @"{0}.plan_handle = i.plan_handle
      AND   {0}.sql_handle = i.sql_handle
      AND   {0}.statement_start_offset = i.statement_start_offset
      AND   {0}.statement_end_offset = i.statement_end_offset";

    private static string SameKey(string alias) => SameKeyAs.Replace("{0}", alias, StringComparison.Ordinal);

    /// <summary>
    /// The sightings of one collection cycle for one server, as ONE statement and so one transaction. $1 is the server, $2 the
    /// collector, $3 to $9 the sightings as parallel arrays (plan handle, sql handle, start offset, end offset, database,
    /// query hash, observed bytes), $10 the sighting time, $11 the pending cap and $12 the most rows a trim drops in one pass
    /// (<c>OversizedPlanBacklogSweep.PendingRowsPerServerCap</c> and <c>MaxRowsDroppedPerServerPerTick</c>). It returns how many
    /// new rows it did not insert, see below.
    ///
    /// <para><b>One upsert, as before.</b> <c>first_seen_at</c> and <c>last_seen_at</c> both take $10 on the insert, so a first
    /// sighting reads as seen-once rather than as a row with an unknown age. A key given twice in one batch ends as the two
    /// statements it used to be: the later observed bytes, and the later database and query hash that is not null.</para>
    ///
    /// <para><b><c>expired_at</c> is CLEARED on a re-sighting, and that is load-bearing.</b> A collector only
    /// sees a row while its plan is in cache, so a fresh sighting is positive evidence the handle resolves —
    /// which retires whatever made the previous fetch come back empty. Without the clear, one transient miss
    /// would retire the row permanently, recreating the blind spot this table exists to close.</para>
    ///
    /// <para><c>captured_at</c> and <c>plan_xml</c> are deliberately left alone: the key pins the handle AND
    /// the offsets, so a row already captured describes the same document and there is nothing to go back
    /// for.</para>
    ///
    /// <para><b>A new row the trim would drop in the same pass is not inserted (#8789).</b> The statement runs the trim's two
    /// rules (<see cref="SupersedeSql"/>, then <see cref="DropOverCapSql"/>) over the rows the table would hold after this
    /// write, and leaves out the new rows the cap would drop. Inserting them only to have the next trim delete them
    /// is what churned the table. The statement leaves a row in, and the trim
    /// handles it as it always did, when leaving it out could change what the trim does to another row: when the
    /// supersede or the cap would stop at the pass's drop limit, and when the row is the newest sighting of a query
    /// whose older rows only it supersedes. It does none of this unless the server's pending rows plus the new ones are
    /// past the cap, so a server under the cap pays one count.</para>
    ///
    /// <para><b>How many it left out is recorded, not counted.</b> The trim counted an over-cap plan once per sweep pass. A
    /// collector cycle runs far more often than a pass (every minute on the Balanced preset, a pass every 15 minutes), and the
    /// same plans are left out again each cycle, so adding to <c>collect.oversized_plan_backlog_drops</c> here would read about
    /// 15 times too high, and the drops would never reach the sweep's run record. This statement instead overwrites one number
    /// per server and collector in <c>collect.collector_state</c> (<see cref="LeftOutStateOwner"/>, state key the collector): the
    /// rows it left out on its LATEST cycle. It writes 0 when a cycle leaves none out and the number was not 0, so a server
    /// that drops back under the cap does not leave a stale number. The sweep takes the number once per pass
    /// (<see cref="TakeLeftOutSql"/>).</para>
    /// </summary>
    public static readonly string RecordSightingsSql = @"
WITH incoming AS
(
    SELECT
        u.plan_handle,
        u.sql_handle,
        u.statement_start_offset,
        u.statement_end_offset,
        (array_agg(u.database_name ORDER BY u.ord DESC) FILTER (WHERE u.database_name IS NOT NULL))[1] AS database_name,
        (array_agg(u.query_hash ORDER BY u.ord DESC) FILTER (WHERE u.query_hash IS NOT NULL))[1] AS query_hash,
        (array_agg(u.observed_bytes ORDER BY u.ord DESC))[1] AS observed_bytes
    FROM unnest($3::text[], $4::text[], $5::integer[], $6::integer[], $7::text[], $8::text[], $9::bigint[])
        WITH ORDINALITY AS u
        (plan_handle, sql_handle, statement_start_offset, statement_end_offset, database_name, query_hash, observed_bytes, ord)
    GROUP BY
        u.plan_handle,
        u.sql_handle,
        u.statement_start_offset,
        u.statement_end_offset
),
existing AS
(
    SELECT b.*
    FROM incoming AS i
    JOIN collect.oversized_plan_backlog AS b
      ON  b.server_id = $1
      AND b.collector_name = $2
      AND " + SameKey("b") + @"
),
gate AS
(
    SELECT
        (
            (
                SELECT count(*)
                FROM collect.oversized_plan_backlog
                WHERE server_id = $1
                AND   " + NeverTriedPendingPredicate + @"
            )
            +
            (
                SELECT count(*)
                FROM incoming AS i
                WHERE NOT EXISTS (SELECT 1 FROM existing AS e WHERE " + SameKey("e") + @")
            )
        ) > $11 AS over_cap
),
pending_after AS
(
    SELECT
        t.collector_name,
        t.plan_handle,
        t.sql_handle,
        t.statement_start_offset,
        t.statement_end_offset,
        t.database_name,
        t.query_hash,
        t.last_seen_at,
        t.first_seen_at,
        t.observed_bytes,
        false AS is_new
    FROM collect.oversized_plan_backlog AS t
    WHERE (SELECT g.over_cap FROM gate AS g)
    AND   t.server_id = $1
    AND   " + NeverTriedPendingPredicate + @"
    AND   NOT
    (
        t.collector_name = $2
        AND EXISTS (SELECT 1 FROM incoming AS i WHERE " + SameKey("t") + @")
    )
    UNION ALL
    SELECT
        $2::text,
        i.plan_handle,
        i.sql_handle,
        i.statement_start_offset,
        i.statement_end_offset,
        COALESCE(i.database_name, e.database_name),
        COALESCE(i.query_hash, e.query_hash),
        $10::timestamp,
        COALESCE(e.first_seen_at, $10::timestamp),
        i.observed_bytes,
        e.plan_handle IS NULL
    FROM incoming AS i
    LEFT JOIN existing AS e
      ON " + SameKey("e") + @"
    WHERE (SELECT g.over_cap FROM gate AS g)
    AND   (e.plan_handle IS NULL OR (e.captured_at IS NULL AND e.last_attempt_at IS NULL))
),
superseded AS
(
    SELECT
        s.collector_name,
        s.plan_handle,
        s.sql_handle,
        s.statement_start_offset,
        s.statement_end_offset
    FROM
    (
        SELECT
            p.collector_name,
            p.plan_handle,
            p.sql_handle,
            p.statement_start_offset,
            p.statement_end_offset,
            p.last_seen_at,
            max(p.last_seen_at) OVER
            (
                PARTITION BY p.collector_name, p.database_name, p.query_hash
            ) AS newest_sighting_at
        FROM pending_after AS p
        WHERE p.query_hash IS NOT NULL
    ) AS s
    WHERE s.last_seen_at < s.newest_sighting_at
),
ranked AS
(
    SELECT
        r.*,
        r.is_new
        AND r.sighting_rank > $11
        AND r.total_rows - $11 <= $12
        AND (SELECT count(*) FROM superseded) < $12 AS would_drop
    FROM
    (
        SELECT
            p.*,
            row_number() OVER (ORDER BY " + PendingRankOrder + @") AS sighting_rank,
            count(*) OVER () AS total_rows
        FROM pending_after AS p
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM superseded AS s
            WHERE s.collector_name = p.collector_name
            AND   s.plan_handle = p.plan_handle
            AND   s.sql_handle = p.sql_handle
            AND   s.statement_start_offset = p.statement_start_offset
            AND   s.statement_end_offset = p.statement_end_offset
        )
    ) AS r
),
unsafe_queries AS
(
    SELECT
        q.collector_name,
        q.database_name,
        q.query_hash
    FROM
    (
        SELECT
            p.collector_name,
            p.database_name,
            p.query_hash,
            p.last_seen_at,
            COALESCE(r.would_drop, false) AS would_drop,
            max(p.last_seen_at) OVER
            (
                PARTITION BY p.collector_name, p.database_name, p.query_hash
            ) AS newest_sighting_at
        FROM pending_after AS p
        LEFT JOIN ranked AS r
          ON  r.collector_name = p.collector_name
          AND r.plan_handle = p.plan_handle
          AND r.sql_handle = p.sql_handle
          AND r.statement_start_offset = p.statement_start_offset
          AND r.statement_end_offset = p.statement_end_offset
        WHERE p.query_hash IS NOT NULL
    ) AS q
    GROUP BY
        q.collector_name,
        q.database_name,
        q.query_hash
    HAVING bool_and(q.would_drop) FILTER (WHERE q.last_seen_at = q.newest_sighting_at)
    AND    bool_or(q.last_seen_at < q.newest_sighting_at)
),
skipped AS
(
    SELECT
        r.plan_handle,
        r.sql_handle,
        r.statement_start_offset,
        r.statement_end_offset
    FROM ranked AS r
    WHERE r.would_drop
    AND   NOT EXISTS
    (
        SELECT 1
        FROM unsafe_queries AS u
        WHERE u.collector_name = r.collector_name
        AND   u.database_name IS NOT DISTINCT FROM r.database_name
        AND   u.query_hash = r.query_hash
    )
),
written AS
(
    INSERT INTO collect.oversized_plan_backlog AS b
    (server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
     database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
    SELECT
        $1, $2, i.plan_handle, i.sql_handle, i.statement_start_offset, i.statement_end_offset,
        i.database_name, i.query_hash, i.observed_bytes, $10, $10
    FROM incoming AS i
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM skipped AS s
        WHERE s.plan_handle = i.plan_handle
        AND   s.sql_handle = i.sql_handle
        AND   s.statement_start_offset = i.statement_start_offset
        AND   s.statement_end_offset = i.statement_end_offset
    )
    ON CONFLICT ON CONSTRAINT pk_oversized_plan_backlog DO UPDATE SET
        last_seen_at = EXCLUDED.last_seen_at,
        observed_bytes = EXCLUDED.observed_bytes,
        database_name = COALESCE(EXCLUDED.database_name, b.database_name),
        query_hash = COALESCE(EXCLUDED.query_hash, b.query_hash),
        expired_at = NULL
),
left_out AS
(
    INSERT INTO collect.collector_state AS s
    (server_id, collector_name, state_key, state_value, updated_at)
    SELECT
        $1,
        '" + LeftOutStateOwner + @"',
        $2,
        c.n::text,
        $10::timestamp
    FROM
    (
        SELECT count(*) AS n
        FROM skipped
    ) AS c
    WHERE c.n > 0
    OR    EXISTS
    (
        SELECT 1
        FROM collect.collector_state AS x
        WHERE x.server_id = $1
        AND   x.collector_name = '" + LeftOutStateOwner + @"'
        AND   x.state_key = $2
        AND   x.state_value <> '0'
    )
    ON CONFLICT (server_id, collector_name, state_key) DO UPDATE SET
        state_value = EXCLUDED.state_value,
        updated_at = EXCLUDED.updated_at
    WHERE s.state_value <> EXCLUDED.state_value
)
SELECT count(*)::integer
FROM skipped;";

    /// <summary>
    /// How many already-tried rows one claim of <paramref name="limit"/> rows may take while there are never-tried
    /// rows to take instead: half, rounded down (5 of 10). The other half is reserved for never-tried rows, so a
    /// backlog of tried rows whose fetch keeps failing cannot fill every claim and keep a new row out for good
    /// (#5367 review round 2, N2). When one side has fewer rows than its share, the other side takes the rest.
    /// </summary>
    public static int TriedRowsPerClaim(int limit) => limit / 2;

    /// <summary>
    /// The sweep's claim for one server: the rows with no content yet and no standing expiry, at most
    /// <paramref name="limit"/> of them, in the order the sweep works them: rows already tried first, oldest ATTEMPT
    /// first, then the rows never attempted, largest plan first. A never-tried row's size is what ranks it, because
    /// those are the plans the cap cost the most visibility on, which is the whole reason to go back for them.
    ///
    /// <para><b>Which rows make the cut (#5367).</b> Tried rows get at most <see cref="TriedRowsPerClaim"/> of the
    /// slots (oldest attempt first) and never-tried rows get the rest (largest first); a side that is short leaves
    /// its unused slots to the other. A row tried once used to sit behind every row never tried, so with new rows
    /// arriving each pass it could wait without end, and the sweep judges a tried row on a session of its own
    /// (<c>OversizedPlanBacklogSweep.SessionFor</c>) so that each claim of it makes progress. Each tried row whose
    /// fetch succeeds leaves in at most two claims after the one that tried it: judged, or a judge timeout counted
    /// and then the marker. A tried row whose fetch FAILS is not counted and never retires, so it stays in the
    /// tried queue: that queue is served round-robin by attempt age, <see cref="TriedRowsPerClaim"/> rows a pass,
    /// and the cap is what stops it from taking every slot.</para>
    ///
    /// <para>The limit is interpolated from the caller's own constant rather than written here, so the
    /// statement and the policy cannot disagree about how many plans one tick may fetch. It is never user
    /// input.</para>
    /// </summary>
    public static string ClaimSql(int limit)
    {
        var triedShare = TriedRowsPerClaim(limit).ToString(CultureInfo.InvariantCulture);
        var newShare = (limit - TriedRowsPerClaim(limit)).ToString(CultureInfo.InvariantCulture);
        var all = limit.ToString(CultureInfo.InvariantCulture);
        return @"
SELECT
    collector_name,
    plan_handle,
    sql_handle,
    statement_start_offset,
    statement_end_offset,
    database_name,
    observed_bytes,
    attempt_count,
    tried
FROM
(
    SELECT
        collector_name,
        plan_handle,
        sql_handle,
        statement_start_offset,
        statement_end_offset,
        database_name,
        observed_bytes,
        attempt_count,
        last_attempt_at,
        tried
    FROM
    (
        SELECT
            collector_name,
            plan_handle,
            sql_handle,
            statement_start_offset,
            statement_end_offset,
            database_name,
            observed_bytes,
            attempt_count,
            last_attempt_at,
            last_attempt_at IS NOT NULL AS tried,
            row_number() OVER
            (
                PARTITION BY (last_attempt_at IS NOT NULL)
                ORDER BY last_attempt_at ASC, observed_bytes DESC
            ) AS share_rank
        FROM collect.oversized_plan_backlog
        WHERE server_id = $1
        AND   captured_at IS NULL
        AND   expired_at IS NULL
    ) AS ranked
    ORDER BY
        (share_rank <= CASE WHEN tried THEN " + triedShare + " ELSE " + newShare + @" END) DESC,
        tried DESC,
        last_attempt_at ASC,
        observed_bytes DESC
    LIMIT " + all + @"
) AS claimed
ORDER BY
    tried DESC,
    last_attempt_at ASC,
    observed_bytes DESC;";
    }

    /// <summary>A fetch that returned the plan: store it, and retire any standing expiry.</summary>
    public const string RecordCaptureSql = @"
UPDATE collect.oversized_plan_backlog
SET plan_xml = $8,
    captured_at = $7,
    last_attempt_at = $7,
    attempt_count = attempt_count + 1,
    expired_at = NULL" + KeyPredicate + ";";

    /// <summary>
    /// A fetch that came back with nothing: the handle no longer renders a plan. Stamped rather than deleted
    /// — the row is the record that this plan was seen, measured and then lost, and a delete would let the
    /// next sighting present it as new.
    /// </summary>
    public const string RecordExpirySql = @"
UPDATE collect.oversized_plan_backlog
SET expired_at = $7,
    last_attempt_at = $7" + KeyPredicate + ";";

    /// <summary>
    /// A fetch that could not complete — connect failure, driver fault, its own budget, or a judging budget an
    /// earlier plan had already used. NOT an expiry: nothing was learned about the handle, so the row stays
    /// claimable and simply goes to the back of the tried rows. Not counted in <c>attempt_count</c>, which is the
    /// retirement limit's count of judge timeouts and nothing else (#5367 review, A-M1).
    /// </summary>
    public const string RecordFailureSql = @"
UPDATE collect.oversized_plan_backlog
SET last_attempt_at = $7" + KeyPredicate + ";";

    /// <summary>
    /// A plan that had the whole judging budget to itself and overran it: the one outcome that adds to
    /// <c>attempt_count</c>. The row stays claimable and goes to the back of the tried rows, until the count reaches
    /// the sweep's limit and the whole-plan marker is stored as the capture.
    /// </summary>
    public const string RecordAttemptSql = @"
UPDATE collect.oversized_plan_backlog
SET last_attempt_at = $7,
    attempt_count = attempt_count + 1" + KeyPredicate + ";";

    /// <summary>
    /// The key of one row, in the order every statement here lists it after <c>server_id</c>.
    /// </summary>
    private const string RowKeyColumns = @"collector_name,
        plan_handle,
        sql_handle,
        statement_start_offset,
        statement_end_offset";

    /// <summary>
    /// The rows a trim statement may drop: waiting, and never attempted. A row the sweep has tried stays in the
    /// claim's round-robin whatever arrives after it, so an attempt is never discarded unjudged, and it keeps
    /// <c>last_attempt_at</c>, which is how a row dropped unfetched is told from one a fetch expired.
    /// </summary>
    private const string NeverTriedPendingPredicate = @"captured_at IS NULL
        AND   expired_at IS NULL
        AND   last_attempt_at IS NULL";

    /// <summary>
    /// #8691: the head of both trim statements: the rows they drop are deleted, and the statement goes on to count them
    /// (<see cref="DeleteAndCountDroppedRows"/>). Until #8691 a drop stamped <c>expired_at</c> and the row stayed a day so the read
    /// could count it. A held row never kept a plan from coming back: a re-sighting cleared the stamp and the row waited again, so
    /// a deleted row's re-sighting inserts it again as a new pending row and the backlog ends the same, except that the row's first
    /// sighting is the new one.
    /// </summary>
    private const string DeleteDroppedRowsHead = @"
WITH gone AS
(
DELETE FROM collect.oversized_plan_backlog AS b
USING
(";

    /// <summary>
    /// #8691: the tail of both trim statements. Deletes the listed rows, only those of them still waiting untried, so a row the
    /// sweep attempted in between is left as it now is, and in the same statement adds them to the hour's counts
    /// (<see cref="CreateDropCountsTableSql"/>): every row to the hour of $2, the pass's clock, and each row first sighted at or
    /// after the hour $3 falls in to the hour of that first sighting. $3 is the start of the day the counts are kept for
    /// (<c>OversizedPlanBacklogSweep.DroppedRowHold</c> before $2): a row first sighted before it can no longer count in any read's
    /// day. One statement, so a drop is never deleted without being counted, nor counted without being deleted. It returns how many
    /// rows it dropped.
    /// </summary>
    private const string DeleteAndCountDroppedRows = @") AS dropped
WHERE b.server_id = $1
AND   b.collector_name = dropped.collector_name
AND   b.plan_handle = dropped.plan_handle
AND   b.sql_handle = dropped.sql_handle
AND   b.statement_start_offset = dropped.statement_start_offset
AND   b.statement_end_offset = dropped.statement_end_offset
AND   b.captured_at IS NULL
AND   b.expired_at IS NULL
AND   b.last_attempt_at IS NULL
RETURNING
    b.collector_name,
    b.first_seen_at
),
counted AS
(
INSERT INTO collect.oversized_plan_backlog_drops AS d
(server_id, collector_name, hour_start, rows_dropped, rows_first_seen)
SELECT
    $1,
    c.collector_name,
    c.hour_start,
    sum(c.rows_dropped),
    sum(c.rows_first_seen)
FROM
(
    SELECT
        g.collector_name,
        date_trunc('hour', $2::timestamp) AS hour_start,
        1 AS rows_dropped,
        0 AS rows_first_seen
    FROM gone AS g
    UNION ALL
    SELECT
        g.collector_name,
        date_trunc('hour', g.first_seen_at),
        0,
        1
    FROM gone AS g
    WHERE g.first_seen_at >= date_trunc('hour', $3::timestamp)
) AS c
GROUP BY
    c.collector_name,
    c.hour_start
ON CONFLICT ON CONSTRAINT pk_oversized_plan_backlog_drops DO UPDATE SET
    rows_dropped = d.rows_dropped + EXCLUDED.rows_dropped,
    rows_first_seen = d.rows_first_seen + EXCLUDED.rows_first_seen
)
SELECT count(*)::integer
FROM gone;";

    /// <summary>
    /// #8567: drops, unfetched, every never-tried pending row of one server ($1) that was last sighted before
    /// the newest sighting among the never-tried pending rows of its query, at most $4 rows, and counts them at $2's hour
    /// (#8691: <see cref="DeleteAndCountDroppedRows"/>, which also says what $3 is). Returns how many it dropped.
    ///
    /// <para><b>One query, many keys.</b> Plan-cache churn re-keys one query's plan under a fresh
    /// <c>plan_handle</c> every time it recompiles, and an ad hoc workload gives one <c>query_hash</c> a fresh
    /// <c>sql_handle</c> for every literal, so the key that makes a sighting an upsert admits a new row for a
    /// plan the table already waits on. The read surfaces serve the latest capture for a query
    /// (<see cref="QueryStatsFallbackSql"/> keys on <c>query_hash</c> and the database), so the rows of a
    /// query's latest sighting are all a fetch can add, and theirs are the handles most likely still in
    /// cache.</para>
    ///
    /// <para><b>Only a LATER sighting supersedes.</b> Rows of one query sighted in the same collection are plans
    /// that were in cache together, so all of them keep waiting and each is claimable. Keeping one of them would
    /// drop the others before every claim while the collector's next sighting returned them to pending, and
    /// they would read as waiting without ever being claimed.</para>
    ///
    /// <para>A row with no <c>query_hash</c> is left alone: the module-grain collector describes none, and its
    /// fallbacks key on the handles, so each such row is a plan of its own. A dropped row is not gone for good:
    /// a later sighting inserts it again (<see cref="RecordSightingsSql"/>) and it waits again, except that the sighting
    /// does not insert a new row this trim would drop at once (#8789).</para>
    ///
    /// <para>Oldest sighting first under the limit, so a server far over it sheds its stalest rows first and
    /// the rest on later passes.</para>
    /// </summary>
    public const string SupersedeSql = DeleteDroppedRowsHead + @"
    SELECT
        " + RowKeyColumns + @"
    FROM
    (
        SELECT
            " + RowKeyColumns + @",
            last_seen_at,
            max(last_seen_at) OVER
            (
                PARTITION BY collector_name, database_name, query_hash
            ) AS newest_sighting_at
        FROM collect.oversized_plan_backlog
        WHERE server_id = $1
        AND   query_hash IS NOT NULL
        AND   " + NeverTriedPendingPredicate + @"
    ) AS sighted
    WHERE last_seen_at < newest_sighting_at
    ORDER BY last_seen_at, " + RowKeyColumns + @"
    LIMIT $4
" + DeleteAndCountDroppedRows;

    /// <summary>
    /// #8567: drops, unfetched, the never-tried pending rows of one server ($1) past the pending cap ($4), least
    /// recently sighted first, at most $5 rows, and counts them at $2's hour (#8691: <see cref="DeleteAndCountDroppedRows"/>,
    /// which also says what $3 is). Returns how many it dropped. Rows sighted at the same instant rank the longest-waiting
    /// first (<see cref="PendingRankOrder"/>, #8789), so a row already in the table is not the one a new row replaces.
    ///
    /// <para>The claim takes never-tried rows largest first, so on a server whose distinct plans arrive faster
    /// than the claim drains them a small plan is never reached, and nothing but the retention horizon removed
    /// it. The cap is what the sweep's ceiling drains in one retention period
    /// (<c>OversizedPlanBacklogSweep.PendingRowsPerServerCap</c>). What is kept is the most recently sighted,
    /// because a collector only sees a row while its plan is in cache, so those are the handles most likely to
    /// still resolve.</para>
    /// </summary>
    public const string DropOverCapSql = DeleteDroppedRowsHead + @"
    SELECT
        " + RowKeyColumns + @"
    FROM
    (
        SELECT
            " + RowKeyColumns + @",
            row_number() OVER
            (
                ORDER BY " + PendingRankOrder + @"
            ) AS sighting_rank
        FROM collect.oversized_plan_backlog
        WHERE server_id = $1
        AND   " + NeverTriedPendingPredicate + @"
    ) AS ranked
    WHERE sighting_rank > $4
    ORDER BY sighting_rank DESC
    LIMIT $5
" + DeleteAndCountDroppedRows;

    /// <summary>
    /// #8567: removes one server's ($1) rows that were dropped unfetched before $2. Before #8691 a dropped row stayed in the
    /// table, held for the day the read counts it in (<c>OversizedPlanBacklogSweep.DroppedRowHold</c>). This build deletes a row
    /// when it drops it, so what this removes is the rows an older build dropped and held: those of an upgraded store, for a day
    /// after the upgrade, and those of an older build run against the store again. A row a fetch expired carries
    /// <c>last_attempt_at</c> and is not matched: it leaves on the retention horizon. These conditions imply the predicate of
    /// <c>idx_oversized_plan_backlog_dropped</c> (V258), which is what lets the statement read that index.
    /// </summary>
    public const string PurgeDroppedSql = @"
DELETE FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   captured_at IS NULL
AND   last_attempt_at IS NULL
AND   expired_at < $2;";

    /// <summary>
    /// #8691: removes every server's drop counts for the hours before the hour $1 falls in, $1 being the start of the day the
    /// counts are kept for (<c>OversizedPlanBacklogSweep.DroppedRowHold</c> before the pass's clock). The hour $1 falls in stays,
    /// because a read counts from the start of the hour its day starts in. Every server's rather than one's: the table holds about
    /// a day of hours per server and collector, and a server the sweep no longer reaches keeps no counts past their day.
    /// </summary>
    public const string PurgeDropCountsSql = @"
DELETE FROM collect.oversized_plan_backlog_drops
WHERE hour_start < date_trunc('hour', $1::timestamp);";

    /// <summary>
    /// The read-surface fallback for <c>query_stats</c>: the most recently captured over-cap plan for a
    /// (server, query_hash) — optionally narrowed to one database, the shape both the MCP read (database
    /// optional) and the viewer's (database required) need from one statement.
    ///
    /// <para><b>Read from the backlog, not joined to the fact table.</b> When this was written
    /// <c>query_stats</c> did not store the statement offsets — they were read for the delta key and never
    /// persisted — so a join from a stored fact row could only match on <c>plan_handle</c> + <c>sql_handle</c>,
    /// which for a multi-statement plan is several backlog rows describing DIFFERENT statements' plans.
    /// Serving one of those as "the plan for this query" is worse than serving nothing. V128 (#3540) stores
    /// the offsets, so an exact join is POSSIBLE for rows written since; it is not taken here because the
    /// readers ask at the <c>query_hash</c> grain (the Dashboard's and viewer's key), which
    /// <c>query_hash</c> on the backlog row already serves, and because every row written before V128
    /// carries NULL offsets, so an exact join would go dark on an upgraded store for a raw retention's
    /// worth of history. If a reader ever asks at the statement grain, the join is now available to
    /// it.</para>
    ///
    /// <para>A non-null <c>plan_xml</c> is itself the "this row was capped" test the caller would otherwise
    /// make against <c>query_plan_xml_bytes</c>: the row exists only because the measurement exceeded the
    /// cap.</para>
    /// </summary>
    public const string QueryStatsFallbackSql = @"
SELECT plan_xml
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   collector_name = 'query_stats'
AND   query_hash = $2
AND   ($3::text IS NULL OR database_name = $3)
AND   plan_xml IS NOT NULL
ORDER BY captured_at DESC
LIMIT 1;";

    /// <summary>
    /// The read-surface fallback for <c>procedure_stats</c> keyed on <c>sql_handle</c>, matching that
    /// collector's MCP read.
    /// </summary>
    public const string ProcedureStatsFallbackBySqlHandleSql = @"
SELECT plan_xml
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   collector_name = 'procedure_stats'
AND   sql_handle = $2
AND   plan_xml IS NOT NULL
ORDER BY captured_at DESC
LIMIT 1;";

    /// <summary>
    /// The read-surface fallback for <c>procedure_stats</c> keyed on the OBJECT, matching the viewer's Top
    /// Procedures grid, which groups by (database, schema, object) and never sees a handle.
    ///
    /// <para>This one CAN join the fact table exactly, unlike its <c>query_stats</c> sibling: the three
    /// module-level DMVs aggregate at the whole-object grain and expose no offsets, so this collector's plan
    /// fetch passes fixed literals and every backlog row for it carries that same pair. Joining on
    /// <c>plan_handle</c> + <c>sql_handle</c> + those offsets is therefore the full key, not a prefix of
    /// it.</para>
    /// </summary>
    public const string ProcedureStatsFallbackByObjectSql = @"
SELECT b.plan_xml
FROM procedure_stats AS ps
JOIN collect.oversized_plan_backlog AS b
  ON  b.server_id = ps.server_id
  AND b.collector_name = 'procedure_stats'
  AND b.plan_handle = ps.plan_handle
  AND b.sql_handle = ps.sql_handle
WHERE ps.server_id = $1
AND   ps.database_name = $2
AND   ps.schema_name = $3
AND   ps.object_name = $4
AND   b.plan_xml IS NOT NULL
ORDER BY ps.collection_time DESC
LIMIT 1;";

    /// <summary>One claimed backlog row: its identity, and what the cap measured.</summary>
    /// <param name="CollectorName">Which collector saw it — decides nothing about the fetch, but rides
    /// along because the outcome statements address the full key.</param>
    /// <param name="Observation">The cache coordinates and the measured size, in the same shape the
    /// collector described it.</param>
    /// <param name="AttemptCount">The row's <c>attempt_count</c> at claim time: how many times the plan has had the
    /// whole judging budget to itself and overrun it. The sweep retires a plan the judging budget keeps failing to
    /// cover once this reaches its limit (#5320); a connect failure or an expiry does not add to it.</param>
    /// <param name="Tried">Whether the row had been attempted before this claim (<c>last_attempt_at</c> is set), whatever
    /// the outcome was. The sweep judges a tried row on a judging session of its own (#5367).</param>
    public sealed record PendingPlan(string CollectorName, OversizedPlanObservation Observation, int AttemptCount = 0, bool Tried = false);

    /// <summary>
    /// Records this cycle's over-cap sightings for one server. Opens NO connection when there are none,
    /// which is the overwhelmingly common case — a fleet with nothing over the cap pays nothing.
    ///
    /// <para>Never throws. A best-effort backlog that cannot be written is a lost opportunity to go back for
    /// a plan, not a reason to fail a collection cycle that stored every row it read.</para>
    /// </summary>
    /// <returns>How many sightings were written: the batch less the new rows the statement left out for the trim (#8789), all or nothing
    /// in one transaction, or 0 when none were attempted or the write failed.</returns>
    public static async Task<int> RecordSightingsAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string collectorName,
        IReadOnlyList<OversizedPlanObservation> observations,
        DateTime sightingUtc,
        int pendingRowCap,
        int maxRowsDroppedPerPass,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(observations);

        if (observations.Count == 0)
        {
            return 0;
        }

        /* Naive-UTC storage: Npgsql 6+ infers timestamptz from Kind=Utc and silently zone-shifts — see
           PgCollectorRowWriter. */
        var seenAt = DateTime.SpecifyKind(sightingUtc, DateTimeKind.Unspecified);

        var count = observations.Count;
        var planHandles = new string[count];
        var sqlHandles = new string[count];
        var startOffsets = new int[count];
        var endOffsets = new int[count];
        var databaseNames = new string?[count];
        var queryHashes = new string?[count];
        var observedBytes = new long[count];

        for (var i = 0; i < count; i++)
        {
            var observation = observations[i];
            planHandles[i] = observation.PlanHandle;
            sqlHandles[i] = observation.SqlHandle;
            startOffsets[i] = observation.StatementStartOffset;
            endOffsets[i] = observation.StatementEndOffset;
            databaseNames[i] = observation.DatabaseName;
            queryHashes[i] = observation.QueryHash;
            observedBytes[i] = observation.ObservedBytes;
        }

        try
        {
            await using var command = postgres.CreateCommand(RecordSightingsSql);
            command.CommandTimeout = CommandTimeoutSeconds;

            /* $1..$12 in statement order. One statement, so the whole batch is one transaction. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = collectorName });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = planHandles });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = sqlHandles });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = startOffsets });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = endOffsets });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databaseNames });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = queryHashes });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = observedBytes });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = seenAt });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = pendingRowCap });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = maxRowsDroppedPerPass });

            var notInserted = Convert.ToInt32(
                await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);

            if (notInserted > 0)
            {
                logger?.LogDebug(
                    "Oversized-plan backlog: {Collector} on server {ServerId} did not insert {NotInserted} of {Total} new sighting(s) the trim would drop at once (#8789)",
                    collectorName, serverId, notInserted, count);
            }

            return count - notInserted;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(
                "Oversized-plan backlog: {Collector} on server {ServerId} recorded none of {Total} sighting(s): {Message}",
                collectorName, serverId, count, ex.Message);
            return 0;
        }
    }
}
