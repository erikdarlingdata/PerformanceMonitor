/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly row-count ledger of <c>collect.query_stats</c> (#4605): one row per (hour, server) holding how many
/// non-restart rows the collector has written into that hour. A long-window read that takes its middle hours from the
/// hourly interval rollup may do so only when the rollup holds every raw row those hours have, and the count guard
/// (<see cref="IntervalRollupCountGuard"/>) proves that by comparing, per (server, hour), the raw row count with the
/// rollup's <c>sum(sample_count)</c>. Counting raw is a scan of millions of rows and runs past any panel deadline. The
/// ledger moves that count to write time: the collector adds each batch's count in the same transaction as its COPY,
/// so under one read snapshot the ledger, raw and the rollup agree, and the guard reads one small row per (server,
/// hour) instead of every raw row. This class and the V164 rung are the storage half; the guard's read of the ledger is
/// <see cref="IntervalRollupCountGuard"/>, and the writer in the collector's COPY, the census pin on writers and the
/// retention prune are the other lanes of #4605.
///
/// <para><b>What is counted.</b> Exactly the population the rollup counts: rows whose <c>sample_interval_seconds</c> is
/// not 0. A restart row (0, a delta that was not knowable) is not counted, and a row with a NULL interval (written
/// before the column existed) is, the same as the rollup's own <c>WHERE sample_interval_seconds IS DISTINCT FROM 0</c>.
/// An hour with no counted rows has no ledger row, as it has no rollup row.</para>
///
/// <para><b>Hours and names.</b> <c>bucket</c> is the whole hour, naive UTC like every instant in the store.
/// <c>date_trunc('hour', ...)</c> is used rather than <c>time_bucket('1 hour', ...)</c> because it gives the same
/// hour for a naive timestamp and exists on a store without TimescaleDB. <c>server_name</c> is the spelling the COPY
/// writes into <c>collect.query_stats.server_name</c> (the server's storage name), so a ledger row and the rollup's row
/// for the same server and hour join on equal keys.</para>
///
/// <para><b>Where counting starts.</b> The ledger holds nothing for rows written before the store reached V164. The
/// one-row state table's <c>counted_since</c> is the first whole hour from which every row is counted: the rung sets
/// it to the next whole hour after it ran, and an hour that was only partly written before the upgrade stays below it.
/// A reader must treat any window that starts before <c>counted_since</c> as uncounted and stay on raw. Moving it
/// earlier is the job of a backfill that recounts closed hours with <see cref="RecountSql"/>; no such job exists yet.</para>
///
/// <para><b>Plain tables, no grants.</b> Both tables are plain, not hypertables, and not collectors: a server adds 24
/// rows a day and the retention pass will prune the table to the hourly rollup's horizon. They sit in the <c>collect</c> schema, so the
/// blanket SELECT and the owner's default privileges that provisioning re-asserts at every start (admin, viewer and
/// mcp) cover them with no statement of their own. The collector writes as the owner.</para>
///
/// <para><b>The key is hour first.</b> The guard reads a range of hours across every server and the retention pass
/// deletes below one hour, so the primary key leads with <c>bucket</c>: both are index range scans, and no second index
/// costs a write. The writer's conflict target names all three key columns, which matches the key in any order.</para>
/// </summary>
public static class QueryStatsHourLedger
{
    /// <summary>The ledger table: one row per (hour, server) with the count of non-restart <c>collect.query_stats</c> rows written into that hour.</summary>
    public const string LedgerTable = "collect.query_stats_hour_ledger";

    /// <summary>The one-row state table: <c>counted_since</c> is the first whole hour from which every row is in the ledger.</summary>
    public const string StateTable = "collect.query_stats_hour_ledger_state";

    /// <summary>
    /// The store schema version of the rung that creates both tables (V164). A store below it has neither table, so a
    /// reader checks the store's schema version against this before it names them: the hourly-edges runner does, and a
    /// store below the rung reads raw with no fault to log (#4605). <c>QueryStatsHourLedgerTests</c> pins it to the
    /// version the rung is registered under.
    /// </summary>
    public const int RungVersion = 164;

    /// <summary>
    /// Both tables and the state row, as the V164 rung runs them. Idempotent: the tables are guarded and the state row
    /// is inserted once (<c>ON CONFLICT DO NOTHING</c>), so a re-run of the rung keeps the original
    /// <c>counted_since</c>. The rung embeds this text, so editing it edits a shipped rung: add a new rung instead, and
    /// <c>QueryStatsHourLedgerTests</c> pins the shape so such an edit fails there first.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.query_stats_hour_ledger
(
    server_id integer NOT NULL,
    server_name text NOT NULL,
    bucket timestamp NOT NULL,
    n bigint NOT NULL,
    CONSTRAINT pk_query_stats_hour_ledger PRIMARY KEY (bucket, server_id, server_name)
);

CREATE TABLE IF NOT EXISTS collect.query_stats_hour_ledger_state
(
    id integer NOT NULL,
    counted_since timestamp NOT NULL,
    CONSTRAINT pk_query_stats_hour_ledger_state PRIMARY KEY (id),
    CONSTRAINT ck_query_stats_hour_ledger_state_single_row CHECK (id = 1)
);

INSERT INTO collect.query_stats_hour_ledger_state (id, counted_since)
VALUES (1, date_trunc('hour', now() AT TIME ZONE 'UTC') + interval '1 hour')
ON CONFLICT (id) DO NOTHING;";

    /// <summary>
    /// Adds one batch's count to its hour: <c>$1</c> is the server id, <c>$2</c> the server's storage name (the
    /// spelling the COPY writes), <c>$3</c> the batch's <c>collection_time</c> (naive UTC <c>timestamp</c>, truncated
    /// to its hour here) and <c>$4</c> the number of rows the batch wrote whose <c>sample_interval_seconds</c> is not 0
    /// (a NULL interval counts). A new hour inserts the count and an existing one adds to it. A count of 0 or less
    /// writes nothing, because the rollup has no row for an hour with no counted rows and a ledger row of 0 would not
    /// equal that. Run it on the COPY's own transaction and after the COPY, so the ledger and the raw rows commit
    /// together or not at all.
    /// </summary>
    public const string UpsertSql = @"
INSERT INTO collect.query_stats_hour_ledger AS ledger (server_id, server_name, bucket, n)
SELECT $1::integer, $2::text, date_trunc('hour', $3::timestamp), $4::bigint
WHERE $4::bigint > 0
ON CONFLICT (bucket, server_id, server_name) DO UPDATE SET n = ledger.n + EXCLUDED.n;";

    /// <summary>
    /// Recomputes the ledger from raw for a range of hours: <c>$1</c> is the start and <c>$2</c> the end (exclusive),
    /// both naive UTC <c>timestamp</c>. The range is widened to whole hours (the start rounds down, the end up), because
    /// a partial hour would write a count that is too low. Within the range the ledger is made equal to raw: a
    /// (server, hour) with raw rows is set to its count (not added), and a ledger row with no raw rows is deleted. It
    /// counts the rollup's population (<c>sample_interval_seconds IS DISTINCT FROM 0</c>, so restart rows are
    /// skipped and NULL intervals counted) and writes <c>server_name</c> as raw holds it, which is the spelling the
    /// COPY wrote. Returns one row: the number of ledger rows written and the number deleted.
    /// <para>It does not touch the state row. Run it over closed hours: a batch that commits while it runs can be
    /// overwritten, which leaves the ledger below the rollup and so fails the guard safe, never wrongly passes it.
    /// Tests use it to seed the ledger from raw rows they insert, and a backfill uses it before it moves
    /// <c>counted_since</c> back.</para>
    /// </summary>
    public const string RecountSql = @"
WITH raw AS (
    SELECT server_id, server_name, date_trunc('hour', collection_time) AS bucket, count(*) AS n
    FROM collect.query_stats
    WHERE collection_time >= date_trunc('hour', $1::timestamp)
      AND collection_time < date_trunc('hour', $2::timestamp - interval '1 microsecond') + interval '1 hour'
      AND sample_interval_seconds IS DISTINCT FROM 0
    GROUP BY 1, 2, 3
),
cleared AS (
    DELETE FROM collect.query_stats_hour_ledger AS stale
    WHERE stale.bucket >= date_trunc('hour', $1::timestamp)
      AND stale.bucket < date_trunc('hour', $2::timestamp - interval '1 microsecond') + interval '1 hour'
      AND NOT EXISTS (
          SELECT 1 FROM raw
          WHERE raw.bucket = stale.bucket AND raw.server_id = stale.server_id AND raw.server_name = stale.server_name)
    RETURNING 1
),
written AS (
    INSERT INTO collect.query_stats_hour_ledger AS ledger (server_id, server_name, bucket, n)
    SELECT server_id, server_name, bucket, n FROM raw
    ON CONFLICT (bucket, server_id, server_name) DO UPDATE SET n = EXCLUDED.n
    RETURNING 1
)
SELECT (SELECT count(*) FROM written)::bigint AS rows_written, (SELECT count(*) FROM cleared)::bigint AS rows_deleted;";

    /// <summary>
    /// The retention prune (#4605): deletes every ledger bucket below <c>$1</c> (naive UTC <c>timestamp</c>, truncated to
    /// its hour here so a cutoff mid-hour never splits a bucket). The caller passes the successor hourly rollup's
    /// horizon, <c>now - <see cref="TimescaleSupport.HourlyRetentionSpan"/></c>, and NOT raw's: raw keeps four days and
    /// the rollup keeps ninety, and the guard compares the ledger with the rollup, so a ledger pruned at raw's horizon
    /// would be missing the hours the rollup still holds and the guard would fail every window past four days. A plain
    /// single DELETE is enough: the table holds about one row per server per hour (servers x 24 x retention days), and
    /// the primary key leads with <c>bucket</c>, so the predicate is an index range scan. Idempotent, and a table with
    /// nothing below the cutoff deletes nothing.
    /// </summary>
    public const string PruneSql = @"
DELETE FROM collect.query_stats_hour_ledger
WHERE bucket < date_trunc('hour', $1::timestamp);";
}
