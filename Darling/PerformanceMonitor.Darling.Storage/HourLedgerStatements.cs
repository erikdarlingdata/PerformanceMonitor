/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one definition of the three statements every hourly row-count ledger runs (#4605, #9357): the batch add, the
/// recount from raw, and the retention prune. <see cref="QueryStatsHourLedger"/> and <see cref="ProcedureStatsHourLedger"/>
/// each build theirs here from their own ledger and raw table names, so the two sources cannot drift apart. The state table
/// is not named by any of the three. The create script is not built here: a shipped rung embeds it, so each ledger keeps
/// it as a frozen literal.
/// </summary>
internal static class HourLedgerStatements
{
    /// <summary>
    /// The batch add: <c>$1</c> server id, <c>$2</c> storage name, <c>$3</c> the batch's <c>collection_time</c> (truncated
    /// to its hour here) and <c>$4</c> the count of rows whose interval is not 0. A count of 0 or less writes nothing.
    /// </summary>
    public static string Upsert(string ledgerTable) => $@"
INSERT INTO {ledgerTable} AS ledger (server_id, server_name, bucket, n)
SELECT $1::integer, $2::text, date_trunc('hour', $3::timestamp), $4::bigint
WHERE $4::bigint > 0
ON CONFLICT (bucket, server_id, server_name) DO UPDATE SET n = ledger.n + EXCLUDED.n;";

    /// <summary>
    /// The recount: <c>$1</c> start and <c>$2</c> end (exclusive), widened to whole hours. Within the range the ledger is
    /// made equal to the raw table's non-restart rows, and the state row is not touched.
    /// </summary>
    public static string Recount(string ledgerTable, string rawTable) => $@"
WITH raw AS (
    SELECT server_id, server_name, date_trunc('hour', collection_time) AS bucket, count(*) AS n
    FROM {rawTable}
    WHERE collection_time >= date_trunc('hour', $1::timestamp)
      AND collection_time < date_trunc('hour', $2::timestamp - interval '1 microsecond') + interval '1 hour'
      AND sample_interval_seconds IS DISTINCT FROM 0
    GROUP BY 1, 2, 3
),
cleared AS (
    DELETE FROM {ledgerTable} AS stale
    WHERE stale.bucket >= date_trunc('hour', $1::timestamp)
      AND stale.bucket < date_trunc('hour', $2::timestamp - interval '1 microsecond') + interval '1 hour'
      AND NOT EXISTS (
          SELECT 1 FROM raw
          WHERE raw.bucket = stale.bucket AND raw.server_id = stale.server_id AND raw.server_name = stale.server_name)
    RETURNING 1
),
written AS (
    INSERT INTO {ledgerTable} AS ledger (server_id, server_name, bucket, n)
    SELECT server_id, server_name, bucket, n FROM raw
    ON CONFLICT (bucket, server_id, server_name) DO UPDATE SET n = EXCLUDED.n
    RETURNING 1
)
SELECT (SELECT count(*) FROM written)::bigint AS rows_written, (SELECT count(*) FROM cleared)::bigint AS rows_deleted;";

    /// <summary>The retention prune: deletes every ledger bucket below <c>$1</c>, truncated to its hour.</summary>
    public static string Prune(string ledgerTable) => $@"
DELETE FROM {ledgerTable}
WHERE bucket < date_trunc('hour', $1::timestamp);";
}
