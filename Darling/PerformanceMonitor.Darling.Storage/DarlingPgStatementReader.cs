/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the PostgreSQL statement store (<c>pg_statement_stats</c>) — the top-queries surface for an
/// Aurora target.
/// </summary>
public static class DarlingPgStatementReader
{
    /// <summary>
    /// One row per query shape over the window.
    /// <para><c>QueryId</c> rather than text: the collector does not store statement text yet, and
    /// <c>queryid</c> is the join key anyway. It is stable within a major version but NOT across one, so
    /// a consumer must not treat it as a permanent identifier.</para>
    /// </summary>
    public sealed record PgStatementRow(
        long QueryId,
        long DatabaseId,
        long Calls,
        long TotalExecTimeMs,
        long RowsReturned,
        double MaxExecTimeMs,
        long SharedBlocksHit,
        long SharedBlocksRead,
        /* Nullable from #2625 on: these three come only from aurora_stat_statements(). On a self-hosted
           target the collector writes NULL, and 0 here would be a claim ABOUT AURORA made about a server
           that is not Aurora — "the storage volume served no reads" rather than "there is no storage
           volume". The read preserves the distinction end to end; the JSON emits null. */
        long? StorageBlocksRead,
        long? OrcacheBlocksHit,
        long TempBlocksRead,
        long TempBlocksWritten,
        long WalBytes,
        long? MaxPeakMemBytes,
        /* #2219: the statement text, or null when none has been captured for this queryid yet. Null is the
           HONEST answer rather than a placeholder: text is refreshed hourly, so a statement first seen minutes
           ago genuinely has none, and after a major-version upgrade re-keys queryid the new ids have none until
           the next refresh. Distinguishing "not captured yet" from "" is what stops a caller reading an empty
           string as the query. */
        string? QueryText = null);

    /// <summary>
    /// Every counter is reported for the WINDOW, never as a lifetime total. Summing the cumulative
    /// counters directly would multiply each query's whole history by the number of snapshots in the
    /// window, so the time/call/row figures come from the stored delta columns and the block/WAL figures
    /// are differenced here.
    /// <para>The block and WAL columns keep no stored deltas, and reporting them as the window's MAX —
    /// which is what this read used to do — put a lifetime cumulative figure in the same row as a
    /// windowed one. A consumer has no way to see that: it reads <c>total_exec_time_ms</c> for the last
    /// hour beside <c>shared_blks_read</c> since the last <c>pg_stat_statements_reset()</c>, possibly
    /// weeks earlier, and any per-call ratio it derives is nonsense. So the difference is computed at
    /// read time instead, per series, which needs no new stored state.</para>
    /// <para><c>GREATEST(value - LAG(value), 0)</c> is what makes that safe. A counter reset — an
    /// explicit <c>pg_stat_statements_reset()</c>, an eviction and re-entry, or a major-version upgrade
    /// (queryid is not stable across majors) — makes one difference negative, and a plain
    /// last-minus-first would report that as a large negative or silently wrong figure. Clamping each
    /// interval at zero drops exactly the reset interval and keeps the rest, which is the same rule the
    /// stored delta machinery applies.</para>
    /// <para>The LAG partition is the FULL series identity (queryid, database_id, user_id, toplevel),
    /// matching how the stored deltas are keyed: the same normalized statement run by another user or
    /// against another database is a separate pg_stat_statements entry with its own counters, so
    /// differencing across those would interleave series. The outer aggregation then rolls up to
    /// (queryid, database_id), which is the grain a "top queries" answer wants.</para>
    /// <para><c>max_exec_peakmem_bytes</c> and <c>max_exec_time_ms</c> stay MAX — they are high-water
    /// marks, not counters, and differencing a high-water mark would be meaningless.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgTopQueriesSql = """
        WITH differenced AS (
            SELECT
                queryid,
                database_id,
                delta_calls,
                delta_total_exec_time_ms,
                delta_rows,
                max_exec_time_ms,
                max_exec_peakmem_bytes,
                /* Carried RAW alongside their deltas (#2625). The aggregate below has to separate two
                   states that both arrive as a NULL sum: a series with one sample in the window (no
                   measurable interval, so 0 is right) and a source that never reported the column at all
                   (self-hosted PostgreSQL, where 0 would be a false statement about Aurora hardware).
                   Only the undifferenced column can tell them apart. */
                storage_blks_read AS raw_storage_blks_read,
                orcache_blks_hit  AS raw_orcache_blks_hit,
                GREATEST(shared_blks_hit    - LAG(shared_blks_hit)    OVER series, 0) AS d_shared_blks_hit,
                GREATEST(shared_blks_read   - LAG(shared_blks_read)   OVER series, 0) AS d_shared_blks_read,
                GREATEST(storage_blks_read  - LAG(storage_blks_read)  OVER series, 0) AS d_storage_blks_read,
                GREATEST(orcache_blks_hit   - LAG(orcache_blks_hit)   OVER series, 0) AS d_orcache_blks_hit,
                GREATEST(temp_blks_read     - LAG(temp_blks_read)     OVER series, 0) AS d_temp_blks_read,
                GREATEST(temp_blks_written  - LAG(temp_blks_written)  OVER series, 0) AS d_temp_blks_written,
                GREATEST(wal_bytes          - LAG(wal_bytes)          OVER series, 0) AS d_wal_bytes
            FROM pg_statement_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            WINDOW series AS (
                PARTITION BY queryid, database_id, user_id, toplevel
                ORDER BY collection_time
            )
        )
        SELECT
            /* #2554: QUALIFIED, and both of these references need it. The LEFT JOIN below puts
               t.queryid in scope alongside differenced.queryid, which made a bare `queryid`
               ambiguous (42702) in the select list AND in the GROUP BY. That is a PARSE-time
               error, so the read threw on every call from #2219 onward, on every engine,
               whether or not the store held a single row. Qualifying only the GROUP BY -- the
               obvious one-line fix -- still fails here, on the select list. */
            differenced.queryid AS queryid,
            database_id,
            CAST(SUM(delta_calls) AS bigint) AS calls,
            CAST(SUM(delta_total_exec_time_ms) AS bigint) AS total_exec_time_ms,
            CAST(SUM(delta_rows) AS bigint) AS rows_returned,
            MAX(max_exec_time_ms) AS max_exec_time_ms,
            /* coalesce to 0, not to the cumulative value: a series with a single sample in the window
               has no measurable interval, and its increment happened before the window began. */
            CAST(coalesce(SUM(d_shared_blks_hit), 0) AS bigint) AS shared_blks_hit,
            CAST(coalesce(SUM(d_shared_blks_read), 0) AS bigint) AS shared_blks_read,
            /* NULL when the source never reported it, 0 when it did and the window holds no interval.
               COUNT ignores NULLs, so COUNT(raw_*) = 0 means every sample was silent. */
            CASE WHEN COUNT(raw_storage_blks_read) = 0 THEN NULL
                 ELSE CAST(coalesce(SUM(d_storage_blks_read), 0) AS bigint) END AS storage_blks_read,
            CASE WHEN COUNT(raw_orcache_blks_hit) = 0 THEN NULL
                 ELSE CAST(coalesce(SUM(d_orcache_blks_hit), 0) AS bigint) END AS orcache_blks_hit,
            CAST(coalesce(SUM(d_temp_blks_read), 0) AS bigint) AS temp_blks_read,
            CAST(coalesce(SUM(d_temp_blks_written), 0) AS bigint) AS temp_blks_written,
            CAST(coalesce(SUM(d_wal_bytes), 0) AS bigint) AS wal_bytes,
            CAST(MAX(max_exec_peakmem_bytes) AS bigint) AS max_exec_peakmem_bytes,
            /* #2219: the statement text, from the (server_id, queryid) store. MAX rather than a join column
               because the grain here is (queryid, database_id) while text is keyed on queryid alone — one text
               per group by construction, so MAX picks it without widening the GROUP BY. LEFT JOIN, so a
               queryid whose text has not been captured yet still ranks; it simply reads as null. */
            MAX(t.query_text) AS query_text
        FROM differenced
        LEFT JOIN collect.pg_statement_text AS t
               ON  t.server_id = $1
               AND t.queryid = differenced.queryid
        GROUP BY differenced.queryid, database_id
        HAVING SUM(delta_total_exec_time_ms) > 0
        ORDER BY SUM(delta_total_exec_time_ms) DESC
        LIMIT 50
        """;

    public static async Task<List<PgStatementRow>> GetPgTopQueriesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgStatementRow>();
        await using var command = postgres.CreateCommand(PgTopQueriesSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. Hidden by UTC-hosted test
           stores; found by the round-2 review. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgStatementRow(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                /* null, not 0 — see PgStatementRow. */
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? null : reader.GetInt64(13),
                /* #2219: null stays null — see PgStatementRow.QueryText for why an empty string would be a lie. */
                reader.IsDBNull(14) ? null : reader.GetString(14)));
        }

        return rows;
    }
}
