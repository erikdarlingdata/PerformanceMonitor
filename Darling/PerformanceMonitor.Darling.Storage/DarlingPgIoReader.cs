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
/// Reads I/O by (backend_type, object, context) from <c>pg_io_stats</c>, differenced across the window.
/// </summary>
public static class DarlingPgIoReader
{
    public sealed record PgIoRow(
        string? BackendType,
        string? ObjectType,
        string? Context,
        long Reads,
        double ReadTimeMs,
        long Hits,
        long Extends,
        double ExtendTimeMs,
        long Evictions,
        long Reuses,
        long Writes,
        double WriteTimeMs,
        long OpBytes,
        bool WriteCountersTracked,
        DateTime? StatsReset,
        /* PG18's measured byte totals (#2655), summed over the window like every other counter here.
           ByteCountersTracked is to these what WriteCountersTracked is to the write side: below 18 the
           columns are NULL and GREATEST(NULL, 0) flattens them to 0, so the zero alone cannot say whether
           nothing was read or bytes are simply not measured on this version. */
        decimal ReadBytes,
        decimal WriteBytes,
        decimal ExtendBytes,
        bool ByteCountersTracked);

    /// <summary>
    /// Positive-difference-per-interval, summed over the window — the same rule the statement read uses,
    /// for the same reason: these are cumulative counters, so a plain last-minus-first goes negative
    /// whenever <c>pg_stat_reset_shared('io')</c> runs or the server restarts.
    /// <para><b>The numeric columns DO come back as 0 for an untracked counter, and
    /// <c>write_counters_tracked</c> is what makes that safe.</b> PostgreSQL uses NULL for "this counter does
    /// not apply to this combination", and on Aurora the entire write side is NULL because backends there do
    /// not write data files. Two things then flatten it: <c>GREATEST(NULL, 0)</c> returns <c>0</c> — GREATEST
    /// ignores NULLs, verified against live Aurora 17.7 — and the outer <c>coalesce(SUM(...), 0)</c> would do
    /// it anyway. So a caller MUST read <c>write_counters_tracked</c> to tell "no writes happened" from
    /// "writes are not measured here"; the zero alone cannot distinguish them, and averaging latency over it
    /// divides by a number that was never measured.</para>
    /// <para>This comment previously claimed NULL survived the arithmetic. It does not, and the claim was
    /// worse than useless: it would have licensed someone to drop the tracked flag believing the NULLs were
    /// carrying the information. The flag is not belt-and-braces — it is the only discriminator.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgIoSql = """
        WITH differenced AS (
            SELECT
                backend_type,
                object_type,
                context,
                stats_reset,
                GREATEST(reads      - LAG(reads)      OVER series, 0) AS d_reads,
                GREATEST(read_time_ms   - LAG(read_time_ms)   OVER series, 0) AS d_read_time_ms,
                GREATEST(hits       - LAG(hits)       OVER series, 0) AS d_hits,
                GREATEST(extends    - LAG(extends)    OVER series, 0) AS d_extends,
                GREATEST(extend_time_ms - LAG(extend_time_ms) OVER series, 0) AS d_extend_time_ms,
                GREATEST(evictions  - LAG(evictions)  OVER series, 0) AS d_evictions,
                GREATEST(reuses     - LAG(reuses)     OVER series, 0) AS d_reuses,
                GREATEST(writes     - LAG(writes)     OVER series, 0) AS d_writes,
                GREATEST(write_time_ms  - LAG(write_time_ms)  OVER series, 0) AS d_write_time_ms,
                op_bytes,
                GREATEST(read_bytes   - LAG(read_bytes)   OVER series, 0) AS d_read_bytes,
                GREATEST(write_bytes  - LAG(write_bytes)  OVER series, 0) AS d_write_bytes,
                GREATEST(extend_bytes - LAG(extend_bytes) OVER series, 0) AS d_extend_bytes,
                /* Whether this combination tracks writes AT ALL, as opposed to having written nothing.
                   Aurora reports NULL here across the board; a self-managed target reports numbers. */
                (writes IS NOT NULL) AS writes_tracked,
                /* Whether the server measures bytes at all — PG18 and above. read_bytes is the probe for
                   all three: WAL rows legitimately report no extend_bytes, so extend_bytes IS NOT NULL
                   would answer "not measured" for a row that simply does not extend. */
                (read_bytes IS NOT NULL) AS byte_counters_tracked
            FROM pg_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            WINDOW series AS (
                PARTITION BY backend_type, object_type, context
                ORDER BY collection_time
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
            bool_or(byte_counters_tracked)                 AS byte_counters_tracked
        FROM differenced
        GROUP BY backend_type, object_type, context
        /* Anything that moved, ordered by the work that actually costs time. A combination with no
           activity in the window is not a finding and would crowd out the ones that are. */
        HAVING coalesce(SUM(d_reads), 0) + coalesce(SUM(d_writes), 0)
             + coalesce(SUM(d_extends), 0) + coalesce(SUM(d_hits), 0) > 0
        ORDER BY coalesce(SUM(d_read_time_ms), 0) DESC, coalesce(SUM(d_reads), 0) DESC
        LIMIT $4
        """;

    public static async Task<List<PgIoRow>> GetPgIoAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgIoRow>();
        await using var command = postgres.CreateCommand(PgIoSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. Hidden by UTC-hosted test
           stores; found by the round-2 review. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgIoRow(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt64(3),
                reader.GetDouble(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetDouble(7),
                reader.GetInt64(8),
                reader.GetInt64(9),
                reader.GetInt64(10),
                reader.GetDouble(11),
                reader.GetInt64(12),
                !reader.IsDBNull(13) && reader.GetBoolean(13),
                reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                reader.GetDecimal(15),
                reader.GetDecimal(16),
                reader.GetDecimal(17),
                !reader.IsDBNull(18) && reader.GetBoolean(18)));
        }

        return rows;
    }

    /// <summary>
    /// What a <c>context</c> value MEANS — the dimension with no SQL Server counterpart, and the one that
    /// changes what you do about a number rather than just how large it is.
    ///
    /// <para>Lives beside the query that produces the value rather than in either front end (#2530): the MCP
    /// tool and the WPF viewer's I/O tab both print it, and the copy that drifts is never the one being
    /// read. A context this build has never seen gets an explicit "unrecognized" answer rather than silence,
    /// because a blank explanation beside a large number reads as "nothing to say about it".</para>
    /// </summary>
    public static string ContextMeaning(string? context) => context switch
    {
        "normal" => "Ordinary buffer-pool traffic. Reads here are cache misses that shared_buffers could "
                  + "have absorbed, so a high read share with a low hit share is the classic case for more "
                  + "memory or a better index.",
        "bulkread" => "A sequential scan deliberately using a small ring buffer so it cannot evict the "
                    + "buffer pool. High volume here is a scan-heavy workload, NOT memory pressure — adding "
                    + "shared_buffers will not reduce it, because these reads bypass the pool by design.",
        "bulkwrite" => "A bulk write (COPY, CREATE TABLE AS, some ALTER TABLE) using its own ring buffer.",
        "vacuum" => "Vacuum's ring buffer. Volume here is autovacuum doing its job; pair it with "
                  + "get_pg_autovacuum_health to see whether it is keeping up.",
        "index" => "Index-specific I/O, reported separately from the relation's own.",
        "walreplay" => "A standby applying WAL. This is replica catch-up work, not query I/O, and it is the "
                     + "first thing to check when a reader lags.",
        _ => "Unrecognized context — treat the raw counters as authoritative and check the PostgreSQL "
           + "documentation for this server's major version.",
    };
}
