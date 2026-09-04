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
/// Reads the stored write-side counters (<c>pg_write_stats</c>, #2544) as a CHANGE OVER THE WINDOW, not as
/// the raw cumulative levels.
///
/// <para><b>Why differenced here rather than stored as a delta.</b> Every column is a counter that only ever
/// climbs, so the level says how much work the server has done since its last stats reset — a number that is
/// meaningless without knowing when that was, and that grows without bound. What anybody actually asks is
/// "how many requested checkpoints in the last hour", which is last minus first over the window. Storing
/// deltas instead would have forced the collector to invent a value for a column its major version does not
/// have, and NULL is the whole point of that shape.</para>
///
/// <para><b>The reset guard is not optional.</b> <c>pg_stat_reset_shared</c> can reset the checkpointer, the
/// bgwriter and WAL INDEPENDENTLY, which is why the collector stores three separate <c>stats_reset</c>
/// stamps. If a reset happens inside the window, last-minus-first goes negative, and a naive read would show
/// it as an enormous positive number after an unsigned cast — the exact shape of a false "the server did
/// 4 billion checkpoints" alert. Each family is differenced only when its OWN reset stamp is unchanged
/// across the window, and reports NULL rather than a guess when it is not.</para>
///
/// <para><b>NULL propagates and is never coalesced to zero.</b> A column the target's major version does not
/// supply is NULL in every row, so the subtraction yields NULL and the caller can say "this PostgreSQL
/// version does not expose it" instead of "it was zero". On a fleet holding both 16 and 18 targets those are
/// completely different statements — 16 has no <c>num_done</c>, 18 has no <c>wal_write_time</c>.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgWriteStatsReader
{
    /// <param name="CheckpointsTimed">Checkpoints begun on <c>checkpoint_timeout</c> during the window.</param>
    /// <param name="CheckpointsRequested">Checkpoints begun because WAL volume demanded one. Climbing
    /// against <paramref name="CheckpointsTimed"/> is the <c>max_wal_size</c>-too-small signal.</param>
    /// <param name="BuffersBackend">Buffers a backend wrote itself. NULL on PostgreSQL 17+, where the fact
    /// moved to <c>pg_stat_io</c> rather than to the checkpointer — NOT zero, which would read as "backends
    /// never had to write", the opposite of unknown.</param>
    /// <param name="WalWriteTimeMs">NULL on PostgreSQL 18+, which removed the WAL timing columns.</param>
    /// <param name="ResetDuringWindow">True when at least one of the three <c>stats_reset</c> stamps moved
    /// inside the window, so the affected families report NULL rather than a difference across the reset.</param>
    public sealed record PgWriteStatsRow(
        DateTime WindowStartUtc,
        DateTime WindowEndUtc,
        long? CheckpointsTimed,
        long? CheckpointsRequested,
        long? CheckpointsDone,
        long? RestartpointsTimed,
        long? RestartpointsRequested,
        long? RestartpointsDone,
        double? CheckpointWriteTimeMs,
        double? CheckpointSyncTimeMs,
        long? BuffersWrittenCheckpoint,
        long? SlruWritten,
        long? BuffersClean,
        long? MaxwrittenClean,
        long? BuffersAlloc,
        long? BuffersBackend,
        long? BuffersBackendFsync,
        long? WalRecords,
        long? WalFpi,
        decimal? WalBytes,
        long? WalBuffersFull,
        long? WalWrite,
        long? WalSync,
        double? WalWriteTimeMs,
        double? WalSyncTimeMs,
        bool ResetDuringWindow);

    /* first_value/last_value over the window, then one subtraction — rather than MAX-MIN, which is wrong the
       moment a reset happens (MIN would come from AFTER the reset and MAX from before it, producing a
       plausible-looking number from two unrelated epochs).

       The frame is spelled out. last_value with the default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND
       CURRENT ROW) returns the CURRENT row, not the last one - the single most common window-function bug in
       this shape, and it would silently make every delta zero on the newest row.

       Each family's reset is compared first_value vs last_value on its own stamp. IS DISTINCT FROM rather
       than <>, so a NULL stamp on both ends counts as unchanged instead of poisoning the comparison to NULL
       and blanking a perfectly good difference. */
    public const string PgWriteStatsSql = """
        WITH bounded AS (
            SELECT
                collection_time,
                num_timed, num_requested, num_done,
                restartpoints_timed, restartpoints_req, restartpoints_done,
                checkpoint_write_time_ms, checkpoint_sync_time_ms,
                buffers_written_checkpoint, slru_written, checkpointer_stats_reset,
                buffers_clean, maxwritten_clean, buffers_alloc,
                buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
                wal_records, wal_fpi, wal_bytes, wal_buffers_full,
                wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset
            FROM pg_write_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        edges AS (
            SELECT
                min(collection_time) AS window_start,
                max(collection_time) AS window_end,
                count(*)             AS samples
            FROM bounded
        ),
        firsts AS (
            SELECT b.* FROM bounded AS b JOIN edges AS e ON b.collection_time = e.window_start LIMIT 1
        ),
        lasts AS (
            SELECT b.* FROM bounded AS b JOIN edges AS e ON b.collection_time = e.window_end LIMIT 1
        )
        SELECT
            e.window_start,
            e.window_end,
            /* Checkpointer family: differenced only when its own reset stamp held still. */
            CASE WHEN ck_reset THEN l.num_timed          - f.num_timed          END AS checkpoints_timed,
            CASE WHEN ck_reset THEN l.num_requested      - f.num_requested      END AS checkpoints_requested,
            CASE WHEN ck_reset THEN l.num_done           - f.num_done           END AS checkpoints_done,
            CASE WHEN ck_reset THEN l.restartpoints_timed - f.restartpoints_timed END AS restartpoints_timed,
            CASE WHEN ck_reset THEN l.restartpoints_req  - f.restartpoints_req  END AS restartpoints_requested,
            CASE WHEN ck_reset THEN l.restartpoints_done - f.restartpoints_done END AS restartpoints_done,
            CASE WHEN ck_reset THEN l.checkpoint_write_time_ms - f.checkpoint_write_time_ms END AS checkpoint_write_time_ms,
            CASE WHEN ck_reset THEN l.checkpoint_sync_time_ms  - f.checkpoint_sync_time_ms  END AS checkpoint_sync_time_ms,
            CASE WHEN ck_reset THEN l.buffers_written_checkpoint - f.buffers_written_checkpoint END AS buffers_written_checkpoint,
            CASE WHEN ck_reset THEN l.slru_written       - f.slru_written       END AS slru_written,
            /* Background-writer family. */
            CASE WHEN bg_reset THEN l.buffers_clean      - f.buffers_clean      END AS buffers_clean,
            CASE WHEN bg_reset THEN l.maxwritten_clean   - f.maxwritten_clean   END AS maxwritten_clean,
            CASE WHEN bg_reset THEN l.buffers_alloc      - f.buffers_alloc      END AS buffers_alloc,
            CASE WHEN bg_reset THEN l.buffers_backend    - f.buffers_backend    END AS buffers_backend,
            CASE WHEN bg_reset THEN l.buffers_backend_fsync - f.buffers_backend_fsync END AS buffers_backend_fsync,
            /* WAL family. */
            CASE WHEN wal_reset THEN l.wal_records       - f.wal_records        END AS wal_records,
            CASE WHEN wal_reset THEN l.wal_fpi           - f.wal_fpi            END AS wal_fpi,
            CASE WHEN wal_reset THEN l.wal_bytes         - f.wal_bytes          END AS wal_bytes,
            CASE WHEN wal_reset THEN l.wal_buffers_full  - f.wal_buffers_full   END AS wal_buffers_full,
            CASE WHEN wal_reset THEN l.wal_write         - f.wal_write          END AS wal_write,
            CASE WHEN wal_reset THEN l.wal_sync          - f.wal_sync           END AS wal_sync,
            CASE WHEN wal_reset THEN l.wal_write_time_ms - f.wal_write_time_ms  END AS wal_write_time_ms,
            CASE WHEN wal_reset THEN l.wal_sync_time_ms  - f.wal_sync_time_ms   END AS wal_sync_time_ms,
            NOT (ck_reset AND bg_reset AND wal_reset)                            AS reset_during_window
        FROM edges AS e
        CROSS JOIN firsts AS f
        CROSS JOIN lasts AS l
        CROSS JOIN LATERAL (
            SELECT
                NOT (l.checkpointer_stats_reset IS DISTINCT FROM f.checkpointer_stats_reset) AS ck_reset,
                NOT (l.bgwriter_stats_reset     IS DISTINCT FROM f.bgwriter_stats_reset)     AS bg_reset,
                NOT (l.wal_stats_reset          IS DISTINCT FROM f.wal_stats_reset)          AS wal_reset
        ) AS r
        /* Two samples are the minimum for a difference to exist at all. One sample would difference a row
           against itself and report a confident zero, which is indistinguishable from a genuinely idle
           server - so the read returns nothing and the caller says it needs another cycle. */
        WHERE e.samples >= 2
        """;

    public static async Task<PgWriteStatsRow?> GetPgWriteStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var command = postgres.CreateCommand(PgWriteStatsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, same convention as every other PostgreSQL read here: Npgsql
           does not reject Kind=Utc, it infers timestamptz, and PostgreSQL then resolves the comparison
           against these NAIVE timestamp columns at the store session's TimeZone — so east of UTC the window
           slides off the data and the read returns nothing at all. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new PgWriteStatsRow(
            WindowStartUtc: DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
            WindowEndUtc: DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
            CheckpointsTimed: Long(reader, 2),
            CheckpointsRequested: Long(reader, 3),
            CheckpointsDone: Long(reader, 4),
            RestartpointsTimed: Long(reader, 5),
            RestartpointsRequested: Long(reader, 6),
            RestartpointsDone: Long(reader, 7),
            CheckpointWriteTimeMs: Double(reader, 8),
            CheckpointSyncTimeMs: Double(reader, 9),
            BuffersWrittenCheckpoint: Long(reader, 10),
            SlruWritten: Long(reader, 11),
            BuffersClean: Long(reader, 12),
            MaxwrittenClean: Long(reader, 13),
            BuffersAlloc: Long(reader, 14),
            BuffersBackend: Long(reader, 15),
            BuffersBackendFsync: Long(reader, 16),
            WalRecords: Long(reader, 17),
            WalFpi: Long(reader, 18),
            WalBytes: reader.IsDBNull(19) ? null : reader.GetDecimal(19),
            WalBuffersFull: Long(reader, 20),
            WalWrite: Long(reader, 21),
            WalSync: Long(reader, 22),
            WalWriteTimeMs: Double(reader, 23),
            WalSyncTimeMs: Double(reader, 24),
            ResetDuringWindow: !reader.IsDBNull(25) && reader.GetBoolean(25));

        static long? Long(Npgsql.NpgsqlDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetInt64(ordinal);
        static double? Double(Npgsql.NpgsqlDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDouble(ordinal);
    }
}
