/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9414: the reads behind <c>get_store_checkpoints</c>, over <c>collect.store_checkpoints</c> (one row per checkpoint of the
/// monitoring store's own PostgreSQL server, written by the store log sweep from its <c>checkpoint complete</c> lines).
/// </summary>
internal static class DarlingStoreCheckpointReader
{
    /// <summary>The checkpoints that started in the window, newest first. $1 window start, $2 window end (naive UTC), $3 limit, $4 offset.</summary>
    internal const string RowsSql = @"
SELECT kind, started_at, ended_at, reason, flags, buffers_written, buffers_pct, slru_buffers_written, wal_files_added, wal_files_removed,
       wal_files_recycled, write_ms, sync_ms, total_ms, files_synced, longest_sync_ms, average_sync_ms, distance_kb, estimate_kb, lsn,
       redo_lsn, unparsed
FROM collect.store_checkpoints
WHERE started_at >= $1
AND   started_at <  $2
ORDER BY started_at DESC, id DESC
LIMIT $3 OFFSET $4";

    /// <summary>Per hour of the window (by start time), newest first. $1 window start, $2 window end (naive UTC).</summary>
    internal const string HoursSql = @"
SELECT date_trunc('hour', started_at) AS hour_start,
       count(*) AS checkpoints,
       count(*) FILTER (WHERE reason = 'wal') AS started_by_wal,
       count(*) FILTER (WHERE reason = 'time') AS started_by_time,
       count(*) FILTER (WHERE c.ended_at IS NULL
                        AND EXISTS (SELECT 1
                                    FROM collect.store_checkpoints AS later
                                    WHERE later.kind = c.kind
                                    AND   (later.started_at, later.id) > (c.started_at, c.id))) AS not_completed,
       max(files_synced) AS max_files_synced,
       max(longest_sync_ms) AS longest_sync_ms,
       sum(write_ms)::bigint AS total_write_ms,
       sum(sync_ms)::bigint AS total_sync_ms
FROM collect.store_checkpoints AS c
WHERE c.started_at >= $1
AND   c.started_at <  $2
GROUP BY 1
ORDER BY 1 DESC";

    /// <summary>What the window holds in all, so a page knows how much it left out. $1 window start, $2 window end (naive UTC).</summary>
    internal const string TotalSql = @"
SELECT count(*), count(*) FILTER (WHERE unparsed)
FROM collect.store_checkpoints
WHERE started_at >= $1
AND   started_at <  $2";

    /// <summary>Whether the table exists, and the two settings a missing row can be blamed on.</summary>
    internal const string SettingsSql = @"
SELECT pg_catalog.to_regclass('collect.store_checkpoints') IS NOT NULL,
       pg_catalog.current_setting('log_checkpoints', true),
       pg_catalog.current_setting('lc_messages', true)";

    internal sealed class Checkpoint
    {
        [JsonPropertyName("kind")] public string Kind { get; init; } = string.Empty;

        [JsonPropertyName("started_at")] public DateTime StartedAt { get; init; }

        [JsonPropertyName("ended_at")] public DateTime? EndedAt { get; init; }

        [JsonPropertyName("reason")] public string? Reason { get; init; }

        [JsonPropertyName("flags")] public string? Flags { get; init; }

        [JsonPropertyName("buffers_written")] public int? BuffersWritten { get; init; }

        [JsonPropertyName("buffers_pct")] public float? BuffersPct { get; init; }

        [JsonPropertyName("slru_buffers_written")] public int? SlruBuffersWritten { get; init; }

        [JsonPropertyName("wal_files_added")] public int? WalFilesAdded { get; init; }

        [JsonPropertyName("wal_files_removed")] public int? WalFilesRemoved { get; init; }

        [JsonPropertyName("wal_files_recycled")] public int? WalFilesRecycled { get; init; }

        [JsonPropertyName("write_ms")] public long? WriteMs { get; init; }

        [JsonPropertyName("sync_ms")] public long? SyncMs { get; init; }

        [JsonPropertyName("total_ms")] public long? TotalMs { get; init; }

        [JsonPropertyName("files_synced")] public int? FilesSynced { get; init; }

        [JsonPropertyName("longest_sync_ms")] public long? LongestSyncMs { get; init; }

        [JsonPropertyName("average_sync_ms")] public long? AverageSyncMs { get; init; }

        [JsonPropertyName("distance_kb")] public long? DistanceKb { get; init; }

        [JsonPropertyName("estimate_kb")] public long? EstimateKb { get; init; }

        [JsonPropertyName("lsn")] public string? Lsn { get; init; }

        [JsonPropertyName("redo_lsn")] public string? RedoLsn { get; init; }

        [JsonPropertyName("unparsed")] public bool Unparsed { get; init; }
    }

    internal sealed class Hour
    {
        [JsonPropertyName("hour_start")] public DateTime HourStart { get; init; }

        [JsonPropertyName("checkpoints")] public long Checkpoints { get; init; }

        [JsonPropertyName("started_by_wal")] public long StartedByWal { get; init; }

        [JsonPropertyName("started_by_time")] public long StartedByTime { get; init; }

        [JsonPropertyName("not_completed")] public long NotCompleted { get; init; }

        [JsonPropertyName("max_files_synced")] public int? MaxFilesSynced { get; init; }

        [JsonPropertyName("longest_sync_ms")] public long? LongestSyncMs { get; init; }

        [JsonPropertyName("total_write_ms")] public long? TotalWriteMs { get; init; }

        [JsonPropertyName("total_sync_ms")] public long? TotalSyncMs { get; init; }
    }

    internal sealed class Report
    {
        [JsonPropertyName("window_hours")] public int WindowHours { get; init; }

        [JsonPropertyName("as_of")] public string AsOf { get; init; } = string.Empty;

        [JsonPropertyName("checkpoints_in_window")] public long CheckpointsInWindow { get; init; }

        [JsonPropertyName("checkpoints_unparsed")] public long CheckpointsUnparsed { get; init; }

        [JsonPropertyName("checkpoints_skipped")] public int CheckpointsSkipped { get; init; }

        [JsonPropertyName("note")] public string? Note { get; init; }

        [JsonPropertyName("hours")] public List<Hour> Hours { get; init; } = [];

        [JsonPropertyName("checkpoints")] public List<Checkpoint> Checkpoints { get; init; } = [];

        /// <summary>How many rows <see cref="Checkpoints"/> holds.</summary>
        [JsonPropertyName("checkpoints_returned")] public int CheckpointsReturned => Checkpoints.Count;

        /// <summary>True when checkpoints follow the last row this answer holds, whether <c>limit</c> or the response budget left them off.
        /// The way on is <c>offset</c> at <c>checkpoints_skipped</c> + <c>checkpoints_returned</c>.</summary>
        [JsonPropertyName("truncated")] public bool Truncated => CheckpointsSkipped + Checkpoints.Count < CheckpointsInWindow;

        [JsonPropertyName("budget_truncated")] public bool BudgetTruncated { get; init; }

        [JsonPropertyName("budget_note")] public string? BudgetNote { get; init; }
    }

    internal readonly record struct Settings(bool TableExists, string? LogCheckpoints, string? LcMessages);

    internal static async Task<Settings> GetSettingsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(SettingsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new Settings(
            reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2));
    }

    private static NpgsqlCommand WindowCommand(NpgsqlDataSource postgres, string sql, DateTime start, DateTime end)
    {
        var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        return command;
    }

    internal static async Task<(long Total, long Unparsed)> GetTotalAsync(
        NpgsqlDataSource postgres, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        await using var command = WindowCommand(postgres, TotalSql, start, end);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    internal static async Task<List<Hour>> GetHoursAsync(
        NpgsqlDataSource postgres, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var hours = new List<Hour>();
        await using var command = WindowCommand(postgres, HoursSql, start, end);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            hours.Add(new Hour
            {
                HourStart = DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Unspecified),
                Checkpoints = reader.GetInt64(1),
                StartedByWal = reader.GetInt64(2),
                StartedByTime = reader.GetInt64(3),
                NotCompleted = reader.GetInt64(4),
                MaxFilesSynced = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                LongestSyncMs = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                TotalWriteMs = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                TotalSyncMs = reader.IsDBNull(8) ? null : reader.GetInt64(8),
            });
        }

        return hours;
    }

    internal static async Task<List<Checkpoint>> GetRowsAsync(
        NpgsqlDataSource postgres, DateTime start, DateTime end, int limit, int offset, CancellationToken cancellationToken)
    {
        var rows = new List<Checkpoint>();
        await using var command = WindowCommand(postgres, RowsSql, start, end);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, limit);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, offset);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Checkpoint
            {
                Kind = reader.GetString(0),
                StartedAt = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Unspecified),
                EndedAt = reader.IsDBNull(2) ? null : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Unspecified),
                Reason = reader.IsDBNull(3) ? null : reader.GetString(3),
                Flags = reader.IsDBNull(4) ? null : reader.GetString(4),
                BuffersWritten = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                BuffersPct = reader.IsDBNull(6) ? null : reader.GetFloat(6),
                SlruBuffersWritten = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                WalFilesAdded = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                WalFilesRemoved = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                WalFilesRecycled = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                WriteMs = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                SyncMs = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                TotalMs = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                FilesSynced = reader.IsDBNull(14) ? null : reader.GetInt32(14),
                LongestSyncMs = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                AverageSyncMs = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                DistanceKb = reader.IsDBNull(17) ? null : reader.GetInt64(17),
                EstimateKb = reader.IsDBNull(18) ? null : reader.GetInt64(18),
                Lsn = reader.IsDBNull(19) ? null : reader.GetString(19),
                RedoLsn = reader.IsDBNull(20) ? null : reader.GetString(20),
                Unparsed = reader.GetBoolean(21),
            });
        }

        return rows;
    }
}
