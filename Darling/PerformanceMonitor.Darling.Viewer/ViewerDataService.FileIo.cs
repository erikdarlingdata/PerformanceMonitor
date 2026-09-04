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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One file's I/O-latency point for the File I/O tab's Latency sub-tab — a mirror of Lite's
/// <c>FileIoTrendPoint</c> (<c>LocalDataService.FileIo.cs</c>). The read averages stall-ms / op per
/// (collection_time, database_name, file_name); the chart groups on <c>database_name.file_name</c>,
/// sums read+write latency to rank the top files, and overlays the queued-I/O latencies as dashed
/// lines when any are present.
/// </summary>
public sealed record FileIoLatencyPoint(
    DateTime CollectionTime,
    string DatabaseName,
    string FileName,
    double AvgReadLatencyMs,
    double AvgWriteLatencyMs,
    double AvgQueuedReadLatencyMs,
    double AvgQueuedWriteLatencyMs);

/// <summary>
/// One file's throughput point (MB/s) for the File I/O tab's Throughput sub-tab — a mirror of Lite's
/// <c>FileIoThroughputPoint</c>. <c>file_label</c> is the pre-concatenated <c>database_name.file_name</c>
/// the read builds, and the per-second rates come from the delta bytes divided by the LAG-derived
/// collection interval.
/// </summary>
public sealed record FileIoThroughputPoint(
    DateTime CollectionTime,
    string FileLabel,
    double ReadMbPerSec,
    double WriteMbPerSec);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The File I/O latency read — Lite's <c>GetFileIoLatencyTrendAsync</c> ported to Postgres. Reads
    /// <c>v_file_io_stats</c> (a <c>SELECT *</c> passthrough over <c>file_io_stats</c>) exactly as Lite's
    /// view-based query does: a <c>top_files</c> CTE picks the 10 busiest (database, file) pairs by total
    /// delta ops over the window, then per-collection average read/write (and queued read/write) latency
    /// is computed as stall-ms / op with the delta-stall sums CAST to double precision before division.
    /// The queued-stall columns are COALESCE'd to 0 so a server whose build predates them still reads 0.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string FileIoLatencyTrendSql = """
        WITH top_files AS (
            SELECT database_name, file_name
            FROM v_file_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   (delta_reads > 0 OR delta_writes > 0)
            GROUP BY database_name, file_name
            ORDER BY SUM(delta_reads + delta_writes) DESC
            LIMIT 10
        )
        SELECT
            f.collection_time,
            f.database_name,
            f.file_name,
            CASE WHEN SUM(f.delta_reads) > 0
                 THEN SUM(CAST(f.delta_stall_read_ms AS double precision)) / SUM(f.delta_reads)
                 ELSE 0 END AS avg_read_latency_ms,
            CASE WHEN SUM(f.delta_writes) > 0
                 THEN SUM(CAST(f.delta_stall_write_ms AS double precision)) / SUM(f.delta_writes)
                 ELSE 0 END AS avg_write_latency_ms,
            CASE WHEN SUM(f.delta_reads) > 0
                 THEN SUM(CAST(COALESCE(f.delta_stall_queued_read_ms, 0) AS double precision)) / SUM(f.delta_reads)
                 ELSE 0 END AS avg_queued_read_latency_ms,
            CASE WHEN SUM(f.delta_writes) > 0
                 THEN SUM(CAST(COALESCE(f.delta_stall_queued_write_ms, 0) AS double precision)) / SUM(f.delta_writes)
                 ELSE 0 END AS avg_queued_write_latency_ms
        FROM v_file_io_stats f
        JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
        WHERE f.server_id = $1
        AND   f.collection_time >= $2
        AND   f.collection_time <= $3
        GROUP BY f.collection_time, f.database_name, f.file_name
        ORDER BY f.collection_time, f.database_name, f.file_name
        """;

    /// <summary>
    /// The File I/O throughput read — Lite's <c>GetFileIoThroughputTrendAsync</c> ported to Postgres
    /// verbatim (LAG + EXTRACT(EPOCH ...) run identically on PG). A <c>top_files</c> CTE picks the 10
    /// busiest (database, file) pairs by total delta bytes, a <c>with_interval</c> CTE derives each
    /// row's collection interval from the LAG of the prior collection_time (partitioned by
    /// server/database/file), and the per-second MB rate is delta-bytes / interval-seconds / 1 MiB.
    /// Rows with no prior sample (NULL interval) are dropped so the first point per file isn't plotted.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string FileIoThroughputTrendSql = """
        WITH top_files AS (
            SELECT database_name, file_name
            FROM v_file_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   (delta_read_bytes > 0 OR delta_write_bytes > 0)
            GROUP BY database_name, file_name
            ORDER BY SUM(delta_read_bytes + delta_write_bytes) DESC
            LIMIT 10
        ),
        with_interval AS (
            SELECT
                f.collection_time,
                f.database_name || '.' || f.file_name AS file_label,
                f.delta_read_bytes,
                f.delta_write_bytes,
                EXTRACT(EPOCH FROM (f.collection_time - LAG(f.collection_time) OVER (
                    PARTITION BY f.server_id, f.database_name, f.file_name
                    ORDER BY f.collection_time
                ))) AS interval_seconds
            FROM v_file_io_stats f
            JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
            WHERE f.server_id = $1
            AND   f.collection_time >= $2
            AND   f.collection_time <= $3
        )
        SELECT
            collection_time,
            file_label,
            CASE WHEN interval_seconds > 0
                 THEN CAST(delta_read_bytes AS double precision) / interval_seconds / 1048576.0
                 ELSE 0 END AS read_mb_per_sec,
            CASE WHEN interval_seconds > 0
                 THEN CAST(delta_write_bytes AS double precision) / interval_seconds / 1048576.0
                 ELSE 0 END AS write_mb_per_sec
        FROM with_interval
        WHERE interval_seconds IS NOT NULL AND interval_seconds > 0
        ORDER BY collection_time, file_label
        """;

    /// <summary>
    /// Per-file I/O-latency points (top 10 files by delta ops) for one server over the window, for the
    /// File I/O tab's Latency sub-tab.
    /// </summary>
    public async Task<List<FileIoLatencyPoint>> GetFileIoLatencyTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<FileIoLatencyPoint>();

        await using var command = _dataSource.CreateCommand(FileIoLatencyTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new FileIoLatencyPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetDouble(6)));
        }

        return items;
    }

    /// <summary>
    /// Per-file throughput points (MB/s, top 10 files by delta bytes) for one server over the window,
    /// for the File I/O tab's Throughput sub-tab.
    /// </summary>
    public async Task<List<FileIoThroughputPoint>> GetFileIoThroughputTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<FileIoThroughputPoint>();

        await using var command = _dataSource.CreateCommand(FileIoThroughputTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new FileIoThroughputPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3)));
        }

        return items;
    }
}
