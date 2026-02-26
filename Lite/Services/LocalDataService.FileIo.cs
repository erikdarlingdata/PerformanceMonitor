/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest file I/O stats snapshot with computed latency.
    /// </summary>
    public async Task<List<FileIoRow>> GetLatestFileIoStatsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    database_name,
    file_name,
    file_type,
    physical_name,
    size_mb,
    delta_reads,
    delta_writes,
    delta_read_bytes,
    delta_write_bytes,
    delta_stall_read_ms,
    delta_stall_write_ms
FROM v_file_io_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_file_io_stats WHERE server_id = $1)
ORDER BY (delta_stall_read_ms + delta_stall_write_ms) DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<FileIoRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new FileIoRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileType = reader.IsDBNull(2) ? "" : reader.GetString(2),
                PhysicalName = reader.IsDBNull(3) ? "" : reader.GetString(3),
                SizeMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                DeltaReads = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DeltaWrites = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                DeltaReadBytes = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                DeltaWriteBytes = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                DeltaStallReadMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                DeltaStallWriteMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets file I/O latency trend data broken down by file for charting (top 10 files by I/O activity).
    /// </summary>
    public async Task<List<FileIoTrendPoint>> GetFileIoLatencyTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
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
         THEN SUM(CAST(f.delta_stall_read_ms AS DOUBLE)) / SUM(f.delta_reads)
         ELSE 0 END AS avg_read_latency_ms,
    CASE WHEN SUM(f.delta_writes) > 0
         THEN SUM(CAST(f.delta_stall_write_ms AS DOUBLE)) / SUM(f.delta_writes)
         ELSE 0 END AS avg_write_latency_ms,
    CASE WHEN SUM(f.delta_reads) > 0
         THEN SUM(CAST(COALESCE(f.delta_stall_queued_read_ms, 0) AS DOUBLE)) / SUM(f.delta_reads)
         ELSE 0 END AS avg_queued_read_latency_ms,
    CASE WHEN SUM(f.delta_writes) > 0
         THEN SUM(CAST(COALESCE(f.delta_stall_queued_write_ms, 0) AS DOUBLE)) / SUM(f.delta_writes)
         ELSE 0 END AS avg_queued_write_latency_ms
FROM v_file_io_stats f
JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
WHERE f.server_id = $1
AND   f.collection_time >= $2
AND   f.collection_time <= $3
GROUP BY f.collection_time, f.database_name, f.file_name
ORDER BY f.collection_time, f.database_name, f.file_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<FileIoTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new FileIoTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                AvgReadLatencyMs = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                AvgWriteLatencyMs = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                AvgQueuedReadLatencyMs = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvgQueuedWriteLatencyMs = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets file I/O throughput trend data (MB/s) broken down by file for charting.
    /// Uses LAG() window function to compute collection interval for per-second calculation.
    /// </summary>
    public async Task<List<FileIoThroughputPoint>> GetFileIoThroughputTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
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
         THEN CAST(delta_read_bytes AS DOUBLE) / interval_seconds / 1048576.0
         ELSE 0 END AS read_mb_per_sec,
    CASE WHEN interval_seconds > 0
         THEN CAST(delta_write_bytes AS DOUBLE) / interval_seconds / 1048576.0
         ELSE 0 END AS write_mb_per_sec
FROM with_interval
WHERE interval_seconds IS NOT NULL AND interval_seconds > 0
ORDER BY collection_time, file_label";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<FileIoThroughputPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new FileIoThroughputPoint
            {
                CollectionTime = reader.GetDateTime(0),
                FileLabel = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ReadMbPerSec = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                WriteMbPerSec = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets file I/O latency trend data for tempdb files only, broken down by file name.
    /// </summary>
    public async Task<List<FileIoTrendPoint>> GetTempDbFileIoTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    file_name,
    CASE WHEN SUM(delta_reads) > 0 THEN SUM(CAST(delta_stall_read_ms AS DOUBLE)) / SUM(delta_reads) ELSE 0 END AS avg_read_latency_ms,
    CASE WHEN SUM(delta_writes) > 0 THEN SUM(CAST(delta_stall_write_ms AS DOUBLE)) / SUM(delta_writes) ELSE 0 END AS avg_write_latency_ms
FROM v_file_io_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   database_name = 'tempdb'
GROUP BY collection_time, file_name
ORDER BY collection_time, file_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<FileIoTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new FileIoTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                AvgReadLatencyMs = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                AvgWriteLatencyMs = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3))
            });
        }

        return items;
    }
}

public class FileIoTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string DatabaseName { get; set; } = "";
    public string FileName { get; set; } = "";
    public double AvgReadLatencyMs { get; set; }
    public double AvgWriteLatencyMs { get; set; }
    public double AvgQueuedReadLatencyMs { get; set; }
    public double AvgQueuedWriteLatencyMs { get; set; }
}

public class FileIoThroughputPoint
{
    public DateTime CollectionTime { get; set; }
    public string FileLabel { get; set; } = "";
    public double ReadMbPerSec { get; set; }
    public double WriteMbPerSec { get; set; }
}

public class FileIoRow
{
    public string DatabaseName { get; set; } = "";
    public string FileName { get; set; } = "";
    public string FileType { get; set; } = "";
    public string PhysicalName { get; set; } = "";
    public double SizeMb { get; set; }
    public long DeltaReads { get; set; }
    public long DeltaWrites { get; set; }
    public long DeltaReadBytes { get; set; }
    public long DeltaWriteBytes { get; set; }
    public long DeltaStallReadMs { get; set; }
    public long DeltaStallWriteMs { get; set; }
    public double AvgReadLatencyMs => DeltaReads > 0 ? (double)DeltaStallReadMs / DeltaReads : 0;
    public double AvgWriteLatencyMs => DeltaWrites > 0 ? (double)DeltaStallWriteMs / DeltaWrites : 0;
    public string SizeFormatted => SizeMb >= 1024 ? $"{SizeMb / 1024:F1} GB" : $"{SizeMb:F0} MB";
    public string ReadBytesFormatted => FormatBytes(DeltaReadBytes);
    public string WriteBytesFormatted => FormatBytes(DeltaWriteBytes);

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1048576) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1073741824) return $"{bytes / 1048576.0:F1} MB";
        return $"{bytes / 1073741824.0:F2} GB";
    }
}
