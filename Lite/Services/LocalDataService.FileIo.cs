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
using PerformanceMonitor.Common;

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
    delta_stall_write_ms,
    sample_interval_seconds,
    collection_time
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
                DeltaStallWriteMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                SampleIntervalSeconds = reader.IsDBNull(11) ? null : reader.GetInt32(11),
                CollectionTime = reader.GetDateTime(12)
            });
        }

        return items;
    }

    /// <summary>
    /// The bucketed statement text (#4234), pulled out of <see cref="GetFileIoLatencyTrendAsync"/> so its shape
    /// is checkable without a live DuckDB.
    /// </summary>
    internal static readonly string FileIoLatencyTrendSql = $@"
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
),
rated AS (
    SELECT
        f.database_name,
        f.file_name,
        f.collection_time,
        /* #3540: a stored interval of 0 is the calculator's no-delta-knowable marker — nulled here (not
           filtered in the WHERE) so an unrated row still counts toward collection_count below. IS DISTINCT
           FROM 0 keeps pre-v60 rows (NULL: interval never recorded), which carry on reading exactly as they
           always did. */
        CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN f.delta_reads END AS rated_reads,
        CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN f.delta_writes END AS rated_writes,
        CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(f.delta_stall_read_ms AS DOUBLE PRECISION) END AS rated_stall_read_ms,
        CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(f.delta_stall_write_ms AS DOUBLE PRECISION) END AS rated_stall_write_ms,
        CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(COALESCE(f.delta_stall_queued_read_ms, 0) AS DOUBLE PRECISION) END AS rated_queued_read_ms,
        CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(COALESCE(f.delta_stall_queued_write_ms, 0) AS DOUBLE PRECISION) END AS rated_queued_write_ms
    FROM v_file_io_stats f
    JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
    WHERE f.server_id = $1
    AND   f.collection_time >= $2
    AND   f.collection_time <= $3
)
SELECT
    database_name,
    file_name,
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    CASE WHEN SUM(rated_reads) > 0 THEN SUM(rated_stall_read_ms) / SUM(rated_reads) ELSE 0 END AS avg_read_latency_ms,
    CASE WHEN SUM(rated_writes) > 0 THEN SUM(rated_stall_write_ms) / SUM(rated_writes) ELSE 0 END AS avg_write_latency_ms,
    CASE WHEN SUM(rated_reads) > 0 THEN SUM(rated_queued_read_ms) / SUM(rated_reads) ELSE 0 END AS avg_queued_read_latency_ms,
    CASE WHEN SUM(rated_writes) > 0 THEN SUM(rated_queued_write_ms) / SUM(rated_writes) ELSE 0 END AS avg_queued_write_latency_ms,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM rated
GROUP BY database_name, file_name, 3
HAVING COUNT(rated_reads) > 0
ORDER BY database_name, file_name, 3";

    /// <summary>
    /// Gets file I/O latency trend data broken down by file for charting (top 10 files by I/O activity).
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (<c>seriesCount</c>
    /// always 1 into <see cref="TrendBuckets.AutoMinutes"/>). The top-10 ranking (<c>top_files</c>) stays an
    /// unbucketed scan of the whole call window, exactly as before — only the per-collection SELECT beneath it
    /// buckets. Latency is a ratio (summed stall over summed operations), not a rate, so unlike throughput it
    /// needs no interval arithmetic; a <c>rated</c> row is simply one whose stored interval is not the #3540
    /// sentinel 0, nulled (not filtered) so an unrated row still counts toward <c>collection_count</c> — the
    /// same reason <see cref="WaitTrendsSql"/> keeps its unrated rows in its CTE instead of excluding them
    /// outright. <c>HAVING</c> drops a bucket with no rated row, so it stays absent, not 0. When EVERY bucket
    /// the whole call returned holds exactly one physical collection, each point is stamped at its bucket's raw
    /// <c>first_collection_time</c> instead of the <c>time_bucket</c> grid line; a single merged bucket
    /// anywhere (any file) keeps <c>bucket_start</c> throughout.</para>
    /// </summary>
    public async Task<List<FileIoTrendPoint>> GetFileIoLatencyTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetFileIoLatencyTrendAsync", "v_file_io_stats top-10 files, bucketed");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = FileIoLatencyTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string DatabaseName, string FileName, DateTime BucketStart, DateTime FirstCollectionTime, double ReadLatency, double WriteLatency, double QueuedReadLatency, double QueuedWriteLatency)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (reader.GetInt64(8) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.GetDateTime(2),
                reader.GetDateTime(7),
                reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6))));
        }

        var items = new List<FileIoTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new FileIoTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                DatabaseName = row.DatabaseName,
                FileName = row.FileName,
                AvgReadLatencyMs = row.ReadLatency,
                AvgWriteLatencyMs = row.WriteLatency,
                AvgQueuedReadLatencyMs = row.QueuedReadLatency,
                AvgQueuedWriteLatencyMs = row.QueuedWriteLatency
            });
        }

        return items;
    }

    /// <summary>
    /// Whether this server has EVER recorded a file I/O sample, ignoring any window.
    /// <para>Lets an empty I/O trend say WHICH kind of nothing it found — see
    /// <c>LocalDataService.HasAnyMemoryStatAsync</c> for the reasoning. Reads <c>v_file_io_stats</c>, the
    /// same source <see cref="GetFileIoLatencyTrendAsync"/> reads. Darling's twin is
    /// <c>DarlingTrendReader.HasAnyFileIoStatAsync</c>.</para>
    /// </summary>
    public async Task<bool> HasAnyFileIoStatAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_file_io_stats
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// The bucketed statement text (#4234), pulled out of <see cref="GetFileIoThroughputTrendAsync"/> so its
    /// shape is checkable without a live DuckDB.
    /// </summary>
    internal static readonly string FileIoThroughputTrendSql = $@"
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
        /* #3540: the STORED interval where the row has one — 0 (no delta knowable) becomes NULL through
           NULLIF. NULL (a pre-v60 row that never recorded one) falls back to the LAG this read always used. */
        CASE WHEN f.sample_interval_seconds IS NULL
             THEN EXTRACT(EPOCH FROM (f.collection_time - LAG(f.collection_time) OVER (
                      PARTITION BY f.server_id, f.database_name, f.file_name
                      ORDER BY f.collection_time
                  )))
             ELSE NULLIF(f.sample_interval_seconds, 0)
        END AS interval_seconds
    FROM v_file_io_stats f
    JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
    WHERE f.server_id = $1
    AND   f.collection_time >= $2
    AND   f.collection_time <= $3
),
rated AS (
    SELECT
        collection_time,
        file_label,
        CASE WHEN interval_seconds > 0 THEN CAST(delta_read_bytes AS DOUBLE PRECISION) END AS rated_read_bytes,
        CASE WHEN interval_seconds > 0 THEN CAST(delta_write_bytes AS DOUBLE PRECISION) END AS rated_write_bytes,
        CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds
    FROM with_interval
)
SELECT
    file_label,
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    SUM(rated_read_bytes) / SUM(rated_seconds) / 1048576.0 AS read_mb_per_sec,
    SUM(rated_write_bytes) / SUM(rated_seconds) / 1048576.0 AS write_mb_per_sec,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM rated
GROUP BY file_label, 2
HAVING COUNT(rated_seconds) > 0
ORDER BY file_label, 2";

    /// <summary>
    /// Gets file I/O throughput trend data (MB/s) broken down by file for charting.
    /// Divides by each row's stored sample_interval_seconds; a pre-v60 row that never recorded one falls
    /// back to the LAG() over collection_time this read always used (#3540).
    /// <para>#3653 (#3540 rule 1, readers NULL-not-0 on unknowable): the two rate arms end at <c>END</c>,
    /// not <c>ELSE 0</c>.</para>
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES. The LAG-derived
    /// interval is computed once over the RAW per-collection rows (<c>with_interval</c>), before bucketing —
    /// a bucket's rate is summed bytes over summed rated seconds (time-weighted, never an average of
    /// per-collection rates), and a bucket with no rated collection is dropped (<c>HAVING</c>), same as the
    /// per-collection read always dropped that collection. Singleton stamping follows
    /// <see cref="WaitTrendsSql"/>'s rule.</para>
    /// </summary>
    public async Task<List<FileIoThroughputPoint>> GetFileIoThroughputTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetFileIoThroughputTrendAsync", "v_file_io_stats top-10 files by bytes, bucketed");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = FileIoThroughputTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string FileLabel, DateTime BucketStart, DateTime FirstCollectionTime, double ReadMbPerSec, double WriteMbPerSec)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (reader.GetInt64(5) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(4),
                /* ToDouble, not GetDouble: a SUM over bytes can come back as a boxed DuckDB HUGEINT — see
                   GetWaitStatsTrendsByTypesAsync's own sum columns for the same guard. */
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3))));
        }

        var items = new List<FileIoThroughputPoint>();
        foreach (var row in rows)
        {
            items.Add(new FileIoThroughputPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                FileLabel = row.FileLabel,
                ReadMbPerSec = row.ReadMbPerSec,
                WriteMbPerSec = row.WriteMbPerSec
            });
        }

        return items;
    }

    /// <summary>
    /// The bucketed statement text (#4234), pulled out of <see cref="GetTempDbFileIoTrendAsync"/> so its shape
    /// is checkable without a live DuckDB.
    /// </summary>
    internal static readonly string TempDbFileIoTrendSql = $@"
WITH rated AS (
    SELECT
        file_name,
        collection_time,
        /* #3540: see GetFileIoLatencyTrendAsync's rated CTE for why this nulls rather than filters. */
        CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN delta_reads END AS rated_reads,
        CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN delta_writes END AS rated_writes,
        CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(delta_stall_read_ms AS DOUBLE PRECISION) END AS rated_stall_read_ms,
        CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(delta_stall_write_ms AS DOUBLE PRECISION) END AS rated_stall_write_ms
    FROM v_file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   database_name = 'tempdb'
)
SELECT
    file_name,
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    CASE WHEN SUM(rated_reads) > 0 THEN SUM(rated_stall_read_ms) / SUM(rated_reads) ELSE 0 END AS avg_read_latency_ms,
    CASE WHEN SUM(rated_writes) > 0 THEN SUM(rated_stall_write_ms) / SUM(rated_writes) ELSE 0 END AS avg_write_latency_ms,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM rated
GROUP BY file_name, 2
HAVING COUNT(rated_reads) > 0
ORDER BY file_name, 2";

    /// <summary>
    /// Gets file I/O latency trend data for tempdb files only, broken down by file name.
    /// <para>#4234: bucketed the same way as <see cref="GetFileIoLatencyTrendAsync"/> — see that method's doc
    /// for the rated/HAVING/singleton-stamping rules; this read has no top-10 ranking pass (tempdb's own file
    /// count is already small) and no queued-latency columns (this table's caller chart never drew them).
    /// <c>DatabaseName</c> carries the FILE name here, not the database (always <c>tempdb</c>) — preserved
    /// exactly as the pre-#4234 reader mapped it, since <c>ServerTab.Charts.UpdateTempDbFileIoChart</c> groups
    /// its series on that field.</para>
    /// </summary>
    public async Task<List<FileIoTrendPoint>> GetTempDbFileIoTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetTempDbFileIoTrendAsync", "v_file_io_stats tempdb files, bucketed");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = TempDbFileIoTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string FileName, DateTime BucketStart, DateTime FirstCollectionTime, double ReadLatency, double WriteLatency)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (reader.GetInt64(5) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(4),
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3))));
        }

        var items = new List<FileIoTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new FileIoTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                DatabaseName = row.FileName,
                AvgReadLatencyMs = row.ReadLatency,
                AvgWriteLatencyMs = row.WriteLatency
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
    /// <summary>The snapshot this row belongs to (#3541 A10); the deltas cover the
    /// <see cref="SampleIntervalSeconds"/> ending here.</summary>
    public DateTime CollectionTime { get; set; }
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

    /// <summary>#3540: the measured seconds the deltas accrued over. 0 is the calculator's "no delta
    /// knowable" marker (first sighting, counter reset, gap past the policy); null is a pre-v60 row that
    /// never recorded one and keeps the pre-#3540 reading.</summary>
    public int? SampleIntervalSeconds { get; set; }

    /// <summary>True when the row's deltas are the unknowable marker — a stored interval of exactly 0.</summary>
    public bool IsUnknowable => SampleIntervalSeconds == 0;

    /// <summary>Stall per operation, or null when the row is <see cref="IsUnknowable"/> — a restart's fabricated
    /// (0 stall, 0 reads) used to read here as a confident 0 ms (#3540). A real interval with no reads still
    /// reads 0, the pre-existing convention for an idle file.</summary>
    public double? AvgReadLatencyMs => IsUnknowable ? null : DeltaReads > 0 ? (double)DeltaStallReadMs / DeltaReads : 0;
    public double? AvgWriteLatencyMs => IsUnknowable ? null : DeltaWrites > 0 ? (double)DeltaStallWriteMs / DeltaWrites : 0;
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
