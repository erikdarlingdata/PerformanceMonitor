using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

public partial class DuckDbFactCollector
{
    /// <summary>
    /// Collects total database data size from file_io_stats.
    /// Sums the latest size_mb across all database files for the server.
    /// </summary>
    private async Task CollectDatabaseSizeFactAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT database_name, file_name, size_mb,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_name ORDER BY collection_time DESC) AS rn
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time <= $2
    AND   size_mb > 0
)
SELECT SUM(size_mb) AS total_size_mb
FROM latest
WHERE rn = 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalSize = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            if (totalSize > 0)
                facts.Add(new Fact { Source = "config", Key = "DATABASE_TOTAL_SIZE_MB", Value = totalSize, ServerId = context.ServerId });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects I/O latency from file_io_stats delta columns.
    /// Computes average read and write latency across all database files.
    /// </summary>
    private async Task CollectIoLatencyFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    SUM(delta_stall_read_ms) AS total_stall_read_ms,
    SUM(delta_reads) AS total_reads,
    SUM(delta_stall_write_ms) AS total_stall_write_ms,
    SUM(delta_writes) AS total_writes
FROM v_file_io_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   (delta_reads > 0 OR delta_writes > 0)";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalStallReadMs = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var totalReads = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalStallWriteMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalWrites = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));

            if (totalReads > 0)
            {
                var avgReadLatency = (double)totalStallReadMs / totalReads;
                facts.Add(new Fact
                {
                    Source = "io",
                    Key = "IO_READ_LATENCY_MS",
                    Value = avgReadLatency,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["avg_read_latency_ms"] = avgReadLatency,
                        ["total_stall_read_ms"] = totalStallReadMs,
                        ["total_reads"] = totalReads
                    }
                });
            }

            if (totalWrites > 0)
            {
                var avgWriteLatency = (double)totalStallWriteMs / totalWrites;
                facts.Add(new Fact
                {
                    Source = "io",
                    Key = "IO_WRITE_LATENCY_MS",
                    Value = avgWriteLatency,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["avg_write_latency_ms"] = avgWriteLatency,
                        ["total_stall_write_ms"] = totalStallWriteMs,
                        ["total_writes"] = totalWrites
                    }
                });
            }
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects TempDB usage facts: max usage, version store size, and unallocated space.
    /// Value is max total_reserved_mb over the period.
    /// </summary>
    private async Task CollectTempDbFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    MAX(total_reserved_mb) AS max_total_reserved_mb,
    MAX(user_object_reserved_mb) AS max_user_object_mb,
    MAX(internal_object_reserved_mb) AS max_internal_object_mb,
    MAX(version_store_reserved_mb) AS max_version_store_mb,
    MIN(unallocated_mb) AS min_unallocated_mb,
    AVG(total_reserved_mb) AS avg_total_reserved_mb
FROM v_tempdb_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var maxReserved = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var maxUserObj = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var maxInternalObj = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxVersionStore = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var minUnallocated = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));
            var avgReserved = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));

            if (maxReserved <= 0) return;

            // TempDB usage as fraction of total space (reserved + unallocated)
            var totalSpace = maxReserved + minUnallocated;
            var usageFraction = totalSpace > 0 ? maxReserved / totalSpace : 0;

            facts.Add(new Fact
            {
                Source = "tempdb",
                Key = "TEMPDB_USAGE",
                Value = usageFraction,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["max_reserved_mb"] = maxReserved,
                    ["avg_reserved_mb"] = avgReserved,
                    ["max_user_object_mb"] = maxUserObj,
                    ["max_internal_object_mb"] = maxInternalObj,
                    ["max_version_store_mb"] = maxVersionStore,
                    ["min_unallocated_mb"] = minUnallocated,
                    ["usage_fraction"] = usageFraction
                }
            });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects the percent-autogrowth-on-large-files config fact (WS3): data/log files set
    /// to grow in PERCENTAGE steps that are also large (>= 10 GB), where a single growth is a
    /// huge, stalling allocation. Reads the latest snapshot per file from database_size_stats,
    /// excludes system databases, and emits ONE aggregate FILE_AUTOGROWTH_PERCENT fact carrying
    /// the offending-file/database counts (the per-file detail + copy-paste fix is attached
    /// later by the drill-down collector).
    /// </summary>
    private async Task CollectFileAutogrowthFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT database_name, file_id, total_size_mb, is_percent_growth,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_id ORDER BY collection_time DESC) AS rn
    FROM database_size_stats
    WHERE server_id = $1
)
SELECT
    COUNT(*) AS file_count,
    COUNT(DISTINCT database_name) AS database_count
FROM latest
WHERE rn = 1
AND   is_percent_growth = true
AND   total_size_mb >= 10240
AND   database_name NOT IN ('master', 'msdb', 'model', 'tempdb')";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var fileCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (fileCount == 0) return;

            var databaseCount = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));

            facts.Add(new Fact
            {
                Source = "config",
                Key = "FILE_AUTOGROWTH_PERCENT",
                Value = fileCount,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["file_count"] = fileCount,
                    ["database_count"] = databaseCount
                }
            });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects disk space facts from database_size_stats: volume free space, file sizes.
    /// </summary>
    private async Task CollectDiskSpaceFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT volume_mount_point, volume_total_mb, volume_free_mb,
           ROW_NUMBER() OVER (PARTITION BY volume_mount_point ORDER BY collection_time DESC) AS rn
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time <= $2
    AND   volume_total_mb > 0
)
SELECT
    MIN(volume_free_mb * 1.0 / volume_total_mb) AS min_free_pct,
    MIN(volume_free_mb) AS min_free_mb,
    COUNT(DISTINCT volume_mount_point) AS volume_count,
    SUM(volume_total_mb) AS total_volume_mb,
    SUM(volume_free_mb) AS total_free_mb
FROM latest WHERE rn = 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var minFreePct = reader.IsDBNull(0) ? 1.0 : Convert.ToDouble(reader.GetValue(0));
            var minFreeMb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var volumeCount = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalVolumeMb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var totalFreeMb = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));

            if (volumeCount == 0) return;

            facts.Add(new Fact
            {
                Source = "disk",
                Key = "DISK_SPACE",
                Value = minFreePct,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["min_free_pct"] = minFreePct,
                    ["min_free_mb"] = minFreeMb,
                    ["volume_count"] = volumeCount,
                    ["total_volume_mb"] = totalVolumeMb,
                    ["total_free_mb"] = totalFreeMb
                }
            });
        }
        catch { /* Table may not exist or have no data */ }
    }
}
