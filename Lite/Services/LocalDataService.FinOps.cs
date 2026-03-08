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
    /// Gets the latest database size snapshot per server per file (cross-server).
    /// </summary>
    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestAsync()
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    server_name,
    database_name,
    file_type_desc,
    file_name,
    total_size_mb,
    used_size_mb,
    volume_mount_point,
    volume_total_mb,
    volume_free_mb,
    recovery_model_desc
FROM database_size_stats
WHERE (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time)
    FROM database_size_stats
    GROUP BY server_id
)
ORDER BY server_name, database_name, file_type_desc, file_name";

        var items = new List<DatabaseSizeRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseSizeRow
            {
                ServerName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileTypeDesc = reader.IsDBNull(2) ? "" : reader.GetString(2),
                FileName = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TotalSizeMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                UsedSizeMb = reader.IsDBNull(5) ? null : Convert.ToDecimal(reader.GetValue(5)),
                VolumeMountPoint = reader.IsDBNull(6) ? null : reader.GetString(6),
                VolumeTotalMb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                VolumeFreeMb = reader.IsDBNull(8) ? null : Convert.ToDecimal(reader.GetValue(8)),
                RecoveryModel = reader.IsDBNull(9) ? null : reader.GetString(9)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest server properties snapshot per server (cross-server).
    /// </summary>
    public async Task<List<ServerPropertyRow>> GetServerPropertiesLatestAsync()
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    server_name,
    edition,
    product_version,
    product_level,
    product_update_level,
    engine_edition,
    cpu_count,
    physical_memory_mb,
    socket_count,
    cores_per_socket,
    is_hadr_enabled,
    is_clustered
FROM server_properties
WHERE (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time)
    FROM server_properties
    GROUP BY server_id
)
ORDER BY server_name";

        var items = new List<ServerPropertyRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ServerPropertyRow
            {
                ServerName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                Edition = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ProductVersion = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ProductLevel = reader.IsDBNull(3) ? null : reader.GetString(3),
                ProductUpdateLevel = reader.IsDBNull(4) ? null : reader.GetString(4),
                EngineEdition = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                CpuCount = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                PhysicalMemoryMb = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7)),
                SocketCount = reader.IsDBNull(8) ? null : Convert.ToInt32(reader.GetValue(8)),
                CoresPerSocket = reader.IsDBNull(9) ? null : Convert.ToInt32(reader.GetValue(9)),
                IsHadrEnabled = reader.IsDBNull(10) ? null : reader.GetBoolean(10),
                IsClustered = reader.IsDBNull(11) ? null : reader.GetBoolean(11)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets database size trend (total_size_mb per database per collection) for a specific server.
    /// </summary>
    public async Task<List<DatabaseSizeTrendPoint>> GetDatabaseSizeTrendAsync(int serverId, int daysBack = 30)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddDays(-daysBack);

        command.CommandText = @"
SELECT
    collection_time,
    database_name,
    SUM(total_size_mb) AS total_size_mb
FROM database_size_stats
WHERE server_id = $1
AND   collection_time >= $2
GROUP BY collection_time, database_name
ORDER BY collection_time, database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<DatabaseSizeTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseSizeTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalSizeMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2))
            });
        }

        return items;
    }
    /// <summary>
    /// Computes utilization efficiency from cpu_utilization_stats + memory_stats (last 24 hours).
    /// </summary>
    public async Task<UtilizationEfficiencyRow?> GetUtilizationEfficiencyAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-24);

        command.CommandText = @"
WITH cpu_stats AS (
    SELECT
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct,
        COUNT(*) AS cpu_samples
    FROM cpu_utilization_stats
    WHERE server_id = $1
    AND   collection_time >= $2
),
mem_latest AS (
    SELECT
        total_server_memory_mb,
        target_server_memory_mb,
        total_physical_memory_mb,
        buffer_pool_mb,
        CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0) AS memory_ratio
    FROM memory_stats
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
)
SELECT
    c.avg_cpu_pct,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    c.cpu_samples,
    m.total_server_memory_mb,
    m.target_server_memory_mb,
    m.total_physical_memory_mb,
    m.buffer_pool_mb,
    m.memory_ratio
FROM cpu_stats c
CROSS JOIN mem_latest m";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        var avgCpu = reader.IsDBNull(0) ? 0m : Convert.ToDecimal(reader.GetValue(0));
        var maxCpu = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
        var p95Cpu = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2));
        var memRatio = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8));

        var status = "RIGHT_SIZED";
        if (avgCpu < 15 && maxCpu < 40 && memRatio < 0.5m)
            status = "OVER_PROVISIONED";
        else if (p95Cpu > 85 || memRatio > 0.95m)
            status = "UNDER_PROVISIONED";

        return new UtilizationEfficiencyRow
        {
            AvgCpuPct = avgCpu,
            MaxCpuPct = maxCpu,
            P95CpuPct = p95Cpu,
            CpuSamples = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3)),
            TotalMemoryMb = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
            TargetMemoryMb = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
            PhysicalMemoryMb = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
            BufferPoolMb = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
            MemoryRatio = memRatio,
            ProvisioningStatus = status
        };
    }

    /// <summary>
    /// Computes per-database resource usage from query_stats + file_io_stats deltas (last 24 hours).
    /// </summary>
    public async Task<List<DatabaseResourceUsageRow>> GetDatabaseResourceUsageAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-24);

        command.CommandText = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000 AS cpu_time_ms,
        SUM(delta_logical_reads) AS logical_reads,
        SUM(delta_physical_reads) AS physical_reads,
        SUM(delta_logical_writes) AS logical_writes,
        SUM(delta_execution_count) AS execution_count
    FROM query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    GROUP BY database_name
),
io AS (
    SELECT
        database_name,
        SUM(delta_read_bytes) / 1048576.0 AS io_read_mb,
        SUM(delta_write_bytes) / 1048576.0 AS io_write_mb,
        SUM(delta_stall_read_ms + delta_stall_write_ms) AS io_stall_ms
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_read_bytes IS NOT NULL
    GROUP BY database_name
),
combined AS (
    SELECT
        COALESCE(w.database_name, i.database_name) AS database_name,
        COALESCE(w.cpu_time_ms, 0) AS cpu_time_ms,
        COALESCE(w.logical_reads, 0) AS logical_reads,
        COALESCE(w.physical_reads, 0) AS physical_reads,
        COALESCE(w.logical_writes, 0) AS logical_writes,
        COALESCE(w.execution_count, 0) AS execution_count,
        COALESCE(i.io_read_mb, 0) AS io_read_mb,
        COALESCE(i.io_write_mb, 0) AS io_write_mb,
        COALESCE(i.io_stall_ms, 0) AS io_stall_ms
    FROM workload w
    FULL JOIN io i ON i.database_name = w.database_name
),
totals AS (
    SELECT
        NULLIF(SUM(cpu_time_ms), 0) AS total_cpu,
        NULLIF(SUM(io_read_mb + io_write_mb), 0) AS total_io
    FROM combined
)
SELECT
    c.database_name,
    c.cpu_time_ms,
    c.logical_reads,
    c.physical_reads,
    c.logical_writes,
    c.execution_count,
    CAST(c.io_read_mb AS DECIMAL(19,2)),
    CAST(c.io_write_mb AS DECIMAL(19,2)),
    c.io_stall_ms,
    CAST(c.cpu_time_ms * 100.0 / t.total_cpu AS DECIMAL(5,2)) AS pct_cpu_share,
    CAST((c.io_read_mb + c.io_write_mb) * 100.0 / t.total_io AS DECIMAL(5,2)) AS pct_io_share
FROM combined c
CROSS JOIN totals t
WHERE c.database_name IS NOT NULL
ORDER BY c.cpu_time_ms DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<DatabaseResourceUsageRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseResourceUsageRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CpuTimeMs = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1)),
                LogicalReads = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2)),
                PhysicalReads = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3)),
                LogicalWrites = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4)),
                ExecutionCount = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5)),
                IoReadMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                IoWriteMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                IoStallMs = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8)),
                PctCpuShare = reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                PctIoShare = reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets per-application connection counts from session_stats (last 24 hours).
    /// Aggregates snapshots of sys.dm_exec_sessions grouped by program_name.
    /// </summary>
    public async Task<List<ApplicationConnectionRow>> GetApplicationConnectionsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-24);

        command.CommandText = @"
SELECT
    program_name,
    MAX(connection_count) AS max_connections,
    MIN(collection_time) AS first_seen,
    MAX(collection_time) AS last_seen
FROM session_stats
WHERE server_id = $1
AND   collection_time >= $2
GROUP BY program_name
ORDER BY max_connections DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<ApplicationConnectionRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ApplicationConnectionRow
            {
                ApplicationName = reader.GetString(0),
                SampleCount = ToInt64(reader.GetValue(1)),
                FirstSeen = reader.GetDateTime(2),
                LastSeen = reader.GetDateTime(3)
            });
        }

        return items;
    }
}

public class UtilizationEfficiencyRow
{
    public decimal AvgCpuPct { get; set; }
    public int MaxCpuPct { get; set; }
    public decimal P95CpuPct { get; set; }
    public long CpuSamples { get; set; }
    public int TotalMemoryMb { get; set; }
    public int TargetMemoryMb { get; set; }
    public int PhysicalMemoryMb { get; set; }
    public int BufferPoolMb { get; set; }
    public decimal MemoryRatio { get; set; }
    public string ProvisioningStatus { get; set; } = "";
}

public class DatabaseResourceUsageRow
{
    public string DatabaseName { get; set; } = "";
    public long CpuTimeMs { get; set; }
    public long LogicalReads { get; set; }
    public long PhysicalReads { get; set; }
    public long LogicalWrites { get; set; }
    public long ExecutionCount { get; set; }
    public decimal IoReadMb { get; set; }
    public decimal IoWriteMb { get; set; }
    public long IoStallMs { get; set; }
    public decimal PctCpuShare { get; set; }
    public decimal PctIoShare { get; set; }
}

public class ApplicationConnectionRow
{
    public string ApplicationName { get; set; } = "";
    public long SampleCount { get; set; }
    public DateTime FirstSeen { get; set; }
    public DateTime LastSeen { get; set; }
    public DateTime FirstSeenLocal => FirstSeen.ToLocalTime();
    public DateTime LastSeenLocal => LastSeen.ToLocalTime();
}

public class DatabaseSizeRow
{
    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string FileTypeDesc { get; set; } = "";
    public string FileName { get; set; } = "";
    public decimal TotalSizeMb { get; set; }
    public decimal? UsedSizeMb { get; set; }
    public decimal? FreeSpaceMb => UsedSizeMb.HasValue ? TotalSizeMb - UsedSizeMb.Value : null;
    public decimal? UsedPct => UsedSizeMb.HasValue && TotalSizeMb > 0 ? Math.Round(UsedSizeMb.Value * 100m / TotalSizeMb, 1) : null;
    public string? VolumeMountPoint { get; set; }
    public decimal? VolumeTotalMb { get; set; }
    public decimal? VolumeFreeMb { get; set; }
    public string? RecoveryModel { get; set; }
}

public class ServerPropertyRow
{
    public string ServerName { get; set; } = "";
    public string Edition { get; set; } = "";
    public string ProductVersion { get; set; } = "";
    public string? ProductLevel { get; set; }
    public string? ProductUpdateLevel { get; set; }
    public int EngineEdition { get; set; }
    public int CpuCount { get; set; }
    public long PhysicalMemoryMb { get; set; }
    public int? SocketCount { get; set; }
    public int? CoresPerSocket { get; set; }
    public bool? IsHadrEnabled { get; set; }
    public bool? IsClustered { get; set; }

    public string HadrDisplay => IsHadrEnabled.HasValue ? (IsHadrEnabled.Value ? "Yes" : "No") : "";
    public string ClusteredDisplay => IsClustered.HasValue ? (IsClustered.Value ? "Yes" : "No") : "";
}

public class DatabaseSizeTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string DatabaseName { get; set; } = "";
    public decimal TotalSizeMb { get; set; }
}
