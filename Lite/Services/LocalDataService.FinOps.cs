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
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest database size snapshot per server per file (cross-server).
    /// </summary>
    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    database_name,
    file_type_desc,
    file_name,
    total_size_mb,
    used_size_mb,
    volume_mount_point,
    volume_total_mb,
    volume_free_mb,
    recovery_model_desc
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_database_size_stats
    WHERE server_id = $1
)
ORDER BY database_name, file_type_desc, file_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseSizeRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseSizeRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileTypeDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                TotalSizeMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                UsedSizeMb = reader.IsDBNull(4) ? null : Convert.ToDecimal(reader.GetValue(4)),
                VolumeMountPoint = reader.IsDBNull(5) ? null : reader.GetString(5),
                VolumeTotalMb = reader.IsDBNull(6) ? null : Convert.ToDecimal(reader.GetValue(6)),
                VolumeFreeMb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                RecoveryModel = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        return items;
    }

    /// <summary>
    /// Queries a SQL Server directly for its properties via SERVERPROPERTY + sys.dm_os_sys_info.
    /// Works from any database context — no PerformanceMonitor DB required.
    /// </summary>
    public static async Task<ServerPropertyRow> GetServerPropertiesLiveAsync(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        const string query = @"
SELECT
    CONVERT(nvarchar(256), SERVERPROPERTY('Edition')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductUpdateLevel')),
    si.cpu_count,
    si.physical_memory_kb / 1024,
    si.sqlserver_start_time,
    (SELECT SUM(CAST(size AS bigint)) * 8.0 / 1024.0 / 1024.0 FROM sys.master_files),
    si.socket_count,
    si.cores_per_socket,
    CONVERT(int, SERVERPROPERTY('EngineEdition')),
    CONVERT(int, SERVERPROPERTY('IsHadrEnabled')),
    CONVERT(int, SERVERPROPERTY('IsClustered'))
FROM sys.dm_os_sys_info AS si;";

        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            var version = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var level = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var updateLevel = reader.IsDBNull(3) ? null : reader.GetString(3);
            var versionDisplay = !string.IsNullOrEmpty(updateLevel)
                ? $"{version} - {updateLevel}"
                : $"{version} - {level}";

            return new ServerPropertyRow
            {
                Edition = reader.IsDBNull(0) ? "" : reader.GetString(0),
                ProductVersion = versionDisplay,
                CpuCount = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                PhysicalMemoryMb = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                SqlServerStartTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                StorageTotalGb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                SocketCount = reader.IsDBNull(8) ? null : Convert.ToInt32(reader.GetValue(8)),
                CoresPerSocket = reader.IsDBNull(9) ? null : Convert.ToInt32(reader.GetValue(9)),
                EngineEdition = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)),
                IsHadrEnabled = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)) == 1,
                IsClustered = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12)) == 1,
                LastUpdated = DateTime.Now
            };
        }

        return new ServerPropertyRow();
    }

    /// <summary>
    /// Gets collected metrics (CPU, storage, idle DBs) for a specific server from DuckDB.
    /// </summary>
    public async Task<(decimal? AvgCpuPct, decimal? StorageTotalGb, int? IdleDbCount, string? ProvisioningStatus)> GetServerMetricsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cpuCutoff = DateTime.UtcNow.AddHours(-24);
        var idleCutoff = DateTime.UtcNow.AddDays(-7);

        command.CommandText = @"
WITH cpu_24h AS (
    SELECT
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct
    FROM v_cpu_utilization_stats
    WHERE server_id = $1
    AND   collection_time >= $2
),
mem_latest AS (
    SELECT
        CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0) AS memory_ratio
    FROM v_memory_stats
    WHERE server_id = $1
    AND   (server_id, collection_time) IN (
        SELECT server_id, MAX(collection_time)
        FROM v_memory_stats
        WHERE server_id = $1
        GROUP BY server_id
    )
),
storage_totals AS (
    SELECT
        SUM(total_size_mb) / 1024.0 AS total_storage_gb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   (server_id, collection_time) IN (
        SELECT server_id, MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        GROUP BY server_id
    )
),
idle_dbs AS (
    SELECT
        COUNT(DISTINCT database_name) AS idle_db_count
    FROM (
        SELECT database_name
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   (server_id, collection_time) IN (
            SELECT server_id, MAX(collection_time)
            FROM v_database_size_stats
            WHERE server_id = $1
            GROUP BY server_id
        )
        AND database_name NOT IN ('master', 'model', 'msdb', 'tempdb')
        EXCEPT
        SELECT DISTINCT database_name
        FROM v_query_stats
        WHERE server_id = $1
        AND   collection_time >= $3
        AND   delta_execution_count > 0
    ) AS idle
)
SELECT
    c.avg_cpu_pct,
    st.total_storage_gb,
    id.idle_db_count,
    CASE
        WHEN c.avg_cpu_pct < 15 AND c.max_cpu_pct < 40 AND COALESCE(m.memory_ratio, 0) < 0.5
        THEN 'OVER_PROVISIONED'
        WHEN c.p95_cpu_pct > 85 OR COALESCE(m.memory_ratio, 0) > 0.95
        THEN 'UNDER_PROVISIONED'
        ELSE 'RIGHT_SIZED'
    END AS provisioning_status
FROM (SELECT 1) AS anchor
LEFT JOIN cpu_24h c ON true
LEFT JOIN mem_latest m ON true
LEFT JOIN storage_totals st ON true
LEFT JOIN idle_dbs id ON true";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cpuCutoff });
        command.Parameters.Add(new DuckDBParameter { Value = idleCutoff });

        using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return (
                reader.IsDBNull(0) ? null : Convert.ToDecimal(reader.GetValue(0)),
                reader.IsDBNull(1) ? null : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : reader.GetString(3)
            );
        }

        return (null, null, null, null);
    }

    /// <summary>
    /// Gets the latest server properties snapshot per server (cross-server) from DuckDB.
    /// Fallback for when live query is not available.
    /// </summary>
    public async Task<List<ServerPropertyRow>> GetServerPropertiesLatestAsync(IEnumerable<int>? activeServerIds = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cpuCutoff = DateTime.UtcNow.AddHours(-24);
        var idleCutoff = DateTime.UtcNow.AddDays(-7);
        var recentCutoff = DateTime.UtcNow.AddHours(-24);

        // Build server ID filter — integers only, safe to inline
        var serverFilter = "";
        if (activeServerIds != null)
        {
            var idList = string.Join(",", activeServerIds);
            if (!string.IsNullOrEmpty(idList))
                serverFilter = $"AND server_id IN ({idList})";
        }

        command.CommandText = $@"
WITH active_servers AS (
    SELECT DISTINCT server_id, server_name
    FROM v_cpu_utilization_stats
    WHERE collection_time >= $3
    {serverFilter}
),
latest_props AS (
    SELECT *
    FROM v_server_properties
    WHERE (server_id, collection_time) IN (
        SELECT server_id, MAX(collection_time)
        FROM v_server_properties
        GROUP BY server_id
    )
),
cpu_24h AS (
    SELECT
        server_id,
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct
    FROM v_cpu_utilization_stats
    WHERE collection_time >= $1
    GROUP BY server_id
),
mem_latest AS (
    SELECT
        server_id,
        CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0) AS memory_ratio
    FROM v_memory_stats
    WHERE (server_id, collection_time) IN (
        SELECT server_id, MAX(collection_time)
        FROM v_memory_stats
        GROUP BY server_id
    )
),
storage_totals AS (
    SELECT
        server_id,
        SUM(total_size_mb) / 1024.0 AS total_storage_gb
    FROM v_database_size_stats
    WHERE (server_id, collection_time) IN (
        SELECT server_id, MAX(collection_time)
        FROM v_database_size_stats
        GROUP BY server_id
    )
    GROUP BY server_id
),
idle_dbs AS (
    SELECT
        server_id,
        COUNT(DISTINCT database_name) AS idle_db_count
    FROM (
        SELECT server_id, database_name
        FROM v_database_size_stats
        WHERE (server_id, collection_time) IN (
            SELECT server_id, MAX(collection_time)
            FROM v_database_size_stats
            GROUP BY server_id
        )
        AND database_name NOT IN ('master', 'model', 'msdb', 'tempdb')
        EXCEPT
        SELECT DISTINCT server_id, database_name
        FROM v_query_stats
        WHERE collection_time >= $2
        AND   delta_execution_count > 0
    ) AS idle
    GROUP BY server_id
)
SELECT
    a.server_name,
    sp.edition,
    sp.product_version,
    sp.product_level,
    sp.product_update_level,
    sp.engine_edition,
    sp.cpu_count,
    sp.physical_memory_mb,
    sp.socket_count,
    sp.cores_per_socket,
    sp.is_hadr_enabled,
    sp.is_clustered,
    c.avg_cpu_pct,
    st.total_storage_gb,
    id.idle_db_count,
    CASE
        WHEN c.avg_cpu_pct < 15 AND c.max_cpu_pct < 40 AND COALESCE(m.memory_ratio, 0) < 0.5
        THEN 'OVER_PROVISIONED'
        WHEN c.p95_cpu_pct > 85 OR COALESCE(m.memory_ratio, 0) > 0.95
        THEN 'UNDER_PROVISIONED'
        ELSE 'RIGHT_SIZED'
    END AS provisioning_status
FROM active_servers a
LEFT JOIN latest_props sp ON sp.server_id = a.server_id
LEFT JOIN cpu_24h c ON c.server_id = a.server_id
LEFT JOIN mem_latest m ON m.server_id = a.server_id
LEFT JOIN storage_totals st ON st.server_id = a.server_id
LEFT JOIN idle_dbs id ON id.server_id = a.server_id
ORDER BY a.server_name";

        command.Parameters.Add(new DuckDBParameter { Value = cpuCutoff });
        command.Parameters.Add(new DuckDBParameter { Value = idleCutoff });
        command.Parameters.Add(new DuckDBParameter { Value = recentCutoff });

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
                IsClustered = reader.IsDBNull(11) ? null : reader.GetBoolean(11),
                AvgCpuPct = reader.IsDBNull(12) ? null : Convert.ToDecimal(reader.GetValue(12)),
                StorageTotalGb = reader.IsDBNull(13) ? null : Convert.ToDecimal(reader.GetValue(13)),
                IdleDbCount = reader.IsDBNull(14) ? null : Convert.ToInt32(reader.GetValue(14)),
                ProvisioningStatus = reader.IsDBNull(15) ? null : reader.GetString(15)
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
FROM v_database_size_stats
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
        using var _q = TimeQuery("GetUtilizationEfficiencyAsync", "utilization efficiency stats");
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
    FROM v_cpu_utilization_stats
    WHERE server_id = $1
    AND   collection_time >= $2
),
mem_latest AS (
    SELECT
        total_server_memory_mb,
        target_server_memory_mb,
        total_physical_memory_mb,
        buffer_pool_mb,
        max_workers_count,
        current_workers_count,
        CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0) AS memory_ratio
    FROM v_memory_stats
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
),
server_info AS (
    SELECT cpu_count
    FROM v_server_properties
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
    m.memory_ratio,
    m.max_workers_count,
    m.current_workers_count,
    s.cpu_count
FROM cpu_stats c
CROSS JOIN mem_latest m
LEFT JOIN server_info s ON true";

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
            ProvisioningStatus = status,
            MaxWorkersCount = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
            CurrentWorkersCount = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)),
            CpuCount = reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11))
        };
    }

    /// <summary>
    /// Computes per-database resource usage from query_stats + file_io_stats deltas.
    /// </summary>
    public async Task<List<DatabaseResourceUsageRow>> GetDatabaseResourceUsageAsync(int serverId, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000 AS cpu_time_ms,
        SUM(delta_logical_reads) AS logical_reads,
        SUM(delta_physical_reads) AS physical_reads,
        SUM(delta_logical_writes) AS logical_writes,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
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
    FROM v_file_io_stats
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
    CAST(AVG(connection_count) AS INTEGER) AS avg_connections,
    MAX(connection_count) AS max_connections,
    COUNT(*) AS sample_count,
    MIN(collection_time) AS first_seen,
    MAX(collection_time) AS last_seen
FROM v_session_stats
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
                AvgConnections = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                MaxConnections = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                SampleCount = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                FirstSeen = reader.GetDateTime(4),
                LastSeen = reader.GetDateTime(5)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets top N databases by total CPU for the utilization summary.
    /// </summary>
    public async Task<List<TopResourceConsumerRow>> GetTopResourceConsumersByTotalAsync(int serverId, int hoursBack = 24, int topN = 5)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000 AS cpu_time_ms,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    GROUP BY database_name
),
io AS (
    SELECT
        database_name,
        SUM(delta_read_bytes + delta_write_bytes) / 1048576.0 AS io_total_mb
    FROM v_file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_read_bytes IS NOT NULL
    GROUP BY database_name
),
combined AS (
    SELECT
        COALESCE(w.database_name, i.database_name) AS database_name,
        COALESCE(w.cpu_time_ms, 0) AS cpu_time_ms,
        COALESCE(w.execution_count, 0) AS execution_count,
        COALESCE(i.io_total_mb, 0) AS io_total_mb
    FROM workload w
    FULL JOIN io i ON i.database_name = w.database_name
),
totals AS (
    SELECT
        NULLIF(SUM(cpu_time_ms), 0) AS total_cpu,
        NULLIF(SUM(io_total_mb), 0) AS total_io
    FROM combined
)
SELECT
    c.database_name,
    c.cpu_time_ms,
    c.execution_count,
    CAST(c.io_total_mb AS DECIMAL(19,2)),
    CAST(c.cpu_time_ms * 100.0 / t.total_cpu AS DECIMAL(5,2)),
    CAST(c.io_total_mb * 100.0 / t.total_io AS DECIMAL(5,2))
FROM combined c
CROSS JOIN totals t
WHERE c.database_name IS NOT NULL
ORDER BY c.cpu_time_ms DESC
LIMIT $3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });
        command.Parameters.Add(new DuckDBParameter { Value = topN });

        var items = new List<TopResourceConsumerRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TopResourceConsumerRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CpuTimeMs = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                IoTotalMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                PctCpu = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                PctIo = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets top N databases by average CPU per execution for the utilization summary.
    /// </summary>
    public async Task<List<TopResourceConsumerRow>> GetTopResourceConsumersByAvgAsync(int serverId, int hoursBack = 24, int topN = 5)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000 AS cpu_time_ms,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    GROUP BY database_name
    HAVING SUM(delta_execution_count) > 0
),
io AS (
    SELECT
        database_name,
        SUM(delta_read_bytes + delta_write_bytes) / 1048576.0 AS io_total_mb
    FROM v_file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_read_bytes IS NOT NULL
    GROUP BY database_name
)
SELECT
    w.database_name,
    CAST(w.cpu_time_ms * 1.0 / w.execution_count AS DECIMAL(19,2)) AS avg_cpu_ms,
    w.execution_count,
    CAST(COALESCE(i.io_total_mb, 0) AS DECIMAL(19,2)),
    w.cpu_time_ms,
    CAST(COALESCE(i.io_total_mb, 0) * 1.0 / w.execution_count AS DECIMAL(19,4)) AS avg_io_mb
FROM workload w
LEFT JOIN io i ON i.database_name = w.database_name
ORDER BY avg_cpu_ms DESC
LIMIT $3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });
        command.Parameters.Add(new DuckDBParameter { Value = topN });

        var items = new List<TopResourceConsumerRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TopResourceConsumerRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CpuTimeMs = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                IoTotalMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                TotalCpuTimeMs = reader.IsDBNull(4) ? 0 : ToInt64(reader.GetValue(4)),
                AvgIoMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets per-database total allocated and used space for the utilization size chart.
    /// Aggregates across all files per database for the selected server.
    /// </summary>
    public async Task<List<DatabaseSizeSummaryRow>> GetDatabaseSizeSummaryAsync(int serverId, int topN = 10)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    database_name,
    SUM(total_size_mb) AS total_mb,
    SUM(used_size_mb) AS used_mb
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1
)
GROUP BY database_name
ORDER BY total_mb DESC
LIMIT $2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = topN });

        var items = new List<DatabaseSizeSummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseSizeSummaryRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                UsedMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets per-database storage growth trends comparing current size to 7d and 30d ago.
    /// </summary>
    public async Task<List<StorageGrowthRow>> GetStorageGrowthAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var now = DateTime.UtcNow;
        var cutoff7d = now.AddDays(-7);
        var cutoff30d = now.AddDays(-30);

        command.CommandText = @"
WITH latest AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS current_size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
    )
    GROUP BY database_name
),
past_7d AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $2
    )
    GROUP BY database_name
),
past_30d AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $3
    )
    GROUP BY database_name
)
SELECT
    l.database_name,
    l.current_size_mb,
    p7.size_mb,
    p30.size_mb,
    l.current_size_mb - COALESCE(p7.size_mb, l.current_size_mb) AS growth_7d_mb,
    l.current_size_mb - COALESCE(p30.size_mb, l.current_size_mb) AS growth_30d_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL
        THEN (l.current_size_mb - p30.size_mb) / 30.0
        WHEN p7.size_mb IS NOT NULL
        THEN (l.current_size_mb - p7.size_mb) / 7.0
        ELSE 0
    END AS daily_growth_rate_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL AND p30.size_mb > 0
        THEN (l.current_size_mb - p30.size_mb) * 100.0 / p30.size_mb
        ELSE 0
    END AS growth_pct_30d
FROM latest l
LEFT JOIN past_7d p7 ON p7.database_name = l.database_name
LEFT JOIN past_30d p30 ON p30.database_name = l.database_name
ORDER BY growth_30d_mb DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff7d });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff30d });

        var items = new List<StorageGrowthRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new StorageGrowthRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                Size7dAgoMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                Size30dAgoMb = reader.IsDBNull(3) ? null : Convert.ToDecimal(reader.GetValue(3)),
                Growth7dMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                Growth30dMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                DailyGrowthRateMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                GrowthPct30d = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7))
            });
        }
        return items;
    }

    /// <summary>
    /// Detects databases with zero query executions over the last N days.
    /// </summary>
    public async Task<List<IdleDatabaseRow>> GetIdleDatabasesAsync(int serverId, int daysBack = 7)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddDays(-daysBack);

        command.CommandText = @"
WITH db_sizes AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS total_size_mb,
        COUNT(*) AS file_count
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
    )
    GROUP BY database_name
),
db_activity AS (
    SELECT
        database_name,
        SUM(delta_execution_count) AS total_executions,
        MAX(last_execution_time) AS last_execution
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_execution_count IS NOT NULL
    GROUP BY database_name
)
SELECT
    ds.database_name,
    ds.total_size_mb,
    ds.file_count,
    a.last_execution
FROM db_sizes ds
LEFT JOIN db_activity a ON a.database_name = ds.database_name
WHERE COALESCE(a.total_executions, 0) = 0
AND   ds.database_name NOT IN ('master', 'model', 'msdb', 'tempdb')
ORDER BY ds.total_size_mb DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<IdleDatabaseRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new IdleDatabaseRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                FileCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                LastExecutionTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
            });
        }
        return items;
    }

    /// <summary>
    /// Gets tempdb pressure summary: latest and 24h peak values.
    /// </summary>
    public async Task<List<TempdbSummaryRow>> GetTempdbSummaryAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-24);

        command.CommandText = @"
WITH latest AS (
    SELECT
        user_object_reserved_mb,
        internal_object_reserved_mb,
        version_store_reserved_mb,
        total_reserved_mb
    FROM v_tempdb_stats
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
),
peak AS (
    SELECT
        MAX(user_object_reserved_mb) AS max_user_mb,
        MAX(internal_object_reserved_mb) AS max_internal_mb,
        MAX(version_store_reserved_mb) AS max_version_store_mb,
        MAX(total_reserved_mb) AS max_total_mb
    FROM v_tempdb_stats
    WHERE server_id = $1
    AND   collection_time >= $2
)
SELECT 'User Objects', l.user_object_reserved_mb, p.max_user_mb,
    CASE WHEN p.max_user_mb > 1024 THEN 'High user object usage' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Internal Objects', l.internal_object_reserved_mb, p.max_internal_mb,
    CASE WHEN p.max_internal_mb > 1024 THEN 'High internal object usage (sorts/hashes)' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Version Store', l.version_store_reserved_mb, p.max_version_store_mb,
    CASE WHEN p.max_version_store_mb > 2048 THEN 'Version store pressure — check long-running transactions' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Total Reserved', l.total_reserved_mb, p.max_total_mb, ''
FROM latest l CROSS JOIN peak p";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<TempdbSummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TempdbSummaryRow
            {
                Metric = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                Peak24hMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                Warning = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }
        return items;
    }

    /// <summary>
    /// Gets wait stats grouped by cost category over the last 24 hours.
    /// </summary>
    public async Task<List<WaitCategorySummaryRow>> GetWaitCategorySummaryAsync(int serverId, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
WITH categorized AS (
    SELECT
        CASE
            WHEN wait_type IN ('SOS_SCHEDULER_YIELD', 'CXPACKET', 'CXCONSUMER', 'CXSYNC_PORT', 'CXSYNC_CONSUMER') THEN 'CPU'
            WHEN wait_type ILIKE 'PAGEIOLATCH%'
            OR   wait_type IN ('WRITELOG', 'IO_COMPLETION', 'ASYNC_IO_COMPLETION') THEN 'Storage'
            WHEN wait_type IN ('RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE', 'CMEMTHREAD') THEN 'Memory'
            WHEN wait_type = 'ASYNC_NETWORK_IO' THEN 'Network'
            WHEN wait_type ILIKE 'LCK_M_%' THEN 'Locks'
            ELSE 'Other'
        END AS category,
        wait_type,
        SUM(delta_wait_time_ms) AS wait_time_ms,
        SUM(delta_waiting_tasks) AS waiting_tasks
    FROM v_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_wait_time_ms IS NOT NULL
    AND   delta_wait_time_ms > 0
    GROUP BY
        CASE
            WHEN wait_type IN ('SOS_SCHEDULER_YIELD', 'CXPACKET', 'CXCONSUMER', 'CXSYNC_PORT', 'CXSYNC_CONSUMER') THEN 'CPU'
            WHEN wait_type ILIKE 'PAGEIOLATCH%'
            OR   wait_type IN ('WRITELOG', 'IO_COMPLETION', 'ASYNC_IO_COMPLETION') THEN 'Storage'
            WHEN wait_type IN ('RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE', 'CMEMTHREAD') THEN 'Memory'
            WHEN wait_type = 'ASYNC_NETWORK_IO' THEN 'Network'
            WHEN wait_type ILIKE 'LCK_M_%' THEN 'Locks'
            ELSE 'Other'
        END,
        wait_type
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY wait_time_ms DESC) AS rn
    FROM categorized
),
by_category AS (
    SELECT
        category,
        SUM(wait_time_ms) AS total_wait_time_ms,
        SUM(waiting_tasks) AS total_waiting_tasks,
        MAX(CASE WHEN rn = 1 THEN wait_type END) AS top_wait_type,
        MAX(CASE WHEN rn = 1 THEN wait_time_ms END) AS top_wait_time_ms
    FROM ranked
    GROUP BY category
),
grand_total AS (
    SELECT NULLIF(SUM(total_wait_time_ms), 0) AS total
    FROM by_category
)
SELECT
    bc.category,
    bc.total_wait_time_ms,
    bc.total_waiting_tasks,
    CAST(bc.total_wait_time_ms * 100.0 / gt.total AS DECIMAL(5,1)),
    bc.top_wait_type,
    bc.top_wait_time_ms
FROM by_category bc
CROSS JOIN grand_total gt
ORDER BY bc.total_wait_time_ms DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<WaitCategorySummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitCategorySummaryRow
            {
                Category = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalWaitTimeMs = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
                WaitingTasks = reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                PctOfTotal = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                TopWaitType = reader.IsDBNull(4) ? "" : reader.GetString(4),
                TopWaitTimeMs = reader.IsDBNull(5) ? 0 : ToInt64(reader.GetValue(5))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets top 20 most expensive queries by total CPU over the last 24 hours.
    /// </summary>
    public async Task<List<ExpensiveQueryRow>> GetExpensiveQueriesAsync(int serverId, int hoursBack = 24, int topN = 20)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
SELECT
    database_name,
    SUM(delta_worker_time) / 1000 AS total_cpu_ms,
    CAST(SUM(delta_worker_time) / 1000.0 / NULLIF(SUM(delta_execution_count), 0) AS DECIMAL(19,2)) AS avg_cpu_ms,
    SUM(delta_logical_reads) AS total_reads,
    CAST(SUM(delta_logical_reads) * 1.0 / NULLIF(SUM(delta_execution_count), 0) AS DECIMAL(19,0)) AS avg_reads,
    SUM(delta_execution_count) AS executions,
    LEFT(query_text, 200) AS query_preview
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   delta_worker_time IS NOT NULL
AND   delta_worker_time > 0
GROUP BY
    database_name,
    sql_handle,
    statement_start_offset,
    statement_end_offset,
    query_text
ORDER BY SUM(delta_worker_time) DESC
LIMIT $3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });
        command.Parameters.Add(new DuckDBParameter { Value = topN });

        var items = new List<ExpensiveQueryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ExpensiveQueryRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalCpuMs = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
                AvgCpuMsPerExec = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                TotalReads = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                AvgReadsPerExec = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                Executions = reader.IsDBNull(5) ? 0 : ToInt64(reader.GetValue(5)),
                QueryPreview = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }
        return items;
    }

    /// <summary>
    /// Checks if sp_IndexCleanup is installed on the target SQL Server.
    /// </summary>
    public static async Task<bool> CheckSpIndexCleanupExistsAsync(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand("SELECT OBJECT_ID('dbo.sp_IndexCleanup', 'P')", connection) { CommandTimeout = 30 };
        var result = await command.ExecuteScalarAsync();
        return result != null && result != DBNull.Value;
    }

    /// <summary>
    /// Runs sp_IndexCleanup on the remote SQL Server and returns detail + summary result sets.
    /// </summary>
    public static async Task<(List<IndexCleanupResultRow> Details, List<IndexCleanupSummaryRow> Summaries)> RunIndexAnalysisAsync(
        string connectionString, string? databaseName, bool getAllDatabases)
    {
        var details = new List<IndexCleanupResultRow>();
        var summaries = new List<IndexCleanupSummaryRow>();

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        using var command = new SqlCommand("dbo.sp_IndexCleanup", connection);
        command.CommandType = System.Data.CommandType.StoredProcedure;
        command.CommandTimeout = 300;

        if (getAllDatabases)
        {
            command.Parameters.AddWithValue("@get_all_databases", 1);
        }
        else if (!string.IsNullOrWhiteSpace(databaseName))
        {
            command.Parameters.AddWithValue("@database_name", databaseName);
        }

        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            details.Add(new IndexCleanupResultRow
            {
                ScriptType = reader.IsDBNull(0) ? "" : reader.GetValue(0).ToString() ?? "",
                AdditionalInfo = reader.IsDBNull(1) ? "" : reader.GetValue(1).ToString() ?? "",
                DatabaseName = reader.IsDBNull(2) ? "" : reader.GetValue(2).ToString() ?? "",
                SchemaName = reader.IsDBNull(3) ? "" : reader.GetValue(3).ToString() ?? "",
                TableName = reader.IsDBNull(4) ? "" : reader.GetValue(4).ToString() ?? "",
                IndexName = reader.IsDBNull(5) ? "" : reader.GetValue(5).ToString() ?? "",
                ConsolidationRule = reader.IsDBNull(6) ? "" : reader.GetValue(6).ToString() ?? "",
                TargetIndexName = reader.IsDBNull(7) ? "" : reader.GetValue(7).ToString() ?? "",
                SupersededInfo = reader.IsDBNull(8) ? "" : reader.GetValue(8).ToString() ?? "",
                IndexSizeGb = reader.IsDBNull(9) ? "" : reader.GetValue(9).ToString() ?? "",
                IndexRows = reader.IsDBNull(10) ? "" : reader.GetValue(10).ToString() ?? "",
                IndexReads = reader.IsDBNull(11) ? "" : reader.GetValue(11).ToString() ?? "",
                IndexWrites = reader.IsDBNull(12) ? "" : reader.GetValue(12).ToString() ?? "",
                OriginalIndexDefinition = reader.IsDBNull(13) ? "" : reader.GetValue(13).ToString() ?? "",
                Script = reader.IsDBNull(14) ? "" : reader.GetValue(14).ToString() ?? ""
            });
        }

        if (await reader.NextResultAsync())
        {
            while (await reader.ReadAsync())
            {
                var fc = reader.FieldCount;
                string Col(int i) => fc > i && !reader.IsDBNull(i) ? reader.GetValue(i).ToString() ?? "" : "";
                summaries.Add(new IndexCleanupSummaryRow
                {
                    Level = Col(0),
                    DatabaseInfo = Col(1),
                    SchemaName = Col(2),
                    TableName = Col(3),
                    TablesAnalyzed = Col(4),
                    TotalIndexes = Col(5),
                    RemovableIndexes = Col(6),
                    MergeableIndexes = Col(7),
                    CompressableIndexes = Col(8),
                    PercentRemovable = Col(9),
                    CurrentSizeGb = Col(10),
                    SizeAfterCleanupGb = Col(11),
                    SpaceSavedGb = Col(12),
                    SpaceReductionPercent = Col(13),
                    CompressionSavingsPotential = Col(14),
                    CompressionSavingsPotentialTotal = Col(15),
                    ComputedColumnsWithUdfs = Col(16),
                    CheckConstraintsWithUdfs = Col(17),
                    FilteredIndexesNeedingIncludes = Col(18),
                    TotalRows = Col(19),
                    ReadsBreakdown = Col(20),
                    Writes = Col(21),
                    DailyWriteOpsSaved = Col(22),
                    LockWaitCount = Col(23),
                    DailyLockWaitsSaved = Col(24),
                    AvgLockWaitMs = Col(25),
                    LatchWaitCount = Col(26),
                    DailyLatchWaitsSaved = Col(27),
                    AvgLatchWaitMs = Col(28)
                });
            }
        }

        return (details, summaries);
    }

    /// <summary>
    /// Gets 7-day daily provisioning classification trend.
    /// </summary>
    public async Task<List<ProvisioningTrendRow>> GetProvisioningTrendAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddDays(-7);

        command.CommandText = @"
WITH daily_cpu AS (
    SELECT
        CAST(collection_time AS DATE) AS day,
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct
    FROM v_cpu_utilization_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY CAST(collection_time AS DATE)
),
daily_mem AS (
    SELECT
        CAST(collection_time AS DATE) AS day,
        AVG(CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0)) AS avg_memory_ratio
    FROM v_memory_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY CAST(collection_time AS DATE)
)
SELECT
    c.day,
    c.avg_cpu_pct,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    COALESCE(m.avg_memory_ratio, 0)
FROM daily_cpu c
LEFT JOIN daily_mem m ON m.day = c.day
ORDER BY c.day";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<ProvisioningTrendRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var avgCpu = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1));
            var maxCpu = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
            var p95Cpu = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3));
            var memRatio = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4));

            var status = "RIGHT_SIZED";
            if (avgCpu < 15 && maxCpu < 40 && memRatio < 0.5m)
                status = "OVER_PROVISIONED";
            else if (p95Cpu > 85 || memRatio > 0.95m)
                status = "UNDER_PROVISIONED";

            items.Add(new ProvisioningTrendRow
            {
                Day = reader.GetDateTime(0),
                AvgCpuPct = avgCpu,
                MaxCpuPct = maxCpu,
                P95CpuPct = p95Cpu,
                MemoryRatio = memRatio,
                Status = status
            });
        }
        return items;
    }

    /// <summary>
    /// Gets memory grant efficiency stats for the Optimization tab.
    /// Shows pool-level grant vs used efficiency from resource semaphore snapshots.
    /// </summary>
    public async Task<List<MemoryGrantEfficiencyRow>> GetMemoryGrantEfficiencyAsync(int serverId, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
SELECT
    CAST(collection_time AS DATE) AS day,
    AVG(granted_memory_mb) AS avg_granted_mb,
    AVG(used_memory_mb) AS avg_used_mb,
    CAST(AVG(used_memory_mb) * 100.0 / NULLIF(AVG(granted_memory_mb), 0) AS DECIMAL(5,1)) AS efficiency_pct,
    MAX(granted_memory_mb) AS peak_granted_mb,
    SUM(grantee_count) AS total_grantees,
    SUM(waiter_count) AS total_waiters,
    SUM(timeout_error_count_delta) AS timeout_errors,
    SUM(forced_grant_count_delta) AS forced_grants
FROM v_memory_grant_stats
WHERE server_id = $1
AND   collection_time >= $2
GROUP BY CAST(collection_time AS DATE)
ORDER BY CAST(collection_time AS DATE)";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<MemoryGrantEfficiencyRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryGrantEfficiencyRow
            {
                Day = reader.GetDateTime(0),
                AvgGrantedMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                AvgUsedMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                EfficiencyPct = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                PeakGrantedMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                TotalGrantees = reader.IsDBNull(5) ? 0 : ToInt64(reader.GetValue(5)),
                TotalWaiters = reader.IsDBNull(6) ? 0 : ToInt64(reader.GetValue(6)),
                TimeoutErrors = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                ForcedGrants = reader.IsDBNull(8) ? 0 : ToInt64(reader.GetValue(8))
            });
        }
        return items;
    }
}

public class ProvisioningTrendRow
{
    public DateTime Day { get; set; }
    public decimal AvgCpuPct { get; set; }
    public int MaxCpuPct { get; set; }
    public decimal P95CpuPct { get; set; }
    public decimal MemoryRatio { get; set; }
    public string Status { get; set; } = "";
    public string DayDisplay => Day.ToString("ddd MM/dd");
    public string StatusDisplay => Status.Replace("_", " ");
}

public class MemoryGrantEfficiencyRow
{
    public DateTime Day { get; set; }
    public decimal AvgGrantedMb { get; set; }
    public decimal AvgUsedMb { get; set; }
    public decimal EfficiencyPct { get; set; }
    public decimal PeakGrantedMb { get; set; }
    public long TotalGrantees { get; set; }
    public long TotalWaiters { get; set; }
    public long TimeoutErrors { get; set; }
    public long ForcedGrants { get; set; }
    public string DayDisplay => Day.ToString("ddd MM/dd");
    public decimal WastedMb => AvgGrantedMb - AvgUsedMb;
}

public class TopResourceConsumerRow
{
    public string DatabaseName { get; set; } = "";
    public long CpuTimeMs { get; set; }
    public long ExecutionCount { get; set; }
    public decimal IoTotalMb { get; set; }
    public decimal PctCpu { get; set; }
    public decimal PctIo { get; set; }
    public long TotalCpuTimeMs { get; set; }
    public decimal AvgIoMb { get; set; }
}

public class DatabaseSizeSummaryRow
{
    public string DatabaseName { get; set; } = "";
    public decimal TotalMb { get; set; }
    public decimal? UsedMb { get; set; }
    public decimal FreeMb => UsedMb.HasValue ? TotalMb - UsedMb.Value : TotalMb;
    public decimal UsedPct => TotalMb > 0 && UsedMb.HasValue ? Math.Round(UsedMb.Value * 100m / TotalMb, 1) : 0;

    /* Star-width GridLength for XAML binding — drives the stacked bar proportions */
    public System.Windows.GridLength UsedStarWidth =>
        new(Math.Max((double)(UsedMb ?? 0m), 0.1), System.Windows.GridUnitType.Star);
    public System.Windows.GridLength FreeStarWidth =>
        new(Math.Max((double)FreeMb, 0.1), System.Windows.GridUnitType.Star);
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
    public int MaxWorkersCount { get; set; }
    public int CurrentWorkersCount { get; set; }
    public int CpuCount { get; set; }
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
    public int AvgConnections { get; set; }
    public int MaxConnections { get; set; }
    public long SampleCount { get; set; }
    public DateTime FirstSeen { get; set; }
    public DateTime LastSeen { get; set; }
    public DateTime FirstSeenLocal => FirstSeen.ToLocalTime();
    public DateTime LastSeenLocal => LastSeen.ToLocalTime();
}

public class DatabaseSizeRow
{
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
    public DateTime? SqlServerStartTime { get; set; }
    public DateTime? LastUpdated { get; set; }
    public bool? IsHadrEnabled { get; set; }
    public bool? IsClustered { get; set; }

    public decimal? AvgCpuPct { get; set; }
    public decimal? StorageTotalGb { get; set; }
    public int? IdleDbCount { get; set; }
    public string? ProvisioningStatus { get; set; }

    public string UptimeDisplay
    {
        get
        {
            if (SqlServerStartTime == null) return "";
            var uptime = DateTime.Now - SqlServerStartTime.Value;
            return $"{(int)uptime.TotalDays}d {uptime.Hours}h";
        }
    }
    public string HadrDisplay => IsHadrEnabled.HasValue ? (IsHadrEnabled.Value ? "Yes" : "No") : "";
    public string ClusteredDisplay => IsClustered.HasValue ? (IsClustered.Value ? "Yes" : "No") : "";
    public string ProvisioningDisplay => ProvisioningStatus?.Replace("_", " ") ?? "";
}

public class DatabaseSizeTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string DatabaseName { get; set; } = "";
    public decimal TotalSizeMb { get; set; }
}

public class StorageGrowthRow
{
    public string DatabaseName { get; set; } = "";
    public decimal CurrentSizeMb { get; set; }
    public decimal? Size7dAgoMb { get; set; }
    public decimal? Size30dAgoMb { get; set; }
    public decimal Growth7dMb { get; set; }
    public decimal Growth30dMb { get; set; }
    public decimal DailyGrowthRateMb { get; set; }
    public decimal GrowthPct30d { get; set; }
}

public class IdleDatabaseRow
{
    public string DatabaseName { get; set; } = "";
    public decimal TotalSizeMb { get; set; }
    public int FileCount { get; set; }
    public DateTime? LastExecutionTime { get; set; }
}

public class TempdbSummaryRow
{
    public string Metric { get; set; } = "";
    public decimal CurrentMb { get; set; }
    public decimal Peak24hMb { get; set; }
    public string Warning { get; set; } = "";
}

public class WaitCategorySummaryRow
{
    public string Category { get; set; } = "";
    public long TotalWaitTimeMs { get; set; }
    public long WaitingTasks { get; set; }
    public decimal PctOfTotal { get; set; }
    public string TopWaitType { get; set; } = "";
    public long TopWaitTimeMs { get; set; }
}

public class ExpensiveQueryRow
{
    public string DatabaseName { get; set; } = "";
    public long TotalCpuMs { get; set; }
    public decimal AvgCpuMsPerExec { get; set; }
    public long TotalReads { get; set; }
    public decimal AvgReadsPerExec { get; set; }
    public long Executions { get; set; }
    public string QueryPreview { get; set; } = "";
}

public class IndexCleanupResultRow
{
    public string ScriptType { get; set; } = "";
    public string AdditionalInfo { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string IndexName { get; set; } = "";
    public string ConsolidationRule { get; set; } = "";
    public string TargetIndexName { get; set; } = "";
    public string SupersededInfo { get; set; } = "";
    public string IndexSizeGb { get; set; } = "";
    public string IndexRows { get; set; } = "";
    public string IndexReads { get; set; } = "";
    public string IndexWrites { get; set; } = "";
    public string OriginalIndexDefinition { get; set; } = "";
    public string Script { get; set; } = "";
}

public class IndexCleanupSummaryRow
{
    public string Level { get; set; } = "";
    public string DatabaseInfo { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string TablesAnalyzed { get; set; } = "";
    public string TotalIndexes { get; set; } = "";
    public string RemovableIndexes { get; set; } = "";
    public string MergeableIndexes { get; set; } = "";
    public string CompressableIndexes { get; set; } = "";
    public string PercentRemovable { get; set; } = "";
    public string CurrentSizeGb { get; set; } = "";
    public string SizeAfterCleanupGb { get; set; } = "";
    public string SpaceSavedGb { get; set; } = "";
    public string SpaceReductionPercent { get; set; } = "";
    public string CompressionSavingsPotential { get; set; } = "";
    public string CompressionSavingsPotentialTotal { get; set; } = "";
    public string ComputedColumnsWithUdfs { get; set; } = "";
    public string CheckConstraintsWithUdfs { get; set; } = "";
    public string FilteredIndexesNeedingIncludes { get; set; } = "";
    public string TotalRows { get; set; } = "";
    public string ReadsBreakdown { get; set; } = "";
    public string Writes { get; set; } = "";
    public string DailyWriteOpsSaved { get; set; } = "";
    public string LockWaitCount { get; set; } = "";
    public string DailyLockWaitsSaved { get; set; } = "";
    public string AvgLockWaitMs { get; set; } = "";
    public string LatchWaitCount { get; set; } = "";
    public string DailyLatchWaitsSaved { get; set; } = "";
    public string AvgLatchWaitMs { get; set; } = "";
}
