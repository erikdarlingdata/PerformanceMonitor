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
    /// Queries a SQL Server directly for its properties via SERVERPROPERTY + sys.dm_os_sys_info.
    /// Works from any database context — no PerformanceMonitor DB required.
    /// </summary>
    public static async Task<ServerPropertyRow> GetServerPropertiesLiveAsync(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // sys.master_files doesn't exist on Azure SQL DB — dynamic SQL picks the right catalog view
        const string query = @"
DECLARE
    @storage_sql nvarchar(MAX) =
        CASE
            WHEN CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
            THEN N'SELECT @gb = SUM(CAST(size AS bigint)) * 8.0 / 1024.0 / 1024.0 FROM sys.database_files'
            ELSE N'SELECT @gb = SUM(CAST(size AS bigint)) * 8.0 / 1024.0 / 1024.0 FROM sys.master_files'
        END,
    @storage_gb decimal(19,2),
    @host_os nvarchar(256);

EXEC sys.sp_executesql @storage_sql, N'@gb decimal(19,2) OUTPUT', @gb = @storage_gb OUTPUT;

IF OBJECT_ID(N'sys.dm_os_host_info', N'V') IS NOT NULL
    EXEC sys.sp_executesql N'SELECT @os = host_distribution FROM sys.dm_os_host_info',
        N'@os nvarchar(256) OUTPUT', @os = @host_os OUTPUT;

IF @host_os IS NULL
BEGIN
    DECLARE @ver nvarchar(4000) = @@VERSION;
    DECLARE @on_pos int = CHARINDEX(N' on ', @ver);
    IF @on_pos > 0
        SET @host_os = LTRIM(SUBSTRING(@ver, @on_pos + 4, LEN(@ver)));
END;

SELECT
    CONVERT(nvarchar(256), SERVERPROPERTY('Edition')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductUpdateLevel')),
    si.cpu_count,
    si.physical_memory_kb / 1024,
    si.sqlserver_start_time,
    @storage_gb,
    si.socket_count,
    si.cores_per_socket,
    CONVERT(int, SERVERPROPERTY('EngineEdition')),
    CONVERT(int, SERVERPROPERTY('IsHadrEnabled')),
    CONVERT(int, SERVERPROPERTY('IsClustered')),
    @host_os
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
                HostOsVersion = reader.IsDBNull(13) ? "" : reader.GetString(13),
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
        AND database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
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
        AND database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
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
    COALESCE(sp.vcore_count, sp.cpu_count) AS cpu_count,
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
}
