/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // FinOps — Server Inventory & Utilization
        // ============================================

        /// <summary>
        /// Fetches utilization efficiency metrics from report.finops_utilization_efficiency.
        /// </summary>
        public async Task<FinOpsUtilizationEfficiency?> GetFinOpsUtilizationEfficiencyAsync()
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    v.avg_cpu_pct,
                    v.max_cpu_pct,
                    v.p95_cpu_pct,
                    v.cpu_samples,
                    v.total_memory_mb,
                    v.target_memory_mb,
                    v.physical_memory_mb,
                    v.memory_ratio,
                    v.memory_utilization_pct,
                    v.worker_threads_current,
                    v.worker_threads_max,
                    v.worker_thread_ratio,
                    v.cpu_count,
                    v.provisioning_status,
                    m.buffer_pool_mb,
                    tsm.total_server_memory_mb
                FROM report.finops_utilization_efficiency AS v
                OUTER APPLY
                (
                    SELECT TOP (1)
                        ms.buffer_pool_mb
                    FROM collect.memory_stats AS ms
                    ORDER BY
                        ms.collection_time DESC
                ) AS m
                OUTER APPLY
                (
                    SELECT
                        total_server_memory_mb =
                            pc.cntr_value / 1024
                    FROM sys.dm_os_performance_counters AS pc
                    WHERE pc.counter_name = N'Total Server Memory (KB)'
                ) AS tsm
                OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_UtilizationEfficiency", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    return new FinOpsUtilizationEfficiency
                    {
                        AvgCpuPct = reader.IsDBNull(0) ? 0m : Convert.ToDecimal(reader.GetValue(0)),
                        MaxCpuPct = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                        P95CpuPct = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                        CpuSamples = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                        TotalMemoryMb = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                        TargetMemoryMb = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                        PhysicalMemoryMb = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                        MemoryRatio = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                        MemoryUtilizationPct = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8)),
                        WorkerThreadsCurrent = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                        WorkerThreadsMax = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)),
                        WorkerThreadRatio = reader.IsDBNull(11) ? 0m : Convert.ToDecimal(reader.GetValue(11)),
                        CpuCount = reader.IsDBNull(12) ? 0 : Convert.ToInt32(reader.GetValue(12)),
                        ProvisioningStatus = reader.IsDBNull(13) ? "" : reader.GetString(13),
                        BufferPoolMb = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14)),
                        TotalServerMemoryMb = reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15))
                    };
                }
            }

            return null;
        }

        /// <summary>
        /// Fetches server inventory from config.server_info.
        /// </summary>
        /// <summary>
        /// Queries a SQL Server directly for its properties via SERVERPROPERTY + sys.dm_os_sys_info.
        /// Works from any database context — no PerformanceMonitor DB required.
        /// </summary>
        public static async Task<FinOpsServerInventory> GetServerPropertiesLiveAsync(string connectionString)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            const string query = @"
DECLARE @host_os nvarchar(256);
IF OBJECT_ID(N'sys.dm_os_host_info', N'V') IS NOT NULL
    EXEC sys.sp_executesql N'SELECT @os = host_distribution FROM sys.dm_os_host_info',
        N'@os nvarchar(256) OUTPUT', @os = @host_os OUTPUT;

IF @host_os IS NULL
BEGIN
    /* SQL 2016 or Azure SQL DB: parse OS from @@VERSION */
    DECLARE @ver nvarchar(4000) = @@VERSION;
    DECLARE @on_pos int = CHARINDEX(N' on ', @ver);
    IF @on_pos > 0
        SET @host_os = LTRIM(SUBSTRING(@ver, @on_pos + 4, LEN(@ver)));
END;

SELECT
    edition =
        CONVERT(nvarchar(256), SERVERPROPERTY('Edition')),
    product_version =
        CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')),
    product_level =
        CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')),
    product_update_level =
        CONVERT(nvarchar(128), SERVERPROPERTY('ProductUpdateLevel')),
    cpu_count =
        si.cpu_count,
    physical_memory_mb =
        si.physical_memory_kb / 1024,
    sqlserver_start_time =
        si.sqlserver_start_time,
    total_storage_gb =
        (SELECT SUM(CAST(size AS bigint)) * 8.0 / 1024.0 / 1024.0 FROM sys.master_files),
    socket_count =
        si.socket_count,
    cores_per_socket =
        si.cores_per_socket,
    engine_edition =
        CONVERT(int, SERVERPROPERTY('EngineEdition')),
    is_hadr_enabled =
        CONVERT(int, SERVERPROPERTY('IsHadrEnabled')),
    is_clustered =
        CONVERT(int, SERVERPROPERTY('IsClustered')),
    host_os =
        @host_os
FROM sys.dm_os_sys_info AS si;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 30;

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var version = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var level = reader.IsDBNull(2) ? "" : reader.GetString(2);
                var updateLevel = reader.IsDBNull(3) ? null : reader.GetString(3);
                var versionDisplay = !string.IsNullOrEmpty(updateLevel)
                    ? $"{version} - {updateLevel}"
                    : $"{version} - {level}";

                return new FinOpsServerInventory
                {
                    Edition = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    SqlVersion = versionDisplay,
                    CpuCount = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                    PhysicalMemoryMb = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                    SqlServerStartTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                    StorageTotalGb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                    SocketCount = reader.IsDBNull(8) ? null : Convert.ToInt32(reader.GetValue(8)),
                    CoresPerSocket = reader.IsDBNull(9) ? null : Convert.ToInt32(reader.GetValue(9)),
                    EngineEdition = reader.IsDBNull(10) ? null : Convert.ToInt32(reader.GetValue(10)),
                    IsHadrEnabled = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)) == 1,
                    IsClustered = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12)) == 1,
                    HostOsVersion = reader.IsDBNull(13) ? "" : reader.GetString(13),
                    LastUpdated = DateTime.Now
                };
            }

            return new FinOpsServerInventory();
        }

        /// <summary>
        /// Gets collected metrics (CPU, storage, idle DBs) from the PerformanceMonitor database.
        /// Returns null values if no data is collected yet.
        /// </summary>
        public async Task<(decimal? AvgCpuPct, decimal? StorageTotalGb, int? IdleDbCount, string? ProvisioningStatus)> GetServerMetricsAsync()
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    cpu_24h AS
    (
        SELECT DISTINCT
            avg_cpu_pct =
                AVG(CONVERT(decimal(5,2), cu.sqlserver_cpu_utilization)) OVER (),
            max_cpu_pct =
                MAX(cu.sqlserver_cpu_utilization) OVER (),
            p95_cpu_pct =
                CONVERT
                (
                    decimal(5,2),
                    PERCENTILE_CONT(0.95)
                    WITHIN GROUP (ORDER BY cu.sqlserver_cpu_utilization)
                    OVER ()
                )
        FROM collect.cpu_utilization_stats AS cu
        WHERE cu.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
    ),
    mem_latest AS
    (
        SELECT TOP (1)
            memory_ratio =
                CONVERT(decimal(10,4), ms.total_memory_mb) /
                NULLIF(ms.committed_target_memory_mb, 0)
        FROM collect.memory_stats AS ms
        ORDER BY
            ms.collection_time DESC
    ),
    storage_total AS
    (
        SELECT
            total_storage_gb =
                SUM(ds.total_size_mb) / 1024.0
        FROM collect.database_size_stats AS ds
        WHERE ds.collection_time =
        (
            SELECT MAX(ds2.collection_time)
            FROM collect.database_size_stats AS ds2
        )
    ),
    idle_dbs AS
    (
        SELECT
            idle_db_count = COUNT(DISTINCT d.database_name)
        FROM
        (
            SELECT DISTINCT ds.database_name
            FROM collect.database_size_stats AS ds
            WHERE ds.collection_time =
            (
                SELECT MAX(ds2.collection_time)
                FROM collect.database_size_stats AS ds2
            )
            AND ds.database_name NOT IN (N'master', N'model', N'msdb', N'tempdb')
            EXCEPT
            SELECT DISTINCT qs.database_name
            FROM collect.query_stats AS qs
            WHERE qs.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
            AND   qs.execution_count_delta > 0
        ) AS d
    )
SELECT
    c.avg_cpu_pct,
    st.total_storage_gb,
    id.idle_db_count,
    provisioning_status =
        CASE
            WHEN c.avg_cpu_pct < 15
            AND  c.max_cpu_pct < 40
            AND  ISNULL(m.memory_ratio, 0) < 0.5
            THEN N'OVER_PROVISIONED'
            WHEN c.p95_cpu_pct > 85
            OR   ISNULL(m.memory_ratio, 0) > 0.95
            THEN N'UNDER_PROVISIONED'
            ELSE N'RIGHT_SIZED'
        END
FROM (SELECT 1 AS x) AS anchor
LEFT JOIN cpu_24h AS c
  ON 1 = 1
LEFT JOIN mem_latest AS m
  ON 1 = 1
LEFT JOIN storage_total AS st
  ON 1 = 1
LEFT JOIN idle_dbs AS id
  ON 1 = 1
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_ServerMetrics", query, connection))
            {
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
            }

            return (null, null, null, null);
        }
    }
}
