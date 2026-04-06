/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // FinOps Tab Data Access
        // ============================================

        /// <summary>
        /// Fetches per-database resource usage from report.finops_database_resource_usage.
        /// </summary>
        public async Task<List<FinOpsDatabaseResourceUsage>> GetFinOpsDatabaseResourceUsageAsync(int hoursBack = 24)
        {
            var items = new List<FinOpsDatabaseResourceUsage>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    workload_stats AS
    (
        SELECT
            database_name = qs.database_name,
            cpu_time_ms =
                SUM(qs.total_worker_time_delta) / 1000,
            logical_reads =
                SUM(qs.total_logical_reads_delta),
            physical_reads =
                SUM(qs.total_physical_reads_delta),
            logical_writes =
                SUM(qs.total_logical_writes_delta),
            execution_count =
                SUM(qs.execution_count_delta)
        FROM collect.query_stats AS qs
        WHERE qs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
        AND   qs.total_worker_time_delta IS NOT NULL
        GROUP BY
            qs.database_name
    ),
    io_stats AS
    (
        SELECT
            database_name = fio.database_name,
            io_read_bytes =
                SUM(fio.num_of_bytes_read_delta),
            io_write_bytes =
                SUM(fio.num_of_bytes_written_delta),
            io_stall_ms =
                SUM(fio.io_stall_ms_delta)
        FROM collect.file_io_stats AS fio
        WHERE fio.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
        AND   fio.num_of_bytes_read_delta IS NOT NULL
        GROUP BY
            fio.database_name
    ),
    totals AS
    (
        SELECT
            total_cpu_ms =
                NULLIF(SUM(ws.cpu_time_ms), 0),
            total_io_bytes =
                NULLIF
                (
                    SUM(ios.io_read_bytes) +
                    SUM(ios.io_write_bytes),
                    0
                )
        FROM workload_stats AS ws
        FULL JOIN io_stats AS ios
          ON ios.database_name = ws.database_name
    )
SELECT
    database_name =
        COALESCE(ws.database_name, ios.database_name),
    cpu_time_ms =
        ISNULL(ws.cpu_time_ms, 0),
    logical_reads =
        ISNULL(ws.logical_reads, 0),
    physical_reads =
        ISNULL(ws.physical_reads, 0),
    logical_writes =
        ISNULL(ws.logical_writes, 0),
    execution_count =
        ISNULL(ws.execution_count, 0),
    io_read_mb =
        CONVERT
        (
            decimal(19,2),
            ISNULL(ios.io_read_bytes, 0) / 1048576.0
        ),
    io_write_mb =
        CONVERT
        (
            decimal(19,2),
            ISNULL(ios.io_write_bytes, 0) / 1048576.0
        ),
    io_stall_ms =
        ISNULL(ios.io_stall_ms, 0),
    pct_cpu_share =
        CONVERT
        (
            decimal(5,2),
            ISNULL(ws.cpu_time_ms, 0) * 100.0 /
              t.total_cpu_ms
        ),
    pct_io_share =
        CONVERT
        (
            decimal(5,2),
            (ISNULL(ios.io_read_bytes, 0) + ISNULL(ios.io_write_bytes, 0)) * 100.0 /
              t.total_io_bytes
        )
FROM workload_stats AS ws
FULL JOIN io_stats AS ios
  ON ios.database_name = ws.database_name
CROSS JOIN totals AS t
ORDER BY
    ISNULL(ws.cpu_time_ms, 0) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@hoursBack", hoursBack);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_DatabaseResourceUsage", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsDatabaseResourceUsage
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        CpuTimeMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                        LogicalReads = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                        PhysicalReads = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                        LogicalWrites = reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4)),
                        ExecutionCount = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                        IoReadMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                        IoWriteMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                        IoStallMs = reader.IsDBNull(8) ? 0 : Convert.ToInt64(reader.GetValue(8)),
                        PctCpuShare = reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                        PctIoShare = reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10))
                    });
                }
            }

            return items;
        }

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
        /// Fetches per-application resource usage from report.finops_application_resource_usage.
        /// </summary>
        public async Task<List<FinOpsApplicationResourceUsage>> GetFinOpsApplicationResourceUsageAsync()
        {
            var items = new List<FinOpsApplicationResourceUsage>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    application_name,
                    avg_connections,
                    max_connections,
                    sample_count,
                    first_seen,
                    last_seen
                FROM report.finops_application_resource_usage
                ORDER BY
                    max_connections DESC
                OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_ApplicationResourceUsage", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsApplicationResourceUsage
                    {
                        ApplicationName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        AvgConnections = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                        MaxConnections = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                        SampleCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                        FirstSeen = reader.IsDBNull(4) ? DateTime.MinValue : reader.GetDateTime(4),
                        LastSeen = reader.IsDBNull(5) ? DateTime.MinValue : reader.GetDateTime(5)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Fetches latest database size stats from collect.database_size_stats.
        /// </summary>
        public async Task<List<FinOpsDatabaseSizeStats>> GetFinOpsDatabaseSizeStatsAsync()
        {
            var items = new List<FinOpsDatabaseSizeStats>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    collection_time,
                    database_name,
                    database_id,
                    file_id,
                    file_type_desc,
                    file_name,
                    physical_name,
                    total_size_mb,
                    used_size_mb,
                    free_space_mb,
                    used_pct,
                    auto_growth_mb,
                    max_size_mb,
                    recovery_model_desc,
                    compatibility_level,
                    state_desc,
                    volume_mount_point,
                    volume_total_mb,
                    volume_free_mb,
                    is_percent_growth,
                    growth_pct,
                    vlf_count
                FROM collect.database_size_stats
                WHERE collection_time =
                (
                    SELECT
                        MAX(collection_time)
                    FROM collect.database_size_stats
                )
                ORDER BY
                    database_name,
                    file_type_desc,
                    file_name
                OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_DatabaseSizeStats", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsDatabaseSizeStats
                    {
                        CollectionTime = reader.IsDBNull(0) ? DateTime.MinValue : reader.GetDateTime(0),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        DatabaseId = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                        FileId = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                        FileTypeDesc = reader.IsDBNull(4) ? "" : reader.GetString(4),
                        FileName = reader.IsDBNull(5) ? "" : reader.GetString(5),
                        PhysicalName = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        TotalSizeMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                        UsedSizeMb = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8)),
                        FreeSpaceMb = reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                        UsedPct = reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10)),
                        AutoGrowthMb = reader.IsDBNull(11) ? 0m : Convert.ToDecimal(reader.GetValue(11)),
                        MaxSizeMb = reader.IsDBNull(12) ? 0m : Convert.ToDecimal(reader.GetValue(12)),
                        RecoveryModelDesc = reader.IsDBNull(13) ? "" : reader.GetString(13),
                        CompatibilityLevel = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14)),
                        StateDesc = reader.IsDBNull(15) ? "" : reader.GetString(15),
                        VolumeMountPoint = reader.IsDBNull(16) ? "" : reader.GetString(16),
                        VolumeTotalMb = reader.IsDBNull(17) ? 0m : Convert.ToDecimal(reader.GetValue(17)),
                        VolumeFreeMb = reader.IsDBNull(18) ? 0m : Convert.ToDecimal(reader.GetValue(18)),
                        IsPercentGrowth = reader.IsDBNull(19) ? null : (bool?)(Convert.ToInt32(reader.GetValue(19)) == 1),
                        GrowthPct = reader.IsDBNull(20) ? null : Convert.ToInt32(reader.GetValue(20)),
                        VlfCount = reader.IsDBNull(21) ? null : Convert.ToInt32(reader.GetValue(21))
                    });
                }
            }

            return items;
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
        CONVERT(int, SERVERPROPERTY('IsClustered'))
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

        /// <summary>
        /// Gets top N databases by total CPU for the utilization summary.
        /// </summary>
        public async Task<List<FinOpsTopResourceConsumer>> GetFinOpsTopResourceConsumersByTotalAsync(int hoursBack = 24, int topN = 5)
        {
            var items = new List<FinOpsTopResourceConsumer>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    workload AS
    (
        SELECT
            database_name,
            cpu_time_ms =
                SUM(qs.total_worker_time_delta) / 1000,
            execution_count =
                SUM(qs.execution_count_delta)
        FROM collect.query_stats AS qs
        WHERE qs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
        AND   qs.total_worker_time_delta IS NOT NULL
        GROUP BY
            qs.database_name
    ),
    io AS
    (
        SELECT
            database_name,
            io_total_bytes =
                SUM(fio.num_of_bytes_read_delta + fio.num_of_bytes_written_delta)
        FROM collect.file_io_stats AS fio
        WHERE fio.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
        AND   fio.num_of_bytes_read_delta IS NOT NULL
        GROUP BY
            fio.database_name
    ),
    combined AS
    (
        SELECT
            database_name =
                COALESCE(w.database_name, i.database_name),
            cpu_time_ms =
                ISNULL(w.cpu_time_ms, 0),
            execution_count =
                ISNULL(w.execution_count, 0),
            io_total_mb =
                CONVERT(decimal(19,2), ISNULL(i.io_total_bytes, 0) / 1048576.0)
        FROM workload AS w
        FULL JOIN io AS i
          ON i.database_name = w.database_name
    ),
    totals AS
    (
        SELECT
            total_cpu =
                NULLIF(SUM(cpu_time_ms), 0),
            total_io =
                NULLIF(SUM(io_total_mb), 0)
        FROM combined
    )
SELECT TOP(@topN)
    c.database_name,
    c.cpu_time_ms,
    c.execution_count,
    c.io_total_mb,
    pct_cpu =
        CONVERT(decimal(5,2), c.cpu_time_ms * 100.0 / t.total_cpu),
    pct_io =
        CONVERT(decimal(5,2), c.io_total_mb * 100.0 / t.total_io)
FROM combined AS c
CROSS JOIN totals AS t
ORDER BY
    c.cpu_time_ms DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@hoursBack", hoursBack);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_TopResourceByTotal", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsTopResourceConsumer
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        CpuTimeMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                        ExecutionCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                        IoTotalMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        PctCpu = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                        PctIo = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets top N databases by average CPU per execution for the utilization summary.
        /// </summary>
        public async Task<List<FinOpsTopResourceConsumer>> GetFinOpsTopResourceConsumersByAvgAsync(int hoursBack = 24, int topN = 5)
        {
            var items = new List<FinOpsTopResourceConsumer>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    workload AS
    (
        SELECT
            database_name,
            cpu_time_ms =
                SUM(qs.total_worker_time_delta) / 1000,
            execution_count =
                SUM(qs.execution_count_delta)
        FROM collect.query_stats AS qs
        WHERE qs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
        AND   qs.total_worker_time_delta IS NOT NULL
        GROUP BY
            qs.database_name
        HAVING
            SUM(qs.execution_count_delta) > 0
    ),
    io AS
    (
        SELECT
            database_name,
            io_total_mb =
                SUM(fio.num_of_bytes_read_delta + fio.num_of_bytes_written_delta) / 1048576.0
        FROM collect.file_io_stats AS fio
        WHERE fio.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
        AND   fio.num_of_bytes_read_delta IS NOT NULL
        GROUP BY
            fio.database_name
    )
SELECT TOP(@topN)
    w.database_name,
    avg_cpu_ms =
        CONVERT(decimal(19,2), w.cpu_time_ms * 1.0 / w.execution_count),
    w.execution_count,
    io_total_mb =
        CONVERT(decimal(19,2), ISNULL(i.io_total_mb, 0)),
    w.cpu_time_ms,
    avg_io_mb =
        CONVERT(decimal(19,4), ISNULL(i.io_total_mb, 0) * 1.0 / w.execution_count)
FROM workload AS w
LEFT JOIN io AS i
  ON i.database_name = w.database_name
ORDER BY
    avg_cpu_ms DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@hoursBack", hoursBack);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_TopResourceByAvg", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsTopResourceConsumer
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        CpuTimeMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                        ExecutionCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                        IoTotalMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        TotalCpuTimeMs = reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4)),
                        AvgIoMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets per-database total allocated and used space for the utilization size chart.
        /// </summary>
        public async Task<List<FinOpsDatabaseSizeSummary>> GetFinOpsDatabaseSizeSummaryAsync(int topN = 10)
        {
            var items = new List<FinOpsDatabaseSizeSummary>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP(@topN)
    database_name,
    total_mb =
        SUM(total_size_mb),
    used_mb =
        SUM(used_size_mb)
FROM collect.database_size_stats
WHERE collection_time =
(
    SELECT MAX(collection_time)
    FROM collect.database_size_stats
)
GROUP BY
    database_name
ORDER BY
    SUM(total_size_mb) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_DatabaseSizeSummary", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsDatabaseSizeSummary
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        TotalMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        UsedMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets per-database storage growth trends comparing current size to 7d and 30d ago.
        /// </summary>
        public async Task<List<FinOpsStorageGrowthRow>> GetFinOpsStorageGrowthAsync()
        {
            var items = new List<FinOpsStorageGrowthRow>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    boundaries AS
    (
        SELECT
            latest_time  = MAX(collection_time),
            earliest_time = MIN(collection_time),
            days_of_data = DATEDIFF(DAY, MIN(collection_time), MAX(collection_time))
        FROM collect.database_size_stats
    ),
    latest AS
    (
        SELECT
            database_name,
            current_size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT latest_time
            FROM boundaries
        )
        GROUP BY
            database_name
    ),
    past_7d AS
    (
        SELECT
            database_name,
            size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.database_size_stats
            WHERE collection_time <= DATEADD(DAY, -7, SYSDATETIME())
        )
        GROUP BY
            database_name
    ),
    past_30d AS
    (
        SELECT
            database_name,
            size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.database_size_stats
            WHERE collection_time <= DATEADD(DAY, -30, SYSDATETIME())
        )
        GROUP BY
            database_name
    ),
    oldest AS
    (
        SELECT
            database_name,
            size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT earliest_time
            FROM boundaries
        )
        GROUP BY
            database_name
    )
SELECT
    l.database_name,
    l.current_size_mb,
    COALESCE(p7.size_mb, o.size_mb),
    COALESCE(p30.size_mb, p7.size_mb, o.size_mb),
    growth_7d_mb =
        l.current_size_mb - COALESCE(p7.size_mb, o.size_mb, l.current_size_mb),
    growth_30d_mb =
        l.current_size_mb - COALESCE(p30.size_mb, p7.size_mb, o.size_mb, l.current_size_mb),
    daily_growth_rate_mb =
        CASE
            WHEN b.days_of_data >= 1
            THEN (l.current_size_mb - COALESCE(o.size_mb, l.current_size_mb)) / CAST(b.days_of_data AS decimal(10,1))
            ELSE 0
        END,
    growth_pct_30d =
        CASE
            WHEN COALESCE(p30.size_mb, p7.size_mb, o.size_mb) IS NOT NULL
            AND  COALESCE(p30.size_mb, p7.size_mb, o.size_mb) > 0
            THEN (l.current_size_mb - COALESCE(p30.size_mb, p7.size_mb, o.size_mb)) * 100.0
                 / COALESCE(p30.size_mb, p7.size_mb, o.size_mb)
            ELSE 0
        END
FROM latest AS l
CROSS JOIN boundaries AS b
LEFT JOIN past_7d AS p7
  ON p7.database_name = l.database_name
LEFT JOIN past_30d AS p30
  ON p30.database_name = l.database_name
LEFT JOIN oldest AS o
  ON o.database_name = l.database_name
ORDER BY
    l.current_size_mb - COALESCE(p30.size_mb, p7.size_mb, o.size_mb, l.current_size_mb) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_StorageGrowth", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsStorageGrowthRow
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
            }

            return items;
        }

        /// <summary>
        /// Detects databases with zero query executions over the last N days.
        /// </summary>
        public async Task<List<FinOpsIdleDatabase>> GetFinOpsIdleDatabasesAsync(int daysBack = 7)
        {
            var items = new List<FinOpsIdleDatabase>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    db_sizes AS
    (
        SELECT
            database_name,
            total_size_mb =
                SUM(total_size_mb),
            file_count =
                COUNT(*)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.database_size_stats
        )
        GROUP BY
            database_name
    ),
    db_activity AS
    (
        SELECT
            database_name,
            total_executions =
                SUM(execution_count_delta),
            last_execution =
                MAX(last_execution_time)
        FROM collect.query_stats
        WHERE collection_time >= DATEADD(DAY, -@daysBack, SYSDATETIME())
        AND   execution_count_delta IS NOT NULL
        GROUP BY
            database_name
    )
SELECT
    ds.database_name,
    ds.total_size_mb,
    ds.file_count,
    a.last_execution
FROM db_sizes AS ds
LEFT JOIN db_activity AS a
  ON a.database_name = ds.database_name
WHERE ISNULL(a.total_executions, 0) = 0
AND   ds.database_name NOT IN (N'master', N'model', N'msdb', N'tempdb', N'PerformanceMonitor')
ORDER BY
    ds.total_size_mb DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@daysBack", daysBack);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_IdleDatabases", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsIdleDatabase
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        TotalSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        FileCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                        LastExecutionTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets tempdb pressure summary: latest and 24h peak values.
        /// </summary>
        public async Task<List<FinOpsTempdbSummary>> GetFinOpsTempdbSummaryAsync()
        {
            var items = new List<FinOpsTempdbSummary>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    latest AS
    (
        SELECT TOP (1)
            user_object_reserved_mb,
            internal_object_reserved_mb,
            version_store_reserved_mb,
            total_reserved_mb
        FROM collect.tempdb_stats
        ORDER BY
            collection_time DESC
    ),
    peak AS
    (
        SELECT
            max_user_mb =
                MAX(user_object_reserved_mb),
            max_internal_mb =
                MAX(internal_object_reserved_mb),
            max_version_store_mb =
                MAX(version_store_reserved_mb),
            max_total_mb =
                MAX(total_reserved_mb)
        FROM collect.tempdb_stats
        WHERE collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
    )
SELECT
    metric = N'User Objects',
    current_mb = l.user_object_reserved_mb,
    peak_24h_mb = p.max_user_mb,
    warning =
        CASE
            WHEN p.max_user_mb > 1024
            THEN N'High user object usage'
            ELSE N''
        END
FROM latest AS l
CROSS JOIN peak AS p
UNION ALL
SELECT
    N'Internal Objects',
    l.internal_object_reserved_mb,
    p.max_internal_mb,
    CASE
        WHEN p.max_internal_mb > 1024
        THEN N'High internal object usage (sorts/hashes)'
        ELSE N''
    END
FROM latest AS l
CROSS JOIN peak AS p
UNION ALL
SELECT
    N'Version Store',
    l.version_store_reserved_mb,
    p.max_version_store_mb,
    CASE
        WHEN p.max_version_store_mb > 2048
        THEN N'Version store pressure — check long-running transactions'
        ELSE N''
    END
FROM latest AS l
CROSS JOIN peak AS p
UNION ALL
SELECT
    N'Total Reserved',
    l.total_reserved_mb,
    p.max_total_mb,
    N''
FROM latest AS l
CROSS JOIN peak AS p
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_TempdbSummary", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsTempdbSummary
                    {
                        Metric = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        CurrentMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        Peak24hMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                        Warning = reader.IsDBNull(3) ? "" : reader.GetString(3)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets wait stats grouped by cost category over the last 24 hours.
        /// </summary>
        public async Task<List<FinOpsWaitCategorySummary>> GetFinOpsWaitCategorySummaryAsync(int hoursBack = 24)
        {
            var items = new List<FinOpsWaitCategorySummary>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    categorized AS
    (
        SELECT
            category =
                CASE
                    WHEN wait_type IN (N'SOS_SCHEDULER_YIELD', N'CXPACKET', N'CXCONSUMER', N'CXSYNC_PORT', N'CXSYNC_CONSUMER')
                    THEN N'CPU'
                    WHEN wait_type LIKE N'PAGEIOLATCH%'
                    OR   wait_type IN (N'WRITELOG', N'IO_COMPLETION', N'ASYNC_IO_COMPLETION')
                    THEN N'Storage'
                    WHEN wait_type IN (N'RESOURCE_SEMAPHORE', N'RESOURCE_SEMAPHORE_QUERY_COMPILE', N'CMEMTHREAD')
                    THEN N'Memory'
                    WHEN wait_type = N'ASYNC_NETWORK_IO'
                    THEN N'Network'
                    WHEN wait_type LIKE N'LCK_M_%'
                    THEN N'Locks'
                    ELSE N'Other'
                END,
            wait_type,
            wait_time_ms =
                SUM(wait_time_ms_delta),
            waiting_tasks =
                SUM(waiting_tasks_count_delta)
        FROM collect.wait_stats
        WHERE collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
        AND   wait_time_ms_delta IS NOT NULL
        AND   wait_time_ms_delta > 0
        GROUP BY
            CASE
                WHEN wait_type IN (N'SOS_SCHEDULER_YIELD', N'CXPACKET', N'CXCONSUMER', N'CXSYNC_PORT', N'CXSYNC_CONSUMER')
                THEN N'CPU'
                WHEN wait_type LIKE N'PAGEIOLATCH%'
                OR   wait_type IN (N'WRITELOG', N'IO_COMPLETION', N'ASYNC_IO_COMPLETION')
                THEN N'Storage'
                WHEN wait_type IN (N'RESOURCE_SEMAPHORE', N'RESOURCE_SEMAPHORE_QUERY_COMPILE', N'CMEMTHREAD')
                THEN N'Memory'
                WHEN wait_type = N'ASYNC_NETWORK_IO'
                THEN N'Network'
                WHEN wait_type LIKE N'LCK_M_%'
                THEN N'Locks'
                ELSE N'Other'
            END,
            wait_type
    ),
    by_category AS
    (
        SELECT
            category,
            total_wait_time_ms =
                SUM(wait_time_ms),
            total_waiting_tasks =
                SUM(waiting_tasks),
            top_wait_type =
                MAX(CASE WHEN rn = 1 THEN wait_type END),
            top_wait_time_ms =
                MAX(CASE WHEN rn = 1 THEN wait_time_ms END)
        FROM
        (
            SELECT
                *,
                rn = ROW_NUMBER() OVER
                (
                    PARTITION BY category
                    ORDER BY wait_time_ms DESC
                )
            FROM categorized
        ) AS ranked
        GROUP BY
            category
    ),
    grand_total AS
    (
        SELECT
            total = NULLIF(SUM(total_wait_time_ms), 0)
        FROM by_category
    )
SELECT
    bc.category,
    bc.total_wait_time_ms,
    bc.total_waiting_tasks,
    pct_of_total =
        CONVERT
        (
            decimal(5,1),
            bc.total_wait_time_ms * 100.0 / gt.total
        ),
    bc.top_wait_type,
    bc.top_wait_time_ms
FROM by_category AS bc
CROSS JOIN grand_total AS gt
ORDER BY
    bc.total_wait_time_ms DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@hoursBack", hoursBack);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_WaitCategorySummary", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsWaitCategorySummary
                    {
                        Category = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        TotalWaitTimeMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                        WaitingTasks = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                        PctOfTotal = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        TopWaitType = reader.IsDBNull(4) ? "" : reader.GetString(4),
                        TopWaitTimeMs = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets top 20 most expensive queries by total CPU over the last 24 hours.
        /// </summary>
        public async Task<List<FinOpsExpensiveQuery>> GetFinOpsExpensiveQueriesAsync(int hoursBack = 24, int topN = 20)
        {
            var items = new List<FinOpsExpensiveQuery>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP(@topN)
    qs.database_name,
    total_cpu_ms =
        SUM(qs.total_worker_time_delta) / 1000,
    avg_cpu_ms_per_exec =
        CONVERT
        (
            decimal(19,2),
            SUM(qs.total_worker_time_delta) / 1000.0 /
              NULLIF(SUM(qs.execution_count_delta), 0)
        ),
    total_reads =
        SUM(qs.total_logical_reads_delta),
    avg_reads_per_exec =
        CONVERT
        (
            decimal(19,0),
            SUM(qs.total_logical_reads_delta) * 1.0 /
              NULLIF(SUM(qs.execution_count_delta), 0)
        ),
    executions =
        SUM(qs.execution_count_delta),
    query_preview =
        LEFT
        (
            CONVERT
            (
                nvarchar(max),
                DECOMPRESS(qs.query_text)
            ),
            200
        ),
    full_query_text =
        CONVERT
        (
            nvarchar(max),
            DECOMPRESS(qs.query_text)
        )
FROM collect.query_stats AS qs
WHERE qs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
AND   qs.total_worker_time_delta IS NOT NULL
AND   qs.total_worker_time_delta > 0
GROUP BY
    qs.database_name,
    qs.sql_handle,
    qs.statement_start_offset,
    qs.statement_end_offset,
    qs.query_text
ORDER BY
    SUM(qs.total_worker_time_delta) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@hoursBack", hoursBack);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_ExpensiveQueries", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsExpensiveQuery
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        TotalCpuMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                        AvgCpuMsPerExec = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                        TotalReads = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                        AvgReadsPerExec = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                        Executions = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                        QueryPreview = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        FullQueryText = reader.IsDBNull(7) ? "" : reader.GetString(7)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Fetches high-impact queries — 80/20 analysis across CPU, duration, reads, writes, memory, executions.
        /// </summary>
        public async Task<List<FinOpsHighImpactQuery>> GetFinOpsHighImpactQueriesAsync(int hoursBack = 24)
        {
            var items = new List<FinOpsHighImpactQuery>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @cutoff datetime2(7) = DATEADD(HOUR, -@hoursBack, SYSDATETIME());

WITH
    agg AS
    (
        SELECT
            qs.query_hash,
            database_name = MIN(qs.database_name),
            total_executions = SUM(qs.execution_count_delta),
            total_cpu_ms = SUM(qs.total_worker_time_delta) / 1000.0,
            total_duration_ms = SUM(qs.total_elapsed_time_delta) / 1000.0,
            total_reads = SUM(qs.total_logical_reads_delta),
            total_writes = SUM(qs.total_logical_writes_delta),
            total_memory_mb = SUM(ISNULL(qs.max_grant_kb, 0)) / 1024.0
        FROM collect.query_stats AS qs
        WHERE qs.collection_time >= @cutoff
        AND   qs.query_hash IS NOT NULL
        AND   qs.execution_count_delta > 0
        GROUP BY
            qs.query_hash
        HAVING
            SUM(qs.execution_count_delta) > 0
    ),
    interesting AS
    (
        SELECT query_hash FROM (SELECT TOP (10) query_hash FROM agg ORDER BY total_cpu_ms DESC) x
        UNION
        SELECT query_hash FROM (SELECT TOP (10) query_hash FROM agg ORDER BY total_duration_ms DESC) x
        UNION
        SELECT query_hash FROM (SELECT TOP (10) query_hash FROM agg ORDER BY total_reads DESC) x
        UNION
        SELECT query_hash FROM (SELECT TOP (10) query_hash FROM agg ORDER BY total_writes DESC) x
        UNION
        SELECT query_hash FROM (SELECT TOP (10) query_hash FROM agg ORDER BY total_memory_mb DESC) x
        UNION
        SELECT query_hash FROM (SELECT TOP (10) query_hash FROM agg ORDER BY total_executions DESC) x
    ),
    scored AS
    (
        SELECT
            a.*,
            cpu_pctl = PERCENT_RANK() OVER (ORDER BY a.total_cpu_ms),
            duration_pctl = PERCENT_RANK() OVER (ORDER BY a.total_duration_ms),
            reads_pctl = PERCENT_RANK() OVER (ORDER BY a.total_reads),
            writes_pctl = PERCENT_RANK() OVER (ORDER BY a.total_writes),
            memory_pctl = PERCENT_RANK() OVER (ORDER BY a.total_memory_mb),
            executions_pctl = PERCENT_RANK() OVER (ORDER BY a.total_executions),
            cpu_share = CONVERT(decimal(5,1), 100.0 * a.total_cpu_ms / NULLIF(SUM(a.total_cpu_ms) OVER (), 0)),
            duration_share = CONVERT(decimal(5,1), 100.0 * a.total_duration_ms / NULLIF(SUM(a.total_duration_ms) OVER (), 0)),
            reads_share = CONVERT(decimal(5,1), 100.0 * a.total_reads / NULLIF(SUM(CONVERT(float, a.total_reads)) OVER (), 0)),
            writes_share = CONVERT(decimal(5,1), 100.0 * a.total_writes / NULLIF(SUM(CONVERT(float, a.total_writes)) OVER (), 0)),
            memory_share = CONVERT(decimal(5,1), 100.0 * a.total_memory_mb / NULLIF(SUM(a.total_memory_mb) OVER (), 0)),
            executions_share = CONVERT(decimal(5,1), 100.0 * a.total_executions / NULLIF(SUM(CONVERT(float, a.total_executions)) OVER (), 0))
        FROM agg AS a
        JOIN interesting AS i
          ON a.query_hash = i.query_hash
    ),
    with_text AS
    (
        SELECT
            s.*,
            sample_query_text =
            (
                SELECT TOP (1)
                    CASE
                        WHEN qs2.query_text IS NOT NULL
                        THEN CAST(DECOMPRESS(qs2.query_text) AS nvarchar(max))
                        ELSE N''
                    END
                FROM collect.query_stats AS qs2
                WHERE qs2.query_hash = s.query_hash
                AND   qs2.collection_time >= @cutoff
                AND   qs2.query_text IS NOT NULL
                ORDER BY
                    qs2.execution_count_delta DESC
            )
        FROM scored AS s
    )
SELECT
    query_hash_display = CONVERT(varchar(20), query_hash, 1),
    database_name,
    total_executions,
    total_cpu_ms,
    total_duration_ms,
    total_reads,
    total_writes,
    total_memory_mb,
    cpu_share,
    duration_share,
    reads_share,
    writes_share,
    memory_share,
    executions_share,
    impact_score =
        CONVERT(int,
        (
            ISNULL(cpu_pctl, 0) +
            ISNULL(duration_pctl, 0) +
            ISNULL(reads_pctl, 0) +
            ISNULL(writes_pctl, 0) +
            ISNULL(memory_pctl, 0) +
            ISNULL(executions_pctl, 0)
        ) /
        (
            CASE WHEN cpu_pctl IS NOT NULL THEN 1.0 ELSE 0 END +
            CASE WHEN duration_pctl IS NOT NULL THEN 1.0 ELSE 0 END +
            CASE WHEN reads_pctl IS NOT NULL THEN 1.0 ELSE 0 END +
            CASE WHEN writes_pctl IS NOT NULL THEN 1.0 ELSE 0 END +
            CASE WHEN memory_pctl IS NOT NULL THEN 1.0 ELSE 0 END +
            CASE WHEN executions_pctl IS NOT NULL THEN 1.0 ELSE 0 END
        ) * 100),
    query_preview = LEFT(sample_query_text, 200),
    full_query_text = sample_query_text
FROM with_text
ORDER BY
    (
        ISNULL(cpu_pctl, 0) +
        ISNULL(duration_pctl, 0) +
        ISNULL(reads_pctl, 0) +
        ISNULL(writes_pctl, 0) +
        ISNULL(memory_pctl, 0) +
        ISNULL(executions_pctl, 0)
    ) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@hoursBack", hoursBack);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_HighImpactQueries", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsHighImpactQuery
                    {
                        QueryHashDisplay = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        TotalExecutions = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                        TotalCpuMs = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        TotalDurationMs = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                        TotalReads = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                        TotalWrites = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                        TotalMemoryMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                        CpuShare = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8)),
                        DurationShare = reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                        ReadsShare = reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10)),
                        WritesShare = reader.IsDBNull(11) ? 0m : Convert.ToDecimal(reader.GetValue(11)),
                        MemoryShare = reader.IsDBNull(12) ? 0m : Convert.ToDecimal(reader.GetValue(12)),
                        ExecutionsShare = reader.IsDBNull(13) ? 0m : Convert.ToDecimal(reader.GetValue(13)),
                        ImpactScore = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14)),
                        SampleQueryText = reader.IsDBNull(15) ? "" : reader.GetString(15),
                        FullQueryText = reader.IsDBNull(16) ? "" : reader.GetString(16)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Checks if sp_IndexCleanup is installed on the target server.
        /// </summary>
        public async Task<bool> CheckSpIndexCleanupExistsAsync()
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            using var command = new SqlCommand("SELECT OBJECT_ID('dbo.sp_IndexCleanup', 'P')", connection);
            command.CommandTimeout = 30;
            var result = await command.ExecuteScalarAsync();
            return result != null && result != DBNull.Value;
        }

        /// <summary>
        /// Runs sp_IndexCleanup and returns both detail and summary result sets.
        /// </summary>
        public async Task<(List<IndexCleanupResult> Details, List<IndexCleanupSummary> Summaries)> RunIndexAnalysisAsync(string? databaseName, bool getAllDatabases)
        {
            var details = new List<IndexCleanupResult>();
            var summaries = new List<IndexCleanupSummary>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

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

            // Result set 1: Detail rows
            while (await reader.ReadAsync())
            {
                details.Add(new IndexCleanupResult
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

            // Result set 2: Summary rows (if present)
            if (await reader.NextResultAsync())
            {
                while (await reader.ReadAsync())
                {
                    var fc = reader.FieldCount;
                    string Col(int i) => fc > i && !reader.IsDBNull(i) ? reader.GetValue(i).ToString() ?? "" : "";
                    summaries.Add(new IndexCleanupSummary
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
        public async Task<List<FinOpsProvisioningTrend>> GetFinOpsProvisioningTrendAsync()
        {
            var items = new List<FinOpsProvisioningTrend>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    daily_cpu AS
    (
        SELECT DISTINCT
            day = CONVERT(date, cu.collection_time),
            avg_cpu_pct =
                AVG(CONVERT(decimal(5,2), cu.sqlserver_cpu_utilization))
                OVER (PARTITION BY CONVERT(date, cu.collection_time)),
            max_cpu_pct =
                MAX(cu.sqlserver_cpu_utilization)
                OVER (PARTITION BY CONVERT(date, cu.collection_time)),
            p95_cpu_pct =
                CONVERT
                (
                    decimal(5,2),
                    PERCENTILE_CONT(0.95)
                    WITHIN GROUP (ORDER BY cu.sqlserver_cpu_utilization)
                    OVER (PARTITION BY CONVERT(date, cu.collection_time))
                )
        FROM collect.cpu_utilization_stats AS cu
        WHERE cu.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
    ),
    daily_mem AS
    (
        SELECT
            day = CONVERT(date, ms.collection_time),
            avg_memory_ratio =
                AVG
                (
                    CONVERT(decimal(10,4), ms.total_memory_mb) /
                    NULLIF(ms.committed_target_memory_mb, 0)
                )
        FROM collect.memory_stats AS ms
        WHERE ms.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
        GROUP BY
            CONVERT(date, ms.collection_time)
    )
SELECT
    c.day,
    c.avg_cpu_pct,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    ISNULL(m.avg_memory_ratio, 0),
    provisioning_status =
        CASE
            WHEN c.avg_cpu_pct < 15
            AND  c.max_cpu_pct < 40
            AND  ISNULL(m.avg_memory_ratio, 0) < 0.5
            THEN N'OVER_PROVISIONED'
            WHEN c.p95_cpu_pct > 85
            OR   ISNULL(m.avg_memory_ratio, 0) > 0.95
            THEN N'UNDER_PROVISIONED'
            ELSE N'RIGHT_SIZED'
        END
FROM daily_cpu AS c
LEFT JOIN daily_mem AS m
  ON m.day = c.day
ORDER BY
    c.day
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_ProvisioningTrend", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsProvisioningTrend
                    {
                        Day = reader.GetDateTime(0),
                        AvgCpuPct = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        MaxCpuPct = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                        P95CpuPct = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        MemoryRatio = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                        Status = reader.IsDBNull(5) ? "" : reader.GetString(5)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets memory grant efficiency from resource semaphore data.
        /// </summary>
        public async Task<List<FinOpsMemoryGrantEfficiency>> GetFinOpsMemoryGrantEfficiencyAsync(int hoursBack = 24)
        {
            var items = new List<FinOpsMemoryGrantEfficiency>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    day = CONVERT(date, mg.collection_time),
    avg_granted_mb =
        AVG(mg.granted_memory_mb),
    avg_used_mb =
        AVG(mg.used_memory_mb),
    efficiency_pct =
        CONVERT
        (
            decimal(5,1),
            AVG(mg.used_memory_mb) * 100.0 /
            NULLIF(AVG(mg.granted_memory_mb), 0)
        ),
    peak_granted_mb =
        MAX(mg.granted_memory_mb),
    total_grantees =
        SUM(mg.grantee_count),
    total_waiters =
        SUM(mg.waiter_count),
    timeout_errors =
        SUM(mg.timeout_error_count_delta),
    forced_grants =
        SUM(mg.forced_grant_count_delta)
FROM collect.memory_grant_stats AS mg
WHERE mg.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
GROUP BY
    CONVERT(date, mg.collection_time)
ORDER BY
    CONVERT(date, mg.collection_time)
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@hoursBack", hoursBack);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_MemoryGrantEfficiency", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsMemoryGrantEfficiency
                    {
                        Day = reader.GetDateTime(0),
                        AvgGrantedMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        AvgUsedMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                        EfficiencyPct = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        PeakGrantedMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                        TotalGrantees = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                        TotalWaiters = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                        TimeoutErrors = reader.IsDBNull(7) ? 0 : Convert.ToInt64(reader.GetValue(7)),
                        ForcedGrants = reader.IsDBNull(8) ? 0 : Convert.ToInt64(reader.GetValue(8))
                    });
                }
            }

            return items;
        }

        // ============================================
        // FinOps Recommendations Engine
        // ============================================

        /// <summary>
        /// Runs all Phase 1 recommendation checks and returns a consolidated list.
        /// </summary>
        public async Task<List<FinOpsRecommendation>> GetFinOpsRecommendationsAsync(decimal monthlyCost)
        {
            var recommendations = new List<FinOpsRecommendation>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            // 1. Enterprise feature usage audit
            try
            {
                using var editionCmd = new SqlCommand(
                    "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128))", connection);
                editionCmd.CommandTimeout = 30;
                var edition = (string?)await editionCmd.ExecuteScalarAsync() ?? "";

                if (edition.Contains("Enterprise", StringComparison.OrdinalIgnoreCase))
                {
                    /*
                    sys.dm_db_persisted_sku_features is database-scoped on all versions.
                    Query across all online user databases for TDE usage — the only feature
                    still Enterprise-only since 2016 SP1 (Compression, Partitioning,
                    ColumnStoreIndex are all available in Standard).
                    */
                    using var featCmd = new SqlCommand(@"
DECLARE
    @sql nvarchar(max) = N'';

SELECT
    @sql += N'
SELECT ' + QUOTENAME(name, '''') + N' AS database_name
FROM ' + QUOTENAME(name) + N'.sys.dm_db_persisted_sku_features
WHERE feature_name = N''TransparentDataEncryption''
UNION ALL'
FROM sys.databases
WHERE database_id > 4
AND   state_desc = N'ONLINE';

IF @sql <> N''
BEGIN
    SET @sql = LEFT(@sql, LEN(@sql) - 10);
    EXEC sys.sp_executesql @sql;
END;", connection);
                    featCmd.CommandTimeout = 30;

                    var tdeDbNames = new List<string>();
                    using var featReader = await featCmd.ExecuteReaderAsync();
                    while (await featReader.ReadAsync())
                    {
                        if (!featReader.IsDBNull(0))
                            tdeDbNames.Add(featReader.GetString(0));
                    }

                    if (tdeDbNames.Count == 0)
                    {
                        recommendations.Add(new FinOpsRecommendation
                        {
                            Category = "Licensing",
                            Severity = "High",
                            Confidence = "High",
                            Finding = "Enterprise Edition with no Enterprise-only features detected",
                            Detail = "No databases use Transparent Data Encryption (TDE), the only feature " +
                                     "still restricted to Enterprise Edition since SQL Server 2016 SP1. " +
                                     "Review whether Standard Edition would meet workload requirements for potential license savings.",
                            EstMonthlySavings = monthlyCost > 0 ? monthlyCost * 0.40m : null
                        });
                    }
                    else
                    {
                        recommendations.Add(new FinOpsRecommendation
                        {
                            Category = "Licensing",
                            Severity = "Low",
                            Confidence = "High",
                            Finding = "TDE in use — Enterprise Edition downgrade blocker",
                            Detail = $"The following databases use Transparent Data Encryption: {string.Join(", ", tdeDbNames.Take(20))}" +
                                     (tdeDbNames.Count > 20 ? $" and {tdeDbNames.Count - 20} more" : "") +
                                     ". TDE must be removed before downgrading to Standard Edition."
                        });

                        // Check 10: License cost impact estimate (only when features ARE in use)
                        using var cpuInfoCmd = new SqlCommand(
                            "SELECT cpu_count FROM sys.dm_os_sys_info", connection);
                        cpuInfoCmd.CommandTimeout = 30;
                        var cpuCountObj = await cpuInfoCmd.ExecuteScalarAsync();
                        var coreLicenseCount = cpuCountObj != null ? Convert.ToInt32(cpuCountObj) : 0;
                        if (coreLicenseCount > 0)
                        {
                            var monthlySavings = coreLicenseCount * 5000m / 12m;
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Licensing",
                                Severity = "Low",
                                Confidence = "Low",
                                Finding = $"Enterprise to Standard would save ~${monthlySavings:N0}/mo at list pricing ({coreLicenseCount} cores)",
                                Detail = "Based on list pricing differential of ~$5,000/core/year between Enterprise and Standard. " +
                                         "Actual savings depend on your licensing agreement. See Enterprise feature audit for downgrade blockers.",
                                EstMonthlySavings = monthlySavings
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Enterprise features): {ex.Message}", ex);
            }

            // 2. CPU right-sizing score
            try
            {
                using var cpuCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    v.p95_cpu_pct,
    v.cpu_count,
    v.avg_cpu_pct,
    v.max_cpu_pct
FROM report.finops_utilization_efficiency AS v
OPTION(MAXDOP 1, RECOMPILE);", connection);
                cpuCmd.CommandTimeout = 120;

                using var cpuReader = await cpuCmd.ExecuteReaderAsync();
                if (await cpuReader.ReadAsync())
                {
                    var p95 = cpuReader.IsDBNull(0) ? 0m : Convert.ToDecimal(cpuReader.GetValue(0));
                    var cpuCount = cpuReader.IsDBNull(1) ? 0 : Convert.ToInt32(cpuReader.GetValue(1));
                    var avg = cpuReader.IsDBNull(2) ? 0m : Convert.ToDecimal(cpuReader.GetValue(2));
                    var max = cpuReader.IsDBNull(3) ? 0 : Convert.ToInt32(cpuReader.GetValue(3));

                    if (p95 < 30 && cpuCount > 4)
                    {
                        var targetCores = Math.Max(4, (int)(cpuCount * (p95 / 70m)));
                        var savingsPct = 1m - ((decimal)targetCores / cpuCount);
                        recommendations.Add(new FinOpsRecommendation
                        {
                            Category = "Compute",
                            Severity = p95 < 15 ? "High" : "Medium",
                            Confidence = "Medium",
                            Finding = $"CPU over-provisioned ({cpuCount} cores, P95 = {p95:N1}%)",
                            Detail = $"P95 CPU utilization is {p95:N1}% (avg {avg:N1}%, max {max}%) across {cpuCount} cores. " +
                                     $"Consider reducing to ~{targetCores} cores.",
                            EstMonthlySavings = monthlyCost > 0 ? monthlyCost * savingsPct * 0.60m : null
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (CPU right-sizing): {ex.Message}", ex);
            }

            // 3. Memory right-sizing score
            try
            {
                using var memCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT TOP (1)
    ms.buffer_pool_mb,
    (SELECT CAST(SERVERPROPERTY('PhysicalMemoryInMB') AS INT)) AS physical_memory_mb
FROM collect.memory_stats AS ms
ORDER BY ms.collection_time DESC
OPTION(MAXDOP 1);", connection);
                memCmd.CommandTimeout = 30;

                using var memReader = await memCmd.ExecuteReaderAsync();
                if (await memReader.ReadAsync())
                {
                    var bpMb = memReader.IsDBNull(0) ? 0 : Convert.ToInt32(memReader.GetValue(0));
                    var physMb = memReader.IsDBNull(1) ? 0 : Convert.ToInt32(memReader.GetValue(1));

                    if (physMb > 0)
                    {
                        var bpRatio = (decimal)bpMb / physMb;
                        if (bpRatio < 0.50m && physMb > 8192)
                        {
                            var targetMb = Math.Max(8192, bpMb * 2);
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Memory",
                                Severity = bpRatio < 0.30m ? "High" : "Medium",
                                Confidence = "Medium",
                                Finding = $"Memory over-provisioned (buffer pool uses {bpRatio:P0} of {physMb / 1024}GB RAM)",
                                Detail = $"Buffer pool is {bpMb:N0} MB out of {physMb:N0} MB physical RAM ({bpRatio:P0} utilization). " +
                                         $"Consider reducing to ~{targetMb / 1024}GB.",
                                EstMonthlySavings = monthlyCost > 0 ? monthlyCost * (1m - (decimal)targetMb / physMb) * 0.30m : null
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Memory right-sizing): {ex.Message}", ex);
            }

            // 4. Unused index cost quantification
            try
            {
                var spExists = await CheckSpIndexCleanupExistsAsync();
                if (!spExists)
                {
                    recommendations.Add(new FinOpsRecommendation
                    {
                        Category = "Indexes",
                        Severity = "Low",
                        Confidence = "Low",
                        Finding = "Index analysis unavailable (sp_IndexCleanup not installed)",
                        Detail = "Install sp_IndexCleanup from https://github.com/erikdarlingdata/DarlingData " +
                                 "to identify unused and duplicate indexes that waste storage and add write overhead."
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Index analysis): {ex.Message}", ex);
            }

            // 5. Compression savings estimator
            try
            {
                using var compCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc,
    p.data_compression_desc,
    SUM(a.total_pages) * 8 / 1024.0 AS size_mb
FROM sys.tables AS t
JOIN sys.schemas AS s ON t.schema_id = s.schema_id
JOIN sys.indexes AS i ON t.object_id = i.object_id
JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units AS a ON p.partition_id = a.container_id
WHERE p.data_compression_desc = N'NONE'
AND   t.is_ms_shipped = 0
GROUP BY
    s.name,
    t.name,
    i.name,
    i.type_desc,
    p.data_compression_desc
HAVING SUM(a.total_pages) * 8 / 1024.0 >= 1024
ORDER BY
    size_mb DESC
OPTION(MAXDOP 1, RECOMPILE);", connection);
                compCmd.CommandTimeout = 60;

                var candidates = new List<(string Schema, string Table, string Index, string Type, decimal SizeMb)>();
                using var compReader = await compCmd.ExecuteReaderAsync();
                while (await compReader.ReadAsync())
                {
                    candidates.Add((
                        compReader.IsDBNull(0) ? "" : compReader.GetString(0),
                        compReader.IsDBNull(1) ? "" : compReader.GetString(1),
                        compReader.IsDBNull(2) ? "" : compReader.GetString(2),
                        compReader.IsDBNull(3) ? "" : compReader.GetString(3),
                        compReader.IsDBNull(5) ? 0m : Convert.ToDecimal(compReader.GetValue(5))
                    ));
                }

                if (candidates.Count > 0)
                {
                    var totalGb = candidates.Sum(c => c.SizeMb) / 1024m;
                    var topItems = candidates.Take(5)
                        .Select(c => $"{c.Schema}.{c.Table} ({c.SizeMb / 1024:N1}GB)")
                        .ToList();
                    recommendations.Add(new FinOpsRecommendation
                    {
                        Category = "Storage",
                        Severity = totalGb > 50 ? "High" : totalGb > 10 ? "Medium" : "Low",
                        Confidence = "High",
                        Finding = $"{candidates.Count} uncompressed object(s) >= 1GB ({totalGb:N1}GB total)",
                        Detail = $"Large uncompressed tables/indexes: {string.Join("; ", topItems)}" +
                                 (candidates.Count > 5 ? $" and {candidates.Count - 5} more" : "") +
                                 ". Consider PAGE or ROW compression to reduce storage and improve I/O."
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Compression): {ex.Message}", ex);
            }

            // 6. Dormant database detection with cost impact
            try
            {
                var idleDbs = await GetFinOpsIdleDatabasesAsync();
                if (idleDbs.Count > 0)
                {
                    var totalSizeGb = idleDbs.Sum(d => d.TotalSizeMb) / 1024m;
                    var dbNames = string.Join(", ", idleDbs.Take(5).Select(d => d.DatabaseName));
                    var costShare = 0m;
                    if (monthlyCost > 0)
                    {
                        // Estimate cost share proportional to storage footprint
                        var allDbSizes = await GetFinOpsDatabaseSizeStatsAsync();
                        var totalMb = allDbSizes.Sum(d => d.TotalSizeMb);
                        if (totalMb > 0)
                            costShare = (idleDbs.Sum(d => d.TotalSizeMb) / totalMb) * monthlyCost;
                    }

                    recommendations.Add(new FinOpsRecommendation
                    {
                        Category = "Databases",
                        Severity = idleDbs.Count >= 3 ? "High" : "Medium",
                        Confidence = "High",
                        Finding = $"{idleDbs.Count} idle database(s) consuming {totalSizeGb:N1}GB",
                        Detail = $"No query activity in 7 days: {dbNames}" +
                                 (idleDbs.Count > 5 ? $" and {idleDbs.Count - 5} more" : "") +
                                 ". Consider archiving or removing these databases.",
                        EstMonthlySavings = costShare > 0 ? costShare : null
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Dormant databases): {ex.Message}", ex);
            }

            // 7. Dev/test workload detection
            try
            {
                using var devTestCmd = new SqlCommand(@"
SELECT name
FROM sys.databases
WHERE (name LIKE N'%dev%' OR name LIKE N'%test%' OR name LIKE N'%staging%' OR name LIKE N'%qa%')
AND   database_id > 4", connection);
                devTestCmd.CommandTimeout = 30;

                var devDbs = new List<string>();
                using var devReader = await devTestCmd.ExecuteReaderAsync();
                while (await devReader.ReadAsync())
                {
                    if (!devReader.IsDBNull(0))
                        devDbs.Add(devReader.GetString(0));
                }

                if (devDbs.Count > 0)
                {
                    recommendations.Add(new FinOpsRecommendation
                    {
                        Category = "Environment",
                        Severity = "Medium",
                        Confidence = "Low",
                        Finding = $"{devDbs.Count} possible dev/test database(s) on production server",
                        Detail = $"Databases matching dev/test patterns: {string.Join(", ", devDbs.Take(10))}" +
                                 (devDbs.Count > 10 ? $" and {devDbs.Count - 10} more" : "") +
                                 ". If these are non-production workloads, consider moving to a lower-cost tier or separate server."
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Dev/test detection): {ex.Message}", ex);
            }

            // 11. Maintenance window efficiency — jobs running long
            try
            {
                using var jobCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 10
    job_name,
    avg_runs = COUNT(*),
    avg_duration_seconds = AVG(current_duration_seconds),
    max_duration_seconds = MAX(current_duration_seconds),
    avg_historical = AVG(avg_duration_seconds),
    times_ran_long = SUM(CAST(is_running_long AS int))
FROM collect.running_jobs
WHERE collection_time >= DATEADD(DAY, -7, SYSDATETIME())
AND   avg_duration_seconds > 0
GROUP BY job_name
HAVING SUM(CAST(is_running_long AS int)) >= 3
ORDER BY SUM(CAST(is_running_long AS int)) DESC", connection);
                jobCmd.CommandTimeout = 60;

                using var jobReader = await jobCmd.ExecuteReaderAsync();
                while (await jobReader.ReadAsync())
                {
                    var jobName = jobReader.IsDBNull(0) ? "" : jobReader.GetString(0);
                    var avgDuration = jobReader.IsDBNull(2) ? 0L : Convert.ToInt64(jobReader.GetValue(2));
                    var maxDuration = jobReader.IsDBNull(3) ? 0L : Convert.ToInt64(jobReader.GetValue(3));
                    var avgHistorical = jobReader.IsDBNull(4) ? 0L : Convert.ToInt64(jobReader.GetValue(4));
                    var timesLong = jobReader.IsDBNull(5) ? 0 : Convert.ToInt32(jobReader.GetValue(5));

                    recommendations.Add(new FinOpsRecommendation
                    {
                        Category = "Maintenance",
                        Severity = timesLong >= 5 ? "Medium" : "Low",
                        Confidence = "High",
                        Finding = $"{jobName} ran long {timesLong} times in 7 days",
                        Detail = $"Average duration: {FormatDuration(avgDuration)}, max: {FormatDuration(maxDuration)}, " +
                                 $"historical average: {FormatDuration(avgHistorical)}. " +
                                 "Review whether this job's schedule or operations need tuning."
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Maintenance window): {ex.Message}", ex);
            }

            // 12. VM right-sizing — prescriptive core/memory targets
            try
            {
                using var vmCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    p95_cpu = (SELECT TOP (1) PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cus.sqlserver_cpu_utilization) OVER ()
               FROM collect.cpu_utilization_stats AS cus
               WHERE cus.collection_time >= DATEADD(DAY, -7, SYSDATETIME())),
    cpu_count = (SELECT si.cpu_count FROM sys.dm_os_sys_info AS si),
    buffer_pool_mb = (SELECT pc.cntr_value / 1024
                      FROM sys.dm_os_performance_counters AS pc
                      WHERE pc.counter_name = N'Database Cache Memory (KB)'
                      AND   pc.object_name LIKE N'%Buffer Manager%'),
    physical_memory_mb = (SELECT si.physical_memory_kb / 1024 FROM sys.dm_os_sys_info AS si)
OPTION(MAXDOP 1, RECOMPILE);", connection);
                vmCmd.CommandTimeout = 60;

                using var vmReader = await vmCmd.ExecuteReaderAsync();
                if (await vmReader.ReadAsync())
                {
                    var p95Cpu = vmReader.IsDBNull(0) ? 0m : Convert.ToDecimal(vmReader.GetValue(0));
                    var cpuCount = vmReader.IsDBNull(1) ? 0 : Convert.ToInt32(vmReader.GetValue(1));
                    var bpMb = vmReader.IsDBNull(2) ? 0 : Convert.ToInt32(vmReader.GetValue(2));
                    var physMb = vmReader.IsDBNull(3) ? 0 : Convert.ToInt32(vmReader.GetValue(3));

                    // CPU prescription: only if >= 4 cores
                    if (cpuCount >= 4)
                    {
                        int targetCores = 0;
                        if (p95Cpu < 15)
                            targetCores = Math.Max(2, cpuCount / 4);
                        else if (p95Cpu < 30)
                            targetCores = Math.Max(2, cpuCount / 2);

                        if (targetCores > 0 && targetCores < cpuCount)
                        {
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Hardware",
                                Severity = "Medium",
                                Confidence = "Medium",
                                Finding = $"CPU: reduce from {cpuCount} to {targetCores} cores (P95 CPU {p95Cpu:N1}%)",
                                Detail = $"Over the last 7 days, P95 CPU utilization was {p95Cpu:N1}%. " +
                                         $"Current allocation of {cpuCount} cores can safely be reduced to {targetCores} cores.",
                                EstMonthlySavings = monthlyCost > 0
                                    ? monthlyCost * (1m - (decimal)targetCores / cpuCount) * 0.50m
                                    : null
                            });
                        }
                    }

                    // Memory prescription: only if >= 4096 MB
                    if (physMb >= 4096 && physMb > 0)
                    {
                        var bpRatio = (decimal)bpMb / physMb;
                        int targetMb = 0;
                        if (bpRatio < 0.25m)
                            targetMb = Math.Max(4096, physMb / 4);
                        else if (bpRatio < 0.40m)
                            targetMb = Math.Max(4096, physMb / 2);

                        if (targetMb > 0 && targetMb < physMb)
                        {
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Hardware",
                                Severity = "Medium",
                                Confidence = "Medium",
                                Finding = $"Memory: reduce from {physMb / 1024}GB to {targetMb / 1024}GB (buffer pool uses {bpRatio:P0})",
                                Detail = $"Buffer pool is using {bpMb:N0} MB of {physMb:N0} MB physical RAM ({bpRatio:P0}). " +
                                         $"Reducing to {targetMb / 1024}GB would still leave headroom.",
                                EstMonthlySavings = monthlyCost > 0
                                    ? monthlyCost * (1m - (decimal)targetMb / physMb) * 0.30m
                                    : null
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (VM right-sizing): {ex.Message}", ex);
            }

            // 13. Storage tier optimization — flag databases with low IO latency
            try
            {
                using var storageCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    database_name = fio.database_name,
    total_reads = SUM(fio.num_of_reads_delta),
    total_stall_read_ms = SUM(fio.io_stall_read_ms_delta),
    total_writes = SUM(fio.num_of_writes_delta),
    total_stall_write_ms = SUM(fio.io_stall_write_ms_delta)
FROM collect.file_io_stats AS fio
WHERE fio.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
AND   fio.num_of_reads_delta > 0
GROUP BY
    fio.database_name
HAVING SUM(fio.num_of_reads_delta) > 1000
ORDER BY
    SUM(fio.io_stall_read_ms_delta) * 1.0 / SUM(fio.num_of_reads_delta)
OPTION(MAXDOP 1, RECOMPILE);", connection);
                storageCmd.CommandTimeout = 60;

                var lowLatencyDbs = new List<(string Name, decimal AvgReadMs, decimal AvgWriteMs)>();
                using var storageReader = await storageCmd.ExecuteReaderAsync();
                while (await storageReader.ReadAsync())
                {
                    var dbName = storageReader.IsDBNull(0) ? "" : storageReader.GetString(0);
                    var totalReads = storageReader.IsDBNull(1) ? 0L : Convert.ToInt64(storageReader.GetValue(1));
                    var totalStallRead = storageReader.IsDBNull(2) ? 0L : Convert.ToInt64(storageReader.GetValue(2));
                    var totalWrites = storageReader.IsDBNull(3) ? 0L : Convert.ToInt64(storageReader.GetValue(3));
                    var totalStallWrite = storageReader.IsDBNull(4) ? 0L : Convert.ToInt64(storageReader.GetValue(4));

                    var avgReadMs = totalReads > 0 ? (decimal)totalStallRead / totalReads : 0m;
                    var avgWriteMs = totalWrites > 0 ? (decimal)totalStallWrite / totalWrites : 0m;

                    if (avgReadMs < 5m && avgWriteMs < 3m)
                    {
                        lowLatencyDbs.Add((dbName, avgReadMs, avgWriteMs));
                    }
                }

                if (lowLatencyDbs.Count > 0)
                {
                    var detail = string.Join("; ", lowLatencyDbs.Take(10)
                        .Select(d => $"{d.Name} (read {d.AvgReadMs:N1}ms, write {d.AvgWriteMs:N1}ms)"));
                    recommendations.Add(new FinOpsRecommendation
                    {
                        Category = "Storage",
                        Severity = "Low",
                        Confidence = "Medium",
                        Finding = $"{lowLatencyDbs.Count} database(s) with low IO latency — standard storage may suffice",
                        Detail = $"These databases have avg read latency under 5ms and write under 3ms over 7 days: {detail}" +
                                 (lowLatencyDbs.Count > 10 ? $" and {lowLatencyDbs.Count - 10} more" : "") +
                                 ". Premium/high-performance storage may not be needed."
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Storage tier): {ex.Message}", ex);
            }

            // 14. Reserved capacity candidates — stable CPU utilization
            try
            {
                using var rcCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    avg_cpu = AVG(CAST(cus.sqlserver_cpu_utilization AS decimal(5,2))),
    stddev_cpu = STDEV(CAST(cus.sqlserver_cpu_utilization AS decimal(5,2))),
    sample_count = COUNT(*)
FROM collect.cpu_utilization_stats AS cus
WHERE cus.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
HAVING COUNT(*) >= 24
OPTION(MAXDOP 1, RECOMPILE);", connection);
                rcCmd.CommandTimeout = 60;

                using var rcReader = await rcCmd.ExecuteReaderAsync();
                if (await rcReader.ReadAsync() && !rcReader.IsDBNull(0))
                {
                    var avgCpu = Convert.ToDecimal(rcReader.GetValue(0));
                    var stddevCpu = rcReader.IsDBNull(1) ? 0m : Convert.ToDecimal(rcReader.GetValue(1));

                    if (avgCpu > 20 && stddevCpu > 0)
                    {
                        var cv = stddevCpu / avgCpu;
                        if (cv < 0.3m)
                        {
                            var confidence = cv < 0.15m ? "High" : "Medium";
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Cloud",
                                Severity = "Low",
                                Confidence = confidence,
                                Finding = $"Stable CPU utilization (avg {avgCpu:N1}%, CV {cv:N2}) — reserved capacity candidate",
                                Detail = $"CPU utilization is consistently {avgCpu:N1}% with low variance (\u00b1{stddevCpu:N1}%). " +
                                         "Reserved pricing typically saves 30-40% over pay-as-you-go for predictable workloads."
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Reserved capacity): {ex.Message}", ex);
            }

            return recommendations;
        }

        private static string FormatDuration(long seconds)
        {
            if (seconds >= 3600)
                return $"{seconds / 3600}h {(seconds % 3600) / 60}m {seconds % 60}s";
            if (seconds >= 60)
                return $"{seconds / 60}m {seconds % 60}s";
            return $"{seconds}s";
        }
    }

    // ============================================
    // FinOps Model Classes
    // ============================================

    public class FinOpsDatabaseResourceUsage
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

    public class FinOpsUtilizationEfficiency
    {
        public decimal AvgCpuPct { get; set; }
        public int MaxCpuPct { get; set; }
        public decimal P95CpuPct { get; set; }
        public long CpuSamples { get; set; }
        public int TotalMemoryMb { get; set; }
        public int TargetMemoryMb { get; set; }
        public int PhysicalMemoryMb { get; set; }
        public decimal MemoryRatio { get; set; }
        public int MemoryUtilizationPct { get; set; }
        public int WorkerThreadsCurrent { get; set; }
        public int WorkerThreadsMax { get; set; }
        public decimal WorkerThreadRatio { get; set; }
        public int CpuCount { get; set; }
        public int BufferPoolMb { get; set; }
        public int TotalServerMemoryMb { get; set; }
        public string ProvisioningStatus { get; set; } = "";

        // FinOps cost — proportional to server monthly budget
        public decimal MonthlyCost { get; set; }
        public decimal AnnualCost => MonthlyCost * 12m;

        // Health score (Increment 6)
        public decimal FreeSpacePct { get; set; }
        public int HealthScore { get; set; }
        public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);
    }

    public class FinOpsApplicationResourceUsage
    {
        public string ApplicationName { get; set; } = "";
        public int AvgConnections { get; set; }
        public int MaxConnections { get; set; }
        public long SampleCount { get; set; }
        public DateTime FirstSeen { get; set; }
        public DateTime LastSeen { get; set; }
    }

    public class FinOpsServerInventory
    {
        public string ServerName { get; set; } = "";
        public string Edition { get; set; } = "";
        public string SqlVersion { get; set; } = "";
        public int CpuCount { get; set; }
        public long PhysicalMemoryMb { get; set; }
        public int? SocketCount { get; set; }
        public int? CoresPerSocket { get; set; }
        public int? EngineEdition { get; set; }
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
        public string ProvisioningDisplay => ProvisioningStatus?.Replace("_", " ") ?? "";
        public string HadrDisplay => IsHadrEnabled.HasValue ? (IsHadrEnabled.Value ? "Yes" : "No") : "";
        public string ClusteredDisplay => IsClustered.HasValue ? (IsClustered.Value ? "Yes" : "No") : "";

        // FinOps cost — from server config
        public decimal MonthlyCost { get; set; }
        public decimal AnnualCost => MonthlyCost * 12m;

        // License warning (Increment 5)
        public string? LicenseWarning
        {
            get
            {
                if (!Edition.Contains("Standard", StringComparison.OrdinalIgnoreCase)) return null;
                var warnings = new List<string>();
                if (CpuCount > 24) warnings.Add($"CPU: {CpuCount} cores (Standard limited to 24)");
                if (PhysicalMemoryMb > 131072) warnings.Add($"RAM: {PhysicalMemoryMb / 1024}GB (Standard limited to 128GB)");
                return warnings.Count > 0 ? string.Join("; ", warnings) : null;
            }
        }

        // Health score (Increment 6)
        public int HealthScore { get; set; }
        public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);
    }

    public class FinOpsDatabaseSizeStats
    {
        public DateTime CollectionTime { get; set; }
        public string DatabaseName { get; set; } = "";
        public int DatabaseId { get; set; }
        public int FileId { get; set; }
        public string FileTypeDesc { get; set; } = "";
        public string FileName { get; set; } = "";
        public string PhysicalName { get; set; } = "";
        public decimal TotalSizeMb { get; set; }
        public decimal UsedSizeMb { get; set; }
        public decimal FreeSpaceMb { get; set; }
        public decimal UsedPct { get; set; }
        public decimal AutoGrowthMb { get; set; }
        public decimal MaxSizeMb { get; set; }
        public string RecoveryModelDesc { get; set; } = "";
        public int CompatibilityLevel { get; set; }
        public string StateDesc { get; set; } = "";
        public string VolumeMountPoint { get; set; } = "";
        public decimal VolumeTotalMb { get; set; }
        public decimal VolumeFreeMb { get; set; }
        public bool? IsPercentGrowth { get; set; }
        public int? GrowthPct { get; set; }
        public int? VlfCount { get; set; }

        // FinOps cost — proportional share of server monthly budget
        public decimal MonthlyCostShare { get; set; }

        public string GrowthDisplay => IsPercentGrowth switch
        {
            null  => "-",
            true  => GrowthPct.HasValue ? $"{GrowthPct}%" : "-",
            false => AutoGrowthMb == 0 ? "Disabled" : $"{AutoGrowthMb:N0} MB"
        };

        public decimal AutoGrowthSort => IsPercentGrowth switch
        {
            null  => -1m,
            true  => (decimal)(GrowthPct ?? -1),
            false => AutoGrowthMb
        };

        public string VlfCountDisplay => string.Equals(FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase)
            ? (VlfCount?.ToString() ?? "-") : "N/A";

        public int VlfCountSort => string.Equals(FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase)
            ? (VlfCount ?? 0) : -1;
    }

    public class FinOpsTopResourceConsumer
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

    public class FinOpsDatabaseSizeSummary
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

    public class FinOpsStorageGrowthRow
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

    public class FinOpsIdleDatabase
    {
        public string DatabaseName { get; set; } = "";
        public decimal TotalSizeMb { get; set; }
        public int FileCount { get; set; }
        public DateTime? LastExecutionTime { get; set; }
    }

    public class FinOpsTempdbSummary
    {
        public string Metric { get; set; } = "";
        public decimal CurrentMb { get; set; }
        public decimal Peak24hMb { get; set; }
        public string Warning { get; set; } = "";
    }

    public class FinOpsWaitCategorySummary
    {
        public string Category { get; set; } = "";
        public long TotalWaitTimeMs { get; set; }
        public long WaitingTasks { get; set; }
        public decimal PctOfTotal { get; set; }
        public string TopWaitType { get; set; } = "";
        public long TopWaitTimeMs { get; set; }

        // FinOps cost — proportional share of server monthly budget based on wait time fraction
        public decimal MonthlyCostShare { get; set; }
    }

    public class FinOpsExpensiveQuery
    {
        public string DatabaseName { get; set; } = "";
        public long TotalCpuMs { get; set; }
        public decimal AvgCpuMsPerExec { get; set; }
        public long TotalReads { get; set; }
        public decimal AvgReadsPerExec { get; set; }
        public long Executions { get; set; }
        public string QueryPreview { get; set; } = "";
        public string FullQueryText { get; set; } = "";

        // FinOps cost — proportional share of server monthly budget based on CPU fraction
        public decimal MonthlyCostShare { get; set; }
    }

    public class IndexCleanupResult
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

        public decimal IndexSizeGbSort => NumericSortHelper.Parse(IndexSizeGb);
        public decimal IndexRowsSort => NumericSortHelper.Parse(IndexRows);
        public decimal IndexReadsSort => NumericSortHelper.Parse(IndexReads);
        public decimal IndexWritesSort => NumericSortHelper.Parse(IndexWrites);
    }

    public class FinOpsProvisioningTrend
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

    public class FinOpsMemoryGrantEfficiency
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

    public static class FinOpsHealthCalculator
    {
        public static int CpuScore(decimal p95Pct)
        {
            if (p95Pct <= 70) return (int)(100 - p95Pct * 50 / 70);
            return (int)Math.Max(0, 50 - (p95Pct - 70) * 50 / 30);
        }

        public static int MemoryScore(decimal bufferPoolRatio)
        {
            if (bufferPoolRatio <= 0.30m) return 60;
            if (bufferPoolRatio <= 0.85m) return 100;
            if (bufferPoolRatio <= 0.95m) return (int)(100 - (bufferPoolRatio - 0.85m) * 800);
            return (int)Math.Max(0, 20 - (bufferPoolRatio - 0.95m) * 400);
        }

        public static int StorageScore(decimal freeSpacePct)
        {
            if (freeSpacePct >= 30) return 100;
            if (freeSpacePct >= 10) return (int)(50 + (freeSpacePct - 10) * 2.5m);
            return (int)(freeSpacePct * 5);
        }

        public static int Overall(int cpu, int memory, int storage) =>
            (int)(cpu * 0.40 + memory * 0.30 + storage * 0.30);

        public static string ScoreColor(int score) => score switch
        {
            >= 80 => "#27AE60",
            >= 60 => "#F39C12",
            _ => "#E74C3C"
        };
    }

    public class IndexCleanupSummary
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

        public decimal TotalIndexesSort => NumericSortHelper.Parse(TotalIndexes);
        public decimal RemovableIndexesSort => NumericSortHelper.Parse(RemovableIndexes);
        public decimal MergeableIndexesSort => NumericSortHelper.Parse(MergeableIndexes);
        public decimal CompressableIndexesSort => NumericSortHelper.Parse(CompressableIndexes);
        public decimal PercentRemovableSort => NumericSortHelper.Parse(PercentRemovable);
        public decimal CurrentSizeGbSort => NumericSortHelper.Parse(CurrentSizeGb);
        public decimal SizeAfterCleanupGbSort => NumericSortHelper.Parse(SizeAfterCleanupGb);
        public decimal SpaceSavedGbSort => NumericSortHelper.Parse(SpaceSavedGb);
        public decimal SpaceReductionPercentSort => NumericSortHelper.Parse(SpaceReductionPercent);
        public decimal TotalRowsSort => NumericSortHelper.Parse(TotalRows);
        public decimal WritesSort => NumericSortHelper.Parse(Writes);
        public decimal DailyWriteOpsSavedSort => NumericSortHelper.Parse(DailyWriteOpsSaved);
        public decimal LockWaitCountSort => NumericSortHelper.Parse(LockWaitCount);
        public decimal DailyLockWaitsSavedSort => NumericSortHelper.Parse(DailyLockWaitsSaved);
        public decimal AvgLockWaitMsSort => NumericSortHelper.Parse(AvgLockWaitMs);
        public decimal LatchWaitCountSort => NumericSortHelper.Parse(LatchWaitCount);
        public decimal DailyLatchWaitsSavedSort => NumericSortHelper.Parse(DailyLatchWaitsSaved);
        public decimal AvgLatchWaitMsSort => NumericSortHelper.Parse(AvgLatchWaitMs);
    }

    public class FinOpsRecommendation
    {
        public string Category { get; set; } = "";
        public string Severity { get; set; } = "";
        public string Confidence { get; set; } = "";
        public string Finding { get; set; } = "";
        public string Detail { get; set; } = "";
        public decimal? EstMonthlySavings { get; set; }
        public string EstMonthlySavingsDisplay => EstMonthlySavings.HasValue ? $"${EstMonthlySavings.Value:N0}" : "";
    }

    public class FinOpsHighImpactQuery
    {
        public string QueryHashDisplay { get; set; } = "";
        public string DatabaseName { get; set; } = "";
        public long TotalExecutions { get; set; }
        public decimal TotalCpuMs { get; set; }
        public decimal TotalDurationMs { get; set; }
        public long TotalReads { get; set; }
        public long TotalWrites { get; set; }
        public decimal TotalMemoryMb { get; set; }
        public decimal CpuShare { get; set; }
        public decimal DurationShare { get; set; }
        public decimal ReadsShare { get; set; }
        public decimal WritesShare { get; set; }
        public decimal MemoryShare { get; set; }
        public decimal ExecutionsShare { get; set; }
        public int ImpactScore { get; set; }
        public string SampleQueryText { get; set; } = "";
        public string FullQueryText { get; set; } = "";

        public string ImpactScoreColor => ImpactScore switch
        {
            >= 80 => "#E74C3C",
            >= 60 => "#F39C12",
            _ => "#27AE60"
        };
    }

    internal static class NumericSortHelper
    {
        internal static decimal Parse(string? s)
        {
            if (string.IsNullOrWhiteSpace(s)) return -1m;
            var cleaned = s.Replace(",", "").Replace("%", "").Trim();
            return decimal.TryParse(cleaned, System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : -1m;
        }
    }
}
