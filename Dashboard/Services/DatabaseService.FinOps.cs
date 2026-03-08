/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
                    volume_free_mb
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
                    file_id
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
                        VolumeFreeMb = reader.IsDBNull(18) ? 0m : Convert.ToDecimal(reader.GetValue(18))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Fetches server inventory from config.server_info.
        /// </summary>
        public async Task<List<FinOpsServerInventory>> GetFinOpsServerInventoryAsync()
        {
            var items = new List<FinOpsServerInventory>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    server_name,
                    edition,
                    sql_version,
                    environment_type,
                    cpu_count,
                    physical_memory_mb,
                    sqlserver_start_time,
                    uptime_days,
                    uptime_hours,
                    last_updated
                FROM config.server_info
                ORDER BY
                    server_name
                OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_ServerInventory", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsServerInventory
                    {
                        ServerName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        Edition = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        SqlVersion = reader.IsDBNull(2) ? "" : reader.GetString(2),
                        EnvironmentType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                        CpuCount = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                        PhysicalMemoryMb = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                        SqlServerStartTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                        UptimeDays = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
                        UptimeHours = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8)),
                        LastUpdated = reader.IsDBNull(9) ? null : reader.GetDateTime(9)
                    });
                }
            }

            return items;
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
        public string EnvironmentType { get; set; } = "";
        public int CpuCount { get; set; }
        public long PhysicalMemoryMb { get; set; }
        public DateTime? SqlServerStartTime { get; set; }
        public int UptimeDays { get; set; }
        public int UptimeHours { get; set; }
        public DateTime? LastUpdated { get; set; }
        public string UptimeDisplay => $"{UptimeDays}d {UptimeHours}h";
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
}
