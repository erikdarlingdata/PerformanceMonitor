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
        public async Task<List<FinOpsDatabaseResourceUsage>> GetFinOpsDatabaseResourceUsageAsync()
        {
            var items = new List<FinOpsDatabaseResourceUsage>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    database_name,
                    cpu_time_ms,
                    logical_reads,
                    physical_reads,
                    logical_writes,
                    execution_count,
                    io_read_mb,
                    io_write_mb,
                    io_stall_ms,
                    pct_cpu_share,
                    pct_io_share
                FROM report.finops_database_resource_usage
                ORDER BY
                    pct_cpu_share DESC
                OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
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
                    avg_cpu_pct,
                    max_cpu_pct,
                    p95_cpu_pct,
                    cpu_samples,
                    total_memory_mb,
                    target_memory_mb,
                    physical_memory_mb,
                    memory_ratio,
                    memory_utilization_pct,
                    worker_threads_current,
                    worker_threads_max,
                    worker_thread_ratio,
                    cpu_count,
                    provisioning_status
                FROM report.finops_utilization_efficiency
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
                        ProvisioningStatus = reader.IsDBNull(13) ? "" : reader.GetString(13)
                    };
                }
            }

            return null;
        }

        /// <summary>
        /// Fetches peak utilization by hour from report.finops_peak_utilization.
        /// </summary>
        public async Task<List<FinOpsPeakUtilization>> GetFinOpsPeakUtilizationAsync()
        {
            var items = new List<FinOpsPeakUtilization>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    hour_of_day,
                    avg_cpu_pct,
                    max_cpu_pct,
                    avg_memory_pct,
                    max_memory_pct,
                    cpu_samples,
                    hour_classification
                FROM report.finops_peak_utilization
                ORDER BY
                    hour_of_day ASC
                OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_PeakUtilization", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsPeakUtilization
                    {
                        HourOfDay = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0)),
                        AvgCpuPct = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        MaxCpuPct = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                        AvgMemoryPct = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        MaxMemoryPct = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                        CpuSamples = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                        HourClassification = reader.IsDBNull(6) ? "" : reader.GetString(6)
                    });
                }
            }

            return items;
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
        public string ProvisioningStatus { get; set; } = "";
    }

    public class FinOpsPeakUtilization
    {
        public int HourOfDay { get; set; }
        public decimal AvgCpuPct { get; set; }
        public int MaxCpuPct { get; set; }
        public decimal AvgMemoryPct { get; set; }
        public int MaxMemoryPct { get; set; }
        public long CpuSamples { get; set; }
        public string HourClassification { get; set; } = "";
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
}
