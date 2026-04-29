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

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // FinOps — Workload (Database & Application Resource Usage)
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
    }
}
