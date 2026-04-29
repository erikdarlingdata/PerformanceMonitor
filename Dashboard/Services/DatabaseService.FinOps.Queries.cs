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
        // FinOps — Query/Wait/Memory Analysis & Trends
        // ============================================

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
    }
}
