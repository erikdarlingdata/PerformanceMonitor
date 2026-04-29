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
    LEFT(query_text, 200) AS query_preview,
    query_text AS full_query_text
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   delta_worker_time IS NOT NULL
AND   delta_worker_time > 0
GROUP BY
    database_name,
    sql_handle,
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
                QueryPreview = reader.IsDBNull(6) ? "" : reader.GetString(6),
                FullQueryText = reader.IsDBNull(7) ? "" : reader.GetString(7)
            });
        }
        return items;
    }

    /// <summary>
    /// Fetches high-impact queries — 80/20 analysis across CPU, duration, reads, writes, memory, executions.
    /// Aggregates to query_hash level from DuckDB, then scores in C#.
    /// </summary>
    public async Task<List<HighImpactQueryRow>> GetHighImpactQueriesAsync(int serverId, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
SELECT
    query_hash,
    MIN(database_name) AS database_name,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) / 1000.0 AS total_cpu_ms,
    SUM(delta_elapsed_time) / 1000.0 AS total_duration_ms,
    SUM(delta_logical_reads) AS total_reads,
    SUM(delta_logical_writes) AS total_writes,
    SUM(COALESCE(max_grant_kb, 0)) / 1024.0 AS total_memory_mb,
    (SELECT LEFT(qs2.query_text, 200) FROM query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS sample_query_text,
    (SELECT qs2.query_text FROM query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS full_query_text
FROM query_stats AS qs
WHERE server_id = $1
AND   collection_time >= $2
AND   query_hash IS NOT NULL AND query_hash != ''
AND   delta_execution_count > 0
GROUP BY query_hash
HAVING SUM(delta_execution_count) > 0
ORDER BY SUM(delta_worker_time) DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        // Step 1: Read aggregated data
        var allRows = new List<HighImpactQueryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            allRows.Add(new HighImpactQueryRow
            {
                QueryHash = reader.IsDBNull(0) ? "" : reader.GetString(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalExecutions = reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                TotalCpuMs = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                TotalDurationMs = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                TotalReads = reader.IsDBNull(5) ? 0 : ToInt64(reader.GetValue(5)),
                TotalWrites = reader.IsDBNull(6) ? 0 : ToInt64(reader.GetValue(6)),
                TotalMemoryMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                SampleQueryText = reader.IsDBNull(8) ? "" : reader.GetString(8),
                FullQueryText = reader.IsDBNull(9) ? "" : reader.GetString(9)
            });
        }

        return HighImpactScorer.Score(allRows);
    }
}
