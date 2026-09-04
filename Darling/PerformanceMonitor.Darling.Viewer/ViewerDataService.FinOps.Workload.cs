/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Database Resources / Application Connections / Optimization (wait + expensive) / High Impact
/// reads — Lite's <c>LocalDataService.FinOps.Workload.cs</c> ported to Postgres. The DuckDB SQL ports
/// near-verbatim: positional params, <c>FULL JOIN</c>, <c>NULLIF</c>/<c>COALESCE</c>, <c>ILIKE</c>, the
/// repeated wait-category CASE, and the correlated sample-text subqueries all run identically on PG. The
/// high-impact query reads the <c>query_stats</c> base table (as Lite does) for its correlated subqueries;
/// every other read uses the <c>v_*</c> passthrough views. SQL kept in <c>public const</c> so tests pin it.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Per-database resource usage from query_stats + file_io_stats deltas. $1 server_id, $2 cutoff.</summary>

    /* ── #1661: retention-tier routing for the aggregate workload queries ───────────────────────────────
       These sum additively over database_name, which is exactly what the rollups materialize, so routing
       them is lossless. Each swap throws if it matches nothing, so editing the SQL without updating the
       matching fragment fails loudly instead of silently leaving the panel on raw. */

    /// <summary>The database-grain CTE in <see cref="DatabaseResourceUsageSql"/>, verbatim.</summary>
    private const string WorkloadCteRaw = """
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_logical_reads) AS logical_reads,
        SUM(delta_physical_reads) AS physical_reads,
        SUM(delta_logical_writes) AS logical_writes,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    GROUP BY database_name
""";

    /// <summary>
    /// The same CTE over the per-database rollup. This is the ONE reader that needs
    /// <c>query_stats_db_hourly</c>: it sums the I/O columns, which the query-grain aggregate does not carry.
    /// The rollup already applies the same <c>delta_worker_time IS NOT NULL</c> filter, so it is dropped here.
    /// </summary>
    private static string WorkloadCteForCagg(string relation) => $"""
        database_name,
        SUM(worker_time_sum) / 1000.0 AS cpu_time_ms,
        SUM(logical_reads_sum) AS logical_reads,
        SUM(physical_reads_sum) AS physical_reads,
        SUM(logical_writes_sum) AS logical_writes,
        SUM(execution_count_sum) AS execution_count
    FROM collect.{relation}
    WHERE server_id = $1
    AND   bucket >= $2
    GROUP BY database_name
""";

    /// <summary>The CPU/execution CTE shared by both top-consumer queries, verbatim.</summary>
    private const string ConsumerCteRaw = """
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    GROUP BY database_name
""";

    /// <summary>
    /// The same CTE over the QUERY-grain rollup — these two need only CPU and executions, both of which
    /// <c>query_stats_hourly</c> already carries, so they route without the per-database aggregate.
    /// </summary>
    private static string ConsumerCteForCagg(string relation) => $"""
        database_name,
        SUM(worker_time_sum) / 1000.0 AS cpu_time_ms,
        SUM(execution_count_sum) AS execution_count
    FROM collect.{relation}
    WHERE server_id = $1
    AND   bucket >= $2
    GROUP BY database_name
""";

    private static string RouteOrThrow(string sql, string from, string to, string what)
    {
        var routed = sql.Replace(from, to, StringComparison.Ordinal);
        if (string.Equals(routed, sql, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"FinOps {what} CAGG routing found nothing to replace — its raw fragment has drifted from the SQL (#1661).");
        }

        return routed;
    }

    /// <summary>Database resource usage for <paramref name="tier"/>. Raw returns the constant untouched.</summary>
    public static string DatabaseResourceUsageSqlFor(RetentionTier tier) =>
        tier == RetentionTier.Raw
            ? DatabaseResourceUsageSql
            : RouteOrThrow(
                DatabaseResourceUsageSql,
                WorkloadCteRaw,
                WorkloadCteForCagg(tier == RetentionTier.Hourly ? TimescaleSupport.QueryStatsDbHourlyView : TimescaleSupport.QueryStatsDbDailyView),
                "database resource usage");

    /// <summary>Top consumers (by total) for <paramref name="tier"/>.</summary>
    public static string TopResourceConsumersByTotalSqlFor(RetentionTier tier) =>
        tier == RetentionTier.Raw
            ? TopResourceConsumersByTotalSql
            : RouteOrThrow(
                TopResourceConsumersByTotalSql,
                ConsumerCteRaw,
                ConsumerCteForCagg(tier == RetentionTier.Hourly ? TimescaleSupport.QueryStatsHourlyView : TimescaleSupport.QueryStatsDailyView),
                "top consumers by total");

    /// <summary>Top consumers (by average) for <paramref name="tier"/>.</summary>
    public static string TopResourceConsumersByAvgSqlFor(RetentionTier tier) =>
        tier == RetentionTier.Raw
            ? TopResourceConsumersByAvgSql
            : RouteOrThrow(
                TopResourceConsumersByAvgSql,
                ConsumerCteRaw,
                ConsumerCteForCagg(tier == RetentionTier.Hourly ? TimescaleSupport.QueryStatsHourlyView : TimescaleSupport.QueryStatsDailyView),
                "top consumers by average");

    public const string DatabaseResourceUsageSql = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
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

    public async Task<List<DatabaseResourceUsageRow>> GetDatabaseResourceUsageAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, cutoff, rollups.DbGrainHourly, rollups.DbGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsDbHourlyView, TimescaleSupport.QueryStatsDbDailyView));

        await using var command = _dataSource.CreateCommand(DatabaseResourceUsageSqlFor(tier));
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<DatabaseResourceUsageRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseResourceUsageRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CpuTimeMs = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1)),
                LogicalReads = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
                PhysicalReads = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                LogicalWrites = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                ExecutionCount = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                IoReadMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                IoWriteMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                IoStallMs = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                PctCpuShare = reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                PctIoShare = reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10))
            });
        }
        return items;
    }

    /// <summary>
    /// Per-application connection counts plus the collected per-app resource + session-status metrics from
    /// session_stats (last 24h). AVG/MAX over the window for connection/running/sleeping/dormant counts and
    /// CPU/reads/writes/logical-reads (the resource columns are nullable, so AVG/MAX yield NULL until populated).
    /// $1 server_id, $2 cutoff.
    /// </summary>
    public const string ApplicationConnectionsSql = @"
SELECT
    program_name,
    CAST(AVG(connection_count) AS INTEGER) AS avg_connections,
    MAX(connection_count) AS max_connections,
    CAST(AVG(running_count) AS INTEGER) AS avg_running,
    MAX(running_count) AS max_running,
    CAST(AVG(sleeping_count) AS INTEGER) AS avg_sleeping,
    MAX(sleeping_count) AS max_sleeping,
    CAST(AVG(dormant_count) AS INTEGER) AS avg_dormant,
    MAX(dormant_count) AS max_dormant,
    CAST(AVG(total_cpu_time_ms) AS BIGINT) AS avg_cpu_time_ms,
    MAX(total_cpu_time_ms) AS max_cpu_time_ms,
    CAST(AVG(total_reads) AS BIGINT) AS avg_reads,
    MAX(total_reads) AS max_reads,
    CAST(AVG(total_writes) AS BIGINT) AS avg_writes,
    MAX(total_writes) AS max_writes,
    CAST(AVG(total_logical_reads) AS BIGINT) AS avg_logical_reads,
    MAX(total_logical_reads) AS max_logical_reads,
    COUNT(*) AS sample_count,
    MIN(collection_time) AS first_seen,
    MAX(collection_time) AS last_seen
FROM v_session_stats
WHERE server_id = $1
AND   collection_time >= $2
GROUP BY program_name
ORDER BY max_connections DESC";

    public async Task<List<ApplicationConnectionRow>> GetApplicationConnectionsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);

        await using var command = _dataSource.CreateCommand(ApplicationConnectionsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<ApplicationConnectionRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ApplicationConnectionRow
            {
                ApplicationName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                AvgConnections = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                MaxConnections = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                AvgRunning = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                MaxRunning = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                AvgSleeping = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                MaxSleeping = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                AvgDormant = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
                MaxDormant = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8)),
                AvgCpuTimeMs = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                MaxCpuTimeMs = reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                AvgReads = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                MaxReads = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
                AvgWrites = reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)),
                MaxWrites = reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)),
                AvgLogicalReads = reader.IsDBNull(15) ? 0L : Convert.ToInt64(reader.GetValue(15)),
                MaxLogicalReads = reader.IsDBNull(16) ? 0L : Convert.ToInt64(reader.GetValue(16)),
                SampleCount = reader.IsDBNull(17) ? 0 : Convert.ToInt64(reader.GetValue(17)),
                FirstSeenLocal = ViewerTimeHelper.ForDisplay(reader.GetDateTime(18)),
                LastSeenLocal = ViewerTimeHelper.ForDisplay(reader.GetDateTime(19))
            });
        }
        return items;
    }

    /// <summary>Top-N databases by total CPU for the Utilization summary. $1 server_id, $2 cutoff, $3 topN.</summary>
    public const string TopResourceConsumersByTotalSql = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
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

    public async Task<List<TopResourceConsumerRow>> GetTopResourceConsumersByTotalAsync(int serverId, int hoursBack = 24, int topN = 5, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, cutoff, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        await using var command = _dataSource.CreateCommand(TopResourceConsumersByTotalSqlFor(tier));
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

        var items = new List<TopResourceConsumerRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new TopResourceConsumerRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CpuTimeMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                IoTotalMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                PctCpu = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                PctIo = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5))
            });
        }
        return items;
    }

    /// <summary>Top-N databases by average CPU per execution for the Utilization summary. $1 server_id, $2 cutoff, $3 topN.</summary>
    public const string TopResourceConsumersByAvgSql = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
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

    public async Task<List<TopResourceConsumerRow>> GetTopResourceConsumersByAvgAsync(int serverId, int hoursBack = 24, int topN = 5, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, cutoff, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        await using var command = _dataSource.CreateCommand(TopResourceConsumersByAvgSqlFor(tier));
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

        var items = new List<TopResourceConsumerRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new TopResourceConsumerRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CpuTimeMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                IoTotalMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                TotalCpuTimeMs = reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4)),
                AvgIoMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5))
            });
        }
        return items;
    }

    /// <summary>Wait stats grouped by cost category over the window. $1 server_id, $2 cutoff.</summary>
    public const string WaitCategorySummarySql = @"
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

    public async Task<List<WaitCategorySummaryRow>> GetWaitCategorySummaryAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        await using var command = _dataSource.CreateCommand(WaitCategorySummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<WaitCategorySummaryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new WaitCategorySummaryRow
            {
                Category = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalWaitTimeMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                WaitingTasks = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                PctOfTotal = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                TopWaitType = reader.IsDBNull(4) ? "" : reader.GetString(4),
                TopWaitTimeMs = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5))
            });
        }
        return items;
    }

    /// <summary>Top-N most expensive queries by total CPU over the window. $1 server_id, $2 cutoff, $3 topN.</summary>
    public const string ExpensiveQueriesSql = @"
SELECT
    database_name,
    SUM(delta_worker_time) / 1000.0 AS total_cpu_ms,
    CAST(SUM(delta_worker_time) / 1000.0 / NULLIF(SUM(delta_execution_count), 0) AS DECIMAL(19,2)) AS avg_cpu_ms,
    SUM(delta_logical_reads) AS total_reads,
    CAST(SUM(delta_logical_reads) * 1.0 / NULLIF(SUM(delta_execution_count), 0) AS DECIMAL(19,0)) AS avg_reads,
    SUM(delta_execution_count) AS executions,
    LEFT(query_text, 200) AS query_preview,
    query_text AS full_query_text,
    MAX(query_plan_xml) AS query_plan_xml,
    MAX(query_plan_gz) AS query_plan_gz
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

    public async Task<List<ExpensiveQueryRow>> GetExpensiveQueriesAsync(int serverId, int hoursBack = 24, int topN = 20, CancellationToken cancellationToken = default)
    {
        /* #1661: this reader projects query_text and query_plan_xml, and NO rollup carries per-row text — the
           CAGGs group by identity and sum deltas. So unlike the aggregate FinOps queries this one cannot be
           routed; it is limited to whatever raw still retains, however wide a window the picker offers. Clamp to
           that horizon so the query states the range it can actually answer. The UI labels the clamp
           (FinOpsTab.Loaders) using the same router, rather than quietly showing a few days of a month. */
        var now = DateTime.UtcNow;
        var (cutoff, _) = RetentionTierRouter.ClampToTextHorizon(now, now.AddHours(-hoursBack));

        await using var command = _dataSource.CreateCommand(ExpensiveQueriesSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

        var items = new List<ExpensiveQueryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ExpensiveQueryRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalCpuMs = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                AvgCpuMsPerExec = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                TotalReads = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                AvgReadsPerExec = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                Executions = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                QueryPreview = reader.IsDBNull(6) ? "" : reader.GetString(6),
                FullQueryText = reader.IsDBNull(7) ? "" : reader.GetString(7),
                /* #2069: plans written since V54 ride as gzip bytes with the text column NULL —
                   text-else-gz, same rule as every plan reader. */
                QueryPlanXml = PayloadDimensions.ResolveContent(
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    reader.IsDBNull(9) ? null : reader.GetFieldValue<byte[]>(9))
            });
        }
        return items;
    }

    /// <summary>
    /// High-impact queries — 80/20 analysis across CPU/duration/reads/writes/memory/executions. Aggregates
    /// to query_hash level in SQL (with correlated sample-text subqueries, as Lite does), then scores in C#
    /// via <see cref="HighImpactScorer"/>. $1 server_id, $2 cutoff.
    /// </summary>
    public const string HighImpactQueriesSql = @"
SELECT
    query_hash,
    MIN(database_name) AS database_name,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) / 1000.0 AS total_cpu_ms,
    SUM(delta_elapsed_time) / 1000.0 AS total_duration_ms,
    SUM(delta_logical_reads) AS total_reads,
    SUM(delta_logical_writes) AS total_writes,
    SUM(COALESCE(max_grant_kb, 0)) / 1024.0 AS total_memory_mb,
    (SELECT LEFT(qs2.query_text, 200) FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS sample_query_text,
    (SELECT qs2.query_text FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS full_query_text,
    (SELECT qs2.query_plan_xml FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_plan_xml IS NOT NULL AND qs2.query_plan_xml != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS query_plan_xml,
    (SELECT qs2.query_plan_gz FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_plan_gz IS NOT NULL
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS query_plan_gz
FROM v_query_stats AS qs
WHERE server_id = $1
AND   collection_time >= $2
AND   query_hash IS NOT NULL AND query_hash != ''
AND   delta_execution_count > 0
GROUP BY query_hash
HAVING SUM(delta_execution_count) > 0
ORDER BY SUM(delta_worker_time) DESC";

    public async Task<List<HighImpactQueryRow>> GetHighImpactQueriesAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        await using var command = _dataSource.CreateCommand(HighImpactQueriesSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var allRows = new List<HighImpactQueryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            allRows.Add(new HighImpactQueryRow
            {
                QueryHash = reader.IsDBNull(0) ? "" : reader.GetString(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalExecutions = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                TotalCpuMs = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                TotalDurationMs = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                TotalReads = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                TotalWrites = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                TotalMemoryMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                SampleQueryText = reader.IsDBNull(8) ? "" : reader.GetString(8),
                FullQueryText = reader.IsDBNull(9) ? "" : reader.GetString(9),
                /* #2069: the two correlated subqueries may land on DIFFERENT sample rows (one
                   pre-V54 text row, one post-V54 gz row); either is "a sample plan for the hash",
                   and text-first keeps the free form when both exist. */
                QueryPlanXml = PayloadDimensions.ResolveContent(
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetFieldValue<byte[]>(11))
            });
        }

        return HighImpactScorer.Score(allRows);
    }
}
