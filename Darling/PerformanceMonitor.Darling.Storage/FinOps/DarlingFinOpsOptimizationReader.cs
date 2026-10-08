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

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>A database with zero query executions over the window. <see cref="LastExecutionTime"/> is
/// <c>query_stats.last_execution_time</c>, the monitored server's own wall clock, read verbatim.</summary>
public sealed record IdleDatabase(string DatabaseName, decimal TotalSizeMb, int FileCount, DateTime? LastExecutionTime);

/// <summary>The idle-database read: whether query stats cover each of the last 7 UTC days (<see cref="Covered"/>), and the idle
/// databases, which are empty when they do not (an idle claim needs the days to have been watched).</summary>
public sealed record IdleDatabaseRead(bool Covered, List<IdleDatabase> Rows);

/// <summary>One tempdb pressure metric: the latest value against the window's peak.</summary>
public sealed record TempdbSummaryMetric(string Metric, decimal CurrentMb, decimal Peak24hMb, string Warning);

/// <summary>Wait time grouped by cost category over the window.</summary>
public sealed record WaitCategorySummary(
    string Category, long TotalWaitTimeMs, long WaitingTasks, decimal PctOfTotal, string TopWaitType, long TopWaitTimeMs);

/// <summary>One of the most expensive statements by total CPU over the window.</summary>
public sealed record ExpensiveQuery(
    string DatabaseName, long TotalCpuMs, decimal AvgCpuMsPerExec, long TotalReads, decimal AvgReadsPerExec,
    long Executions, string QueryPreview, string FullQueryText, string? QueryPlanXml, string? QueryHash = null);

/// <summary>
/// The FinOps Optimization reads: idle databases, the tempdb summary, wait time by category and the most expensive
/// queries. The viewer and the Darling service read through the same copy. Every method takes the window's lower
/// bound as a naive-UTC <c>cutoffUtc</c> the caller computed, so the clock is read where the caller reads it.
/// </summary>
public static class DarlingFinOpsOptimizationReader
{
    /// <summary>Databases with zero query executions over the last N days. $1 server_id, $2 cutoff.</summary>
    public const string IdleDatabasesSql = $@"
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
        SUM(delta_execution_count) FILTER (WHERE {TimescaleSupport.IntervalHonestSourceFilter}) AS total_executions,
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
AND   (a.last_execution IS NULL OR a.last_execution < $2)
AND   ds.database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
ORDER BY ds.total_size_mb DESC";

    /// <summary>The number of COMPLETE UTC days (D-7 through D-1, today excluded) that must each hold a query-stats sample before a database is called idle for 7 days.</summary>
    public const int IdleCoverageDays = 7;

    /// <summary>The first complete UTC day of the idle-coverage window, D-7 at 00:00. Every idle claim (the recommendation row, the Idle Databases grid, the Server Inventory count) shares it.</summary>
    public static DateTime IdleCoverageStartUtc(DateTime? nowUtc = null) => (nowUtc ?? DateTime.UtcNow).Date.AddDays(-IdleCoverageDays);

    /// <summary>The end of the idle-coverage days: today at 00:00 (exclusive). Today is not required to hold a sample, so just after 00:00 UTC a covered server stays covered.</summary>
    public static DateTime IdleCoverageEndUtc(DateTime? nowUtc = null) => (nowUtc ?? DateTime.UtcNow).Date;

    /// <summary>
    /// The coverage rule in one place: the oldest query-stats sample is at or before now - 7 days (so 7 days of history exist, not 6 days
    /// that happen to touch 7 UTC dates) AND each of the 7 complete UTC days before today holds a sample (so a gap inside the window
    /// reads as not covered). The inventory SQL (<c>idle_coverage</c>) is the same rule fleet-wide.
    /// </summary>
    public static bool IdleCoverageHolds(DateTime? oldestSampleUtc, long completeDaysWithSamples, DateTime? nowUtc = null) =>
        oldestSampleUtc is DateTime oldest
        && oldest <= (nowUtc ?? DateTime.UtcNow).AddDays(-IdleCoverageDays)
        && completeDaysWithSamples >= IdleCoverageDays;

    /// <summary>What the Idle Databases grid says when it is empty: that no database is idle, or (when the query stats do not cover
    /// the last 7 UTC days) that idle cannot be judged yet. An empty grid after a collection gap must not read as a clean bill.</summary>
    public static string IdleDatabasesEmptyText(bool hasCoverage) => hasCoverage
        ? "No idle databases detected"
        : "Idle databases cannot be judged yet: query stats do not cover each of the last 7 days";

    /// <summary>
    /// The oldest query-stats sample, and the number of distinct COMPLETE UTC days in [$2, $3) that hold one. The advice text claims
    /// "no query activity in 7 days", so 7 days must have been watched: the oldest sample at or before now - 7 days (with fewer days of
    /// history, six days of samples touch seven UTC dates), and a sample on each complete day (after a collection gap the oldest
    /// sample is old, yet the days since hold none, and every database reads as idle because nothing was watching, not because
    /// nothing ran). <see cref="IdleCoverageHolds"/> reads both. $1 server_id, $2 D-7 00:00, $3 today 00:00 (naive UTC).
    /// </summary>
    public const string IdleCoverageSql = @"
SELECT (SELECT MIN(collection_time) FROM v_query_stats WHERE server_id = $1),
       (SELECT COUNT(DISTINCT CAST(collection_time AS DATE))
        FROM v_query_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <  $3)";

    /// <summary>True once <see cref="IdleCoverageHolds"/>: 7 days of query-stats history and a sample on each complete UTC day of them.</summary>
    public static Task<bool> HasIdleCoverageAsync(
        NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default) =>
        HasIdleCoverageAtAsync(dataSource, serverId, DateTime.UtcNow, commandTimeoutSeconds, cancellationToken);

    /// <summary><see cref="HasIdleCoverageAsync"/> as of <paramref name="now"/> (UTC), so a test can ask just after 00:00.</summary>
    public static async Task<bool> HasIdleCoverageAtAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime now, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(IdleCoverageSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(IdleCoverageStartUtc(now), DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(IdleCoverageEndUtc(now), DateTimeKind.Unspecified) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0))
            return false;
        var days = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture);
        return IdleCoverageHolds(reader.GetDateTime(0), days, now);
    }

    /// <summary>The idle databases, only when the query stats cover each of the last 7 UTC days; otherwise an uncovered read with no rows.</summary>
    public static async Task<IdleDatabaseRead> GetIdleDatabaseReadAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime cutoffUtc, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        if (!await HasIdleCoverageAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken))
            return new IdleDatabaseRead(false, new List<IdleDatabase>());
        return new IdleDatabaseRead(true, await GetIdleDatabasesAsync(dataSource, serverId, cutoffUtc, commandTimeoutSeconds, cancellationToken));
    }

    public static async Task<List<IdleDatabase>> GetIdleDatabasesAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime cutoffUtc, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(IdleDatabasesSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });

        var items = new List<IdleDatabase>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new IdleDatabase(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                /* last_execution_time (sys.dm_exec_query_stats) is SERVER-LOCAL in the store, not naive UTC, so it is
                   read verbatim: no caller may shift it as if it were a collection_time. */
                reader.IsDBNull(3) ? null : reader.GetDateTime(3)));
        }
        return items;
    }

    /// <summary>tempdb pressure summary: latest and 24h peak values. $1 server_id, $2 cutoff.</summary>
    public const string TempdbSummarySql = @"
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

    public static async Task<List<TempdbSummaryMetric>> GetTempdbSummaryAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime cutoffUtc, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(TempdbSummarySql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });

        var items = new List<TempdbSummaryMetric>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new TempdbSummaryMetric(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                reader.IsDBNull(3) ? "" : reader.GetString(3)));
        }
        return items;
    }

    /// <summary>Wait stats grouped by cost category over the window. A wait stored with and without the trailing
    /// space the collector trimmed from #4884 on is one wait, with its summed time, when the category's top wait is
    /// picked: <c>per_spelling</c> sums per stored name, <c>per_wait</c> merges the spellings on
    /// <c>rtrim(wait_type)</c> (once per group, not per row), and the category is read from the clean name, so both
    /// spellings always land in the same category. $1 server_id, $2 cutoff.</summary>
    public const string WaitCategorySummarySql = @"
WITH per_spelling AS (
    SELECT
        wait_type,
        SUM(delta_wait_time_ms) AS wait_time_ms,
        SUM(delta_waiting_tasks) AS waiting_tasks
    FROM v_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_wait_time_ms IS NOT NULL
    AND   delta_wait_time_ms > 0
    GROUP BY wait_type
),
per_wait AS (
    SELECT
        rtrim(wait_type) AS wait_type,
        SUM(wait_time_ms) AS wait_time_ms,
        SUM(waiting_tasks) AS waiting_tasks
    FROM per_spelling
    GROUP BY rtrim(wait_type)
),
categorized AS (
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
        wait_time_ms,
        waiting_tasks
    FROM per_wait
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

    public static async Task<List<WaitCategorySummary>> GetWaitCategorySummaryAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime cutoffUtc, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(WaitCategorySummarySql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });

        var items = new List<WaitCategorySummary>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new WaitCategorySummary(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                reader.IsDBNull(4) ? "" : reader.GetString(4),
                reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5))));
        }
        return items;
    }

    /// <summary>Top-N most expensive queries by total CPU over the window. $1 server_id, $2 cutoff, $3 topN.</summary>
    public const string ExpensiveQueriesSql = $@"
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
    MAX(query_plan_gz) AS query_plan_gz,
    MAX(query_hash) AS query_hash
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   delta_worker_time IS NOT NULL
AND   delta_worker_time > 0
AND   {TimescaleSupport.IntervalHonestSourceFilter}
GROUP BY
    database_name,
    sql_handle,
    query_text
ORDER BY SUM(delta_worker_time) DESC, database_name, sql_handle, query_text
LIMIT $3";

    /// <summary>Reads the top-N statements. The caller clamps <paramref name="cutoffUtc"/> to the horizon that still
    /// retains query text, because no rollup carries per-row text.</summary>
    public static async Task<List<ExpensiveQuery>> GetExpensiveQueriesAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime cutoffUtc, int topN, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(ExpensiveQueriesSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

        var items = new List<ExpensiveQuery>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ExpensiveQuery(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                reader.IsDBNull(6) ? "" : reader.GetString(6),
                reader.IsDBNull(7) ? "" : reader.GetString(7),
                /* #2069: plans written since V54 ride as gzip bytes with the text column NULL: text-else-gz. */
                PayloadDimensions.ResolveContent(
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    reader.IsDBNull(9) ? null : reader.GetFieldValue<byte[]>(9)),
                reader.IsDBNull(10) ? null : reader.GetString(10)));
        }
        return items;
    }
}
