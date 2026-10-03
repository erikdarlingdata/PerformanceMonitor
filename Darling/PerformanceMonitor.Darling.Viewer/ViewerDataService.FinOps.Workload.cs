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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;

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
    /// <summary>Per-database resource usage from query_stats + file_io_stats deltas. $1 server_id, $2 cutoff. The SQL and the
    /// retention-tier routing live in <see cref="DarlingFinOpsDatabaseResourcesReader"/>.</summary>
    public const string DatabaseResourceUsageSql = DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSql;

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(RetentionTier)"/>
    public static string DatabaseResourceUsageSqlFor(RetentionTier tier) =>
        DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(tier);

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(RetentionTier, RollupCoverage, DateTime)"/>
    public static string DatabaseResourceUsageSqlFor(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc) =>
        DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(tier, coverage, windowStartUtc);

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(RetentionTier)"/>
    public static string TopResourceConsumersSqlFor(RetentionTier tier) =>
        DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(tier);

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(RetentionTier, RollupCoverage, DateTime)"/>
    public static string TopResourceConsumersSqlFor(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc) =>
        DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(tier, coverage, windowStartUtc);

    /* The window start is taken BEFORE the rollup probe; the reader resolves the tier AFTER it (its own clock read), as before the move. */
    public async Task<List<DatabaseResourceUsageRow>> GetDatabaseResourceUsageAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var items = await DarlingFinOpsDatabaseResourcesReader.GetDatabaseResourceUsageAsync(
            _dataSource, serverId, rollups, coverage, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return items.ConvertAll(DatabaseResourceUsageRow.From);
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

        /* #4766: the rows read their times on THIS server's clock (its collected one, else the viewer machine's offset,
           the rule every list row uses), read once per load, and not on the active server tab's: the FinOps tab lists the
           server it was opened for. */
        var clock = ViewerTimeHelper.ClockForServerOrMachine(
            await GetServerClocksAsync(serverId, cancellationToken), serverId, TimeZoneInfo.Local, DateTime.UtcNow);

        await using var command = _dataSource.CreateCommand(ApplicationConnectionsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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
                FirstSeenLocal = ViewerTimeHelper.ConvertToDisplay(reader.GetDateTime(18), ViewerTimeHelper.CurrentDisplayMode, clock),
                LastSeenLocal = ViewerTimeHelper.ConvertToDisplay(reader.GetDateTime(19), ViewerTimeHelper.CurrentDisplayMode, clock),
                /* #4766: the UTC instants too, so the columns' text can name the offset in the repeated autumn hour. */
                FirstSeenUtc = reader.GetDateTime(18),
                LastSeenUtc = reader.GetDateTime(19),
                Clock = clock
            });
        }
        return items;
    }

    /// <summary>
    /// Top databases by total CPU AND by average CPU per execution for the Utilization summary — one pass feeding both
    /// grids (#4227). The SQL lives in <see cref="DarlingFinOpsDatabaseResourcesReader"/>. $1 server_id, $2 cutoff.
    /// </summary>
    public const string TopResourceConsumersSql = DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSql;

    /// <summary>
    /// Both top-consumer grids from the one <see cref="TopResourceConsumersSql"/> round trip, ranked and sliced to
    /// <paramref name="topN"/> by <see cref="DarlingFinOpsDatabaseResourcesReader.GetTopResourceConsumersAsync"/>.
    /// </summary>
    public async Task<(List<TopResourceConsumerRow> ByTotal, List<TopResourceConsumerRow> ByAvg)> GetTopResourceConsumersAsync(int serverId, int hoursBack = 24, int topN = 5, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var (byTotal, byAvg) = await DarlingFinOpsDatabaseResourcesReader.GetTopResourceConsumersAsync(
            _dataSource, serverId, rollups, coverage, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, topN, cancellationToken);
        return (byTotal.Select(TopResourceConsumerRow.From).ToList(), byAvg.Select(TopResourceConsumerRow.From).ToList());
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

    public async Task<List<WaitCategorySummaryRow>> GetWaitCategorySummaryAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        await using var command = _dataSource.CreateCommand(WaitCategorySummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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
    MAX(query_plan_gz) AS query_plan_gz
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
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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

    /// <summary>The high-impact read's SQL; lives in <see cref="DarlingFinOpsHighImpactReader"/>.</summary>
    public const string HighImpactQueriesSql = DarlingFinOpsHighImpactReader.HighImpactQueriesSql;

    public async Task<List<HighImpactQueryRow>> GetHighImpactQueriesAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var rows = await DarlingFinOpsHighImpactReader.ReadAsync(
            _dataSource, serverId, hoursBack, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return rows.Select(HighImpactQueryRow.From).ToList();
    }
}
