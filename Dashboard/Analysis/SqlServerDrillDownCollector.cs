using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Enriches findings with drill-down data from SQL Server.
/// Runs after graph traversal, only for findings above severity threshold.
/// Each drill-down query is limited to top N results with truncated text.
///
/// This makes analyze_server self-sufficient -- instead of returning a list
/// of "next tools to call," findings include the actual supporting data.
///
/// Port of Lite's DrillDownCollector -- uses SQL Server collect.* tables instead of DuckDB views.
/// No server_id filtering -- Dashboard monitors one server per database.
/// </summary>
public class SqlServerDrillDownCollector
{
    private readonly string _connectionString;
    private readonly IPlanFetcher? _planFetcher;
    private const int TextLimit = 500;

    public SqlServerDrillDownCollector(string connectionString, IPlanFetcher? planFetcher = null)
    {
        _connectionString = connectionString;
        _planFetcher = planFetcher;
    }

    /// <summary>
    /// Enriches each finding's DrillDown dictionary based on its story path.
    /// </summary>
    public async Task EnrichFindingsAsync(List<AnalysisFinding> findings, AnalysisContext context)
    {
        foreach (var finding in findings)
        {
            if (finding.Severity < 0.5) continue;

            try
            {
                finding.DrillDown = new Dictionary<string, object>();
                var pathKeys = finding.StoryPath.Split(" → ", StringSplitOptions.RemoveEmptyEntries).ToHashSet();

                if (pathKeys.Contains("DEADLOCKS"))
                    await CollectTopDeadlocks(finding, context);

                if (pathKeys.Contains("BLOCKING_EVENTS"))
                    await CollectTopBlockingChains(finding, context);

                if (pathKeys.Contains("CPU_SPIKE"))
                    await CollectQueriesAtSpike(finding, context);

                if (pathKeys.Contains("CPU_SQL_PERCENT") || pathKeys.Contains("CPU_SPIKE"))
                    await CollectTopCpuQueries(finding, context);

                if (pathKeys.Contains("QUERY_SPILLS"))
                    await CollectTopSpillingQueries(finding, context);

                if (pathKeys.Contains("IO_READ_LATENCY_MS") || pathKeys.Contains("IO_WRITE_LATENCY_MS"))
                    await CollectFileLatencyBreakdown(finding, context);

                if (pathKeys.Contains("LCK") || pathKeys.Contains("LCK_M_S") || pathKeys.Contains("LCK_M_IS"))
                    await CollectLockModeBreakdown(finding, context);

                if (pathKeys.Contains("DB_CONFIG"))
                    await CollectConfigIssues(finding, context);

                if (pathKeys.Contains("TEMPDB_USAGE"))
                    await CollectTempDbBreakdown(finding, context);

                if (pathKeys.Contains("MEMORY_GRANT_PENDING"))
                    await CollectPendingGrants(finding, context);

                if (pathKeys.Any(k => k.StartsWith("BAD_ACTOR_")))
                    await CollectBadActorDetail(finding, context);

                // Plan analysis: for findings with top queries, analyze their cached plans
                await CollectPlanAnalysis(finding, context);

                // Remove empty drill-down dictionaries
                if (finding.DrillDown.Count == 0)
                    finding.DrillDown = null;
            }
            catch (Exception ex)
            {
                Logger.Error(
                    $"[SqlServerDrillDownCollector] Drill-down failed for {finding.StoryPath}: {ex.GetType().Name}: {ex.Message}\n{ex.StackTrace}");
                // Don't null out -- keep whatever was collected before the error
            }
        }
    }

    private async Task CollectTopDeadlocks(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 3
    collection_time,
    event_date,
    spid,
    LEFT(CAST(query AS NVARCHAR(MAX)), 500) AS victim_sql
FROM collect.deadlocks
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY collection_time DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                deadlock_time = reader.IsDBNull(1) ? "" : reader.GetDateTime(1).ToString("o"),
                victim = reader.IsDBNull(2) ? "" : reader.GetValue(2).ToString(),
                victim_sql = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_deadlocks"] = items;
    }

    private async Task CollectTopBlockingChains(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    database_name,
    spid AS blocked_spid,
    0 AS blocking_spid,
    wait_time_ms,
    lock_mode,
    LEFT(CAST(query_text AS NVARCHAR(MAX)), 500) AS blocked_sql,
    LEFT(blocking_tree, 500) AS blocking_sql
FROM collect.blocking_BlockedProcessReport
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY wait_time_ms DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                database = reader.IsDBNull(1) ? "" : reader.GetString(1),
                blocked_spid = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                blocking_spid = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                wait_time_ms = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                lock_mode = reader.IsDBNull(5) ? "" : reader.GetString(5),
                blocked_sql = reader.IsDBNull(6) ? "" : reader.GetString(6),
                blocking_sql = reader.IsDBNull(7) ? "" : reader.GetString(7)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_blocking_chains"] = items;
    }

    private async Task CollectQueriesAtSpike(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // Check if query_snapshots table exists (created dynamically by sp_WhoIsActive)
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT OBJECT_ID(N'collect.query_snapshots', N'U')";
        var tableExists = await checkCmd.ExecuteScalarAsync();
        if (tableExists == null || tableExists == DBNull.Value) return;

        // Step 1: Find when the spike occurred
        using var peakCmd = connection.CreateCommand();
        peakCmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1 collection_time, sqlserver_cpu_utilization
FROM collect.cpu_utilization_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY sqlserver_cpu_utilization DESC;";

        peakCmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        peakCmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        DateTime? peakTime = null;
        int peakCpu = 0;
        using (var peakReader = await peakCmd.ExecuteReaderAsync())
        {
            if (await peakReader.ReadAsync())
            {
                peakTime = peakReader.GetDateTime(0);
                peakCpu = peakReader.GetInt32(1);
            }
        }

        if (peakTime == null) return;

        // Step 2: Get queries active within 2 minutes of peak
        using var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    [session_id],
    [database_name],
    [status],
    DATEDIFF(MILLISECOND, 0, [CPU]) AS cpu_time_ms,
    DATEDIFF(MILLISECOND, 0, [elapsed_time]) AS total_elapsed_time_ms,
    [reads] AS logical_reads,
    [wait_info] AS wait_type,
    0 AS dop,
    0 AS parallel_worker_count,
    LEFT(CAST([sql_text] AS NVARCHAR(MAX)), 500) AS query_text
FROM collect.query_snapshots
WHERE collection_time >= @spikeStart
AND   collection_time <= @spikeEnd
AND   CAST([sql_text] AS NVARCHAR(MAX)) NOT LIKE 'WAITFOR%'
ORDER BY DATEDIFF(MILLISECOND, 0, [CPU]) DESC;";

        queryCmd.Parameters.Add(new SqlParameter("@spikeStart", peakTime.Value.AddMinutes(-2)));
        queryCmd.Parameters.Add(new SqlParameter("@spikeEnd", peakTime.Value.AddMinutes(2)));

        var items = new List<object>();
        using (var reader = await queryCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                items.Add(new
                {
                    time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                    session_id = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                    database = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    status = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    cpu_time_ms = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                    elapsed_time_ms = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                    logical_reads = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                    wait_type = reader.IsDBNull(7) ? "" : reader.GetString(7),
                    dop = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8)),
                    parallel_workers = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                    query_text = reader.IsDBNull(10) ? "" : reader.GetString(10)
                });
            }
        }

        if (items.Count > 0)
        {
            finding.DrillDown!["spike_peak"] = new
            {
                time = peakTime.Value.ToString("o"),
                cpu_percent = peakCpu
            };
            finding.DrillDown!["queries_at_spike"] = items;
        }
    }

    private async Task CollectTopCpuQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    database_name,
    CONVERT(VARCHAR(18), query_hash, 1) AS query_hash,
    CAST(SUM(total_worker_time_delta) AS BIGINT) AS total_cpu_us,
    CAST(SUM(execution_count_delta) AS BIGINT) AS exec_count,
    MAX(max_dop) AS max_dop,
    CAST(SUM(total_spills) AS BIGINT) AS spills,
    LEFT(CAST(DECOMPRESS(MAX(query_text)) AS NVARCHAR(MAX)), 500) AS query_text
FROM collect.query_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   total_worker_time_delta > 0
GROUP BY database_name, query_hash
ORDER BY CAST(SUM(total_worker_time_delta) AS BIGINT) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_hash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                total_cpu_ms = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)) / 1000.0,
                execution_count = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                max_dop = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                spills = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                query_text = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        if (items.Count > 0 && !finding.DrillDown!.ContainsKey("top_cpu_queries"))
            finding.DrillDown!["top_cpu_queries"] = items;
    }

    private async Task CollectTopSpillingQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    database_name,
    CONVERT(VARCHAR(18), query_hash, 1) AS query_hash,
    CAST(SUM(total_spills) AS BIGINT) AS total_spills,
    CAST(SUM(execution_count_delta) AS BIGINT) AS exec_count,
    LEFT(CAST(DECOMPRESS(MAX(query_text)) AS NVARCHAR(MAX)), 500) AS query_text
FROM collect.query_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   total_spills > 0
GROUP BY database_name, query_hash
ORDER BY CAST(SUM(total_spills) AS BIGINT) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_hash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                total_spills = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
                execution_count = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                query_text = reader.IsDBNull(4) ? "" : reader.GetString(4)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_spilling_queries"] = items;
    }

    private async Task CollectFileLatencyBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 10
    database_name,
    file_type_desc AS file_type,
    AVG(io_stall_read_ms_delta * 1.0 / NULLIF(num_of_reads_delta, 0)) AS avg_read_ms,
    AVG(io_stall_write_ms_delta * 1.0 / NULLIF(num_of_writes_delta, 0)) AS avg_write_ms,
    CAST(SUM(num_of_reads_delta) AS BIGINT) AS total_reads,
    CAST(SUM(num_of_writes_delta) AS BIGINT) AS total_writes
FROM collect.file_io_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   (num_of_reads_delta > 0 OR num_of_writes_delta > 0)
GROUP BY database_name, file_type_desc
ORDER BY AVG(io_stall_read_ms_delta * 1.0 / NULLIF(num_of_reads_delta, 0)) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                file_type = reader.IsDBNull(1) ? "" : reader.GetString(1),
                avg_read_latency_ms = reader.IsDBNull(2) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(2)), 2),
                avg_write_latency_ms = reader.IsDBNull(3) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(3)), 2),
                total_reads = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                total_writes = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["file_latency_breakdown"] = items;
    }

    private async Task CollectLockModeBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 10
    wait_type,
    CAST(SUM(wait_time_ms_delta) AS BIGINT) AS total_wait_ms,
    CAST(SUM(waiting_tasks_count_delta) AS BIGINT) AS total_count
FROM collect.wait_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   wait_type LIKE 'LCK%'
AND   wait_time_ms_delta > 0
GROUP BY wait_type
ORDER BY CAST(SUM(wait_time_ms_delta) AS BIGINT) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                lock_type = reader.IsDBNull(0) ? "" : reader.GetString(0),
                total_wait_ms = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                waiting_tasks = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["lock_mode_breakdown"] = items;
    }

    private async Task CollectConfigIssues(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // The Dashboard uses config.database_configuration_history which stores
        // settings as rows (setting_type, setting_name, setting_value) not columns.
        // Pivot the latest snapshot into the format we need.
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest AS (
    SELECT database_name, setting_name,
           CAST(setting_value AS NVARCHAR(256)) AS setting_value,
           ROW_NUMBER() OVER (PARTITION BY database_name, setting_name ORDER BY collection_time DESC) AS rn
    FROM config.database_configuration_history
    WHERE setting_name IN (
        'recovery_model_desc', 'is_auto_shrink_on', 'is_auto_close_on',
        'is_read_committed_snapshot_on', 'page_verify_option_desc', 'is_query_store_on'
    )
),
pivoted AS (
    SELECT
        database_name,
        MAX(CASE WHEN setting_name = 'recovery_model_desc' THEN setting_value END) AS recovery_model,
        MAX(CASE WHEN setting_name = 'is_auto_shrink_on' THEN setting_value END) AS is_auto_shrink_on,
        MAX(CASE WHEN setting_name = 'is_auto_close_on' THEN setting_value END) AS is_auto_close_on,
        MAX(CASE WHEN setting_name = 'is_read_committed_snapshot_on' THEN setting_value END) AS is_rcsi_on,
        MAX(CASE WHEN setting_name = 'page_verify_option_desc' THEN setting_value END) AS page_verify_option,
        MAX(CASE WHEN setting_name = 'is_query_store_on' THEN setting_value END) AS is_query_store_on
    FROM latest
    WHERE rn = 1
    GROUP BY database_name
)
SELECT database_name, recovery_model,
       is_auto_shrink_on, is_auto_close_on,
       is_rcsi_on, page_verify_option, is_query_store_on
FROM pivoted
WHERE is_auto_shrink_on = '1' OR is_auto_close_on = '1'
   OR is_rcsi_on = '0' OR page_verify_option != 'CHECKSUM'
ORDER BY database_name;";

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var issues = new List<string>();
            var autoShrink = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var autoClose = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var rcsi = reader.IsDBNull(4) ? "" : reader.GetString(4);
            var pageVerify = reader.IsDBNull(5) ? "" : reader.GetString(5);
            var queryStore = reader.IsDBNull(6) ? "" : reader.GetString(6);

            if (autoShrink == "1") issues.Add("auto_shrink ON");
            if (autoClose == "1") issues.Add("auto_close ON");
            if (rcsi == "0") issues.Add("RCSI OFF");
            if (!string.IsNullOrEmpty(pageVerify) && pageVerify != "CHECKSUM") issues.Add($"page_verify={pageVerify}");

            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                recovery_model = reader.IsDBNull(1) ? "" : reader.GetString(1),
                rcsi = rcsi == "1",
                query_store = queryStore == "1",
                issues
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["config_issues"] = items;
    }

    private async Task CollectTempDbBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    user_object_reserved_mb,
    internal_object_reserved_mb,
    version_store_reserved_mb,
    unallocated_mb
FROM collect.tempdb_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY (user_object_reserved_mb + internal_object_reserved_mb + version_store_reserved_mb) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                time = reader.GetDateTime(0).ToString("o"),
                user_objects_mb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                internal_objects_mb = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                version_store_mb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                unallocated_mb = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["tempdb_breakdown"] = items;
    }

    private async Task CollectPendingGrants(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    target_memory_mb, total_memory_mb, available_memory_mb,
    granted_memory_mb, used_memory_mb,
    grantee_count, waiter_count,
    timeout_error_count_delta, forced_grant_count_delta
FROM collect.memory_grant_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   waiter_count > 0
ORDER BY waiter_count DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                target_memory_mb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                total_memory_mb = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                available_memory_mb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                granted_memory_mb = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4)),
                used_memory_mb = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5)),
                grantee_count = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                waiter_count = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                timeout_errors = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                forced_grants = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["pending_grants"] = items;
    }

    /// <summary>
    /// For findings that have query hashes (bad actors), fetch the execution plan
    /// live from SQL Server via IPlanFetcher, then run PlanAnalyzer to surface
    /// warnings and missing indexes. No plan storage needed -- fetch on demand
    /// only for queries that make it into high-impact findings.
    /// </summary>
    private async Task CollectPlanAnalysis(AnalysisFinding finding, AnalysisContext context)
    {
        if (finding.DrillDown == null || _planFetcher == null) return;

        // Only analyze plans for bad actor findings (1 plan each).
        // Skip top_cpu_queries (5 plans would be too heavy).
        if (!finding.RootFactKey.StartsWith("BAD_ACTOR_")) return;

        var queryHash = finding.RootFactKey.Replace("BAD_ACTOR_", "");
        if (string.IsNullOrEmpty(queryHash)) return;

        // Look up plan_handle from collect.query_stats for this query_hash
        string? planHandle = null;
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1 CONVERT(VARCHAR(130), plan_handle, 1) AS plan_handle
FROM collect.query_stats
WHERE query_hash = CONVERT(BINARY(8), @queryHash, 1)
AND   plan_handle IS NOT NULL
ORDER BY collection_time DESC;";

            cmd.Parameters.Add(new SqlParameter("@queryHash", queryHash));

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync() && !reader.IsDBNull(0))
                planHandle = reader.GetString(0);
        }
        catch { return; }

        if (string.IsNullOrEmpty(planHandle)) return;

        // Fetch plan XML live from SQL Server
        var planXml = await _planFetcher.FetchPlanXmlAsync(context.ServerId, planHandle);
        if (string.IsNullOrEmpty(planXml)) return;

        try
        {
            var plan = ShowPlanParser.Parse(planXml);
            PlanAnalyzer.Analyze(plan);

            var allWarnings = plan.Batches
                .SelectMany(b => b.Statements)
                .Where(s => s.RootNode != null)
                .SelectMany(s =>
                {
                    var nodeWarnings = new List<PlanNode>();
                    CollectPlanNodes(s.RootNode!, nodeWarnings);
                    return s.PlanWarnings
                        .Concat(nodeWarnings.SelectMany(n => n.Warnings));
                })
                .ToList();

            var missingIndexes = plan.AllMissingIndexes;

            if (allWarnings.Count == 0 && missingIndexes.Count == 0) return;

            finding.DrillDown["plan_analysis"] = new
            {
                query_hash = queryHash,
                warning_count = allWarnings.Count,
                critical_count = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Critical),
                warnings = allWarnings
                    .OrderByDescending(w => w.Severity)
                    .Take(10)
                    .Select(w => new
                    {
                        severity = w.Severity.ToString(),
                        type = w.WarningType,
                        message = McpHelpers.Truncate(w.Message, 300)
                    }),
                missing_indexes = missingIndexes.Take(5).Select(idx => new
                {
                    table = $"{idx.Schema}.{idx.Table}",
                    impact = idx.Impact,
                    create_statement = idx.CreateStatement
                })
            };
        }
        catch
        {
            // Plan parsing can fail on malformed XML -- skip silently
        }
    }

    private static void CollectPlanNodes(PlanNode node, List<PlanNode> nodes)
    {
        nodes.Add(node);
        foreach (var child in node.Children)
            CollectPlanNodes(child, nodes);
    }

    private async Task CollectBadActorDetail(AnalysisFinding finding, AnalysisContext context)
    {
        // Extract query_hash from the fact key (BAD_ACTOR_0x...)
        var queryHash = finding.RootFactKey.Replace("BAD_ACTOR_", "");
        if (string.IsNullOrEmpty(queryHash)) return;

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    database_name,
    CONVERT(VARCHAR(18), query_hash, 1) AS query_hash,
    LEFT(CAST(DECOMPRESS(MAX(query_text)) AS NVARCHAR(MAX)), 500) AS query_text,
    CAST(SUM(execution_count_delta) AS BIGINT) AS exec_count,
    CASE WHEN SUM(execution_count_delta) > 0
         THEN CAST(SUM(total_worker_time_delta) AS FLOAT) / SUM(execution_count_delta) / 1000.0
         ELSE 0 END AS avg_cpu_ms,
    CASE WHEN SUM(execution_count_delta) > 0
         THEN CAST(SUM(total_elapsed_time_delta) AS FLOAT) / SUM(execution_count_delta) / 1000.0
         ELSE 0 END AS avg_elapsed_ms,
    CASE WHEN SUM(execution_count_delta) > 0
         THEN CAST(SUM(total_logical_reads_delta) AS FLOAT) / SUM(execution_count_delta)
         ELSE 0 END AS avg_reads,
    CAST(SUM(total_worker_time_delta) AS BIGINT) AS total_cpu_us,
    CAST(SUM(total_logical_reads_delta) AS BIGINT) AS total_reads,
    CAST(SUM(total_spills) AS BIGINT) AS total_spills,
    MAX(max_dop) AS max_dop
FROM collect.query_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   query_hash = CONVERT(BINARY(8), @queryHash, 1)
GROUP BY database_name, query_hash;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));
        cmd.Parameters.Add(new SqlParameter("@queryHash", queryHash));

        using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            finding.DrillDown!["bad_actor_query"] = new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_hash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                query_text = reader.IsDBNull(2) ? "" : reader.GetString(2),
                execution_count = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                avg_cpu_ms = reader.IsDBNull(4) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(4)), 2),
                avg_elapsed_ms = reader.IsDBNull(5) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(5)), 2),
                avg_reads = reader.IsDBNull(6) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(6)), 0),
                total_cpu_ms = reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7)) / 1000.0,
                total_reads = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                total_spills = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                max_dop = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10))
            };
        }
    }
}
