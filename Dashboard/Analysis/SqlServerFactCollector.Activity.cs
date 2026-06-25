using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerFactCollector
{
    /// <summary>
    /// Identifies individual queries that are consistently terrible ("bad actors").
    /// These queries don't necessarily cause server-level symptoms but waste resources
    /// on every execution. Detection uses execution count tiers x per-execution impact.
    /// Top 5 worst offenders become individual BAD_ACTOR facts.
    /// Dashboard query_hash is binary(8) — convert to hex string for fact key.
    /// </summary>
    private async Task CollectBadActorFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    database_name,
    CONVERT(VARCHAR(18), query_hash, 1) AS query_hash,
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
    MAX(max_dop) AS max_dop,
    LEFT(CAST(DECOMPRESS(MAX(query_text)) AS NVARCHAR(MAX)), 200) AS query_text
FROM collect.query_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   execution_count_delta > 0
GROUP BY database_name, query_hash
HAVING SUM(execution_count_delta) >= 100
ORDER BY CAST(SUM(total_worker_time_delta) AS FLOAT) / NULLIF(SUM(execution_count_delta), 0) *
         LOG(NULLIF(SUM(execution_count_delta), 0)) DESC";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var dbName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var queryHash = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var execCount = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
                var avgCpuMs = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                var avgElapsedMs = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));
                var avgReads = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));
                var totalCpuUs = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6));
                var totalReads = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7));
                var totalSpills = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8));
                var maxDop = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9));
                var queryText = reader.IsDBNull(10) ? "" : reader.GetString(10);

                // Skip low-impact queries — need meaningful per-execution cost
                if (avgCpuMs < 10 && avgReads < 1000) continue;

                facts.Add(new Fact
                {
                    Source = "bad_actor",
                    Key = $"BAD_ACTOR_{queryHash}",
                    Value = avgCpuMs, // Primary scoring dimension
                    ServerId = context.ServerId,
                    DatabaseName = dbName,
                    Metadata = new Dictionary<string, double>
                    {
                        ["execution_count"] = execCount,
                        ["avg_cpu_ms"] = avgCpuMs,
                        ["avg_elapsed_ms"] = avgElapsedMs,
                        ["avg_reads"] = avgReads,
                        ["total_cpu_us"] = totalCpuUs,
                        ["total_reads"] = totalReads,
                        ["total_spills"] = totalSpills,
                        ["max_dop"] = maxDop
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectBadActorFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects active query snapshot facts: long-running queries, blocked sessions, high DOP.
    /// Dashboard query_snapshots table is created by sp_WhoIsActive dynamically.
    /// We query it if it exists.
    /// </summary>
    private async Task CollectActiveQueryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // Check if the table exists first (created dynamically by sp_WhoIsActive)
            using var checkCmd = connection.CreateCommand();
            checkCmd.CommandText = "SELECT OBJECT_ID(N'collect.query_snapshots', N'U')";
            var tableExists = await checkCmd.ExecuteScalarAsync();
            if (tableExists == null || tableExists == DBNull.Value) return;

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    COUNT(*) AS total_snapshots,
    COUNT(CASE WHEN DATEDIFF(MILLISECOND, 0, [elapsed_time]) > 30000 THEN 1 END) AS long_running_count,
    COUNT(CASE WHEN [blocking_session_id] IS NOT NULL AND [blocking_session_id] != '' THEN 1 END) AS blocked_count,
    MAX(DATEDIFF(MILLISECOND, 0, [elapsed_time])) AS max_elapsed_ms,
    COUNT(DISTINCT [session_id]) AS distinct_sessions
FROM collect.query_snapshots
WHERE collection_time >= @startTime
AND   collection_time <= @endTime";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalSnapshots = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            if (totalSnapshots == 0) return;

            var longRunning = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var blocked = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var maxElapsed = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var distinctSessions = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "ACTIVE_QUERIES",
                Value = longRunning,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["total_snapshots"] = totalSnapshots,
                    ["long_running_count"] = longRunning,
                    ["blocked_count"] = blocked,
                    ["max_elapsed_ms"] = maxElapsed,
                    ["distinct_sessions"] = distinctSessions
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectActiveQueryFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects running job facts: jobs currently running long vs historical averages.
    /// </summary>
    private async Task CollectRunningJobFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    COUNT(*) AS running_count,
    COUNT(CASE WHEN is_running_long = 1 THEN 1 END) AS running_long_count,
    MAX(percent_of_average) AS max_percent_of_avg,
    MAX(current_duration_seconds) AS max_duration_seconds
FROM collect.running_jobs
WHERE collection_time >= @startTime
AND   collection_time <= @endTime";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var runningCount = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            if (runningCount == 0) return;

            var runningLong = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var maxPctAvg = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxDuration = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));

            facts.Add(new Fact
            {
                Source = "jobs",
                Key = "RUNNING_JOBS",
                Value = runningLong,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["running_count"] = runningCount,
                    ["running_long_count"] = runningLong,
                    ["max_percent_of_average"] = maxPctAvg,
                    ["max_duration_seconds"] = maxDuration
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectRunningJobFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects session stats: connection counts, total connections.
    /// Dashboard session_stats is a flat table (not per-program_name), so we adapt.
    /// </summary>
    private async Task CollectSessionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest AS (
    SELECT
        total_sessions,
        running_sessions,
        sleeping_sessions,
        dormant_sessions,
        databases_with_connections,
        top_application_connections,
        ROW_NUMBER() OVER (ORDER BY collection_time DESC) AS rn
    FROM collect.session_stats
    WHERE collection_time >= @startTime
    AND   collection_time <= @endTime
)
SELECT
    total_sessions AS total_connections,
    running_sessions AS total_running,
    sleeping_sessions AS total_sleeping,
    dormant_sessions AS total_dormant,
    databases_with_connections AS distinct_apps,
    top_application_connections AS max_app_connections
FROM latest WHERE rn = 1";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalConns = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            if (totalConns == 0) return;

            var totalRunning = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var totalSleeping = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var totalDormant = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var distinctApps = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
            var maxAppConns = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));

            facts.Add(new Fact
            {
                Source = "sessions",
                Key = "SESSION_STATS",
                Value = totalConns,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["total_connections"] = totalConns,
                    ["total_running"] = totalRunning,
                    ["total_sleeping"] = totalSleeping,
                    ["total_dormant"] = totalDormant,
                    ["distinct_applications"] = distinctApps,
                    ["max_app_connections"] = maxAppConns
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectSessionFactsAsync failed", ex);
        }
    }
}
