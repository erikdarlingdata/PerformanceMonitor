using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

public partial class DuckDbFactCollector
{
    /// <summary>
    /// Identifies individual queries that are consistently terrible ("bad actors").
    /// These queries don't necessarily cause server-level symptoms but waste resources
    /// on every execution. Detection uses execution count tiers x per-execution impact.
    /// Top 5 worst offenders become individual BAD_ACTOR facts.
    /// </summary>
    private async Task CollectBadActorFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    database_name,
    query_hash,
    SUM(delta_execution_count)::BIGINT AS exec_count,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_worker_time)::DOUBLE PRECISION / SUM(delta_execution_count) / 1000.0
         ELSE 0 END AS avg_cpu_ms,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_elapsed_time)::DOUBLE PRECISION / SUM(delta_execution_count) / 1000.0
         ELSE 0 END AS avg_elapsed_ms,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_logical_reads)::DOUBLE PRECISION / SUM(delta_execution_count)
         ELSE 0 END AS avg_reads,
    SUM(delta_worker_time)::BIGINT AS total_cpu_us,
    SUM(delta_logical_reads)::BIGINT AS total_reads,
    SUM(delta_spills)::BIGINT AS total_spills,
    MAX(max_dop) AS max_dop,
    LEFT(MAX(query_text), 200) AS query_text
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0
GROUP BY database_name, query_hash
HAVING SUM(delta_execution_count) >= 100
ORDER BY SUM(delta_worker_time)::DOUBLE PRECISION / GREATEST(SUM(delta_execution_count), 1) *
         LN(GREATEST(SUM(delta_execution_count), 1)) DESC
LIMIT 5";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects active query snapshot facts: long-running queries, blocked sessions, high DOP.
    /// </summary>
    private async Task CollectActiveQueryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    COUNT(*) AS total_snapshots,
    COUNT(CASE WHEN total_elapsed_time_ms > 30000 THEN 1 END) AS long_running_count,
    COUNT(CASE WHEN blocking_session_id > 0 THEN 1 END) AS blocked_count,
    MAX(total_elapsed_time_ms) AS max_elapsed_ms,
    COUNT(CASE WHEN dop > 1 THEN 1 END) AS parallel_count,
    MAX(dop) AS max_dop,
    COUNT(DISTINCT session_id) AS distinct_sessions
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalSnapshots = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (totalSnapshots == 0) return;

            var longRunning = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var blocked = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var maxElapsed = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var parallel = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var maxDop = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var distinctSessions = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));

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
                    ["parallel_count"] = parallel,
                    ["max_dop"] = maxDop,
                    ["distinct_sessions"] = distinctSessions
                }
            });
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects running job facts: jobs currently running long vs historical averages.
    /// </summary>
    private async Task CollectRunningJobFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    COUNT(*) AS running_count,
    COUNT(CASE WHEN is_running_long THEN 1 END) AS running_long_count,
    MAX(percent_of_average) AS max_percent_of_avg,
    MAX(current_duration_seconds) AS max_duration_seconds
FROM v_running_jobs
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var runningCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (runningCount == 0) return;

            var runningLong = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var maxPctAvg = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxDuration = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects session stats: connection counts per application, total connections.
    /// </summary>
    private async Task CollectSessionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT program_name, connection_count, running_count, sleeping_count, dormant_count,
           ROW_NUMBER() OVER (PARTITION BY program_name ORDER BY collection_time DESC) AS rn
    FROM v_session_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    SUM(connection_count) AS total_connections,
    SUM(running_count) AS total_running,
    SUM(sleeping_count) AS total_sleeping,
    SUM(dormant_count) AS total_dormant,
    COUNT(*) AS distinct_apps,
    MAX(connection_count) AS max_app_connections
FROM latest WHERE rn = 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalConns = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (totalConns == 0) return;

            var totalRunning = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalSleeping = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalDormant = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var distinctApps = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var maxAppConns = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

}
