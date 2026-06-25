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
    /// Collects memory stats: total physical RAM, buffer pool size, target memory.
    /// These facts enable edition-aware memory recommendations in the config audit.
    /// </summary>
    private async Task CollectMemoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1
    total_physical_memory_mb,
    buffer_pool_mb,
    committed_target_memory_mb
FROM collect.memory_stats
WHERE collection_time <= @endTime
ORDER BY collection_time DESC";

            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalPhysical = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var bufferPool = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var targetMemory = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));

            if (totalPhysical > 0)
                facts.Add(new Fact { Source = "memory", Key = "MEMORY_TOTAL_PHYSICAL_MB", Value = totalPhysical, ServerId = context.ServerId });
            if (bufferPool > 0)
                facts.Add(new Fact { Source = "memory", Key = "MEMORY_BUFFER_POOL_MB", Value = bufferPool, ServerId = context.ServerId });
            if (targetMemory > 0)
                facts.Add(new Fact { Source = "memory", Key = "MEMORY_TARGET_MB", Value = targetMemory, ServerId = context.ServerId });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectMemoryFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects CPU utilization: average and max SQL Server CPU % over the period.
    /// Value is average SQL CPU %. Corroborates SOS_SCHEDULER_YIELD.
    /// </summary>
    private async Task CollectCpuUtilizationFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    AVG(CAST(sqlserver_cpu_utilization AS FLOAT)) AS avg_sql_cpu,
    MAX(sqlserver_cpu_utilization) AS max_sql_cpu,
    AVG(CAST(other_process_cpu_utilization AS FLOAT)) AS avg_other_cpu,
    MAX(other_process_cpu_utilization) AS max_other_cpu,
    COUNT(*) AS sample_count
FROM collect.cpu_utilization_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgSqlCpu = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var maxSqlCpu = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var avgOtherCpu = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxOtherCpu = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var sampleCount = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

            if (sampleCount == 0) return;

            var cpuMetadata = new Dictionary<string, double>
            {
                ["avg_sql_cpu"] = avgSqlCpu,
                ["max_sql_cpu"] = maxSqlCpu,
                ["avg_other_cpu"] = avgOtherCpu,
                ["max_other_cpu"] = maxOtherCpu,
                ["avg_total_cpu"] = avgSqlCpu + avgOtherCpu,
                ["sample_count"] = sampleCount
            };

            facts.Add(new Fact
            {
                Source = "cpu",
                Key = "CPU_SQL_PERCENT",
                Value = avgSqlCpu,
                ServerId = context.ServerId,
                Metadata = cpuMetadata
            });

            // Emit a CPU_SPIKE fact when max is high and significantly above average.
            // This catches bursty CPU events that average-based scoring misses entirely.
            // Requires max >= 80% AND at least 3x the average (or avg < 20% with max >= 80%).
            if (maxSqlCpu >= 80 && (avgSqlCpu < 20 || maxSqlCpu / Math.Max(avgSqlCpu, 1) >= 3))
            {
                facts.Add(new Fact
                {
                    Source = "cpu",
                    Key = "CPU_SPIKE",
                    Value = maxSqlCpu,
                    ServerId = context.ServerId,
                    Metadata = cpuMetadata
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectCpuUtilizationFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects memory grant facts from the memory_grant_stats table.
    /// Detects grant waiters (sessions waiting for memory) and grant pressure.
    /// </summary>
    private async Task CollectMemoryGrantFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    MAX(waiter_count) AS max_waiters,
    AVG(CAST(waiter_count AS FLOAT)) AS avg_waiters,
    MAX(grantee_count) AS max_grantees,
    SUM(timeout_error_count_delta) AS total_timeout_errors,
    SUM(forced_grant_count_delta) AS total_forced_grants
FROM collect.memory_grant_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var maxWaiters = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            var avgWaiters = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var maxGrantees = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var totalTimeouts = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var totalForcedGrants = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

            // Only create a fact if there's evidence of grant pressure
            if (maxWaiters <= 0 && totalTimeouts <= 0 && totalForcedGrants <= 0) return;

            facts.Add(new Fact
            {
                Source = "memory",
                Key = "MEMORY_GRANT_PENDING",
                Value = maxWaiters,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["max_waiters"] = maxWaiters,
                    ["avg_waiters"] = avgWaiters,
                    ["max_grantees"] = maxGrantees,
                    ["total_timeout_errors"] = totalTimeouts,
                    ["total_forced_grants"] = totalForcedGrants
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectMemoryGrantFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects key perfmon throughput counters: Batch Requests/sec, compilations, recompilations.
    /// Unscored context that distinguishes a busy server from a sick one (used by the AI surfaces).
    /// </summary>
    private async Task CollectPerfmonFactsAsync(AnalysisContext context, List<Fact> facts)
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
        counter_name,
        cntr_value,
        cntr_value_delta,
        ROW_NUMBER() OVER (PARTITION BY counter_name ORDER BY collection_time DESC) AS rn
    FROM collect.perfmon_stats
    WHERE collection_time >= @startTime
    AND   collection_time <= @endTime
    AND   counter_name IN ('Batch Requests/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec')
)
SELECT counter_name, cntr_value, cntr_value_delta
FROM latest WHERE rn = 1";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var counterName = reader.GetString(0);
                var cntrValue = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
                var deltaValue = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

                var (factKey, source) = counterName switch
                {
                    "Batch Requests/sec" => ("PERFMON_BATCH_REQ_SEC", "perfmon"),
                    "SQL Compilations/sec" => ("PERFMON_COMPILATIONS_SEC", "perfmon"),
                    "SQL Re-Compilations/sec" => ("PERFMON_RECOMPILATIONS_SEC", "perfmon"),
                    _ => (null, null)
                };

                if (factKey == null) continue;

                // All remaining counters are per-second rates — use the delta.
                var value = (double)deltaValue;

                facts.Add(new Fact
                {
                    Source = source!,
                    Key = factKey,
                    Value = value,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["cntr_value"] = cntrValue,
                        ["delta_cntr_value"] = deltaValue
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectPerfmonFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects top memory clerks by size. Context for understanding where memory is allocated.
    /// Dashboard stores pages_kb — convert to MB for consistency with Lite facts.
    /// </summary>
    private async Task CollectMemoryClerkFactsAsync(AnalysisContext context, List<Fact> facts)
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
        clerk_type,
        SUM(pages_kb) / 1024.0 AS memory_mb,
        ROW_NUMBER() OVER (PARTITION BY clerk_type ORDER BY collection_time DESC) AS rn,
        collection_time
    FROM collect.memory_clerks_stats
    WHERE collection_time <= @endTime
    GROUP BY clerk_type, collection_time
)
SELECT TOP 10 clerk_type, memory_mb
FROM latest WHERE rn = 1 AND memory_mb > 0
ORDER BY memory_mb DESC";

            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            var metadata = new Dictionary<string, double>();
            var totalMb = 0.0;
            var clerkCount = 0;

            while (await reader.ReadAsync())
            {
                var clerkType = reader.GetString(0);
                var memoryMb = Convert.ToDouble(reader.GetValue(1));
                metadata[clerkType] = memoryMb;
                totalMb += memoryMb;
                clerkCount++;
            }

            if (clerkCount == 0) return;

            metadata["total_top_clerks_mb"] = totalMb;
            metadata["clerk_count"] = clerkCount;

            facts.Add(new Fact
            {
                Source = "memory",
                Key = "MEMORY_CLERKS",
                Value = totalMb,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectMemoryClerkFactsAsync failed", ex);
        }
    }
}
