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
    /// Collects memory stats: total physical RAM, buffer pool size, target memory.
    /// These facts enable edition-aware memory recommendations in the config audit.
    /// </summary>
    private async Task CollectMemoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT total_physical_memory_mb, buffer_pool_mb, target_server_memory_mb
FROM memory_stats
WHERE server_id = $1
AND   collection_time <= $2
ORDER BY collection_time DESC
LIMIT 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects memory grant facts from the resource semaphore view.
    /// Detects grant waiters (sessions waiting for memory) and grant pressure.
    /// </summary>
    private async Task CollectMemoryGrantFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    MAX(waiter_count) AS max_waiters,
    AVG(waiter_count) AS avg_waiters,
    MAX(grantee_count) AS max_grantees,
    SUM(timeout_error_count_delta) AS total_timeout_errors,
    SUM(forced_grant_count_delta) AS total_forced_grants
FROM v_memory_grant_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var maxWaiters = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var avgWaiters = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var maxGrantees = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalTimeouts = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalForcedGrants = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects top memory clerks by size. Context for understanding where memory is allocated.
    /// </summary>
    private async Task CollectMemoryClerkFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT clerk_type, memory_mb,
           ROW_NUMBER() OVER (PARTITION BY clerk_type ORDER BY collection_time DESC) AS rn
    FROM memory_clerks
    WHERE server_id = $1
    AND   collection_time <= $2
)
SELECT clerk_type, memory_mb
FROM latest WHERE rn = 1 AND memory_mb > 0
ORDER BY memory_mb DESC
LIMIT 10";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects CPU utilization: average and max SQL Server CPU % over the period.
    /// Value is average SQL CPU %. Corroborates SOS_SCHEDULER_YIELD.
    /// </summary>
    private async Task CollectCpuUtilizationFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    AVG(sqlserver_cpu_utilization) AS avg_sql_cpu,
    MAX(sqlserver_cpu_utilization) AS max_sql_cpu,
    AVG(other_process_cpu_utilization) AS avg_other_cpu,
    MAX(other_process_cpu_utilization) AS max_other_cpu,
    COUNT(*) AS sample_count
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgSqlCpu = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var maxSqlCpu = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var avgOtherCpu = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxOtherCpu = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var sampleCount = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects key perfmon throughput counters: Batch Requests/sec, compilations, recompilations.
    /// Unscored context that distinguishes a busy server from a sick one (used by the AI surfaces).
    /// </summary>
    private async Task CollectPerfmonFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT counter_name, cntr_value, delta_cntr_value,
           ROW_NUMBER() OVER (PARTITION BY counter_name ORDER BY collection_time DESC) AS rn
    FROM perfmon_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   counter_name IN ('Batch Requests/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec')
)
SELECT counter_name, cntr_value, delta_cntr_value
FROM latest WHERE rn = 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var counterName = reader.GetString(0);
                var cntrValue = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
                var deltaValue = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));

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
        catch { /* Table may not exist or have no data */ }
    }

}
