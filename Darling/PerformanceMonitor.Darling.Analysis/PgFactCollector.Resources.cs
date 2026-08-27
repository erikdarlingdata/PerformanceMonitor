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
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    public const string MemoryStatsSql = @"
SELECT total_physical_memory_mb, buffer_pool_mb, target_server_memory_mb
FROM memory_stats
WHERE server_id = $1
AND   collection_time <= $2
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// Collects memory stats: total physical RAM, buffer pool size, target memory.
    /// These facts enable edition-aware memory recommendations in the config audit.
    /// </summary>
    private async Task CollectMemoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(MemoryStatsSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

    public const string RunnableTaskStatsSql = @"
SELECT total_runnable_tasks_count, runnable_tasks_warning
FROM cpu_scheduler_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// Collects the runnable-task-queue pressure signal from the EXISTING cpu_scheduler_stats snapshot
    /// (its total_runnable_tasks_count = SUM(runnable_tasks_count) and its runnable_tasks_warning =
    /// SUM(runnable_tasks_count) >= cpu_count heuristic) as one RUNNABLE_TASKS context fact (Source
    /// "cpu" — carried into the FactScorer amplifier lookup but never scored/rooted, like every
    /// Source="cpu" key besides CPU_SQL_PERCENT/CPU_SPIKE). The THREADPOOL runnable-queue amplifier
    /// reads the warning flag to confirm real scheduler CPU pressure behind a thread exhaustion. No new
    /// collector — reuses the data cpu_scheduler_stats already stores (not collected on Azure SQL DB,
    /// where the fact is simply absent and the amplifier no-ops). The read is window-bounded to
    /// [TimeRangeStart, TimeRangeEnd] (a lower bound, not just <= end, matching CpuUtilizationSql) so a
    /// lapsed collection surfaces no stale snapshot from outside the window — the fact is then absent
    /// and the amplifier no-ops. Ported byte-identically from Lite's
    /// DuckDbFactCollector.CollectRunnableTaskFactsAsync.
    /// </summary>
    private async Task CollectRunnableTaskFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(RunnableTaskStatsSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalRunnable = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var runnableWarning = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));

            facts.Add(new Fact
            {
                Source = "cpu",
                Key = "RUNNABLE_TASKS",
                Value = totalRunnable,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["total_runnable_tasks"] = totalRunnable,
                    ["runnable_tasks_warning"] = runnableWarning
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

    public const string MemoryGrantSql = @"
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

    /// <summary>
    /// Collects memory grant facts from the resource semaphore view.
    /// Detects grant waiters (sessions waiting for memory) and grant pressure.
    /// </summary>
    private async Task CollectMemoryGrantFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(MemoryGrantSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

    public const string MemoryClerkSql = @"
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

    /// <summary>
    /// Collects top memory clerks by size. Context for understanding where memory is allocated.
    /// </summary>
    private async Task CollectMemoryClerkFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(MemoryClerkSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            var metadata = new Dictionary<string, double>();
            var totalMb = 0.0;
            var clerkCount = 0;

            while (await reader.ReadAsync(context.CancellationToken))
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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

    public const string CpuUtilizationSql = @"
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

    /// <summary>
    /// Collects CPU utilization: average and max SQL Server CPU % over the period.
    /// Value is average SQL CPU %. Corroborates SOS_SCHEDULER_YIELD.
    /// </summary>
    private async Task CollectCpuUtilizationFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(CpuUtilizationSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

    public const string PerfmonSql = @"
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

    /// <summary>
    /// Collects key perfmon throughput counters: Batch Requests/sec, compilations, recompilations.
    /// Unscored context that distinguishes a busy server from a sick one (used by the AI surfaces).
    /// </summary>
    private async Task CollectPerfmonFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PerfmonSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

    public const string PlanCacheStatsSql = @"
WITH ranked AS (
    SELECT total_plans, single_use_plans, total_size_mb, single_use_size_mb,
           DENSE_RANK() OVER (ORDER BY collection_time DESC) AS rnk
    FROM plan_cache_stats
    WHERE server_id = $1
    AND   collection_time <= $2
)
SELECT
    COALESCE(SUM(total_plans), 0) AS total_plans,
    COALESCE(SUM(single_use_plans), 0) AS single_use_plans,
    COALESCE(SUM(total_size_mb), 0) AS total_size_mb,
    COALESCE(SUM(single_use_size_mb), 0) AS single_use_size_mb
FROM ranked
WHERE rnk = 1";

    /// <summary>
    /// Collects the plan-cache single-use bloat signal from the LATEST plan_cache_stats snapshot. Mirrors
    /// the Dashboard's report.plan_cache_bloat (install/47_create_reporting_views.sql:1456-1496): SUM the
    /// plan/size counts over the newest collection_time, derive single_use_percent. Point-in-time read
    /// (no lower bound, just &lt;= TimeRangeEnd) like CollectMemoryFactsAsync; DENSE_RANK() selects every
    /// row of the newest collection (QUALIFY is DuckDB-only and banned in the shared PG dialect). The
    /// FactScorer applies the &gt; 50/30/20% tiers AND the single_use_size_mb &gt;= 100 noise guard.
    /// Ported byte-identically from Lite's DuckDbFactCollector.CollectPlanCacheFactsAsync.
    /// </summary>
    private async Task CollectPlanCacheFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PlanCacheStatsSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            // SUM(integer) is bigint in Postgres — read every aggregate through ToInt64 (the Lite-parity
            // shape), then widen the MB sizes to double for the metadata.
            var totalPlans = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var singleUsePlans = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalSizeMb = reader.IsDBNull(2) ? 0.0 : (double)ToInt64(reader.GetValue(2));
            var singleUseSizeMb = reader.IsDBNull(3) ? 0.0 : (double)ToInt64(reader.GetValue(3));

            // No cache collected (or an empty cache) — nothing to classify.
            if (totalPlans <= 0) return;

            var singleUsePercent = singleUsePlans * 100.0 / totalPlans;

            facts.Add(new Fact
            {
                Source = "memory",
                Key = "PLAN_CACHE_BLOAT",
                Value = singleUsePercent,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["single_use_plans"] = singleUsePlans,
                    ["total_plans"] = totalPlans,
                    ["single_use_size_mb"] = singleUseSizeMb,
                    ["total_size_mb"] = totalSizeMb,
                    ["single_use_percent"] = singleUsePercent
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

    public const string MemoryPressureEventsSql = @"
SELECT
    SUM(CASE WHEN memory_indicators_process >= 2 OR memory_indicators_system >= 2 THEN 1 ELSE 0 END) AS pressure_event_count,
    MAX(memory_indicators_process) AS max_process,
    MAX(memory_indicators_system) AS max_system
FROM memory_pressure_events
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

    /// <summary>
    /// Collects ring-buffer physical-memory-pressure notifications from memory_pressure_events. Mirrors
    /// the Dashboard's report.memory_pressure_events (install/47_create_reporting_views.sql:220-238),
    /// whose severity keys off memory_indicators_process / memory_indicators_system (&gt;= 3 HIGH, &gt;= 2
    /// MEDIUM, else LOW). Window-bounded to [TimeRangeStart, TimeRangeEnd]. NOISE GATE: emit only when a
    /// genuine MEDIUM+ indicator occurred (max &gt;= 2) — the steady 0-1 samples on every healthy server
    /// stay silent (install/47:232-233). Value = the max overall indicator; event_count = the MEDIUM+
    /// sample count. Ported byte-identically from Lite's
    /// DuckDbFactCollector.CollectMemoryPressureEventFactsAsync.
    /// </summary>
    private async Task CollectMemoryPressureEventFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(MemoryPressureEventsSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var pressureEventCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var maxProcess = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var maxSystem = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxIndicator = Math.Max(maxProcess, maxSystem);

            // Steady ring buffer (indicators 0-1) is the healthy norm on every server — only a genuine
            // MEDIUM+ pressure indicator is worth a fact (install/47:232-233 MEDIUM floor).
            if (maxIndicator < 2) return;

            facts.Add(new Fact
            {
                Source = "memory",
                Key = "MEMORY_PRESSURE_EVENTS",
                Value = maxIndicator,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["event_count"] = pressureEventCount,
                    ["max_process_indicator"] = maxProcess,
                    ["max_system_indicator"] = maxSystem
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Table may not exist or have no data. An abandonment is NOT swallowed here (#2443). */
        }
    }

}
