using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Collects facts from DuckDB for the Lite analysis engine.
/// Each fact category has its own collection method, added incrementally.
/// </summary>
public class DuckDbFactCollector : IFactCollector
{
    private readonly DuckDbInitializer _duckDb;

    public DuckDbFactCollector(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
    {
        var facts = new List<Fact>();

        await CollectWaitStatsFactsAsync(context, facts);
        GroupGeneralLockWaits(facts, context);
        GroupParallelismWaits(facts, context);
        await CollectBlockingFactsAsync(context, facts);
        await CollectDeadlockFactsAsync(context, facts);
        await CollectServerConfigFactsAsync(context, facts);
        await CollectMemoryFactsAsync(context, facts);
        await CollectDatabaseSizeFactAsync(context, facts);
        await CollectServerMetadataFactsAsync(context, facts);
        await CollectCpuUtilizationFactsAsync(context, facts);
        await CollectIoLatencyFactsAsync(context, facts);
        await CollectTempDbFactsAsync(context, facts);
        await CollectMemoryGrantFactsAsync(context, facts);
        await CollectQueryStatsFactsAsync(context, facts);
        await CollectBadActorFactsAsync(context, facts);
        await CollectPerfmonFactsAsync(context, facts);
        await CollectMemoryClerkFactsAsync(context, facts);
        await CollectDatabaseConfigFactsAsync(context, facts);
        await CollectProcedureStatsFactsAsync(context, facts);
        await CollectActiveQueryFactsAsync(context, facts);
        await CollectRunningJobFactsAsync(context, facts);
        await CollectSessionFactsAsync(context, facts);
        await CollectTraceFlagFactsAsync(context, facts);
        await CollectServerPropertiesFactsAsync(context, facts);
        await CollectDiskSpaceFactsAsync(context, facts);

        return facts;
    }

    /// <summary>
    /// Collects wait stats facts — one Fact per significant wait type.
    /// Value is wait_time_ms / period_duration_ms (fraction of examined period).
    /// </summary>
    private async Task CollectWaitStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    wait_type,
    SUM(delta_waiting_tasks) AS total_waiting_tasks,
    SUM(delta_wait_time_ms) AS total_wait_time_ms,
    SUM(delta_signal_wait_time_ms) AS total_signal_wait_time_ms
FROM v_wait_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_wait_time_ms > 0
GROUP BY wait_type
ORDER BY SUM(delta_wait_time_ms) DESC";

        command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var waitType = reader.GetString(0);
            var waitingTasks = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var waitTimeMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var signalWaitTimeMs = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));

            if (waitTimeMs <= 0) continue;

            var fractionOfPeriod = waitTimeMs / context.PeriodDurationMs;
            var avgMsPerWait = waitingTasks > 0 ? (double)waitTimeMs / waitingTasks : 0;

            facts.Add(new Fact
            {
                Source = "waits",
                Key = waitType,
                Value = fractionOfPeriod,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["wait_time_ms"] = waitTimeMs,
                    ["waiting_tasks_count"] = waitingTasks,
                    ["signal_wait_time_ms"] = signalWaitTimeMs,
                    ["resource_wait_time_ms"] = waitTimeMs - signalWaitTimeMs,
                    ["avg_ms_per_wait"] = avgMsPerWait,
                    ["period_duration_ms"] = context.PeriodDurationMs
                }
            });
        }
    }

    /// <summary>
    /// Collects blocking facts from blocked_process_reports.
    /// Produces a single BLOCKING_EVENTS fact with event count, rate, and details.
    /// Value is events per hour for threshold comparison.
    /// </summary>
    private async Task CollectBlockingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    COUNT(*) AS event_count,
    AVG(wait_time_ms) AS avg_wait_time_ms,
    MAX(wait_time_ms) AS max_wait_time_ms,
    COUNT(DISTINCT blocking_spid) AS distinct_head_blockers,
    COUNT(CASE WHEN blocking_status = 'sleeping' THEN 1 END) AS sleeping_blocker_count
FROM blocked_process_reports
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

        command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return;

        var eventCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        if (eventCount <= 0) return;

        var avgWaitTimeMs = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
        var maxWaitTimeMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
        var distinctHeadBlockers = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
        var sleepingBlockerCount = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

        var periodHours = context.PeriodDurationMs / 3_600_000.0;
        var eventsPerHour = periodHours > 0 ? eventCount / periodHours : 0;

        facts.Add(new Fact
        {
            Source = "blocking",
            Key = "BLOCKING_EVENTS",
            Value = eventsPerHour,
            ServerId = context.ServerId,
            Metadata = new Dictionary<string, double>
            {
                ["event_count"] = eventCount,
                ["events_per_hour"] = eventsPerHour,
                ["avg_wait_time_ms"] = avgWaitTimeMs,
                ["max_wait_time_ms"] = maxWaitTimeMs,
                ["distinct_head_blockers"] = distinctHeadBlockers,
                ["sleeping_blocker_count"] = sleepingBlockerCount,
                ["period_hours"] = periodHours
            }
        });
    }

    /// <summary>
    /// Collects deadlock facts from the deadlocks table.
    /// Produces a single DEADLOCKS fact with count and rate.
    /// Value is deadlocks per hour for threshold comparison.
    /// </summary>
    private async Task CollectDeadlockFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT COUNT(*) AS deadlock_count
FROM deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

        command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return;

        var deadlockCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        if (deadlockCount <= 0) return;

        var periodHours = context.PeriodDurationMs / 3_600_000.0;
        var deadlocksPerHour = periodHours > 0 ? deadlockCount / periodHours : 0;

        facts.Add(new Fact
        {
            Source = "blocking",
            Key = "DEADLOCKS",
            Value = deadlocksPerHour,
            ServerId = context.ServerId,
            Metadata = new Dictionary<string, double>
            {
                ["deadlock_count"] = deadlockCount,
                ["deadlocks_per_hour"] = deadlocksPerHour,
                ["period_hours"] = periodHours
            }
        });
    }

    /// <summary>
    /// Collects server configuration settings relevant to analysis.
    /// These become facts that amplifiers and the config audit tool can reference
    /// to make recommendations specific (e.g., "your CTFP is 50" vs "check CTFP").
    /// </summary>
    private async Task CollectServerConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT configuration_name, value_in_use
FROM server_config
WHERE server_id = $1
AND   configuration_name IN (
    'cost threshold for parallelism',
    'max degree of parallelism',
    'max server memory (MB)',
    'max worker threads'
)
ORDER BY capture_time DESC
LIMIT 4";

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var configName = reader.GetString(0);
            var value = Convert.ToDouble(reader.GetValue(1));

            var factKey = configName switch
            {
                "cost threshold for parallelism" => "CONFIG_CTFP",
                "max degree of parallelism" => "CONFIG_MAXDOP",
                "max server memory (MB)" => "CONFIG_MAX_MEMORY_MB",
                "max worker threads" => "CONFIG_MAX_WORKER_THREADS",
                _ => null
            };

            if (factKey == null) continue;

            facts.Add(new Fact
            {
                Source = "config",
                Key = factKey,
                Value = value,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["value_in_use"] = value
                }
            });
        }
    }

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
    /// Collects total database data size from file_io_stats.
    /// Sums the latest size_mb across all database files for the server.
    /// </summary>
    private async Task CollectDatabaseSizeFactAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT database_name, file_name, size_mb,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_name ORDER BY collection_time DESC) AS rn
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time <= $2
    AND   size_mb > 0
)
SELECT SUM(size_mb) AS total_size_mb
FROM latest
WHERE rn = 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalSize = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            if (totalSize > 0)
                facts.Add(new Fact { Source = "config", Key = "DATABASE_TOTAL_SIZE_MB", Value = totalSize, ServerId = context.ServerId });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects SQL Server edition and major version from the servers table.
    /// These are persisted by RemoteCollectorService after connection check.
    /// </summary>
    private async Task CollectServerMetadataFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT engine_edition,
       CAST(SPLIT_PART(product_version, '.', 1) AS INTEGER) AS major_version
FROM server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var edition = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var majorVersion = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));

            if (edition > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_EDITION", Value = edition, ServerId = context.ServerId });
            if (majorVersion > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_MAJOR_VERSION", Value = majorVersion, ServerId = context.ServerId });
        }
        catch { /* Columns may not exist yet (pre-migration) */ }
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
    /// Collects I/O latency from file_io_stats delta columns.
    /// Computes average read and write latency across all database files.
    /// </summary>
    private async Task CollectIoLatencyFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    SUM(delta_stall_read_ms) AS total_stall_read_ms,
    SUM(delta_reads) AS total_reads,
    SUM(delta_stall_write_ms) AS total_stall_write_ms,
    SUM(delta_writes) AS total_writes
FROM v_file_io_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   (delta_reads > 0 OR delta_writes > 0)";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalStallReadMs = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var totalReads = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalStallWriteMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalWrites = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));

            if (totalReads > 0)
            {
                var avgReadLatency = (double)totalStallReadMs / totalReads;
                facts.Add(new Fact
                {
                    Source = "io",
                    Key = "IO_READ_LATENCY_MS",
                    Value = avgReadLatency,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["avg_read_latency_ms"] = avgReadLatency,
                        ["total_stall_read_ms"] = totalStallReadMs,
                        ["total_reads"] = totalReads
                    }
                });
            }

            if (totalWrites > 0)
            {
                var avgWriteLatency = (double)totalStallWriteMs / totalWrites;
                facts.Add(new Fact
                {
                    Source = "io",
                    Key = "IO_WRITE_LATENCY_MS",
                    Value = avgWriteLatency,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["avg_write_latency_ms"] = avgWriteLatency,
                        ["total_stall_write_ms"] = totalStallWriteMs,
                        ["total_writes"] = totalWrites
                    }
                });
            }
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects TempDB usage facts: max usage, version store size, and unallocated space.
    /// Value is max total_reserved_mb over the period.
    /// </summary>
    private async Task CollectTempDbFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    MAX(total_reserved_mb) AS max_total_reserved_mb,
    MAX(user_object_reserved_mb) AS max_user_object_mb,
    MAX(internal_object_reserved_mb) AS max_internal_object_mb,
    MAX(version_store_reserved_mb) AS max_version_store_mb,
    MIN(unallocated_mb) AS min_unallocated_mb,
    AVG(total_reserved_mb) AS avg_total_reserved_mb
FROM v_tempdb_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var maxReserved = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var maxUserObj = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var maxInternalObj = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxVersionStore = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var minUnallocated = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));
            var avgReserved = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));

            if (maxReserved <= 0) return;

            // TempDB usage as fraction of total space (reserved + unallocated)
            var totalSpace = maxReserved + minUnallocated;
            var usageFraction = totalSpace > 0 ? maxReserved / totalSpace : 0;

            facts.Add(new Fact
            {
                Source = "tempdb",
                Key = "TEMPDB_USAGE",
                Value = usageFraction,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["max_reserved_mb"] = maxReserved,
                    ["avg_reserved_mb"] = avgReserved,
                    ["max_user_object_mb"] = maxUserObj,
                    ["max_internal_object_mb"] = maxInternalObj,
                    ["max_version_store_mb"] = maxVersionStore,
                    ["min_unallocated_mb"] = minUnallocated,
                    ["usage_fraction"] = usageFraction
                }
            });
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
    /// Collects query-level aggregate facts from query_stats.
    /// Focuses on spills (memory grant misestimates) and high-parallelism queries.
    /// </summary>
    private async Task CollectQueryStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    SUM(delta_spills) AS total_spills,
    COUNT(CASE WHEN max_dop > 8 THEN 1 END) AS high_dop_queries,
    COUNT(CASE WHEN delta_spills > 0 THEN 1 END) AS spilling_queries,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_time_us
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalSpills = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var highDopQueries = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var spillingQueries = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalExecutions = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalCpuTimeUs = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

            if (totalSpills > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "QUERY_SPILLS",
                    Value = totalSpills,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["total_spills"] = totalSpills,
                        ["spilling_query_count"] = spillingQueries,
                        ["total_executions"] = totalExecutions
                    }
                });
            }

            if (highDopQueries > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "QUERY_HIGH_DOP",
                    Value = highDopQueries,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["high_dop_query_count"] = highDopQueries,
                        ["total_cpu_time_us"] = totalCpuTimeUs,
                        ["total_executions"] = totalExecutions
                    }
                });
            }
        }
        catch { /* Table may not exist or have no data */ }
    }

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
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    database_name,
    query_hash,
    SUM(delta_execution_count)::BIGINT AS exec_count,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_worker_time)::DOUBLE / SUM(delta_execution_count) / 1000.0
         ELSE 0 END AS avg_cpu_ms,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_elapsed_time)::DOUBLE / SUM(delta_execution_count) / 1000.0
         ELSE 0 END AS avg_elapsed_ms,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_logical_reads)::DOUBLE / SUM(delta_execution_count)
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
ORDER BY SUM(delta_worker_time)::DOUBLE / GREATEST(SUM(delta_execution_count), 1) *
         LN(GREATEST(SUM(delta_execution_count), 1)) DESC
LIMIT 5";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects key perfmon counters: Page Life Expectancy, Batch Requests/sec, compilations.
    /// PLE is scored; others are throughput context for the AI.
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
    AND   counter_name IN ('Page life expectancy', 'Batch Requests/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec')
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
                    "Page life expectancy" => ("PERFMON_PLE", "perfmon"),
                    "Batch Requests/sec" => ("PERFMON_BATCH_REQ_SEC", "perfmon"),
                    "SQL Compilations/sec" => ("PERFMON_COMPILATIONS_SEC", "perfmon"),
                    "SQL Re-Compilations/sec" => ("PERFMON_RECOMPILATIONS_SEC", "perfmon"),
                    _ => (null, null)
                };

                if (factKey == null) continue;

                // For PLE, use the absolute value. For rate counters, use delta.
                var value = counterName == "Page life expectancy" ? (double)cntrValue : (double)deltaValue;

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
    /// Collects database configuration facts: RCSI status, auto_shrink, auto_close,
    /// recovery model. Aggregates counts across databases.
    /// </summary>
    private async Task CollectDatabaseConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT database_name, recovery_model, is_auto_shrink_on, is_auto_close_on,
           is_read_committed_snapshot_on, is_auto_create_stats_on, is_auto_update_stats_on,
           is_query_store_on, compatibility_level, page_verify_option,
           is_accelerated_database_recovery_on,
           ROW_NUMBER() OVER (PARTITION BY database_name ORDER BY capture_time DESC) AS rn
    FROM database_config
    WHERE server_id = $1
)
SELECT
    COUNT(*) AS database_count,
    COUNT(CASE WHEN is_auto_shrink_on THEN 1 END) AS auto_shrink_count,
    COUNT(CASE WHEN is_auto_close_on THEN 1 END) AS auto_close_count,
    COUNT(CASE WHEN NOT is_read_committed_snapshot_on THEN 1 END) AS rcsi_off_count,
    COUNT(CASE WHEN NOT is_auto_create_stats_on THEN 1 END) AS auto_create_stats_off_count,
    COUNT(CASE WHEN NOT is_auto_update_stats_on THEN 1 END) AS auto_update_stats_off_count,
    COUNT(CASE WHEN page_verify_option != 'CHECKSUM' THEN 1 END) AS page_verify_not_checksum_count,
    COUNT(CASE WHEN recovery_model = 'FULL' THEN 1 END) AS full_recovery_count,
    COUNT(CASE WHEN recovery_model = 'SIMPLE' THEN 1 END) AS simple_recovery_count,
    COUNT(CASE WHEN is_query_store_on THEN 1 END) AS query_store_on_count
FROM latest WHERE rn = 1
AND database_name NOT IN ('master', 'msdb', 'model', 'tempdb')";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var dbCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (dbCount == 0) return;

            var autoShrink = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var autoClose = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var rcsiOff = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var autoCreateOff = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var autoUpdateOff = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var pageVerifyBad = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
            var fullRecovery = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
            var simpleRecovery = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
            var queryStoreOn = reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9));

            facts.Add(new Fact
            {
                Source = "database_config",
                Key = "DB_CONFIG",
                Value = dbCount,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["database_count"] = dbCount,
                    ["auto_shrink_on_count"] = autoShrink,
                    ["auto_close_on_count"] = autoClose,
                    ["rcsi_off_count"] = rcsiOff,
                    ["auto_create_stats_off_count"] = autoCreateOff,
                    ["auto_update_stats_off_count"] = autoUpdateOff,
                    ["page_verify_not_checksum_count"] = pageVerifyBad,
                    ["full_recovery_count"] = fullRecovery,
                    ["simple_recovery_count"] = simpleRecovery,
                    ["query_store_on_count"] = queryStoreOn
                }
            });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects procedure stats: top procedure by delta CPU time in the period.
    /// </summary>
    private async Task CollectProcedureStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    COUNT(DISTINCT object_name) AS distinct_procs,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_time_us,
    SUM(delta_elapsed_time) AS total_elapsed_time_us,
    SUM(delta_logical_reads) AS total_logical_reads
FROM procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var distinctProcs = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var totalExecs = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalCpuUs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalElapsedUs = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalReads = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

            if (totalExecs == 0) return;

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "PROCEDURE_STATS",
                Value = totalCpuUs,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["distinct_procedures"] = distinctProcs,
                    ["total_executions"] = totalExecs,
                    ["total_cpu_time_us"] = totalCpuUs,
                    ["total_elapsed_time_us"] = totalElapsedUs,
                    ["total_logical_reads"] = totalReads,
                    ["avg_cpu_per_exec_us"] = totalExecs > 0 ? (double)totalCpuUs / totalExecs : 0
                }
            });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects active query snapshot facts: long-running queries, blocked sessions, high DOP.
    /// </summary>
    private async Task CollectActiveQueryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

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
FROM query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects running job facts: jobs currently running long vs historical averages.
    /// </summary>
    private async Task CollectRunningJobFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    COUNT(*) AS running_count,
    COUNT(CASE WHEN is_running_long THEN 1 END) AS running_long_count,
    MAX(percent_of_average) AS max_percent_of_avg,
    MAX(current_duration_seconds) AS max_duration_seconds
FROM running_jobs
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects session stats: connection counts per application, total connections.
    /// </summary>
    private async Task CollectSessionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT program_name, connection_count, running_count, sleeping_count, dormant_count,
           ROW_NUMBER() OVER (PARTITION BY program_name ORDER BY collection_time DESC) AS rn
    FROM session_stats
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

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

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
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects active global trace flags. Context for the AI to factor into recommendations.
    /// </summary>
    private async Task CollectTraceFlagFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT trace_flag, status,
           ROW_NUMBER() OVER (PARTITION BY trace_flag ORDER BY capture_time DESC) AS rn
    FROM trace_flags
    WHERE server_id = $1
    AND   is_global = true
)
SELECT trace_flag
FROM latest WHERE rn = 1 AND status = true
ORDER BY trace_flag";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync();
            var metadata = new Dictionary<string, double>();
            var flagCount = 0;

            while (await reader.ReadAsync())
            {
                var flag = Convert.ToInt32(reader.GetValue(0));
                metadata[$"TF_{flag}"] = 1;
                flagCount++;
            }

            if (flagCount == 0) return;

            metadata["flag_count"] = flagCount;

            facts.Add(new Fact
            {
                Source = "config",
                Key = "TRACE_FLAGS",
                Value = flagCount,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects server hardware properties: CPU count, cores, sockets, memory.
    /// Critical context for MAXDOP and memory recommendations.
    /// </summary>
    private async Task CollectServerPropertiesFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT COALESCE(vcore_count, cpu_count) AS cpu_count, hyperthread_ratio, physical_memory_mb,
       socket_count, cores_per_socket, is_hadr_enabled, edition, product_version
FROM server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var cpuCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var htRatio = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
            var physicalMemMb = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var socketCount = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3));
            var coresPerSocket = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4));
            var hadrEnabled = !reader.IsDBNull(5) && Convert.ToBoolean(reader.GetValue(5));

            if (cpuCount == 0) return;

            facts.Add(new Fact
            {
                Source = "config",
                Key = "SERVER_HARDWARE",
                Value = cpuCount,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["cpu_count"] = cpuCount,
                    ["hyperthread_ratio"] = htRatio,
                    ["physical_memory_mb"] = physicalMemMb,
                    ["socket_count"] = socketCount,
                    ["cores_per_socket"] = coresPerSocket,
                    ["hadr_enabled"] = hadrEnabled ? 1 : 0
                }
            });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Collects disk space facts from database_size_stats: volume free space, file sizes.
    /// </summary>
    private async Task CollectDiskSpaceFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT volume_mount_point, volume_total_mb, volume_free_mb,
           ROW_NUMBER() OVER (PARTITION BY volume_mount_point ORDER BY collection_time DESC) AS rn
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time <= $2
    AND   volume_total_mb > 0
)
SELECT
    MIN(volume_free_mb * 1.0 / volume_total_mb) AS min_free_pct,
    MIN(volume_free_mb) AS min_free_mb,
    COUNT(DISTINCT volume_mount_point) AS volume_count,
    SUM(volume_total_mb) AS total_volume_mb,
    SUM(volume_free_mb) AS total_free_mb
FROM latest WHERE rn = 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var minFreePct = reader.IsDBNull(0) ? 1.0 : Convert.ToDouble(reader.GetValue(0));
            var minFreeMb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var volumeCount = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalVolumeMb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var totalFreeMb = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));

            if (volumeCount == 0) return;

            facts.Add(new Fact
            {
                Source = "disk",
                Key = "DISK_SPACE",
                Value = minFreePct,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["min_free_pct"] = minFreePct,
                    ["min_free_mb"] = minFreeMb,
                    ["volume_count"] = volumeCount,
                    ["total_volume_mb"] = totalVolumeMb,
                    ["total_free_mb"] = totalFreeMb
                }
            });
        }
        catch { /* Table may not exist or have no data */ }
    }

    /// <summary>
    /// Groups general lock waits (X, U, IX, SIX, BU, IU, UIX, etc.) into a single "LCK" fact.
    /// Keeps individual facts for:
    ///   - LCK_M_S, LCK_M_IS (reader/writer blocking — RCSI signal)
    ///   - LCK_M_RS_*, LCK_M_RIn_*, LCK_M_RX_* (serializable/repeatable read signal)
    ///   - SCH_M, SCH_S (schema locks — DDL/index operations)
    /// Individual constituent wait times are preserved in metadata as "{type}_ms" keys.
    /// </summary>
    private static void GroupGeneralLockWaits(List<Fact> facts, AnalysisContext context)
    {
        var generalLocks = facts.Where(f => f.Source == "waits" && IsGeneralLockWait(f.Key)).ToList();
        if (generalLocks.Count == 0) return;

        var totalWaitTimeMs = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("wait_time_ms"));
        var totalWaitingTasks = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("waiting_tasks_count"));
        var totalSignalMs = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("signal_wait_time_ms"));
        var avgMsPerWait = totalWaitingTasks > 0 ? totalWaitTimeMs / totalWaitingTasks : 0;
        var fractionOfPeriod = totalWaitTimeMs / context.PeriodDurationMs;

        var metadata = new Dictionary<string, double>
        {
            ["wait_time_ms"] = totalWaitTimeMs,
            ["waiting_tasks_count"] = totalWaitingTasks,
            ["signal_wait_time_ms"] = totalSignalMs,
            ["resource_wait_time_ms"] = totalWaitTimeMs - totalSignalMs,
            ["avg_ms_per_wait"] = avgMsPerWait,
            ["period_duration_ms"] = context.PeriodDurationMs,
            ["lock_type_count"] = generalLocks.Count
        };

        // Preserve individual constituent wait times for detailed analysis
        foreach (var lck in generalLocks)
            metadata[$"{lck.Key}_ms"] = lck.Metadata.GetValueOrDefault("wait_time_ms");

        // Remove individual facts, add grouped fact
        foreach (var lck in generalLocks)
            facts.Remove(lck);

        facts.Add(new Fact
        {
            Source = "waits",
            Key = "LCK",
            Value = fractionOfPeriod,
            ServerId = context.ServerId,
            Metadata = metadata
        });
    }

    /// <summary>
    /// Groups all CX* parallelism waits (CXPACKET, CXCONSUMER, CXSYNC_PORT, CXSYNC_CONSUMER, etc.)
    /// into a single "CXPACKET" fact. They all indicate the same thing: parallel queries are running.
    /// Individual wait times are preserved in metadata for detailed analysis.
    /// </summary>
    private static void GroupParallelismWaits(List<Fact> facts, AnalysisContext context)
    {
        var cxWaits = facts.Where(f => f.Source == "waits" && f.Key.StartsWith("CX", StringComparison.Ordinal)).ToList();
        if (cxWaits.Count <= 1) return;

        var totalWaitTimeMs = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("wait_time_ms"));
        var totalWaitingTasks = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("waiting_tasks_count"));
        var totalSignalMs = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("signal_wait_time_ms"));
        var avgMsPerWait = totalWaitingTasks > 0 ? totalWaitTimeMs / totalWaitingTasks : 0;
        var fractionOfPeriod = totalWaitTimeMs / context.PeriodDurationMs;

        var metadata = new Dictionary<string, double>
        {
            ["wait_time_ms"] = totalWaitTimeMs,
            ["waiting_tasks_count"] = totalWaitingTasks,
            ["signal_wait_time_ms"] = totalSignalMs,
            ["resource_wait_time_ms"] = totalWaitTimeMs - totalSignalMs,
            ["avg_ms_per_wait"] = avgMsPerWait,
            ["period_duration_ms"] = context.PeriodDurationMs
        };

        // Preserve individual constituent wait times for detailed analysis
        foreach (var cx in cxWaits)
            metadata[$"{cx.Key}_ms"] = cx.Metadata.GetValueOrDefault("wait_time_ms");

        foreach (var cx in cxWaits)
            facts.Remove(cx);

        facts.Add(new Fact
        {
            Source = "waits",
            Key = "CXPACKET",
            Value = fractionOfPeriod,
            ServerId = cxWaits[0].ServerId,
            Metadata = metadata
        });
    }

    /// <summary>
    /// Returns true for general lock waits that should be grouped into "LCK".
    /// Excludes reader locks (S, IS), range locks (RS_*, RIn_*, RX_*), and schema locks.
    /// </summary>
    private static bool IsGeneralLockWait(string waitType)
    {
        if (!waitType.StartsWith("LCK_M_", StringComparison.OrdinalIgnoreCase)) return false;

        // Keep individual: reader/writer locks
        if (waitType is "LCK_M_S" or "LCK_M_IS") return false;

        // Keep individual: range locks (serializable/repeatable read)
        if (waitType.StartsWith("LCK_M_RS_", StringComparison.OrdinalIgnoreCase) ||
            waitType.StartsWith("LCK_M_RIn_", StringComparison.OrdinalIgnoreCase) ||
            waitType.StartsWith("LCK_M_RX_", StringComparison.OrdinalIgnoreCase)) return false;

        // Everything else (X, U, IX, SIX, BU, IU, UIX, etc.) → group
        return true;
    }

    private static long ToInt64(object value)
    {
        if (value is BigInteger bi)
            return (long)bi;
        return Convert.ToInt64(value);
    }
}
