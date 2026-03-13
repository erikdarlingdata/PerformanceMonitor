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
SELECT sql_engine_edition, sql_major_version
FROM servers
WHERE server_id = $1";

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

            facts.Add(new Fact
            {
                Source = "cpu",
                Key = "CPU_SQL_PERCENT",
                Value = avgSqlCpu,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["avg_sql_cpu"] = avgSqlCpu,
                    ["max_sql_cpu"] = maxSqlCpu,
                    ["avg_other_cpu"] = avgOtherCpu,
                    ["max_other_cpu"] = maxOtherCpu,
                    ["avg_total_cpu"] = avgSqlCpu + avgOtherCpu,
                    ["sample_count"] = sampleCount
                }
            });
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
        if (!waitType.StartsWith("LCK_M_")) return false;

        // Keep individual: reader/writer locks
        if (waitType is "LCK_M_S" or "LCK_M_IS") return false;

        // Keep individual: range locks (serializable/repeatable read)
        if (waitType.StartsWith("LCK_M_RS_") ||
            waitType.StartsWith("LCK_M_RIn_") ||
            waitType.StartsWith("LCK_M_RX_")) return false;

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
