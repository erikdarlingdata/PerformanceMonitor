using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Collects facts from SQL Server for the Dashboard analysis engine.
/// Each fact category has its own collection method, added incrementally.
/// Port of DuckDbFactCollector from Lite — queries collect.* tables instead of DuckDB views.
/// </summary>
public class SqlServerFactCollector : IFactCollector
{
    private readonly string _connectionString;

    public SqlServerFactCollector(string connectionString)
    {
        _connectionString = connectionString;
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
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    wait_type,
    SUM(waiting_tasks_count_delta) AS total_waiting_tasks,
    SUM(wait_time_ms_delta) AS total_wait_time_ms,
    SUM(signal_wait_time_ms_delta) AS total_signal_wait_time_ms
FROM collect.wait_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   wait_time_ms_delta > 0
GROUP BY wait_type
ORDER BY SUM(wait_time_ms_delta) DESC";

            command.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            command.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var waitType = reader.GetString(0);
                var waitingTasks = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
                var waitTimeMs = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
                var signalWaitTimeMs = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectWaitStatsFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects blocking facts from blocking_BlockedProcessReport.
    /// Produces a single BLOCKING_EVENTS fact with event count, rate, and details.
    /// Value is events per hour for threshold comparison.
    /// </summary>
    private async Task CollectBlockingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    COUNT(*) AS event_count,
    AVG(CAST(wait_time_ms AS FLOAT)) AS avg_wait_time_ms,
    MAX(wait_time_ms) AS max_wait_time_ms,
    COUNT(DISTINCT spid) AS distinct_head_blockers,
    COUNT(CASE WHEN status = 'sleeping' THEN 1 END) AS sleeping_blocker_count
FROM collect.blocking_BlockedProcessReport
WHERE collection_time >= @startTime
AND   collection_time <= @endTime";

            command.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            command.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var eventCount = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            if (eventCount <= 0) return;

            var avgWaitTimeMs = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var maxWaitTimeMs = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var distinctHeadBlockers = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var sleepingBlockerCount = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectBlockingFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects deadlock facts from the deadlocks table.
    /// Produces a single DEADLOCKS fact with count and rate.
    /// Value is deadlocks per hour for threshold comparison.
    /// </summary>
    private async Task CollectDeadlockFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT COUNT(*) AS deadlock_count
FROM collect.deadlocks
WHERE collection_time >= @startTime
AND   collection_time <= @endTime";

            command.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            command.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var deadlockCount = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectDeadlockFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects server configuration settings relevant to analysis.
    /// These become facts that amplifiers and the config audit tool can reference
    /// to make recommendations specific (e.g., "your CTFP is 50" vs "check CTFP").
    /// </summary>
    private async Task CollectServerConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 4
    configuration_name,
    CAST(value_in_use AS BIGINT) AS value_in_use
FROM config.server_configuration_history
WHERE configuration_name IN (
    'cost threshold for parallelism',
    'max degree of parallelism',
    'max server memory (MB)',
    'max worker threads'
)
ORDER BY collection_time DESC";

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectServerConfigFactsAsync failed", ex);
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
    /// Collects total database data size from file_io_stats.
    /// Sums the latest size_on_disk_bytes across all database files for the server.
    /// </summary>
    private async Task CollectDatabaseSizeFactAsync(AnalysisContext context, List<Fact> facts)
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
        database_name,
        file_name,
        size_on_disk_bytes,
        ROW_NUMBER() OVER (PARTITION BY database_name, file_name ORDER BY collection_time DESC) AS rn
    FROM collect.file_io_stats
    WHERE collection_time <= @endTime
    AND   size_on_disk_bytes > 0
)
SELECT SUM(size_on_disk_bytes / 1048576.0) AS total_size_mb
FROM latest
WHERE rn = 1";

            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalSize = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            if (totalSize > 0)
                facts.Add(new Fact { Source = "config", Key = "DATABASE_TOTAL_SIZE_MB", Value = totalSize, ServerId = context.ServerId });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectDatabaseSizeFactAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects SQL Server edition and major version from the server_properties table.
    /// </summary>
    private async Task CollectServerMetadataFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1
    engine_edition,
    CAST(LEFT(product_version, CHARINDEX('.', product_version) - 1) AS INT) AS major_version
FROM collect.server_properties
ORDER BY collection_time DESC";

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var edition = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var majorVersion = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));

            if (edition > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_EDITION", Value = edition, ServerId = context.ServerId });
            if (majorVersion > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_MAJOR_VERSION", Value = majorVersion, ServerId = context.ServerId });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectServerMetadataFactsAsync failed", ex);
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
    /// Collects I/O latency from file_io_stats delta columns.
    /// Computes average read and write latency across all database files.
    /// </summary>
    private async Task CollectIoLatencyFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    SUM(io_stall_read_ms_delta) AS total_stall_read_ms,
    SUM(num_of_reads_delta) AS total_reads,
    SUM(io_stall_write_ms_delta) AS total_stall_write_ms,
    SUM(num_of_writes_delta) AS total_writes
FROM collect.file_io_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   (num_of_reads_delta > 0 OR num_of_writes_delta > 0)";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalStallReadMs = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            var totalReads = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var totalStallWriteMs = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var totalWrites = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectIoLatencyFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects TempDB usage facts: max usage, version store size, and unallocated space.
    /// Value is max total_reserved_mb over the period.
    /// Dashboard uses computed columns (total_reserved_mb, etc.) from collect.tempdb_stats.
    /// </summary>
    private async Task CollectTempDbFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    MAX(total_reserved_mb) AS max_total_reserved_mb,
    MAX(user_object_reserved_mb) AS max_user_object_mb,
    MAX(internal_object_reserved_mb) AS max_internal_object_mb,
    MAX(version_store_reserved_mb) AS max_version_store_mb,
    MIN(unallocated_mb) AS min_unallocated_mb,
    AVG(CAST(total_reserved_mb AS FLOAT)) AS avg_total_reserved_mb
FROM collect.tempdb_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectTempDbFactsAsync failed", ex);
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
    /// Collects query-level aggregate facts from query_stats.
    /// Focuses on spills (memory grant misestimates) and high-parallelism queries.
    /// </summary>
    private async Task CollectQueryStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    SUM(total_spills) AS total_spills,
    COUNT(CASE WHEN max_dop > 8 THEN 1 END) AS high_dop_queries,
    COUNT(CASE WHEN total_spills > 0 THEN 1 END) AS spilling_queries,
    SUM(execution_count_delta) AS total_executions,
    SUM(total_worker_time_delta) AS total_cpu_time_us
FROM collect.query_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   execution_count_delta > 0";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalSpills = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            var highDopQueries = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var spillingQueries = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var totalExecutions = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var totalCpuTimeUs = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectQueryStatsFactsAsync failed", ex);
        }
    }

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
    /// Collects key perfmon counters: Page Life Expectancy, Batch Requests/sec, compilations.
    /// PLE is scored; others are throughput context for the AI.
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
    AND   counter_name IN ('Page life expectancy', 'Batch Requests/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec')
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

    /// <summary>
    /// Collects database configuration facts: RCSI status, auto_shrink, auto_close,
    /// recovery model. Aggregates counts across databases.
    /// Dashboard stores config as individual setting rows in config.database_configuration_history.
    /// We pivot from the per-setting rows into aggregated counts.
    /// </summary>
    private async Task CollectDatabaseConfigFactsAsync(AnalysisContext context, List<Fact> facts)
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
        database_name,
        setting_name,
        setting_value,
        ROW_NUMBER() OVER (PARTITION BY database_name, setting_name ORDER BY collection_time DESC) AS rn
    FROM config.database_configuration_history
    WHERE setting_type = 'database_option'
    AND   database_name NOT IN ('master', 'msdb', 'model', 'tempdb')
),
pivoted AS (
    SELECT
        database_name,
        MAX(CASE WHEN setting_name = 'recovery_model_desc' THEN CAST(setting_value AS NVARCHAR(128)) END) AS recovery_model,
        MAX(CASE WHEN setting_name = 'is_auto_shrink_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_shrink_on,
        MAX(CASE WHEN setting_name = 'is_auto_close_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_close_on,
        MAX(CASE WHEN setting_name = 'is_read_committed_snapshot_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_read_committed_snapshot_on,
        MAX(CASE WHEN setting_name = 'is_auto_create_stats_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_create_stats_on,
        MAX(CASE WHEN setting_name = 'is_auto_update_stats_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_update_stats_on,
        MAX(CASE WHEN setting_name = 'page_verify_option_desc' THEN CAST(setting_value AS NVARCHAR(128)) END) AS page_verify_option,
        MAX(CASE WHEN setting_name = 'is_query_store_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_query_store_on
    FROM latest
    WHERE rn = 1
    GROUP BY database_name
)
SELECT
    COUNT(*) AS database_count,
    COUNT(CASE WHEN is_auto_shrink_on = '1' OR is_auto_shrink_on = 'True' THEN 1 END) AS auto_shrink_count,
    COUNT(CASE WHEN is_auto_close_on = '1' OR is_auto_close_on = 'True' THEN 1 END) AS auto_close_count,
    COUNT(CASE WHEN is_read_committed_snapshot_on = '0' OR is_read_committed_snapshot_on = 'False' THEN 1 END) AS rcsi_off_count,
    COUNT(CASE WHEN is_auto_create_stats_on = '0' OR is_auto_create_stats_on = 'False' THEN 1 END) AS auto_create_stats_off_count,
    COUNT(CASE WHEN is_auto_update_stats_on = '0' OR is_auto_update_stats_on = 'False' THEN 1 END) AS auto_update_stats_off_count,
    COUNT(CASE WHEN page_verify_option IS NOT NULL AND page_verify_option != 'CHECKSUM' THEN 1 END) AS page_verify_not_checksum_count,
    COUNT(CASE WHEN recovery_model = 'FULL' THEN 1 END) AS full_recovery_count,
    COUNT(CASE WHEN recovery_model = 'SIMPLE' THEN 1 END) AS simple_recovery_count,
    COUNT(CASE WHEN is_query_store_on = '1' OR is_query_store_on = 'True' THEN 1 END) AS query_store_on_count
FROM pivoted";

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var dbCount = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            if (dbCount == 0) return;

            var autoShrink = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var autoClose = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var rcsiOff = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var autoCreateOff = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
            var autoUpdateOff = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));
            var pageVerifyBad = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6));
            var fullRecovery = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7));
            var simpleRecovery = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8));
            var queryStoreOn = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9));

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectDatabaseConfigFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects procedure stats: top procedure by delta CPU time in the period.
    /// </summary>
    private async Task CollectProcedureStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    COUNT(DISTINCT object_name) AS distinct_procs,
    SUM(execution_count_delta) AS total_executions,
    SUM(total_worker_time_delta) AS total_cpu_time_us,
    SUM(total_elapsed_time_delta) AS total_elapsed_time_us,
    SUM(total_logical_reads_delta) AS total_logical_reads
FROM collect.procedure_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   execution_count_delta > 0";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var distinctProcs = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            var totalExecs = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var totalCpuUs = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var totalElapsedUs = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var totalReads = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectProcedureStatsFactsAsync failed", ex);
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

    /// <summary>
    /// Collects active global trace flags. Context for the AI to factor into recommendations.
    /// </summary>
    private async Task CollectTraceFlagFactsAsync(AnalysisContext context, List<Fact> facts)
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
        trace_flag,
        status,
        ROW_NUMBER() OVER (PARTITION BY trace_flag ORDER BY collection_time DESC) AS rn
    FROM config.trace_flags_history
    WHERE is_global = 1
)
SELECT trace_flag
FROM latest WHERE rn = 1 AND status = 1
ORDER BY trace_flag";

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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectTraceFlagFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects server hardware properties: CPU count, cores, sockets, memory.
    /// Critical context for MAXDOP and memory recommendations.
    /// </summary>
    private async Task CollectServerPropertiesFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1
    cpu_count,
    hyperthread_ratio,
    physical_memory_mb,
    socket_count,
    cores_per_socket,
    is_hadr_enabled,
    edition,
    product_version
FROM collect.server_properties
ORDER BY collection_time DESC";

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var cpuCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var htRatio = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
            var physicalMemMb = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectServerPropertiesFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects disk space facts from database_size_stats: volume free space, file sizes.
    /// </summary>
    private async Task CollectDiskSpaceFactsAsync(AnalysisContext context, List<Fact> facts)
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
        volume_mount_point,
        volume_total_mb,
        volume_free_mb,
        ROW_NUMBER() OVER (PARTITION BY volume_mount_point ORDER BY collection_time DESC) AS rn
    FROM collect.database_size_stats
    WHERE collection_time <= @endTime
    AND   volume_total_mb > 0
)
SELECT
    MIN(volume_free_mb * 1.0 / volume_total_mb) AS min_free_pct,
    MIN(volume_free_mb) AS min_free_mb,
    COUNT(DISTINCT volume_mount_point) AS volume_count,
    SUM(volume_total_mb) AS total_volume_mb,
    SUM(volume_free_mb) AS total_free_mb
FROM latest WHERE rn = 1";

            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var minFreePct = reader.IsDBNull(0) ? 1.0 : Convert.ToDouble(reader.GetValue(0));
            var minFreeMb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var volumeCount = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
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
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectDiskSpaceFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Groups general lock waits (X, U, IX, SIX, BU, IU, UIX, etc.) into a single "LCK" fact.
    /// Keeps individual facts for:
    ///   - LCK_M_S, LCK_M_IS (reader/writer blocking -- RCSI signal)
    ///   - LCK_M_RS_*, LCK_M_RIn_*, LCK_M_RX_* (serializable/repeatable read signal)
    ///   - SCH_M, SCH_S (schema locks -- DDL/index operations)
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

        // Everything else (X, U, IX, SIX, BU, IU, UIX, etc.) -> group
        return true;
    }
}
