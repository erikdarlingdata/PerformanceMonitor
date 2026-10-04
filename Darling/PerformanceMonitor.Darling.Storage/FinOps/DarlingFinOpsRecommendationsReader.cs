/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>
/// The server's latest collected edition facts for the license audit: edition, product major version, logical CPU count,
/// Availability Group replica role and the Always On master switch.
/// </summary>
public readonly record struct FinOpsEditionFacts(
    string Edition, int MajorVersion, int CpuCount, string AgReplicaRole, bool IsHadrEnabled);

/// <summary>
/// The SQL behind the FinOps recommendation checks. Each statement reads already-collected Postgres data; the
/// viewer and the service share these constants so both run the same text.
/// </summary>
public static class DarlingFinOpsRecommendationsReader
{
    /// <summary>
    /// The server's latest collected edition / product version / logical CPU count for the license audit, plus the
    /// collected AG replica role + Always On master switch that drive the AG-aware branches. $1 server_id.
    /// </summary>
    public const string EditionFactsSql = @"
SELECT
    edition,
    product_version,
    cpu_count,
    ag_replica_role,
    is_hadr_enabled
FROM server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>7-day P95 of Total Server Memory (MB) + sample count, for the memory right-sizing checks. $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string MemoryP95Sql = @"
SELECT
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_server_memory_mb) AS p95_mb,
    COUNT(*) AS sample_count,
    MIN(collection_time) AS first_sample,
    MAX(collection_time) AS last_sample,
    COUNT(total_server_memory_mb) AS window_samples
FROM v_memory_stats
WHERE server_id = $1
AND   collection_time >= $2";

    /// <summary>7-day P95 of SQL Server CPU utilization, for the VM right-sizing CPU prescription. $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string CpuP95Sql = @"
SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu,
       MIN(collection_time) AS first_sample, MAX(collection_time) AS last_sample,
       COUNT(sqlserver_cpu_utilization) AS window_samples
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2";

    /// <summary>SQL Agent jobs that ran long at least 3 times in the window (maintenance-window efficiency). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string MaintenanceWindowSql = @"
SELECT
    job_name,
    COUNT(*) AS run_count,
    AVG(current_duration_seconds) AS avg_duration_seconds,
    MAX(current_duration_seconds) AS max_duration_seconds,
    AVG(avg_duration_seconds) AS avg_historical,
    SUM(CASE WHEN is_running_long THEN 1 ELSE 0 END) AS times_ran_long
FROM v_running_jobs
WHERE server_id = $1
AND   collection_time >= $2
AND   avg_duration_seconds > 0
GROUP BY job_name
HAVING SUM(CASE WHEN is_running_long THEN 1 ELSE 0 END) >= 3
ORDER BY times_ran_long DESC
LIMIT 10";

    /// <summary>Per-database aggregate read/write I/O + stall over the window (storage-tier optimization). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string StorageTierSql = @"
SELECT
    database_name,
    SUM(delta_reads) AS total_reads,
    SUM(delta_stall_read_ms) AS total_stall_read_ms,
    SUM(delta_writes) AS total_writes,
    SUM(delta_stall_write_ms) AS total_stall_write_ms,
    MIN(collection_time) AS first_sample,
    MAX(collection_time) AS last_sample,
    COUNT(*) AS window_samples
FROM v_file_io_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   delta_reads > 0
GROUP BY database_name
HAVING SUM(delta_reads) > 1000";

    /// <summary>Oldest query-stats sample for the server (idle-database advice waits until it is at or before the 7-day cutoff). $1 server_id.</summary>
    public const string QueryStatsFirstSampleSql = @"
SELECT MIN(collection_time)
FROM v_query_stats
WHERE server_id = $1";

    /// <summary>CPU utilization mean + standard deviation + sample count (reserved-capacity stability). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string ReservedCapacitySql = @"
SELECT
    AVG(sqlserver_cpu_utilization) AS avg_cpu,
    STDDEV(sqlserver_cpu_utilization) AS stddev_cpu,
    COUNT(*) AS sample_count
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2
HAVING COUNT(*) >= 24";

    /// <summary>
    /// The server's latest collected <c>SERVERPROPERTY('EngineEdition')</c>, for the right-sizing rules that do not
    /// apply to Azure SQL Database. Same row Lite reads (<c>GetSqlEngineEditionAsync</c>): the newest collected
    /// <c>server_properties</c> row. $1 server_id.
    /// </summary>
    public const string EngineEditionSql = @"
SELECT engine_edition
FROM server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>Reads the server's latest collected edition / product-version-major / CPU count + AG role / HADR flag, or null when no server_properties row exists yet.</summary>
    public static async Task<FinOpsEditionFacts?> GetEditionFactsAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(EditionFactsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var edition = reader.IsDBNull(0) ? "" : reader.GetString(0);
        var productVersion = reader.IsDBNull(1) ? null : reader.GetString(1);
        var cpuCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture);
        /* The collected AG state: ServerPropertiesCollector resolves ag_replica_role live from
           sys.dm_hadr_availability_replica_states, and is_hadr_enabled is SERVERPROPERTY('IsHadrEnabled').
           Absent/NULL (non-AG platforms, Azure SQL DB, nothing collected yet) => the Standalone / feature-off
           fallback — exactly Lite's own behaviour where the AG DMVs are unavailable. */
        var agReplicaRole = reader.IsDBNull(3) ? "Standalone" : reader.GetString(3);
        var isHadrEnabled = !reader.IsDBNull(4) && reader.GetBoolean(4);
        return new FinOpsEditionFacts(edition, DarlingFinOpsIndexAnalysisReader.ParseMajorVersion(productVersion), cpuCount, agReplicaRole, isHadrEnabled);
    }

    /// <summary>
    /// The server's latest collected engine edition, or <see cref="CollectorEngineCapability.UnknownEngineEdition"/>
    /// when nothing is collected yet (or the row carries no edition).
    /// </summary>
    public static async Task<int> GetEngineEditionAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(EngineEditionSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0)
            ? Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture)
            : CollectorEngineCapability.UnknownEngineEdition;
    }

    /// <summary>True once the server's query stats reach back to the start of the 7-day window. The advice text claims 7 days, so the data must cover all 7: the first sample has to be at or before the cutoff, with no slack.</summary>
    public static async Task<bool> HasQueryStatsCoverageAsync(NpgsqlDataSource dataSource, int serverId, DateTime cutoff, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(QueryStatsFirstSampleSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var first = await command.ExecuteScalarAsync(cancellationToken);
        return first is DateTime firstSample && firstSample <= cutoff;
    }

    /// <summary>Reads the 7-day P95 Total Server Memory (MB) + sample count (shared by the memory + VM right-sizing checks).</summary>
    public static async Task<(int P95Mb, long SampleCount, string Window)> GetMemoryP95Async(NpgsqlDataSource dataSource, int serverId, DateTime cutoff, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(MemoryP95Sql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            var p95Mb = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            var sampleCount = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
            var window = RightSizingWindow.Describe(reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture), reader.IsDBNull(2) || reader.IsDBNull(3) ? TimeSpan.Zero : reader.GetDateTime(3) - reader.GetDateTime(2));
            return (p95Mb, sampleCount, window);
        }

        return (0, 0L, RightSizingWindow.Describe(0, TimeSpan.Zero));
    }

    /// <summary>Reads the SQL Agent jobs that ran long at least 3 times in the window, in read order (most long runs first), one row per job.</summary>
    public static async Task<List<MaintenanceJobRun>> GetMaintenanceJobRunsAsync(NpgsqlDataSource dataSource, int serverId, DateTime cutoff, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var runs = new List<MaintenanceJobRun>();
        await using var command = dataSource.CreateCommand(MaintenanceWindowSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var jobName = reader.IsDBNull(0) ? "" : reader.GetString(0);
            var avgDuration = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture);
            var maxDuration = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture);
            var avgHistorical = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture);
            var timesLong = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture);

            runs.Add(new MaintenanceJobRun(jobName, avgDuration, maxDuration, avgHistorical, timesLong));
        }

        return runs;
    }

    /// <summary>Reads the 7-day P95 SQL Server CPU utilization and the window its samples cover, or null when the window holds no CPU sample.</summary>
    public static async Task<(decimal P95CpuPct, string Window)?> GetCpuP95Async(NpgsqlDataSource dataSource, int serverId, DateTime cutoff, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var cpuCommand = dataSource.CreateCommand(CpuP95Sql);
        cpuCommand.CommandTimeout = commandTimeoutSeconds;
        cpuCommand.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        cpuCommand.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });

        await using var cpuReader = await cpuCommand.ExecuteReaderAsync(cancellationToken);
        if (await cpuReader.ReadAsync(cancellationToken) && !cpuReader.IsDBNull(0))
        {
            var p95Cpu7d = Convert.ToDecimal(cpuReader.GetValue(0), CultureInfo.InvariantCulture);
            var cpuWindow = RightSizingWindow.Describe(cpuReader.IsDBNull(3) ? 0L : Convert.ToInt64(cpuReader.GetValue(3), CultureInfo.InvariantCulture), cpuReader.IsDBNull(1) || cpuReader.IsDBNull(2) ? TimeSpan.Zero : cpuReader.GetDateTime(2) - cpuReader.GetDateTime(1));
            return (p95Cpu7d, cpuWindow);
        }

        return null;
    }

    /// <summary>Reads the per-database aggregate read/write I/O + stall over the window, in read order, for the storage-tier check.</summary>
    public static async Task<List<StorageTierIo>> GetStorageTierIoAsync(NpgsqlDataSource dataSource, int serverId, DateTime cutoff, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var storageRows = new List<StorageTierIo>();

        await using (var command = dataSource.CreateCommand(StorageTierSql))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var dbName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var totalReads = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
                var totalStallRead = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture);
                var totalWrites = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture);
                var totalStallWrite = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture);

                storageRows.Add(new StorageTierIo(dbName, totalReads, totalStallRead, totalWrites, totalStallWrite,
                    reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                    reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                    reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture)));
            }
        }

        return storageRows;
    }

    /// <summary>Reads the CPU utilization mean + standard deviation over the window (reserved-capacity stability), or null when the window holds too few samples (under 24) or no mean.</summary>
    public static async Task<(decimal AvgCpuPct, decimal StddevCpuPct)?> GetReservedCapacityAsync(NpgsqlDataSource dataSource, int serverId, DateTime cutoff, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(ReservedCapacitySql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0))
        {
            var avgCpu = Convert.ToDecimal(reader.GetValue(0), CultureInfo.InvariantCulture);
            var stddevCpu = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture);
            return (avgCpu, stddevCpu);
        }

        return null;
    }
}
