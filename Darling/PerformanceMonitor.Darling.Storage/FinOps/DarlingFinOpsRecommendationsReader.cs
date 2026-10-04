/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


namespace PerformanceMonitor.Darling.Storage.FinOps;

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
}
