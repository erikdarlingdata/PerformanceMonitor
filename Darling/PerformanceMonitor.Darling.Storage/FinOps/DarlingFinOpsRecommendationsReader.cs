/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
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

    /// <summary>
    /// Every database's collected state and encryption flag at the server's latest <c>database_config</c> capture,
    /// ordered by name. Same snapshot the viewer's database-configuration read returns. The capture time is projected so the read names the instant it anchors on. $1 server_id.
    /// </summary>
    public const string DatabaseEncryptionFactsSql = @"
SELECT database_name, state_desc, is_encrypted, capture_time
FROM v_database_config
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)
ORDER BY database_name";

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

    /// <summary>Reads each database's state and encryption flag at the server's latest configuration capture, ordered by name; empty when nothing was captured. A NULL state reads as empty and a NULL flag as not encrypted.</summary>
    public static async Task<List<DatabaseEncryptionFact>> GetDatabaseEncryptionFactsAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var facts = new List<DatabaseEncryptionFact>();
        await using var command = dataSource.CreateCommand(DatabaseEncryptionFactsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            facts.Add(new DatabaseEncryptionFact(
                reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                !reader.IsDBNull(2) && reader.GetBoolean(2)));
        }

        return facts;
    }

    /// <summary>
    /// Runs every monitor-side FinOps recommendation check over the collected store and returns the consolidated
    /// list sorted by severity. All reads are async I/O (they don't block the UI thread), and each check is
    /// isolated in its own try/catch so a single failing read degrades to "that check absent" rather than an
    /// empty tab. <paramref name="monthlyCost"/> is the per-server budget (0 → findings emit with no savings
    /// estimate, mirroring Lite's <c>monthlyCost &gt; 0 ? … : null</c>).
    /// </summary>
    public static async Task<List<FinOpsRecommendation>> GetRecommendationsAsync(NpgsqlDataSource dataSource, int serverId, decimal monthlyCost, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var recommendations = new List<FinOpsRecommendation>();
        var memoryCutoff = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified);

        // 1. Enterprise feature / license audit (collected server_properties + database_config.is_encrypted).
        try
        {
            var facts = await GetEditionFactsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
            if (facts is { } f)
            {
                var isEnterprise = f.Edition.Contains("Enterprise", StringComparison.OrdinalIgnoreCase);

                // TDE is only the deciding factor on pre-2019 Enterprise — read the config snapshot just then.
                var tdeDbNames = new List<string>();
                if (isEnterprise && f.MajorVersion < 15)
                {
                    tdeDbNames = FinOpsRecommendationFigures.SelectTdeDatabaseNames(
                        await GetDatabaseEncryptionFactsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken));
                }

                recommendations.AddRange(
                    FinOpsRecommendationFigures.EditionAudit(
                        f.Edition, f.MajorVersion, f.CpuCount, tdeDbNames, monthlyCost, f.AgReplicaRole, f.IsHadrEnabled));
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Enterprise features): {ex.Message}");
        }

        // 2. CPU right-sizing (collected utilization efficiency).
        try
        {
            var util = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
            var cpuRecommendation = FinOpsRecommendationFigures.CpuRightSizing(util, monthlyCost);
            if (cpuRecommendation != null)
                recommendations.Add(cpuRecommendation);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (CPU right-sizing): {ex.Message}");
        }

        // 3. Memory right-sizing (7-day P95 Total Server Memory vs physical RAM).
        try
        {
            var util = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
            /* No memory advice on an Azure SQL Database (engine_edition 5): its memory comes with its service objective
               and cannot be resized on its own. util.PhysicalMemoryMb is the database's own memory limit there
               (memory_stats, filled from committed_target_kb), not the host's, so the skip is not about a wrong
               denominator: there is nothing to resize. Managed Instance (8) and SQL Server are unchanged. */
            if (util != null && util.PhysicalMemoryMb > 8192
                && await GetEngineEditionAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken) != CollectorEngineCapability.AzureSqlDatabaseEngineEdition)
            {
                var (p95Mb, sampleCount, window) = await GetMemoryP95Async(dataSource, serverId, memoryCutoff, commandTimeoutSeconds, cancellationToken);

                var memoryRecommendation = FinOpsRecommendationFigures.MemoryRightSizing(util, p95Mb, sampleCount, window, monthlyCost);
                if (memoryRecommendation != null)
                    recommendations.Add(memoryRecommendation);
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Memory right-sizing): {ex.Message}");
        }

        // 5. Compression candidates (collected index_object_stats snapshot; Lite's check 4 is dropped — Darling
        //    has native Index Analysis, so the sp_IndexCleanup-existence prompt is obsolete).
        try
        {
            var indexes = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
            var rec = FinOpsRecommendationFigures.Compression(indexes);
            if (rec != null)
            {
                recommendations.Add(rec);
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Compression): {ex.Message}");
        }

        // 6. Dormant database detection with cost impact (collected idle DBs + database sizes).
        try
        {
            /* "No query activity in 7 days" is only true once 7 days of query stats exist: a server enrolled hours
               ago has not been watched long enough to call any database idle. */
            var idleDbs = await HasQueryStatsCoverageAsync(dataSource, serverId, memoryCutoff, commandTimeoutSeconds, cancellationToken)
                ? await DarlingFinOpsOptimizationReader.GetIdleDatabasesAsync(dataSource, serverId, DateTime.UtcNow.AddDays(-7), commandTimeoutSeconds, cancellationToken)
                : new List<IdleDatabase>();
            if (idleDbs.Count > 0)
            {
                var allocatedTotalMb = 0m;
                if (monthlyCost > 0)
                {
                    var totalMb = (await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken))?.AllocatedMb ?? 0m;
                    if (totalMb > 0)
                        allocatedTotalMb = totalMb;
                }

                var dormantRecommendation = FinOpsRecommendationFigures.Dormant(
                    idleDbs.Select(d => (d.DatabaseName, d.TotalSizeMb)).ToList(), allocatedTotalMb, monthlyCost);
                if (dormantRecommendation != null)
                    recommendations.Add(dormantRecommendation);
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Dormant databases): {ex.Message}");
        }

        // 7. Dev/test workload detection (collected database name list).
        try
        {
            var devDbs = FinOpsRecommendationFigures.MatchDevTestDatabases(
                (await GetDatabaseEncryptionFactsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken)).Select(r => r.DatabaseName));
            var devTestRecommendation = FinOpsRecommendationFigures.DevTest(devDbs);
            if (devTestRecommendation != null)
                recommendations.Add(devTestRecommendation);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Dev/test detection): {ex.Message}");
        }

        // 11. Maintenance window efficiency — jobs running long (collected running_jobs).
        try
        {
            foreach (var run in await GetMaintenanceJobRunsAsync(dataSource, serverId, memoryCutoff, commandTimeoutSeconds, cancellationToken))
                recommendations.Add(FinOpsRecommendationFigures.MaintenanceJob(run));
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Maintenance window): {ex.Message}");
        }

        // 12. VM right-sizing — prescriptive core/memory targets (collected 7-day P95 CPU + memory).
        try
        {
            var vmUtil = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
            /* No VM to resize on Azure SQL Database (its cores and memory come with its service objective),
               and no advice from a window with no CPU sample (its P95 of 0 is not a measurement). */
            if (vmUtil != null && FinOpsUtilizationFigures.HasCpuSample(vmUtil)
                && await GetEngineEditionAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken) != CollectorEngineCapability.AzureSqlDatabaseEngineEdition)
            {
                decimal p95Cpu7d = vmUtil.P95CpuPct;
                var cpuWindow = "recent samples"; // neutral until the 7-day read supplies its own span; the 24-hour fallback has no span of its own
                int cpuCount = vmUtil.CpuCount;
                int physMb = vmUtil.PhysicalMemoryMb;

                // Prefer the 7-day P95 CPU; fall back to the 24-hour P95 already on vmUtil.
                try
                {
                    if (await GetCpuP95Async(dataSource, serverId, memoryCutoff, commandTimeoutSeconds, cancellationToken) is { } cpu)
                    {
                        p95Cpu7d = cpu.P95CpuPct;
                        cpuWindow = cpu.Window;
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Recommendation check (VM right-sizing) 7-day CPU P95 fell back to 24h: {ex.Message}");
                }

                var (p95MemMb, memSampleCount, memWindow) = await GetMemoryP95Async(dataSource, serverId, memoryCutoff, commandTimeoutSeconds, cancellationToken);

                foreach (var vmRecommendation in FinOpsRecommendationFigures.VmRightSizing(
                    p95Cpu7d, cpuWindow, cpuCount, physMb, p95MemMb, memSampleCount, memWindow, monthlyCost))
                    recommendations.Add(vmRecommendation);
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (VM right-sizing): {ex.Message}");
        }

        // 13. Storage tier optimization — databases with low I/O latency (collected file_io_stats).
        try
        {
            var storageRows = await GetStorageTierIoAsync(dataSource, serverId, memoryCutoff, commandTimeoutSeconds, cancellationToken);

            var storageRecommendation = FinOpsRecommendationFigures.StorageTier(storageRows);
            if (storageRecommendation != null)
                recommendations.Add(storageRecommendation);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Storage tier): {ex.Message}");
        }

        // 14. Reserved capacity candidates — stable CPU utilization (collected cpu_utilization_stats).
        try
        {
            if (await GetReservedCapacityAsync(dataSource, serverId, memoryCutoff, commandTimeoutSeconds, cancellationToken) is { } reserved)
            {
                var reservedRecommendation = FinOpsRecommendationFigures.ReservedCapacity(reserved.AvgCpuPct, reserved.StddevCpuPct);
                if (reservedRecommendation != null)
                    recommendations.Add(reservedRecommendation);
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Reserved capacity): {ex.Message}");
        }

        return FinOpsRecommendationFigures.Ordered(recommendations);
    }
}
