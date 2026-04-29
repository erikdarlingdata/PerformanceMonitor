/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    // ============================================
    // FinOps Recommendations Engine
    // ============================================

    /// <summary>
    /// Runs all Phase 1 recommendation checks and returns a consolidated list.
    /// Uses DuckDB for collected data and live SQL queries for server-specific checks.
    /// </summary>
    public async Task<List<RecommendationRow>> GetRecommendationsAsync(int serverId, string connectionString, string utilityConnectionString, decimal monthlyCost)
    {
        var recommendations = new List<RecommendationRow>();

        // 1. Enterprise feature usage audit (live SQL query)
        try
        {
            using var sqlConn = new SqlConnection(connectionString);
            await sqlConn.OpenAsync();

            using var editionCmd = new SqlCommand(
                "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)), " +
                "CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)", sqlConn);
            editionCmd.CommandTimeout = 30;
            using var editionReader = await editionCmd.ExecuteReaderAsync();
            string edition = "";
            int majorVersion = 0;
            if (await editionReader.ReadAsync())
            {
                edition = editionReader.IsDBNull(0) ? "" : editionReader.GetString(0);
                majorVersion = editionReader.IsDBNull(1) ? 0 : editionReader.GetInt32(1);
            }

            if (edition.Contains("Enterprise", StringComparison.OrdinalIgnoreCase))
            {
                // SQL Server 2019 (major version 15) moved TDE to Standard Edition.
                // On 2019+, dm_db_persisted_sku_features won't report TDE since it's
                // no longer Enterprise-restricted — so we skip the TDE-specific check
                // and give version-appropriate guidance instead.
                if (majorVersion >= 15)
                {
                    // 2019+: Most features that were Enterprise-only moved to Standard
                    // in 2016 SP1, and TDE moved in 2019. Very few Enterprise-only
                    // features remain (e.g., certain HA configurations).
                    recommendations.Add(new RecommendationRow
                    {
                        Category = "Licensing",
                        Severity = "High",
                        Confidence = "Medium",
                        Finding = "Enterprise Edition may not be required",
                        Detail = "Starting with SQL Server 2019, most previously Enterprise-only features " +
                                 "(including TDE, compression, partitioning, and columnstore) are available " +
                                 "in Standard Edition. Review whether remaining Enterprise-only features " +
                                 "(such as Always On availability groups with multiple secondaries) are in use " +
                                 "before considering a downgrade to Standard Edition.",
                        EstMonthlySavings = monthlyCost > 0 ? monthlyCost * 0.40m : null
                    });
                }
                else
                {
                    /*
                    Pre-2019: TDE is the only commonly-used feature still restricted
                    to Enterprise Edition since 2016 SP1. Use dm_db_persisted_sku_features
                    to detect it — the DMV correctly reports TDE on these versions.
                    */
                    using var featCmd = new SqlCommand(@"
DECLARE
    @sql nvarchar(max) = N'';

SELECT
    @sql += N'
SELECT ' + QUOTENAME(name, '''') + N' AS database_name
FROM ' + QUOTENAME(name) + N'.sys.dm_db_persisted_sku_features
WHERE feature_name = N''TransparentDataEncryption''
UNION ALL'
FROM sys.databases
WHERE database_id > 4
AND   state_desc = N'ONLINE';

IF @sql <> N''
BEGIN
    SET @sql = LEFT(@sql, LEN(@sql) - 10);
    EXEC sys.sp_executesql @sql;
END;", sqlConn);
                    featCmd.CommandTimeout = 30;

                    var tdeDbNames = new List<string>();
                    using var featReader = await featCmd.ExecuteReaderAsync();
                    while (await featReader.ReadAsync())
                    {
                        if (!featReader.IsDBNull(0))
                            tdeDbNames.Add(featReader.GetString(0));
                    }

                    if (tdeDbNames.Count == 0)
                    {
                        recommendations.Add(new RecommendationRow
                        {
                            Category = "Licensing",
                            Severity = "High",
                            Confidence = "High",
                            Finding = "Enterprise Edition with no Enterprise-only features detected",
                            Detail = "No databases use Transparent Data Encryption (TDE), the only feature " +
                                     "still restricted to Enterprise Edition since SQL Server 2016 SP1. " +
                                     "Review whether Standard Edition would meet workload requirements for potential license savings.",
                            EstMonthlySavings = monthlyCost > 0 ? monthlyCost * 0.40m : null
                        });
                    }
                    else
                    {
                        recommendations.Add(new RecommendationRow
                        {
                            Category = "Licensing",
                            Severity = "Low",
                            Confidence = "High",
                            Finding = "TDE in use — Enterprise Edition downgrade blocker",
                            Detail = $"The following databases use Transparent Data Encryption: {string.Join(", ", tdeDbNames.Take(20))}" +
                                     (tdeDbNames.Count > 20 ? $" and {tdeDbNames.Count - 20} more" : "") +
                                     ". TDE must be removed before downgrading to Standard Edition."
                        });

                        // Check 10: License cost impact estimate (only when features ARE in use)
                        using var cpuInfoCmd = new SqlCommand(
                            "SELECT cpu_count FROM sys.dm_os_sys_info", sqlConn);
                        cpuInfoCmd.CommandTimeout = 30;
                        var cpuCountObj = await cpuInfoCmd.ExecuteScalarAsync();
                        var coreLicenseCount = cpuCountObj != null ? Convert.ToInt32(cpuCountObj) : 0;
                        if (coreLicenseCount > 0)
                        {
                            var monthlySavings = coreLicenseCount * 5000m / 12m;
                            recommendations.Add(new RecommendationRow
                            {
                                Category = "Licensing",
                                Severity = "Low",
                                Confidence = "Low",
                                Finding = $"Enterprise to Standard would save ~${monthlySavings:N0}/mo at list pricing ({coreLicenseCount} cores)",
                                Detail = "Based on list pricing differential of ~$5,000/core/year between Enterprise and Standard. " +
                                         "Actual savings depend on your licensing agreement. See Enterprise feature audit for downgrade blockers.",
                                EstMonthlySavings = monthlySavings
                            });
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Enterprise features): {ex.Message}");
        }

        // 2. CPU right-sizing score (from DuckDB)
        try
        {
            var util = await GetUtilizationEfficiencyAsync(serverId);
            if (util != null && util.P95CpuPct < 30 && util.CpuCount > 4)
            {
                var targetCores = Math.Max(4, (int)(util.CpuCount * (util.P95CpuPct / 70m)));
                var savingsPct = 1m - ((decimal)targetCores / util.CpuCount);
                recommendations.Add(new RecommendationRow
                {
                    Category = "Compute",
                    Severity = util.P95CpuPct < 15 ? "High" : "Medium",
                    Confidence = "Medium",
                    Finding = $"CPU over-provisioned ({util.CpuCount} cores, P95 = {util.P95CpuPct:N1}%)",
                    Detail = $"P95 CPU utilization is {util.P95CpuPct:N1}% (avg {util.AvgCpuPct:N1}%, max {util.MaxCpuPct}%) across {util.CpuCount} cores. " +
                             $"Consider reducing to ~{targetCores} cores.",
                    EstMonthlySavings = monthlyCost > 0 ? monthlyCost * savingsPct * 0.60m : null
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (CPU right-sizing): {ex.Message}");
        }

        // 3. Memory right-sizing score (from DuckDB)
        try
        {
            var util = await GetUtilizationEfficiencyAsync(serverId);
            if (util != null && util.PhysicalMemoryMb > 8192)
            {
                var bpRatio = util.PhysicalMemoryMb > 0 ? (decimal)util.BufferPoolMb / util.PhysicalMemoryMb : 0m;
                if (bpRatio < 0.50m)
                {
                    var targetMb = Math.Max(8192, util.BufferPoolMb * 2);
                    recommendations.Add(new RecommendationRow
                    {
                        Category = "Memory",
                        Severity = bpRatio < 0.30m ? "High" : "Medium",
                        Confidence = "Medium",
                        Finding = $"Memory over-provisioned (buffer pool uses {bpRatio:P0} of {util.PhysicalMemoryMb / 1024}GB RAM)",
                        Detail = $"Buffer pool is {util.BufferPoolMb:N0} MB out of {util.PhysicalMemoryMb:N0} MB physical RAM ({bpRatio:P0} utilization). " +
                                 $"Consider reducing to ~{targetMb / 1024}GB.",
                        EstMonthlySavings = monthlyCost > 0 ? monthlyCost * (1m - (decimal)targetMb / util.PhysicalMemoryMb) * 0.30m : null
                    });
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Memory right-sizing): {ex.Message}");
        }

        // 4. Unused index cost quantification (live SQL query)
        try
        {
            var spExists = await CheckSpIndexCleanupExistsAsync(utilityConnectionString);
            if (!spExists)
            {
                recommendations.Add(new RecommendationRow
                {
                    Category = "Indexes",
                    Severity = "Low",
                    Confidence = "Low",
                    Finding = "Index analysis unavailable (sp_IndexCleanup not installed)",
                    Detail = "Install sp_IndexCleanup from https://github.com/erikdarlingdata/DarlingData " +
                             "to identify unused and duplicate indexes that waste storage and add write overhead."
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Index analysis): {ex.Message}");
        }

        // 5. Compression savings estimator (live SQL query)
        try
        {
            using var sqlConn = new SqlConnection(connectionString);
            await sqlConn.OpenAsync();

            using var compCmd = new SqlCommand(@"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc,
    p.data_compression_desc,
    SUM(a.total_pages) * 8 / 1024.0 AS size_mb
FROM sys.tables AS t
JOIN sys.schemas AS s ON t.schema_id = s.schema_id
JOIN sys.indexes AS i ON t.object_id = i.object_id
JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units AS a ON p.partition_id = a.container_id
WHERE p.data_compression_desc = N'NONE'
AND   t.is_ms_shipped = 0
GROUP BY
    s.name,
    t.name,
    i.name,
    i.type_desc,
    p.data_compression_desc
HAVING SUM(a.total_pages) * 8 / 1024.0 >= 1024
ORDER BY
    size_mb DESC", sqlConn);
            compCmd.CommandTimeout = 60;

            var candidates = new List<(string Schema, string Table, string Index, string Type, decimal SizeMb)>();
            using var compReader = await compCmd.ExecuteReaderAsync();
            while (await compReader.ReadAsync())
            {
                candidates.Add((
                    compReader.IsDBNull(0) ? "" : compReader.GetString(0),
                    compReader.IsDBNull(1) ? "" : compReader.GetString(1),
                    compReader.IsDBNull(2) ? "" : compReader.GetString(2),
                    compReader.IsDBNull(3) ? "" : compReader.GetString(3),
                    compReader.IsDBNull(5) ? 0m : Convert.ToDecimal(compReader.GetValue(5))
                ));
            }

            if (candidates.Count > 0)
            {
                var totalGb = candidates.Sum(c => c.SizeMb) / 1024m;
                var topItems = candidates.Take(5)
                    .Select(c => $"{c.Schema}.{c.Table} ({c.SizeMb / 1024:N1}GB)")
                    .ToList();
                recommendations.Add(new RecommendationRow
                {
                    Category = "Storage",
                    Severity = totalGb > 50 ? "High" : totalGb > 10 ? "Medium" : "Low",
                    Confidence = "High",
                    Finding = $"{candidates.Count} uncompressed object(s) >= 1GB ({totalGb:N1}GB total)",
                    Detail = $"Large uncompressed tables/indexes: {string.Join("; ", topItems)}" +
                             (candidates.Count > 5 ? $" and {candidates.Count - 5} more" : "") +
                             ". Consider PAGE or ROW compression to reduce storage and improve I/O."
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Compression): {ex.Message}");
        }

        // 6. Dormant database detection with cost impact (from DuckDB)
        try
        {
            var idleDbs = await GetIdleDatabasesAsync(serverId);
            if (idleDbs.Count > 0)
            {
                var totalSizeGb = idleDbs.Sum(d => d.TotalSizeMb) / 1024m;
                var dbNames = string.Join(", ", idleDbs.Take(5).Select(d => d.DatabaseName));
                var costShare = 0m;
                if (monthlyCost > 0)
                {
                    var allDbSizes = await GetDatabaseSizeLatestAsync(serverId);
                    var totalMb = allDbSizes.Sum(d => d.TotalSizeMb);
                    if (totalMb > 0)
                        costShare = (idleDbs.Sum(d => d.TotalSizeMb) / totalMb) * monthlyCost;
                }

                recommendations.Add(new RecommendationRow
                {
                    Category = "Databases",
                    Severity = idleDbs.Count >= 3 ? "High" : "Medium",
                    Confidence = "High",
                    Finding = $"{idleDbs.Count} idle database(s) consuming {totalSizeGb:N1}GB",
                    Detail = $"No query activity in 7 days: {dbNames}" +
                             (idleDbs.Count > 5 ? $" and {idleDbs.Count - 5} more" : "") +
                             ". Consider archiving or removing these databases.",
                    EstMonthlySavings = costShare > 0 ? costShare : null
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Dormant databases): {ex.Message}");
        }

        // 7. Dev/test workload detection (live SQL query)
        try
        {
            using var sqlConn = new SqlConnection(connectionString);
            await sqlConn.OpenAsync();

            using var devTestCmd = new SqlCommand(@"
SELECT name
FROM sys.databases
WHERE (name LIKE N'%dev%' OR name LIKE N'%test%' OR name LIKE N'%staging%' OR name LIKE N'%qa%')
AND   database_id > 4", sqlConn);
            devTestCmd.CommandTimeout = 30;

            var devDbs = new List<string>();
            using var devReader = await devTestCmd.ExecuteReaderAsync();
            while (await devReader.ReadAsync())
            {
                if (!devReader.IsDBNull(0))
                    devDbs.Add(devReader.GetString(0));
            }

            if (devDbs.Count > 0)
            {
                recommendations.Add(new RecommendationRow
                {
                    Category = "Environment",
                    Severity = "Medium",
                    Confidence = "Low",
                    Finding = $"{devDbs.Count} possible dev/test database(s) on production server",
                    Detail = $"Databases matching dev/test patterns: {string.Join(", ", devDbs.Take(10))}" +
                             (devDbs.Count > 10 ? $" and {devDbs.Count - 10} more" : "") +
                             ". If these are non-production workloads, consider moving to a lower-cost tier or separate server."
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Dev/test detection): {ex.Message}");
        }

        // 11. Maintenance window efficiency — jobs running long (from DuckDB)
        try
        {
            using var jobConn = await OpenConnectionAsync();
            using var jobCmd = jobConn.CreateCommand();
            jobCmd.CommandText = @"
SELECT
    job_name,
    COUNT(*) AS avg_runs,
    AVG(current_duration_seconds) AS avg_duration_seconds,
    MAX(current_duration_seconds) AS max_duration_seconds,
    AVG(avg_duration_seconds) AS avg_historical,
    SUM(CASE WHEN is_running_long THEN 1 ELSE 0 END) AS times_ran_long
FROM running_jobs
WHERE server_id = $1
AND   collection_time >= $2
AND   avg_duration_seconds > 0
GROUP BY job_name
HAVING SUM(CASE WHEN is_running_long THEN 1 ELSE 0 END) >= 3
ORDER BY times_ran_long DESC
LIMIT 10";
            jobCmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            jobCmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow.AddDays(-7) });

            using var jobReader = await jobCmd.ExecuteReaderAsync();
            while (await jobReader.ReadAsync())
            {
                var jobName = jobReader.IsDBNull(0) ? "" : jobReader.GetString(0);
                var avgDuration = jobReader.IsDBNull(2) ? 0L : ToInt64(jobReader.GetValue(2));
                var maxDuration = jobReader.IsDBNull(3) ? 0L : ToInt64(jobReader.GetValue(3));
                var avgHistorical = jobReader.IsDBNull(4) ? 0L : ToInt64(jobReader.GetValue(4));
                var timesLong = jobReader.IsDBNull(5) ? 0 : (int)ToInt64(jobReader.GetValue(5));

                recommendations.Add(new RecommendationRow
                {
                    Category = "Maintenance",
                    Severity = timesLong >= 5 ? "Medium" : "Low",
                    Confidence = "High",
                    Finding = $"{jobName} ran long {timesLong} times in 7 days",
                    Detail = $"Average duration: {FormatDuration(avgDuration)}, max: {FormatDuration(maxDuration)}, " +
                             $"historical average: {FormatDuration(avgHistorical)}. " +
                             "Review whether this job's schedule or operations need tuning."
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Maintenance window): {ex.Message}");
        }

        // 12. VM right-sizing — prescriptive core/memory targets (from DuckDB)
        try
        {
            var vmUtil = await GetUtilizationEfficiencyAsync(serverId);
            if (vmUtil != null)
            {
                // CPU data comes from 24-hour window in GetUtilizationEfficiencyAsync.
                // For a 7-day P95 we query DuckDB directly.
                decimal p95Cpu7d = vmUtil.P95CpuPct;
                int cpuCount = vmUtil.CpuCount;
                int bpMb = vmUtil.BufferPoolMb;
                int physMb = vmUtil.PhysicalMemoryMb;

                // Try 7-day P95 from DuckDB for better accuracy
                try
                {
                    using var cpuConn = await OpenConnectionAsync();
                    using var cpuCmd = cpuConn.CreateCommand();
                    cpuCmd.CommandText = @"
SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2";
                    cpuCmd.Parameters.Add(new DuckDBParameter { Value = serverId });
                    cpuCmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-7) });

                    using var cpuReader = await cpuCmd.ExecuteReaderAsync();
                    if (await cpuReader.ReadAsync() && !cpuReader.IsDBNull(0))
                    {
                        p95Cpu7d = Convert.ToDecimal(cpuReader.GetValue(0));
                    }
                }
                catch { /* fall back to 24-hour P95 */ }

                // CPU prescription: only if >= 4 cores
                if (cpuCount >= 4)
                {
                    int targetCores = 0;
                    if (p95Cpu7d < 15)
                        targetCores = Math.Max(2, cpuCount / 4);
                    else if (p95Cpu7d < 30)
                        targetCores = Math.Max(2, cpuCount / 2);

                    if (targetCores > 0 && targetCores < cpuCount)
                    {
                        recommendations.Add(new RecommendationRow
                        {
                            Category = "Hardware",
                            Severity = "Medium",
                            Confidence = "Medium",
                            Finding = $"CPU: reduce from {cpuCount} to {targetCores} cores (P95 CPU {p95Cpu7d:N1}%)",
                            Detail = $"Over the last 7 days, P95 CPU utilization was {p95Cpu7d:N1}%. " +
                                     $"Current allocation of {cpuCount} cores can safely be reduced to {targetCores} cores.",
                            EstMonthlySavings = monthlyCost > 0
                                ? monthlyCost * (1m - (decimal)targetCores / cpuCount) * 0.50m
                                : null
                        });
                    }
                }

                // Memory prescription: only if >= 4096 MB
                if (physMb >= 4096 && physMb > 0)
                {
                    var bpRatio = (decimal)bpMb / physMb;
                    int targetMb = 0;
                    if (bpRatio < 0.25m)
                        targetMb = Math.Max(4096, physMb / 4);
                    else if (bpRatio < 0.40m)
                        targetMb = Math.Max(4096, physMb / 2);

                    if (targetMb > 0 && targetMb < physMb)
                    {
                        recommendations.Add(new RecommendationRow
                        {
                            Category = "Hardware",
                            Severity = "Medium",
                            Confidence = "Medium",
                            Finding = $"Memory: reduce from {physMb / 1024}GB to {targetMb / 1024}GB (buffer pool uses {bpRatio:P0})",
                            Detail = $"Buffer pool is using {bpMb:N0} MB of {physMb:N0} MB physical RAM ({bpRatio:P0}). " +
                                     $"Reducing to {targetMb / 1024}GB would still leave headroom.",
                            EstMonthlySavings = monthlyCost > 0
                                ? monthlyCost * (1m - (decimal)targetMb / physMb) * 0.30m
                                : null
                        });
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (VM right-sizing): {ex.Message}");
        }

        // 13. Storage tier optimization — flag databases with low IO latency (from DuckDB)
        try
        {
            using var ioConn = await OpenConnectionAsync();
            using var ioCmd = ioConn.CreateCommand();
            ioCmd.CommandText = @"
SELECT
    database_name,
    SUM(delta_reads) AS total_reads,
    SUM(delta_stall_read_ms) AS total_stall_read_ms,
    SUM(delta_writes) AS total_writes,
    SUM(delta_stall_write_ms) AS total_stall_write_ms
FROM file_io_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   delta_reads > 0
GROUP BY database_name
HAVING SUM(delta_reads) > 1000";
            ioCmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            ioCmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-7) });

            var lowLatencyDbs = new List<(string Name, decimal AvgReadMs, decimal AvgWriteMs)>();
            using var ioReader = await ioCmd.ExecuteReaderAsync();
            while (await ioReader.ReadAsync())
            {
                var dbName = ioReader.IsDBNull(0) ? "" : ioReader.GetString(0);
                var totalReads = ioReader.IsDBNull(1) ? 0L : ToInt64(ioReader.GetValue(1));
                var totalStallRead = ioReader.IsDBNull(2) ? 0L : ToInt64(ioReader.GetValue(2));
                var totalWrites = ioReader.IsDBNull(3) ? 0L : ToInt64(ioReader.GetValue(3));
                var totalStallWrite = ioReader.IsDBNull(4) ? 0L : ToInt64(ioReader.GetValue(4));

                var avgReadMs = totalReads > 0 ? (decimal)totalStallRead / totalReads : 0m;
                var avgWriteMs = totalWrites > 0 ? (decimal)totalStallWrite / totalWrites : 0m;

                if (avgReadMs < 5m && avgWriteMs < 3m)
                {
                    lowLatencyDbs.Add((dbName, avgReadMs, avgWriteMs));
                }
            }

            if (lowLatencyDbs.Count > 0)
            {
                var detail = string.Join("; ", lowLatencyDbs.Take(10)
                    .Select(d => $"{d.Name} (read {d.AvgReadMs:N1}ms, write {d.AvgWriteMs:N1}ms)"));
                recommendations.Add(new RecommendationRow
                {
                    Category = "Storage",
                    Severity = "Low",
                    Confidence = "Medium",
                    Finding = $"{lowLatencyDbs.Count} database(s) with low IO latency — standard storage may suffice",
                    Detail = $"These databases have avg read latency under 5ms and write under 3ms over 7 days: {detail}" +
                             (lowLatencyDbs.Count > 10 ? $" and {lowLatencyDbs.Count - 10} more" : "") +
                             ". Premium/high-performance storage may not be needed."
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Storage tier): {ex.Message}");
        }

        // 14. Reserved capacity candidates — stable CPU utilization (from DuckDB)
        try
        {
            using var rcConn = await OpenConnectionAsync();
            using var rcCmd = rcConn.CreateCommand();
            rcCmd.CommandText = @"
SELECT
    AVG(sqlserver_cpu_utilization) AS avg_cpu,
    STDDEV(sqlserver_cpu_utilization) AS stddev_cpu,
    COUNT(*) AS sample_count
FROM cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2
HAVING COUNT(*) >= 24";
            rcCmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            rcCmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-7) });

            using var rcReader = await rcCmd.ExecuteReaderAsync();
            if (await rcReader.ReadAsync() && !rcReader.IsDBNull(0))
            {
                var avgCpu = Convert.ToDecimal(rcReader.GetValue(0));
                var stddevCpu = rcReader.IsDBNull(1) ? 0m : Convert.ToDecimal(rcReader.GetValue(1));

                if (avgCpu > 20 && stddevCpu > 0)
                {
                    var cv = stddevCpu / avgCpu;
                    if (cv < 0.3m)
                    {
                        var confidence = cv < 0.15m ? "High" : "Medium";
                        recommendations.Add(new RecommendationRow
                        {
                            Category = "Cloud",
                            Severity = "Low",
                            Confidence = confidence,
                            Finding = $"Stable CPU utilization (avg {avgCpu:N1}%, CV {cv:N2}) — reserved capacity candidate",
                            Detail = $"CPU utilization is consistently {avgCpu:N1}% with low variance (±{stddevCpu:N1}%). " +
                                     "Reserved pricing typically saves 30-40% over pay-as-you-go for predictable workloads."
                        });
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Recommendation check failed (Reserved capacity): {ex.Message}");
        }

        return recommendations.OrderBy(r => r.SeveritySort).ToList();
    }

    private static string FormatDuration(long seconds)
    {
        if (seconds >= 3600)
            return $"{seconds / 3600}h {(seconds % 3600) / 60}m {seconds % 60}s";
        if (seconds >= 60)
            return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds}s";
    }
}
