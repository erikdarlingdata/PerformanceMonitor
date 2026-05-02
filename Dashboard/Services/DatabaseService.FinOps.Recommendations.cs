/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // FinOps Recommendations Engine
        // ============================================

        /// <summary>
        /// Runs all Phase 1 recommendation checks and returns a consolidated list.
        /// </summary>
        public async Task<List<FinOpsRecommendation>> GetFinOpsRecommendationsAsync(decimal monthlyCost)
        {
            var recommendations = new List<FinOpsRecommendation>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            // 1. Enterprise feature usage audit
            try
            {
                using var editionCmd = new SqlCommand(
                    "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)), " +
                    "CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)", connection);
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
                        recommendations.Add(new FinOpsRecommendation
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
END;", connection);
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
                            recommendations.Add(new FinOpsRecommendation
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
                            recommendations.Add(new FinOpsRecommendation
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
                            "SELECT cpu_count FROM sys.dm_os_sys_info", connection);
                        cpuInfoCmd.CommandTimeout = 30;
                        var cpuCountObj = await cpuInfoCmd.ExecuteScalarAsync();
                        var coreLicenseCount = cpuCountObj != null ? Convert.ToInt32(cpuCountObj) : 0;
                        if (coreLicenseCount > 0)
                        {
                            var monthlySavings = coreLicenseCount * 5000m / 12m;
                            recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Enterprise features): {ex.Message}", ex);
            }

            // 2. CPU right-sizing score
            try
            {
                using var cpuCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    v.p95_cpu_pct,
    v.cpu_count,
    v.avg_cpu_pct,
    v.max_cpu_pct
FROM report.finops_utilization_efficiency AS v
OPTION(MAXDOP 1, RECOMPILE);", connection);
                cpuCmd.CommandTimeout = 120;

                using var cpuReader = await cpuCmd.ExecuteReaderAsync();
                if (await cpuReader.ReadAsync())
                {
                    var p95 = cpuReader.IsDBNull(0) ? 0m : Convert.ToDecimal(cpuReader.GetValue(0));
                    var cpuCount = cpuReader.IsDBNull(1) ? 0 : Convert.ToInt32(cpuReader.GetValue(1));
                    var avg = cpuReader.IsDBNull(2) ? 0m : Convert.ToDecimal(cpuReader.GetValue(2));
                    var max = cpuReader.IsDBNull(3) ? 0 : Convert.ToInt32(cpuReader.GetValue(3));

                    if (p95 < 30 && cpuCount > 4)
                    {
                        var targetCores = Math.Max(4, (int)(cpuCount * (p95 / 70m)));
                        var savingsPct = 1m - ((decimal)targetCores / cpuCount);
                        recommendations.Add(new FinOpsRecommendation
                        {
                            Category = "Compute",
                            Severity = p95 < 15 ? "High" : "Medium",
                            Confidence = "Medium",
                            Finding = $"CPU over-provisioned ({cpuCount} cores, P95 = {p95:N1}%)",
                            Detail = $"P95 CPU utilization is {p95:N1}% (avg {avg:N1}%, max {max}%) across {cpuCount} cores. " +
                                     $"Consider reducing to ~{targetCores} cores.",
                            EstMonthlySavings = monthlyCost > 0 ? monthlyCost * savingsPct * 0.60m : null
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (CPU right-sizing): {ex.Message}", ex);
            }

            // 3. Memory right-sizing score
            try
            {
                /* Use P95 of total_memory_mb (buffer pool + plan cache + other clerks)
                   over 7 days, not a single-sample snapshot. The earlier version read
                   only buffer_pool_mb at a single instant, which understated usage on
                   servers where plan cache / workspace / locks dominate, and could
                   trigger right after a service restart when the cache was cold. */
                using var memCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT TOP (1)
    p95_total_memory_mb = PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms.total_memory_mb)
                          OVER (),
    sample_count = COUNT_BIG(*) OVER (),
    physical_memory_mb = (SELECT CAST(SERVERPROPERTY('PhysicalMemoryInMB') AS INT))
FROM collect.memory_stats AS ms
WHERE ms.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
OPTION(MAXDOP 1);", connection);
                memCmd.CommandTimeout = 30;

                using var memReader = await memCmd.ExecuteReaderAsync();
                if (await memReader.ReadAsync())
                {
                    var p95Mb = memReader.IsDBNull(0) ? 0 : Convert.ToInt32(memReader.GetValue(0));
                    var sampleCount = memReader.IsDBNull(1) ? 0L : Convert.ToInt64(memReader.GetValue(1));
                    var physMb = memReader.IsDBNull(2) ? 0 : Convert.ToInt32(memReader.GetValue(2));

                    // Need at least ~1 day of samples (one per minute baseline) to trust the P95
                    if (physMb > 0 && sampleCount >= 500)
                    {
                        var memRatio = (decimal)p95Mb / physMb;
                        if (memRatio < 0.50m && physMb > 8192)
                        {
                            var targetMb = Math.Max(8192, p95Mb * 2);
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Memory",
                                Severity = memRatio < 0.30m ? "High" : "Medium",
                                Confidence = "Medium",
                                Finding = $"Memory over-provisioned (P95 SQL memory uses {memRatio:P0} of {physMb / 1024}GB RAM)",
                                Detail = $"P95 SQL Server memory over 7 days is {p95Mb:N0} MB out of {physMb:N0} MB physical RAM ({memRatio:P0} utilization). " +
                                         $"Consider reducing to ~{targetMb / 1024}GB.",
                                EstMonthlySavings = monthlyCost > 0 ? monthlyCost * (1m - (decimal)targetMb / physMb) * 0.30m : null
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Memory right-sizing): {ex.Message}", ex);
            }

            // 4. Unused index cost quantification
            try
            {
                var spExists = await CheckSpIndexCleanupExistsAsync();
                if (!spExists)
                {
                    recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Index analysis): {ex.Message}", ex);
            }

            // 5. Compression savings estimator
            try
            {
                using var compCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
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
    size_mb DESC
OPTION(MAXDOP 1, RECOMPILE);", connection);
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
                    recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Compression): {ex.Message}", ex);
            }

            // 6. Dormant database detection with cost impact
            try
            {
                var idleDbs = await GetFinOpsIdleDatabasesAsync();
                if (idleDbs.Count > 0)
                {
                    var totalSizeGb = idleDbs.Sum(d => d.TotalSizeMb) / 1024m;
                    var dbNames = string.Join(", ", idleDbs.Take(5).Select(d => d.DatabaseName));
                    var costShare = 0m;
                    if (monthlyCost > 0)
                    {
                        // Estimate cost share proportional to storage footprint
                        var allDbSizes = await GetFinOpsDatabaseSizeStatsAsync();
                        var totalMb = allDbSizes.Sum(d => d.TotalSizeMb);
                        if (totalMb > 0)
                            costShare = (idleDbs.Sum(d => d.TotalSizeMb) / totalMb) * monthlyCost;
                    }

                    recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Dormant databases): {ex.Message}", ex);
            }

            // 7. Dev/test workload detection
            try
            {
                using var devTestCmd = new SqlCommand(@"
SELECT name
FROM sys.databases
WHERE (name LIKE N'%dev%' OR name LIKE N'%test%' OR name LIKE N'%staging%' OR name LIKE N'%qa%')
AND   database_id > 4", connection);
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
                    recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Dev/test detection): {ex.Message}", ex);
            }

            // 11. Maintenance window efficiency — jobs running long
            try
            {
                using var jobCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 10
    job_name,
    avg_runs = COUNT(*),
    avg_duration_seconds = AVG(current_duration_seconds),
    max_duration_seconds = MAX(current_duration_seconds),
    avg_historical = AVG(avg_duration_seconds),
    times_ran_long = SUM(CAST(is_running_long AS int))
FROM collect.running_jobs
WHERE collection_time >= DATEADD(DAY, -7, SYSDATETIME())
AND   avg_duration_seconds > 0
GROUP BY job_name
HAVING SUM(CAST(is_running_long AS int)) >= 3
ORDER BY SUM(CAST(is_running_long AS int)) DESC", connection);
                jobCmd.CommandTimeout = 60;

                using var jobReader = await jobCmd.ExecuteReaderAsync();
                while (await jobReader.ReadAsync())
                {
                    var jobName = jobReader.IsDBNull(0) ? "" : jobReader.GetString(0);
                    var avgDuration = jobReader.IsDBNull(2) ? 0L : Convert.ToInt64(jobReader.GetValue(2));
                    var maxDuration = jobReader.IsDBNull(3) ? 0L : Convert.ToInt64(jobReader.GetValue(3));
                    var avgHistorical = jobReader.IsDBNull(4) ? 0L : Convert.ToInt64(jobReader.GetValue(4));
                    var timesLong = jobReader.IsDBNull(5) ? 0 : Convert.ToInt32(jobReader.GetValue(5));

                    recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Maintenance window): {ex.Message}", ex);
            }

            // 12. VM right-sizing — prescriptive core/memory targets
            try
            {
                /* Memory side previously read live perfmon "Database Cache Memory (KB)",
                   which is only the data-cache slice of the buffer pool — it ignores
                   plan cache, workspace memory, locks, CLR — and was sampled instantly,
                   so a cold cache after a restart triggered "reduce memory" even on
                   servers under genuine pressure. Now uses 7-day P95 of total_memory_mb
                   from collect.memory_stats, the same signal the Utilization tab shows. */
                using var vmCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    p95_cpu = (SELECT TOP (1) PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cus.sqlserver_cpu_utilization) OVER ()
               FROM collect.cpu_utilization_stats AS cus
               WHERE cus.collection_time >= DATEADD(DAY, -7, SYSDATETIME())),
    cpu_count = (SELECT si.cpu_count FROM sys.dm_os_sys_info AS si),
    p95_total_memory_mb = (SELECT TOP (1) PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms.total_memory_mb) OVER ()
                           FROM collect.memory_stats AS ms
                           WHERE ms.collection_time >= DATEADD(DAY, -7, SYSDATETIME())),
    memory_sample_count = (SELECT COUNT_BIG(*)
                           FROM collect.memory_stats AS ms
                           WHERE ms.collection_time >= DATEADD(DAY, -7, SYSDATETIME())),
    physical_memory_mb = (SELECT si.physical_memory_kb / 1024 FROM sys.dm_os_sys_info AS si)
OPTION(MAXDOP 1, RECOMPILE);", connection);
                vmCmd.CommandTimeout = 60;

                using var vmReader = await vmCmd.ExecuteReaderAsync();
                if (await vmReader.ReadAsync())
                {
                    var p95Cpu = vmReader.IsDBNull(0) ? 0m : Convert.ToDecimal(vmReader.GetValue(0));
                    var cpuCount = vmReader.IsDBNull(1) ? 0 : Convert.ToInt32(vmReader.GetValue(1));
                    var p95Mb = vmReader.IsDBNull(2) ? 0 : Convert.ToInt32(vmReader.GetValue(2));
                    var memSampleCount = vmReader.IsDBNull(3) ? 0L : Convert.ToInt64(vmReader.GetValue(3));
                    var physMb = vmReader.IsDBNull(4) ? 0 : Convert.ToInt32(vmReader.GetValue(4));

                    // CPU prescription: only if >= 4 cores
                    if (cpuCount >= 4)
                    {
                        int targetCores = 0;
                        if (p95Cpu < 15)
                            targetCores = Math.Max(2, cpuCount / 4);
                        else if (p95Cpu < 30)
                            targetCores = Math.Max(2, cpuCount / 2);

                        if (targetCores > 0 && targetCores < cpuCount)
                        {
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Hardware",
                                Severity = "Medium",
                                Confidence = "Medium",
                                Finding = $"CPU: reduce from {cpuCount} to {targetCores} cores (P95 CPU {p95Cpu:N1}%)",
                                Detail = $"Over the last 7 days, P95 CPU utilization was {p95Cpu:N1}%. " +
                                         $"Current allocation of {cpuCount} cores can safely be reduced to {targetCores} cores.",
                                EstMonthlySavings = monthlyCost > 0
                                    ? monthlyCost * (1m - (decimal)targetCores / cpuCount) * 0.50m
                                    : null
                            });
                        }
                    }

                    // Memory prescription: needs >= 4 GB physical and at least ~1 day of samples
                    if (physMb >= 4096 && physMb > 0 && memSampleCount >= 500)
                    {
                        var memRatio = (decimal)p95Mb / physMb;
                        int targetMb = 0;
                        if (memRatio < 0.25m)
                            targetMb = Math.Max(4096, physMb / 4);
                        else if (memRatio < 0.40m)
                            targetMb = Math.Max(4096, physMb / 2);

                        if (targetMb > 0 && targetMb < physMb)
                        {
                            recommendations.Add(new FinOpsRecommendation
                            {
                                Category = "Hardware",
                                Severity = "Medium",
                                Confidence = "Medium",
                                Finding = $"Memory: reduce from {physMb / 1024}GB to {targetMb / 1024}GB (P95 SQL memory uses {memRatio:P0})",
                                Detail = $"P95 SQL Server memory over 7 days is {p95Mb:N0} MB of {physMb:N0} MB physical RAM ({memRatio:P0}). " +
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (VM right-sizing): {ex.Message}", ex);
            }

            // 13. Storage tier optimization — flag databases with low IO latency
            try
            {
                using var storageCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    database_name = fio.database_name,
    total_reads = SUM(fio.num_of_reads_delta),
    total_stall_read_ms = SUM(fio.io_stall_read_ms_delta),
    total_writes = SUM(fio.num_of_writes_delta),
    total_stall_write_ms = SUM(fio.io_stall_write_ms_delta)
FROM collect.file_io_stats AS fio
WHERE fio.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
AND   fio.num_of_reads_delta > 0
GROUP BY
    fio.database_name
HAVING SUM(fio.num_of_reads_delta) > 1000
ORDER BY
    SUM(fio.io_stall_read_ms_delta) * 1.0 / SUM(fio.num_of_reads_delta)
OPTION(MAXDOP 1, RECOMPILE);", connection);
                storageCmd.CommandTimeout = 60;

                var lowLatencyDbs = new List<(string Name, decimal AvgReadMs, decimal AvgWriteMs)>();
                using var storageReader = await storageCmd.ExecuteReaderAsync();
                while (await storageReader.ReadAsync())
                {
                    var dbName = storageReader.IsDBNull(0) ? "" : storageReader.GetString(0);
                    var totalReads = storageReader.IsDBNull(1) ? 0L : Convert.ToInt64(storageReader.GetValue(1));
                    var totalStallRead = storageReader.IsDBNull(2) ? 0L : Convert.ToInt64(storageReader.GetValue(2));
                    var totalWrites = storageReader.IsDBNull(3) ? 0L : Convert.ToInt64(storageReader.GetValue(3));
                    var totalStallWrite = storageReader.IsDBNull(4) ? 0L : Convert.ToInt64(storageReader.GetValue(4));

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
                    recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Storage tier): {ex.Message}", ex);
            }

            // 14. Reserved capacity candidates — stable CPU utilization
            try
            {
                using var rcCmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    avg_cpu = AVG(CAST(cus.sqlserver_cpu_utilization AS decimal(5,2))),
    stddev_cpu = STDEV(CAST(cus.sqlserver_cpu_utilization AS decimal(5,2))),
    sample_count = COUNT(*)
FROM collect.cpu_utilization_stats AS cus
WHERE cus.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
HAVING COUNT(*) >= 24
OPTION(MAXDOP 1, RECOMPILE);", connection);
                rcCmd.CommandTimeout = 60;

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
                            recommendations.Add(new FinOpsRecommendation
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
                Logger.Error($"[{ServerLabel}] Recommendation check failed (Reserved capacity): {ex.Message}", ex);
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
}
