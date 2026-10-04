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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>One FinOps recommendation: category, severity, confidence, the finding and its detail, and the estimated monthly savings when there is one.</summary>
public sealed record FinOpsRecommendation(
    string Category, string Severity, string Confidence, string Finding, string Detail, decimal? EstMonthlySavings)
{
    /// <summary>An empty recommendation, filled in through the initializer.</summary>
    public FinOpsRecommendation() : this("", "", "", "", "", null)
    {
    }
}

/// <summary>One database's collected encryption state.</summary>
public sealed record DatabaseEncryptionFact(string DatabaseName, string StateDesc, bool IsEncrypted);

/// <summary>One database's I/O totals over the storage-tier window.</summary>
public sealed record StorageTierIo(
    string DatabaseName, long TotalReads, long TotalStallReadMs, long TotalWrites, long TotalStallWriteMs,
    DateTime? FirstSample, DateTime? LastSample, long WindowSamples);

/// <summary>One maintenance job's recent and historical run durations.</summary>
public sealed record MaintenanceJobRun(
    string JobName, long AvgDurationSeconds, long MaxDurationSeconds, long AvgHistoricalSeconds, int TimesRanLong);

/// <summary>The FinOps recommendation rules, as pure functions of collected figures.</summary>
public static class FinOpsRecommendationFigures
{
    /// <summary>The four system databases (master/model/msdb/tempdb) — Lite's <c>database_id &gt; 4</c> filter by name.</summary>
    public static readonly IReadOnlySet<string> SystemDatabases =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "master", "model", "msdb", "tempdb" };

    /// <summary>The sort key for a severity: High first, then Medium, Low, and anything else last.</summary>
    public static int SeveritySort(string severity) => severity switch
    {
        "High" => 1,
        "Medium" => 2,
        "Low" => 3,
        _ => 4
    };

    /// <summary>The recommendations in severity order. The sort is stable, so the check order holds within a severity.</summary>
    public static List<FinOpsRecommendation> Ordered(IEnumerable<FinOpsRecommendation> recommendations) =>
        recommendations.OrderBy(r => SeveritySort(r.Severity)).ToList();

    /// <summary>Human-readable duration formatting for the maintenance-window finding (Lite's FinOps FormatDuration, verbatim).</summary>
    public static string FormatDuration(long seconds)
    {
        if (seconds >= 3600)
            return $"{seconds / 3600}h {(seconds % 3600) / 60}m {seconds % 60}s";
        if (seconds >= 60)
            return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds}s";
    }

    /// <summary>
    /// Selects the databases running Transparent Data Encryption from a collected database-config snapshot — the
    /// monitor-side stand-in for Lite's live <c>dm_db_persisted_sku_features</c> probe: <c>is_encrypted = true</c>
    /// marks a TDE database. System databases (Lite's <c>database_id &gt; 4</c>) and non-ONLINE databases are
    /// excluded, mirroring Lite's probe. Ordered by name for stable output.
    /// </summary>
    public static List<string> SelectTdeDatabaseNames(IEnumerable<DatabaseEncryptionFact> configRows) =>
        configRows
            .Where(r => r.IsEncrypted
                && string.Equals(r.StateDesc, "ONLINE", StringComparison.OrdinalIgnoreCase)
                && !SystemDatabases.Contains(r.DatabaseName))
            .Select(r => r.DatabaseName)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToList();

    /// <summary>
    /// Matches the collected database names against Lite's dev/test name patterns (<c>%dev% / %test% / %staging% /
    /// %qa%</c>, case-insensitive) excluding the system databases (Lite's <c>database_id &gt; 4</c>). Pure so the
    /// pattern logic is unit-testable without a store.
    /// </summary>
    public static List<string> MatchDevTestDatabases(IEnumerable<string> databaseNames)
    {
        static bool IsDevTest(string name) =>
            name.Contains("dev", StringComparison.OrdinalIgnoreCase)
            || name.Contains("test", StringComparison.OrdinalIgnoreCase)
            || name.Contains("staging", StringComparison.OrdinalIgnoreCase)
            || name.Contains("qa", StringComparison.OrdinalIgnoreCase);

        return databaseNames
            .Where(n => !SystemDatabases.Contains(n) && IsDevTest(n))
            .ToList();
    }

    /// <summary>
    /// Builds the Edition / license audit recommendations from the collected facts (check 1 + Lite's check 10
    /// license-cost math). Returns an empty list for non-Enterprise editions. The full Enterprise branching is
    /// ported verbatim from Lite — 2019+ (TDE moved to Standard) vs pre-2019 (TDE is the last Enterprise-only
    /// blocker, read from <paramref name="tdeDbNames"/>), the 40%-of-budget downgrade estimate, and the
    /// $5,000/core/year list-price core math — INCLUDING the AG-aware branches driven by the collected
    /// <paramref name="agReplicaRole"/> + <paramref name="isHadrEnabled"/>: the Availability-Group secondary-replica
    /// branch (a downgrade decision belongs to the whole group, evaluated on the primary — no savings estimate), the
    /// advanced-AG Basic-AG-limitation downgrade caveat, and the AG-driven confidence downgrades. Lite gates the
    /// caveat/confidence on <c>GetAdvancedAgCountAsync() &gt; 0</c> (the non-basic <c>sys.availability_groups</c>
    /// count, which Darling does not collect); on an Enterprise instance a hosted AG (role = <c>Primary</c>) is
    /// effectively always advanced — Basic AGs are Standard-only — so the collected primary role + HADR flag is the
    /// faithful stand-in. Standalone / feature-off keeps the standalone confidence and adds no caveat.
    /// </summary>
    public static List<FinOpsRecommendation> EditionAudit(
        string edition,
        int majorVersion,
        int cpuCount,
        IReadOnlyList<string> tdeDbNames,
        decimal monthlyCost,
        string agReplicaRole,
        bool isHadrEnabled)
    {
        var recommendations = new List<FinOpsRecommendation>();

        if (!edition.Contains("Enterprise", StringComparison.OrdinalIgnoreCase))
        {
            return recommendations;
        }

        var agRole = string.IsNullOrWhiteSpace(agReplicaRole) ? "Standalone" : agReplicaRole.Trim();
        var isSecondary = string.Equals(agRole, "Secondary", StringComparison.OrdinalIgnoreCase);

        /* Lite computes advancedAgCount = GetAdvancedAgCountAsync() (COUNT of sys.availability_groups WHERE
           basic_features = 0) and skips the probe on a secondary. Darling's collected stand-in: an Enterprise
           instance acting as an AG PRIMARY with Always On enabled — a Basic AG is a Standard-Edition-only feature,
           so an Enterprise-hosted AG is effectively always advanced. Secondary gets its own branch below (Lite
           forces advancedAgCount = 0 there); Standalone / feature-off => no caveat, no confidence downgrade. */
        var hasAdvancedAg = !isSecondary
            && string.Equals(agRole, "Primary", StringComparison.OrdinalIgnoreCase)
            && isHadrEnabled;

        /* Standard Edition offers only Basic Availability Groups, so caveat any edition-downgrade guidance when this
           instance hosts an (advanced) AG: a downgrade would force the workload onto Basic AG limitations (#1085).
           Lite names the advanced-AG count; Darling has only the collected role, so it names the primary replica. */
        var agDowngradeCaveat = hasAdvancedAg
            ? " Note: this instance is the primary replica of an Always On Availability Group. Standard Edition " +
              "supports only Basic Availability Groups, which are limited to two replicas, a single database per " +
              "group, and provide no readable secondary or backups on the secondary " +
              "(see https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/basic-availability-groups-always-on-availability-groups#limitations). " +
              "Factor this into any downgrade decision."
            : "";

        if (isSecondary)
        {
            /* On an Availability Group secondary, a "downgrade to Standard to save money" recommendation is
               misleading: every replica in an AG must run the same SQL Server edition, so the decision belongs to
               the AG as a whole and must be evaluated on the primary. Emit an informational note instead and skip
               the savings estimates (#980). */
            recommendations.Add(new FinOpsRecommendation
            {
                Category = "Licensing",
                Severity = "Low",
                Confidence = "High",
                Finding = "Enterprise Edition — Availability Group secondary replica",
                Detail = "This instance is currently a secondary replica in an Availability Group. " +
                         "Every replica in an AG must run the same SQL Server edition, so edition and " +
                         "licensing decisions apply to the whole group and should be evaluated on the " +
                         "primary replica. A secondary used only for failover may also be covered by " +
                         "Software Assurance rather than separately licensed."
            });
        }
        // SQL Server 2019 (major version 15) moved TDE to Standard Edition, so on 2019+ we give
        // version-appropriate guidance rather than a TDE-specific check.
        else if (majorVersion >= 15)
        {
            recommendations.Add(new FinOpsRecommendation
            {
                Category = "Licensing",
                Severity = "High",
                Confidence = hasAdvancedAg ? "Low" : "Medium",
                Finding = "Enterprise Edition may not be required",
                Detail = "Starting with SQL Server 2019, most previously Enterprise-only features " +
                         "(including TDE, compression, partitioning, and columnstore) are available " +
                         "in Standard Edition. Review whether remaining Enterprise-only features " +
                         "(such as Always On availability groups with multiple secondaries) are in use " +
                         "before considering a downgrade to Standard Edition." + agDowngradeCaveat,
                EstMonthlySavings = monthlyCost > 0 ? monthlyCost * 0.40m : null
            });
        }
        else if (tdeDbNames.Count == 0)
        {
            // Pre-2019: TDE is the only commonly-used feature still restricted to Enterprise since 2016 SP1.
            recommendations.Add(new FinOpsRecommendation
            {
                Category = "Licensing",
                Severity = "High",
                Confidence = hasAdvancedAg ? "Medium" : "High",
                Finding = hasAdvancedAg
                    ? "Enterprise Edition — review Availability Group requirements before downgrading"
                    : "Enterprise Edition with no Enterprise-only features detected",
                Detail = "No databases use Transparent Data Encryption (TDE), the only feature " +
                         "still restricted to Enterprise Edition since SQL Server 2016 SP1. " +
                         "Review whether Standard Edition would meet workload requirements for potential license savings." +
                         agDowngradeCaveat,
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

            // Check 10: license cost impact estimate (only when features ARE in use).
            if (cpuCount > 0)
            {
                var monthlySavings = cpuCount * 5000m / 12m;
                recommendations.Add(new FinOpsRecommendation
                {
                    Category = "Licensing",
                    Severity = "Low",
                    Confidence = "Low",
                    Finding = $"Enterprise to Standard would save ~${monthlySavings:N0}/mo at list pricing ({cpuCount} cores)",
                    Detail = "Based on list pricing differential of ~$5,000/core/year between Enterprise and Standard. " +
                             "Actual savings depend on your licensing agreement. See Enterprise feature audit for downgrade blockers.",
                    EstMonthlySavings = monthlySavings
                });
            }
        }

        return recommendations;
    }

    /// <summary>
    /// Builds the Compression-candidate recommendation (check 5) from the collected per-index snapshot — the
    /// monitor-side stand-in for Lite's live <c>sys.partitions</c> scan: uncompressed
    /// (<c>data_compression_desc = 'NONE'</c>, which excludes columnstore) rowstore indexes / heaps whose reserved
    /// size is at least 1 GB. Returns null when none qualify. Severity/wording ported verbatim from Lite.
    /// </summary>
    public static FinOpsRecommendation? Compression(IEnumerable<IndexCleanupIndexInput> indexes)
    {
        var candidates = indexes
            .Where(i => string.Equals(i.DataCompressionDesc, "NONE", StringComparison.OrdinalIgnoreCase)
                && (i.ReservedMb ?? 0m) >= 1024m)
            .OrderByDescending(i => i.ReservedMb ?? 0m)
            .ToList();

        if (candidates.Count == 0)
        {
            return null;
        }

        var totalGb = candidates.Sum(c => c.ReservedMb ?? 0m) / 1024m;
        var topItems = candidates.Take(5)
            .Select(c => $"{c.SchemaName}.{c.TableName} ({(c.ReservedMb ?? 0m) / 1024m:N1}GB)")
            .ToList();

        return new FinOpsRecommendation
        {
            Category = "Storage",
            Severity = totalGb > 50 ? "High" : totalGb > 10 ? "Medium" : "Low",
            Confidence = "High",
            Finding = $"{candidates.Count} uncompressed object(s) >= 1GB ({totalGb:N1}GB total)",
            Detail = $"Large uncompressed tables/indexes: {string.Join("; ", topItems)}" +
                     (candidates.Count > 5 ? $" and {candidates.Count - 5} more" : "") +
                     ". Consider PAGE or ROW compression to reduce storage and improve I/O."
        };
    }

    /// <summary>
    /// The CPU right-sizing recommendation for one utilization row, or <c>null</c> when it has nothing to say. A window with no CPU
    /// sample reads a P95 of 0, which is "idle" only because nothing was measured. The utilization row gives that window no verdict
    /// (<c>HasCpuSample</c> is false), and the advice follows it. The count the text prints is the vCores the service objective
    /// gives an Azure SQL Database, so there it is named "vCores", as the utilization card names it; on every other edition it
    /// is the CPU count and the word stays "cores". Lite's recommendation reads the same rule.
    /// </summary>
    public static FinOpsRecommendation? CpuRightSizing(UtilizationEfficiencyDto? util, decimal monthlyCost)
    {
        if (util == null || !FinOpsUtilizationFigures.HasCpuSample(util) || util.P95CpuPct >= 30 || util.CpuCount <= 4
            || util.ProvisioningStatus == ProvisioningVerdict.NotApplicable)
            return null;

        var targetCores = Math.Max(4, (int)(util.CpuCount * (util.P95CpuPct / 70m)));
        var savingsPct = 1m - ((decimal)targetCores / util.CpuCount);
        var cpuNoun = ServerHardwareScope.CpuCoreNoun(util.EngineEdition);
        return new FinOpsRecommendation
        {
            Category = "Compute",
            Severity = util.P95CpuPct < 15 ? "High" : "Medium",
            Confidence = "Medium",
            Finding = $"CPU over-provisioned ({util.CpuCount} {cpuNoun}, P95 = {util.P95CpuPct:N1}%)",
            Detail = $"P95 CPU utilization is {util.P95CpuPct:N1}% (avg {util.AvgCpuPct:N1}%, max {util.MaxCpuPct}%) across {util.CpuCount} {cpuNoun}. " +
                     $"Consider reducing to ~{targetCores} {cpuNoun}.",
            EstMonthlySavings = monthlyCost > 0 ? monthlyCost * savingsPct * 0.60m : null
        };
    }

    /// <summary>The memory right-sizing advice: the 7-day P95 SQL Server memory against the physical RAM, or null when there are too few samples or the memory is already close to its use. The caller gates out an Azure SQL Database and a host of 8 GB or less.</summary>
    public static FinOpsRecommendation? MemoryRightSizing(UtilizationEfficiencyDto util, int p95Mb, long sampleCount, string window, decimal monthlyCost)
    {
        // Need ~16 samples to smooth a single-point anomaly without delaying the recommendation for hours.
        if (sampleCount >= 16)
        {
            var memRatio = (decimal)p95Mb / util.PhysicalMemoryMb;
            var targetMb = Math.Max(8192, p95Mb * 2);
            // Compared in the whole GB the text prints, so the advice never reads "of 8GB RAM ... reducing to ~8GB".
            if (memRatio < 0.50m && targetMb / 1024 < util.PhysicalMemoryMb / 1024)
            {
                return new FinOpsRecommendation
                {
                    Category = "Memory",
                    Severity = memRatio < 0.30m ? "High" : "Medium",
                    Confidence = "Medium",
                    Finding = $"Memory over-provisioned (P95 SQL memory uses {memRatio:P0} of {util.PhysicalMemoryMb / 1024}GB RAM)",
                    Detail = $"P95 SQL Server memory from {window} is {p95Mb:N0} MB out of {util.PhysicalMemoryMb:N0} MB physical RAM ({memRatio:P0} utilization). " +
                             $"Consider reducing to ~{targetMb / 1024}GB.",
                    EstMonthlySavings = monthlyCost > 0 ? monthlyCost * (1m - (decimal)targetMb / util.PhysicalMemoryMb) * 0.30m : null
                };
            }
        }
        return null;
    }

    /// <summary>The VM right-sizing advice: up to two rows, one for the cores and one for the memory, each only when its own prescription qualifies.</summary>
    public static List<FinOpsRecommendation> VmRightSizing(decimal p95Cpu7d, string cpuWindow, int cpuCount, int physMb,
        int p95MemMb, long memSampleCount, string memWindow, decimal monthlyCost)
    {
        var recommendations = new List<FinOpsRecommendation>();
        // CPU prescription: only if >= 4 cores.
        if (cpuCount >= 4)
        {
            int targetCores = 0;
            if (p95Cpu7d < 15)
                targetCores = Math.Max(2, cpuCount / 4);
            else if (p95Cpu7d < 30)
                targetCores = Math.Max(2, cpuCount / 2);

            if (targetCores > 0 && targetCores < cpuCount)
            {
                recommendations.Add(new FinOpsRecommendation
                {
                    Category = "Hardware",
                    Severity = "Medium",
                    Confidence = "Medium",
                    Finding = $"CPU: reduce from {cpuCount} to {targetCores} cores (P95 CPU {p95Cpu7d:N1}%)",
                    Detail = $"From {cpuWindow}, P95 CPU utilization was {p95Cpu7d:N1}%. " +
                             $"Current allocation of {cpuCount} cores can safely be reduced to {targetCores} cores.",
                    EstMonthlySavings = monthlyCost > 0
                        ? monthlyCost * (1m - (decimal)targetCores / cpuCount) * 0.50m
                        : null
                });
            }
        }

        // Memory prescription: needs >= 4 GB physical and a handful of samples.
        if (physMb >= 4096 && physMb > 0 && memSampleCount >= 16)
        {
            var memRatio = (decimal)p95MemMb / physMb;
            int targetMb = 0;
            if (memRatio < 0.25m)
                targetMb = Math.Max(4096, physMb / 4);
            else if (memRatio < 0.40m)
                targetMb = Math.Max(4096, physMb / 2);

            if (targetMb > 0 && targetMb / 1024 < physMb / 1024)
            {
                recommendations.Add(new FinOpsRecommendation
                {
                    Category = "Hardware",
                    Severity = "Medium",
                    Confidence = "Medium",
                    Finding = $"Memory: reduce from {physMb / 1024}GB to {targetMb / 1024}GB (P95 SQL memory uses {memRatio:P0})",
                    Detail = $"P95 SQL Server memory from {memWindow} is {p95MemMb:N0} MB of {physMb:N0} MB physical RAM ({memRatio:P0}). " +
                             $"Reducing to {targetMb / 1024}GB would still leave headroom.",
                    EstMonthlySavings = monthlyCost > 0
                        ? monthlyCost * (1m - (decimal)targetMb / physMb) * 0.30m
                        : null
                });
            }
        }
        return recommendations;
    }

    /// <summary>The reserved-capacity advice for a stable CPU: an average above 20 with a variation under 0.3, or null.</summary>
    public static FinOpsRecommendation? ReservedCapacity(decimal avgCpu, decimal stddevCpu)
    {
        if (avgCpu > 20 && stddevCpu > 0)
        {
            var cv = stddevCpu / avgCpu;
            if (cv < 0.3m)
            {
                var confidence = cv < 0.15m ? "High" : "Medium";
                return new FinOpsRecommendation
                {
                    Category = "Cloud",
                    Severity = "Low",
                    Confidence = confidence,
                    Finding = $"Stable CPU utilization (avg {avgCpu:N1}%, CV {cv:N2}) — reserved capacity candidate",
                    Detail = $"CPU utilization is consistently {avgCpu:N1}% with low variance (±{stddevCpu:N1}%). " +
                             "Reserved pricing typically saves 30-40% over pay-as-you-go for predictable workloads."
                };
            }
        }
        return null;
    }

    /// <summary>The dormant-database advice: the idle databases with their combined size, and the share of the monthly cost when the allocated total is known (0 means no share), or null when none is idle.</summary>
    public static FinOpsRecommendation? Dormant(IReadOnlyList<(string DatabaseName, decimal TotalSizeMb)> idleDbs, decimal allocatedTotalMb, decimal monthlyCost)
    {
        if (idleDbs.Count > 0)
        {
            var totalSizeGb = idleDbs.Sum(d => d.TotalSizeMb) / 1024m;
            var dbNames = string.Join(", ", idleDbs.Take(5).Select(d => d.DatabaseName));
            var costShare = 0m;
            if (monthlyCost > 0)
            {
                var totalMb = allocatedTotalMb;
                if (totalMb > 0)
                    costShare = (idleDbs.Sum(d => d.TotalSizeMb) / totalMb) * monthlyCost;
            }

            return new FinOpsRecommendation
            {
                Category = "Databases",
                Severity = idleDbs.Count >= 3 ? "High" : "Medium",
                Confidence = "High",
                Finding = $"{idleDbs.Count} idle database(s) consuming {totalSizeGb:N1}GB",
                Detail = $"No query activity in 7 days: {dbNames}" +
                         (idleDbs.Count > 5 ? $" and {idleDbs.Count - 5} more" : "") +
                         ". Consider archiving or removing these databases.",
                EstMonthlySavings = costShare > 0 ? costShare : null
            };
        }
        return null;
    }

    /// <summary>The dev/test advice for the database names that match a dev or test pattern, or null when none do.</summary>
    public static FinOpsRecommendation? DevTest(IReadOnlyList<string> devDbs)
    {
        if (devDbs.Count > 0)
        {
            return new FinOpsRecommendation
            {
                Category = "Environment",
                Severity = "Medium",
                Confidence = "Low",
                Finding = $"{devDbs.Count} possible dev/test database(s) on production server",
                Detail = $"Databases matching dev/test patterns: {string.Join(", ", devDbs.Take(10))}" +
                         (devDbs.Count > 10 ? $" and {devDbs.Count - 10} more" : "") +
                         ". If these are non-production workloads, consider moving to a lower-cost tier or separate server."
            };
        }
        return null;
    }

    /// <summary>The maintenance-window advice for one job that ran long.</summary>
    public static FinOpsRecommendation MaintenanceJob(MaintenanceJobRun run)
    {
        var jobName = run.JobName;
        var avgDuration = run.AvgDurationSeconds;
        var maxDuration = run.MaxDurationSeconds;
        var avgHistorical = run.AvgHistoricalSeconds;
        var timesLong = run.TimesRanLong;
        return new FinOpsRecommendation
        {
            Category = "Maintenance",
            Severity = timesLong >= 5 ? "Medium" : "Low",
            Confidence = "High",
            Finding = $"{jobName} ran long {timesLong} times in 7 days",
            Detail = $"Average duration: {FormatDuration(avgDuration)}, max: {FormatDuration(maxDuration)}, " +
                     $"historical average: {FormatDuration(avgHistorical)}. " +
                     "Review whether this job's schedule or operations need tuning."
        };
    }

    /// <summary>The storage-tier advice for the databases whose average read latency is under 5 ms and write latency under 3 ms, or null when none qualify. The window and sample totals cover the qualifying databases only.</summary>
    public static FinOpsRecommendation? StorageTier(IEnumerable<StorageTierIo> rows)
    {
        var lowLatencyDbs = new List<(string Name, decimal AvgReadMs, decimal AvgWriteMs)>();
        var storageMin = DateTime.MaxValue;
        var storageMax = DateTime.MinValue;
        long storageSamples = 0;

        foreach (var row in rows)
        {
            var dbName = row.DatabaseName;
            var totalReads = row.TotalReads;
            var totalStallRead = row.TotalStallReadMs;
            var totalWrites = row.TotalWrites;
            var totalStallWrite = row.TotalStallWriteMs;

            var avgReadMs = totalReads > 0 ? (decimal)totalStallRead / totalReads : 0m;
            var avgWriteMs = totalWrites > 0 ? (decimal)totalStallWrite / totalWrites : 0m;

            if (avgReadMs < 5m && avgWriteMs < 3m)
            {
                lowLatencyDbs.Add((dbName, avgReadMs, avgWriteMs));
                storageSamples += row.WindowSamples;
                if (row.FirstSample.HasValue && row.LastSample.HasValue)
                {
                    storageMin = row.FirstSample.Value < storageMin ? row.FirstSample.Value : storageMin;
                    storageMax = row.LastSample.Value > storageMax ? row.LastSample.Value : storageMax;
                }
            }
        }

        if (lowLatencyDbs.Count > 0)
        {
            var storageWindow = RightSizingWindow.Describe(storageSamples, storageMax > storageMin ? storageMax - storageMin : TimeSpan.Zero);
            var detail = string.Join("; ", lowLatencyDbs.Take(10)
                .Select(d => $"{d.Name} (read {d.AvgReadMs:N1}ms, write {d.AvgWriteMs:N1}ms)"));
            return new FinOpsRecommendation
            {
                Category = "Storage",
                Severity = "Low",
                Confidence = "Medium",
                Finding = $"{lowLatencyDbs.Count} database(s) with low IO latency — standard storage may suffice",
                Detail = $"These databases have avg read latency under 5ms and write under 3ms across {storageWindow}: {detail}" +
                         (lowLatencyDbs.Count > 10 ? $" and {lowLatencyDbs.Count - 10} more" : "") +
                         ". Premium/high-performance storage may not be needed."
            };
        }
        return null;
    }
}
