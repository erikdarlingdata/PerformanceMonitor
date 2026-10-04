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
}
