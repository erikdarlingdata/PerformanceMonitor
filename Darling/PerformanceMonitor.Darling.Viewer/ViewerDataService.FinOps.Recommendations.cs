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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/*
 * FinOps Recommendations sub-tab reads (the last deferred FinOps sub-tab from the #1372 port). Lite's
 * GetRecommendationsAsync (LocalDataService.FinOps.Recommendations.cs) runs ~11 checks, MIXING collected DuckDB
 * reads with LIVE queries against the target. The headless viewer can't reach targets, so every check here runs
 * MONITOR-SIDE over already-collected Postgres data — NO live SQL:
 *   - The time-series checks (CPU/memory right-sizing, dormant DBs, maintenance window, VM right-sizing, storage
 *     tier, reserved capacity) reuse the Utilization/Storage FinOps helpers the port already created, or read the
 *     same v_* views Lite's DuckDB versions did.
 *   - The three checks Lite ran "live" are reproduced from collected data: the Edition/license audit reads the
 *     collected server_properties + database_config.is_encrypted (instead of a live dm_db_persisted_sku_features
 *     probe); Compression candidates come from the collected index_object_stats snapshot (data_compression_desc);
 *     Dev/test detection matches the collected database name list.
 *   - Lite's sp_IndexCleanup-existence check (check 4) is DROPPED — Darling has native monitor-side Index Analysis
 *     (the FinOps Index Analysis sub-tab, #1387), so the "install sp_IndexCleanup" recommendation is obsolete.
 *   - AG-topology nuance (secondary-replica branch, advanced-AG downgrade caveat, AG-driven confidence downgrades)
 *     IS reproduced from the collected server_properties.ag_replica_role + is_hadr_enabled (ServerPropertiesCollector
 *     resolves the role live from sys.dm_hadr_availability_replica_states; is_hadr_enabled is SERVERPROPERTY). The one
 *     Lite input Darling does not collect is the non-basic AG COUNT (sys.availability_groups.basic_features); on an
 *     Enterprise instance a hosted AG (role = Primary) is effectively always advanced — Basic AGs are Standard-only —
 *     so the collected primary role stands in for Lite's advanced-AG count. No AG data is invented.
 * The per-check threshold/severity/confidence logic and wording are ported VERBATIM from Lite (not re-tuned). Each
 * check is independently try/caught (Lite's structure) so one failing read never suppresses the others. SQL kept
 * in public const so tests pin it.
 */

public sealed partial class ViewerDataService
{
    /// <summary>The four system databases (master/model/msdb/tempdb) — Lite's <c>database_id &gt; 4</c> filter by name.</summary>
    private static readonly HashSet<string> RecommendationsSystemDatabases =
        new(StringComparer.OrdinalIgnoreCase) { "master", "model", "msdb", "tempdb" };

    /// <summary>
    /// The server's latest collected edition / product version / logical CPU count for the license audit, plus the
    /// collected AG replica role + Always On master switch that drive the AG-aware branches. $1 server_id.
    /// </summary>
    public const string RecommendationsEditionFactsSql = @"
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
    public const string RecommendationsMemoryP95Sql = @"
SELECT
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_server_memory_mb) AS p95_mb,
    COUNT(*) AS sample_count
FROM v_memory_stats
WHERE server_id = $1
AND   collection_time >= $2";

    /// <summary>7-day P95 of SQL Server CPU utilization, for the VM right-sizing CPU prescription. $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsCpuP95Sql = @"
SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2";

    /// <summary>SQL Agent jobs that ran long at least 3 times in the window (maintenance-window efficiency). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsMaintenanceWindowSql = @"
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
    public const string RecommendationsStorageTierSql = @"
SELECT
    database_name,
    SUM(delta_reads) AS total_reads,
    SUM(delta_stall_read_ms) AS total_stall_read_ms,
    SUM(delta_writes) AS total_writes,
    SUM(delta_stall_write_ms) AS total_stall_write_ms
FROM v_file_io_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   delta_reads > 0
GROUP BY database_name
HAVING SUM(delta_reads) > 1000";

    /// <summary>CPU utilization mean + standard deviation + sample count (reserved-capacity stability). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsReservedCapacitySql = @"
SELECT
    AVG(sqlserver_cpu_utilization) AS avg_cpu,
    STDDEV(sqlserver_cpu_utilization) AS stddev_cpu,
    COUNT(*) AS sample_count
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2
HAVING COUNT(*) >= 24";

    /// <summary>
    /// The server's latest collected edition facts for the license audit (null when nothing collected yet), including
    /// the collected Availability Group replica role (<c>Primary</c> / <c>Secondary</c> / <c>Standalone</c>) and the
    /// Always On master switch (<c>is_hadr_enabled</c>) that drive the AG-aware edition/license branches.
    /// </summary>
    public readonly record struct EditionFacts(
        string Edition, int MajorVersion, int CpuCount, string AgReplicaRole, bool IsHadrEnabled);

    /// <summary>Reads the server's latest collected edition / product-version-major / CPU count + AG role / HADR flag, or null when no server_properties row exists yet.</summary>
    public async Task<EditionFacts?> GetEditionFactsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(RecommendationsEditionFactsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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
        return new EditionFacts(edition, ParseMajorVersion(productVersion), cpuCount, agReplicaRole, isHadrEnabled);
    }

    /// <summary>
    /// Selects the databases running Transparent Data Encryption from a collected database-config snapshot — the
    /// monitor-side stand-in for Lite's live <c>dm_db_persisted_sku_features</c> probe: <c>is_encrypted = true</c>
    /// marks a TDE database. System databases (Lite's <c>database_id &gt; 4</c>) and non-ONLINE databases are
    /// excluded, mirroring Lite's probe. Ordered by name for stable output.
    /// </summary>
    public static List<string> SelectTdeDatabaseNames(IEnumerable<DatabaseConfigRow> configRows) =>
        configRows
            .Where(r => r.IsEncrypted
                && string.Equals(r.StateDesc, "ONLINE", StringComparison.OrdinalIgnoreCase)
                && !RecommendationsSystemDatabases.Contains(r.DatabaseName))
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
            .Where(n => !RecommendationsSystemDatabases.Contains(n) && IsDevTest(n))
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
    public static List<RecommendationRow> BuildEditionAuditRecommendations(
        string edition,
        int majorVersion,
        int cpuCount,
        IReadOnlyList<string> tdeDbNames,
        decimal monthlyCost,
        string agReplicaRole,
        bool isHadrEnabled)
    {
        var recommendations = new List<RecommendationRow>();

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
            recommendations.Add(new RecommendationRow
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
            recommendations.Add(new RecommendationRow
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
            recommendations.Add(new RecommendationRow
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

            // Check 10: license cost impact estimate (only when features ARE in use).
            if (cpuCount > 0)
            {
                var monthlySavings = cpuCount * 5000m / 12m;
                recommendations.Add(new RecommendationRow
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
    public static RecommendationRow? BuildCompressionRecommendation(IEnumerable<IndexCleanupIndexInput> indexes)
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

        return new RecommendationRow
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
    /// Runs every monitor-side FinOps recommendation check over the collected store and returns the consolidated
    /// list sorted by severity. All reads are async I/O (they don't block the UI thread), and each check is
    /// isolated in its own try/catch so a single failing read degrades to "that check absent" rather than an
    /// empty tab. <paramref name="monthlyCost"/> is the per-server budget (0 → findings emit with no savings
    /// estimate, mirroring Lite's <c>monthlyCost &gt; 0 ? … : null</c>).
    /// </summary>
    public async Task<List<RecommendationRow>> GetRecommendationsAsync(int serverId, decimal monthlyCost, CancellationToken cancellationToken = default)
    {
        var recommendations = new List<RecommendationRow>();
        var memoryCutoff = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified);

        // 1. Enterprise feature / license audit (collected server_properties + database_config.is_encrypted).
        try
        {
            var facts = await GetEditionFactsAsync(serverId, cancellationToken);
            if (facts is { } f)
            {
                var isEnterprise = f.Edition.Contains("Enterprise", StringComparison.OrdinalIgnoreCase);

                // TDE is only the deciding factor on pre-2019 Enterprise — read the config snapshot just then.
                var tdeDbNames = new List<string>();
                if (isEnterprise && f.MajorVersion < 15)
                {
                    var configRows = await GetLatestDatabaseConfigAsync(serverId, cancellationToken: cancellationToken);
                    tdeDbNames = SelectTdeDatabaseNames(configRows);
                }

                recommendations.AddRange(
                    BuildEditionAuditRecommendations(
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
            var util = await GetUtilizationEfficiencyAsync(serverId, cancellationToken);
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
            Debug.WriteLine($"Recommendation check failed (CPU right-sizing): {ex.Message}");
        }

        // 3. Memory right-sizing (7-day P95 Total Server Memory vs physical RAM).
        try
        {
            var util = await GetUtilizationEfficiencyAsync(serverId, cancellationToken);
            if (util != null && util.PhysicalMemoryMb > 8192)
            {
                var (p95Mb, sampleCount) = await ReadMemoryP95Async(serverId, memoryCutoff, cancellationToken);

                // Need ~16 samples to smooth a single-point anomaly without delaying the recommendation for hours.
                if (sampleCount >= 16)
                {
                    var memRatio = (decimal)p95Mb / util.PhysicalMemoryMb;
                    if (memRatio < 0.50m)
                    {
                        var targetMb = Math.Max(8192, p95Mb * 2);
                        recommendations.Add(new RecommendationRow
                        {
                            Category = "Memory",
                            Severity = memRatio < 0.30m ? "High" : "Medium",
                            Confidence = "Medium",
                            Finding = $"Memory over-provisioned (P95 SQL memory uses {memRatio:P0} of {util.PhysicalMemoryMb / 1024}GB RAM)",
                            Detail = $"P95 SQL Server memory over 7 days is {p95Mb:N0} MB out of {util.PhysicalMemoryMb:N0} MB physical RAM ({memRatio:P0} utilization). " +
                                     $"Consider reducing to ~{targetMb / 1024}GB.",
                            EstMonthlySavings = monthlyCost > 0 ? monthlyCost * (1m - (decimal)targetMb / util.PhysicalMemoryMb) * 0.30m : null
                        });
                    }
                }
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
            var indexes = await GetIndexCleanupInputsAsync(serverId, cancellationToken);
            var rec = BuildCompressionRecommendation(indexes);
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
            var idleDbs = await GetIdleDatabasesAsync(serverId, cancellationToken: cancellationToken);
            if (idleDbs.Count > 0)
            {
                var totalSizeGb = idleDbs.Sum(d => d.TotalSizeMb) / 1024m;
                var dbNames = string.Join(", ", idleDbs.Take(5).Select(d => d.DatabaseName));
                var costShare = 0m;
                if (monthlyCost > 0)
                {
                    var allDbSizes = await GetDatabaseSizeLatestAsync(serverId, cancellationToken);
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
            Debug.WriteLine($"Recommendation check failed (Dormant databases): {ex.Message}");
        }

        // 7. Dev/test workload detection (collected database name list).
        try
        {
            var configRows = await GetLatestDatabaseConfigAsync(serverId, cancellationToken: cancellationToken);
            var devDbs = MatchDevTestDatabases(configRows.Select(r => r.DatabaseName));
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
            Debug.WriteLine($"Recommendation check failed (Dev/test detection): {ex.Message}");
        }

        // 11. Maintenance window efficiency — jobs running long (collected running_jobs).
        try
        {
            await using var command = _dataSource.CreateCommand(RecommendationsMaintenanceWindowSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = memoryCutoff });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var jobName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var avgDuration = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture);
                var maxDuration = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture);
                var avgHistorical = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture);
                var timesLong = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture);

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
            Debug.WriteLine($"Recommendation check failed (Maintenance window): {ex.Message}");
        }

        // 12. VM right-sizing — prescriptive core/memory targets (collected 7-day P95 CPU + memory).
        try
        {
            var vmUtil = await GetUtilizationEfficiencyAsync(serverId, cancellationToken);
            if (vmUtil != null)
            {
                decimal p95Cpu7d = vmUtil.P95CpuPct;
                int cpuCount = vmUtil.CpuCount;
                int physMb = vmUtil.PhysicalMemoryMb;

                // Prefer the 7-day P95 CPU; fall back to the 24-hour P95 already on vmUtil.
                try
                {
                    await using var cpuCommand = _dataSource.CreateCommand(RecommendationsCpuP95Sql);
                    cpuCommand.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                    cpuCommand.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                    cpuCommand.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = memoryCutoff });

                    await using var cpuReader = await cpuCommand.ExecuteReaderAsync(cancellationToken);
                    if (await cpuReader.ReadAsync(cancellationToken) && !cpuReader.IsDBNull(0))
                    {
                        p95Cpu7d = Convert.ToDecimal(cpuReader.GetValue(0), CultureInfo.InvariantCulture);
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Recommendation check (VM right-sizing) 7-day CPU P95 fell back to 24h: {ex.Message}");
                }

                var (p95MemMb, memSampleCount) = await ReadMemoryP95Async(serverId, memoryCutoff, cancellationToken);

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

                // Memory prescription: needs >= 4 GB physical and a handful of samples.
                if (physMb >= 4096 && physMb > 0 && memSampleCount >= 16)
                {
                    var memRatio = (decimal)p95MemMb / physMb;
                    int targetMb = 0;
                    if (memRatio < 0.25m)
                        targetMb = Math.Max(4096, physMb / 4);
                    else if (memRatio < 0.40m)
                        targetMb = Math.Max(4096, physMb / 2);

                    if (targetMb > 0 && targetMb < physMb)
                    {
                        recommendations.Add(new RecommendationRow
                        {
                            Category = "Hardware",
                            Severity = "Medium",
                            Confidence = "Medium",
                            Finding = $"Memory: reduce from {physMb / 1024}GB to {targetMb / 1024}GB (P95 SQL memory uses {memRatio:P0})",
                            Detail = $"P95 SQL Server memory over 7 days is {p95MemMb:N0} MB of {physMb:N0} MB physical RAM ({memRatio:P0}). " +
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
            Debug.WriteLine($"Recommendation check failed (VM right-sizing): {ex.Message}");
        }

        // 13. Storage tier optimization — databases with low I/O latency (collected file_io_stats).
        try
        {
            var lowLatencyDbs = new List<(string Name, decimal AvgReadMs, decimal AvgWriteMs)>();

            await using (var command = _dataSource.CreateCommand(RecommendationsStorageTierSql))
            {
                command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = memoryCutoff });

                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var dbName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                    var totalReads = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
                    var totalStallRead = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture);
                    var totalWrites = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture);
                    var totalStallWrite = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture);

                    var avgReadMs = totalReads > 0 ? (decimal)totalStallRead / totalReads : 0m;
                    var avgWriteMs = totalWrites > 0 ? (decimal)totalStallWrite / totalWrites : 0m;

                    if (avgReadMs < 5m && avgWriteMs < 3m)
                    {
                        lowLatencyDbs.Add((dbName, avgReadMs, avgWriteMs));
                    }
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
            Debug.WriteLine($"Recommendation check failed (Storage tier): {ex.Message}");
        }

        // 14. Reserved capacity candidates — stable CPU utilization (collected cpu_utilization_stats).
        try
        {
            await using var command = _dataSource.CreateCommand(RecommendationsReservedCapacitySql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = memoryCutoff });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0))
            {
                var avgCpu = Convert.ToDecimal(reader.GetValue(0), CultureInfo.InvariantCulture);
                var stddevCpu = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture);

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
            Debug.WriteLine($"Recommendation check failed (Reserved capacity): {ex.Message}");
        }

        return recommendations.OrderBy(r => r.SeveritySort).ToList();
    }

    /// <summary>Reads the 7-day P95 Total Server Memory (MB) + sample count (shared by the memory + VM right-sizing checks).</summary>
    private async Task<(int P95Mb, long SampleCount)> ReadMemoryP95Async(int serverId, DateTime cutoff, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(RecommendationsMemoryP95Sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            var p95Mb = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            var sampleCount = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
            return (p95Mb, sampleCount);
        }

        return (0, 0L);
    }

    /// <summary>Human-readable duration formatting for the maintenance-window finding (Lite's FinOps FormatDuration, verbatim).</summary>
    private static string FormatDuration(long seconds)
    {
        if (seconds >= 3600)
            return $"{seconds / 3600}h {(seconds % 3600) / 60}m {seconds % 60}s";
        if (seconds >= 60)
            return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds}s";
    }
}

/// <summary>
/// One FinOps recommendation row — a flat projection mirroring Lite's <c>RecommendationRow</c>
/// (LocalDataService.FinOps.cs): Category / Severity / Confidence / Finding / Detail / EstMonthlySavings, with a
/// formatted savings string and the severity sort key the grid orders by. Copied (not promoted) so Lite stays
/// untouched.
/// </summary>
public sealed class RecommendationRow
{
    public string Category { get; set; } = "";
    public string Severity { get; set; } = "";
    public string Confidence { get; set; } = "";
    public string Finding { get; set; } = "";
    public string Detail { get; set; } = "";
    public decimal? EstMonthlySavings { get; set; }
    public string EstMonthlySavingsDisplay => EstMonthlySavings.HasValue ? $"${EstMonthlySavings.Value:N0}" : "";
    public int SeveritySort => Severity switch
    {
        "High" => 1,
        "Medium" => 2,
        "Low" => 3,
        _ => 4
    };
}
