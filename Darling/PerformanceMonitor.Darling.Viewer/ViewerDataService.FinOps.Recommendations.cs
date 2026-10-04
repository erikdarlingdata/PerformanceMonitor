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
using PerformanceMonitor.Darling.Storage.FinOps;

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
    private static readonly IReadOnlySet<string> RecommendationsSystemDatabases = FinOpsRecommendationFigures.SystemDatabases;

    /// <summary>
    /// The server's latest collected edition / product version / logical CPU count for the license audit, plus the
    /// collected AG replica role + Always On master switch that drive the AG-aware branches. $1 server_id.
    /// </summary>
    public const string RecommendationsEditionFactsSql = DarlingFinOpsRecommendationsReader.EditionFactsSql;

    /// <summary>7-day P95 of Total Server Memory (MB) + sample count, for the memory right-sizing checks. $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsMemoryP95Sql = DarlingFinOpsRecommendationsReader.MemoryP95Sql;

    /// <summary>7-day P95 of SQL Server CPU utilization, for the VM right-sizing CPU prescription. $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsCpuP95Sql = DarlingFinOpsRecommendationsReader.CpuP95Sql;

    /// <summary>SQL Agent jobs that ran long at least 3 times in the window (maintenance-window efficiency). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsMaintenanceWindowSql = DarlingFinOpsRecommendationsReader.MaintenanceWindowSql;

    /// <summary>Per-database aggregate read/write I/O + stall over the window (storage-tier optimization). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsStorageTierSql = DarlingFinOpsRecommendationsReader.StorageTierSql;

    /// <summary>Oldest query-stats sample for the server (idle-database advice waits until it is at or before the 7-day cutoff). $1 server_id.</summary>
    public const string RecommendationsQueryStatsFirstSampleSql = DarlingFinOpsRecommendationsReader.QueryStatsFirstSampleSql;

    /// <summary>CPU utilization mean + standard deviation + sample count (reserved-capacity stability). $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string RecommendationsReservedCapacitySql = DarlingFinOpsRecommendationsReader.ReservedCapacitySql;

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
    /// The server's latest collected <c>SERVERPROPERTY('EngineEdition')</c>, for the right-sizing rules that do not
    /// apply to Azure SQL Database. Same row Lite reads (<c>GetSqlEngineEditionAsync</c>): the newest collected
    /// <c>server_properties</c> row. $1 server_id.
    /// </summary>
    public const string RecommendationsEngineEditionSql = DarlingFinOpsRecommendationsReader.EngineEditionSql;

    /// <summary>
    /// The server's latest collected engine edition, or <see cref="CollectorEngineCapability.UnknownEngineEdition"/>
    /// when nothing is collected yet (or the row carries no edition).
    /// </summary>
    public async Task<int> GetRecommendationEngineEditionAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(RecommendationsEngineEditionSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0)
            ? Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture)
            : CollectorEngineCapability.UnknownEngineEdition;
    }

    /// <summary>
    /// Selects the databases running Transparent Data Encryption from a collected database-config snapshot — the
    /// monitor-side stand-in for Lite's live <c>dm_db_persisted_sku_features</c> probe: <c>is_encrypted = true</c>
    /// marks a TDE database. System databases (Lite's <c>database_id &gt; 4</c>) and non-ONLINE databases are
    /// excluded, mirroring Lite's probe. Ordered by name for stable output.
    /// </summary>
    public static List<string> SelectTdeDatabaseNames(IEnumerable<DatabaseConfigRow> configRows) =>
        FinOpsRecommendationFigures.SelectTdeDatabaseNames(
            configRows.Select(r => new DatabaseEncryptionFact(r.DatabaseName, r.StateDesc, r.IsEncrypted)));

    /// <summary>
    /// Matches the collected database names against Lite's dev/test name patterns (<c>%dev% / %test% / %staging% /
    /// %qa%</c>, case-insensitive) excluding the system databases (Lite's <c>database_id &gt; 4</c>). Pure so the
    /// pattern logic is unit-testable without a store.
    /// </summary>
    public static List<string> MatchDevTestDatabases(IEnumerable<string> databaseNames) =>
        FinOpsRecommendationFigures.MatchDevTestDatabases(databaseNames);

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
        return FinOpsRecommendationFigures.EditionAudit(
            edition, majorVersion, cpuCount, tdeDbNames, monthlyCost, agReplicaRole, isHadrEnabled)
            .Select(RecommendationRow.From).ToList();
    }

    /// <summary>
    /// Builds the Compression-candidate recommendation (check 5) from the collected per-index snapshot — the
    /// monitor-side stand-in for Lite's live <c>sys.partitions</c> scan: uncompressed
    /// (<c>data_compression_desc = 'NONE'</c>, which excludes columnstore) rowstore indexes / heaps whose reserved
    /// size is at least 1 GB. Returns null when none qualify. Severity/wording ported verbatim from Lite.
    /// </summary>
    public static RecommendationRow? BuildCompressionRecommendation(IEnumerable<IndexCleanupIndexInput> indexes)
    {
        var recommendation = FinOpsRecommendationFigures.Compression(indexes);
        return recommendation == null ? null : RecommendationRow.From(recommendation);
    }

    /// <summary>
    /// The CPU right-sizing recommendation for one utilization row, or <c>null</c> when it has nothing to say. A window with no CPU
    /// sample reads a P95 of 0, which is "idle" only because nothing was measured. The utilization row gives that window no verdict
    /// (<c>HasCpuSample</c> is false), and the advice follows it. The count the text prints is the vCores the service objective
    /// gives an Azure SQL Database, so there it is named "vCores", as the utilization card names it; on every other edition it
    /// is the CPU count and the word stays "cores". Lite's recommendation reads the same rule.
    /// </summary>
    internal static RecommendationRow? BuildCpuRightSizingRecommendation(UtilizationEfficiencyRow? util, decimal monthlyCost)
    {
        var recommendation = FinOpsRecommendationFigures.CpuRightSizing(util?.ToDto(), monthlyCost);
        return recommendation == null ? null : RecommendationRow.From(recommendation);
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
            var cpuRecommendation = BuildCpuRightSizingRecommendation(util, monthlyCost);
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
            var util = await GetUtilizationEfficiencyAsync(serverId, cancellationToken);
            /* No memory advice on an Azure SQL Database (engine_edition 5): its memory comes with its service objective
               and cannot be resized on its own. util.PhysicalMemoryMb is the database's own memory limit there
               (memory_stats, filled from committed_target_kb), not the host's, so the skip is not about a wrong
               denominator: there is nothing to resize. Managed Instance (8) and SQL Server are unchanged. */
            if (util != null && util.PhysicalMemoryMb > 8192
                && await GetRecommendationEngineEditionAsync(serverId, cancellationToken) != CollectorEngineCapability.AzureSqlDatabaseEngineEdition)
            {
                var (p95Mb, sampleCount, window) = await ReadMemoryP95Async(serverId, memoryCutoff, cancellationToken);

                var memoryRecommendation = FinOpsRecommendationFigures.MemoryRightSizing(util.ToDto(), p95Mb, sampleCount, window, monthlyCost);
                if (memoryRecommendation != null)
                    recommendations.Add(RecommendationRow.From(memoryRecommendation));
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
            /* "No query activity in 7 days" is only true once 7 days of query stats exist: a server enrolled hours
               ago has not been watched long enough to call any database idle. */
            var idleDbs = await HasQueryStatsCoverageAsync(serverId, memoryCutoff, cancellationToken)
                ? await GetIdleDatabasesAsync(serverId, cancellationToken: cancellationToken)
                : new List<IdleDatabaseRow>();
            if (idleDbs.Count > 0)
            {
                var allocatedTotalMb = 0m;
                if (monthlyCost > 0)
                {
                    var allDbSizes = await GetDatabaseSizeLatestAsync(serverId, cancellationToken);
                    var totalMb = DatabaseSizeRow.AllocatedTotalMb(allDbSizes);
                    if (totalMb > 0)
                        allocatedTotalMb = totalMb;
                }

                var dormantRecommendation = FinOpsRecommendationFigures.Dormant(
                    idleDbs.Select(d => (d.DatabaseName, d.TotalSizeMb)).ToList(), allocatedTotalMb, monthlyCost);
                if (dormantRecommendation != null)
                    recommendations.Add(RecommendationRow.From(dormantRecommendation));
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
            var devTestRecommendation = FinOpsRecommendationFigures.DevTest(devDbs);
            if (devTestRecommendation != null)
                recommendations.Add(RecommendationRow.From(devTestRecommendation));
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

                recommendations.Add(RecommendationRow.From(FinOpsRecommendationFigures.MaintenanceJob(
                    new MaintenanceJobRun(jobName, avgDuration, maxDuration, avgHistorical, timesLong))));
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
            /* No VM to resize on Azure SQL Database (its cores and memory come with its service objective),
               and no advice from a window with no CPU sample (its P95 of 0 is not a measurement). */
            if (vmUtil != null && vmUtil.HasCpuSample
                && await GetRecommendationEngineEditionAsync(serverId, cancellationToken) != CollectorEngineCapability.AzureSqlDatabaseEngineEdition)
            {
                decimal p95Cpu7d = vmUtil.P95CpuPct;
                var cpuWindow = "recent samples"; // neutral until the 7-day read supplies its own span; the 24-hour fallback has no span of its own
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
                        cpuWindow = RightSizingWindow.Describe(cpuReader.IsDBNull(3) ? 0L : Convert.ToInt64(cpuReader.GetValue(3), CultureInfo.InvariantCulture), cpuReader.IsDBNull(1) || cpuReader.IsDBNull(2) ? TimeSpan.Zero : cpuReader.GetDateTime(2) - cpuReader.GetDateTime(1));
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Recommendation check (VM right-sizing) 7-day CPU P95 fell back to 24h: {ex.Message}");
                }

                var (p95MemMb, memSampleCount, memWindow) = await ReadMemoryP95Async(serverId, memoryCutoff, cancellationToken);

                foreach (var vmRecommendation in FinOpsRecommendationFigures.VmRightSizing(
                    p95Cpu7d, cpuWindow, cpuCount, physMb, p95MemMb, memSampleCount, memWindow, monthlyCost))
                    recommendations.Add(RecommendationRow.From(vmRecommendation));
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (VM right-sizing): {ex.Message}");
        }

        // 13. Storage tier optimization — databases with low I/O latency (collected file_io_stats).
        try
        {
            var storageRows = new List<StorageTierIo>();

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

                    storageRows.Add(new StorageTierIo(dbName, totalReads, totalStallRead, totalWrites, totalStallWrite,
                        reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                        reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                        reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture)));
                }
            }

            var storageRecommendation = FinOpsRecommendationFigures.StorageTier(storageRows);
            if (storageRecommendation != null)
                recommendations.Add(RecommendationRow.From(storageRecommendation));
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

                var reservedRecommendation = FinOpsRecommendationFigures.ReservedCapacity(avgCpu, stddevCpu);
                if (reservedRecommendation != null)
                    recommendations.Add(RecommendationRow.From(reservedRecommendation));
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Recommendation check failed (Reserved capacity): {ex.Message}");
        }

        return recommendations.OrderBy(r => r.SeveritySort).ToList();
    }

    /// <summary>True once the server's query stats reach back to the start of the 7-day window. The advice text claims 7 days, so the data must cover all 7: the first sample has to be at or before the cutoff, with no slack.</summary>
    private async Task<bool> HasQueryStatsCoverageAsync(int serverId, DateTime cutoff, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(RecommendationsQueryStatsFirstSampleSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var first = await command.ExecuteScalarAsync(cancellationToken);
        return first is DateTime firstSample && firstSample <= cutoff;
    }

    /// <summary>Reads the 7-day P95 Total Server Memory (MB) + sample count (shared by the memory + VM right-sizing checks).</summary>
    private async Task<(int P95Mb, long SampleCount, string Window)> ReadMemoryP95Async(int serverId, DateTime cutoff, CancellationToken cancellationToken)
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
            var window = RightSizingWindow.Describe(reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture), reader.IsDBNull(2) || reader.IsDBNull(3) ? TimeSpan.Zero : reader.GetDateTime(3) - reader.GetDateTime(2));
            return (p95Mb, sampleCount, window);
        }

        return (0, 0L, RightSizingWindow.Describe(0, TimeSpan.Zero));
    }

    /// <summary>Human-readable duration formatting for the maintenance-window finding (Lite's FinOps FormatDuration, verbatim).</summary>
    private static string FormatDuration(long seconds) => FinOpsRecommendationFigures.FormatDuration(seconds);
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
    public int SeveritySort => FinOpsRecommendationFigures.SeveritySort(Severity);

    /// <summary>The viewer row for a Storage recommendation.</summary>
    public static RecommendationRow From(FinOpsRecommendation r) => new()
    {
        Category = r.Category,
        Severity = r.Severity,
        Confidence = r.Confidence,
        Finding = r.Finding,
        Detail = r.Detail,
        EstMonthlySavings = r.EstMonthlySavings
    };
}
