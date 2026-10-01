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
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the FinOps Recommendations sub-tab — the last deferred sub-tab, reproduced MONITOR-SIDE. Lite mixed
/// collected reads with LIVE target queries; every check here runs over the collected Postgres store. These tests
/// cover the three checks Lite ran "live" and now reproduces from collected data (edition/license audit, TDE from
/// <c>database_config.is_encrypted</c>, compression from <c>index_object_stats</c>, dev/test name match), the
/// dropped check (sp_IndexCleanup existence — superseded by native Index Analysis), the AG-nuance deferral, and the
/// collected time-series reads' SQL contract (a representative case, per the port brief — not the ported arithmetic).
/// </summary>
public sealed class ViewerFinOpsRecommendationsTests
{
    // ── Check 1: Edition / license audit branching (reproduced from collected server_properties) ──

    [Fact]
    public void EditionAudit_NonEnterprise_ReturnsNoRecommendations()
    {
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Standard Edition (64-bit)", majorVersion: 16, cpuCount: 8, Array.Empty<string>(), monthlyCost: 1000m,
            agReplicaRole: "Standalone", isHadrEnabled: false);

        Assert.Empty(recs);
    }

    [Fact]
    public void EditionAudit_Enterprise2019Plus_Standalone_SuggestsDowngrade_WithBudgetSavings_NoAgCaveat()
    {
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion: 16, cpuCount: 16, Array.Empty<string>(), monthlyCost: 1000m,
            agReplicaRole: "Standalone", isHadrEnabled: false);

        var rec = Assert.Single(recs);
        Assert.Equal("Licensing", rec.Category);
        Assert.Equal("High", rec.Severity);
        /* Standalone (no AG) => the AG-driven confidence downgrade does not apply; standalone confidence stands. */
        Assert.Equal("Medium", rec.Confidence);
        Assert.Equal("Enterprise Edition may not be required", rec.Finding);
        Assert.Equal(400m, rec.EstMonthlySavings); // 1000 * 0.40
        /* No AG present => no secondary branch, no Basic-AG caveat. */
        Assert.DoesNotContain("secondary replica", rec.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Basic Availability Groups", rec.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void EditionAudit_EnterprisePre2019_NoTde_Standalone_SuggestsDowngrade_HighConfidence()
    {
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion: 13, cpuCount: 8, Array.Empty<string>(), monthlyCost: 1000m,
            agReplicaRole: "Standalone", isHadrEnabled: false);

        var rec = Assert.Single(recs);
        Assert.Equal("High", rec.Severity);
        Assert.Equal("High", rec.Confidence);
        Assert.Equal("Enterprise Edition with no Enterprise-only features detected", rec.Finding);
        Assert.Equal(400m, rec.EstMonthlySavings);
        Assert.Contains("Transparent Data Encryption", rec.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("Basic Availability Groups", rec.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void EditionAudit_EnterprisePre2019_WithTde_BlocksDowngrade_AndAddsLicenseCoreMath()
    {
        var tdeDbs = new[] { "Sales", "Payroll" };
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion: 14, cpuCount: 8, tdeDbs, monthlyCost: 1000m,
            agReplicaRole: "Standalone", isHadrEnabled: false);

        Assert.Equal(2, recs.Count);

        var blocker = recs[0];
        Assert.Equal("TDE in use — Enterprise Edition downgrade blocker", blocker.Finding);
        Assert.Equal("Low", blocker.Severity);
        Assert.Equal("High", blocker.Confidence);
        Assert.Null(blocker.EstMonthlySavings); // a blocker carries no savings estimate
        Assert.Contains("Sales", blocker.Detail, StringComparison.Ordinal);
        Assert.Contains("Payroll", blocker.Detail, StringComparison.Ordinal);

        // Check 10: core-count list-price math (8 cores * $5,000/yr / 12 = $3,333/mo).
        var licenseMath = recs[1];
        Assert.Equal("Low", licenseMath.Confidence);
        Assert.Contains("8 cores", licenseMath.Finding, StringComparison.Ordinal);
        Assert.Contains("$3,333/mo", licenseMath.Finding, StringComparison.Ordinal);
        Assert.NotNull(licenseMath.EstMonthlySavings);
        Assert.Equal(8 * 5000m / 12m, licenseMath.EstMonthlySavings!.Value);
    }

    [Fact]
    public void EditionAudit_ZeroBudget_OmitsSavingsEstimate()
    {
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion: 15, cpuCount: 8, Array.Empty<string>(), monthlyCost: 0m,
            agReplicaRole: "Standalone", isHadrEnabled: false);

        var rec = Assert.Single(recs);
        Assert.Null(rec.EstMonthlySavings);
        Assert.Equal("", rec.EstMonthlySavingsDisplay);
    }

    // ── Check 1: the AG-aware branches restored from the collected ag_replica_role + is_hadr_enabled ──
    // (Lite's LocalDataService.FinOps.Recommendations.cs GetAgReplicaRoleAsync / GetAdvancedAgCountAsync branches.)

    [Fact]
    public void EditionAudit_Enterprise2019Plus_PrimaryReplica_DowngradesConfidenceToLow_AndAddsBasicAgCaveat()
    {
        // AG primary on Enterprise => Lite's advancedAgCount > 0 stand-in: confidence Low + Basic-AG caveat.
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion: 16, cpuCount: 16, Array.Empty<string>(), monthlyCost: 1000m,
            agReplicaRole: "Primary", isHadrEnabled: true);

        var rec = Assert.Single(recs);
        Assert.Equal("Enterprise Edition may not be required", rec.Finding);
        Assert.Equal("High", rec.Severity);
        Assert.Equal("Low", rec.Confidence);                 // downgraded from Medium by the AG signal
        Assert.Equal(400m, rec.EstMonthlySavings);           // savings still emitted for a primary/standalone
        Assert.Contains("Basic Availability Groups", rec.Detail, StringComparison.Ordinal);
        Assert.Contains("primary replica of an Always On Availability Group", rec.Detail, StringComparison.Ordinal);
        Assert.Contains("basic-availability-groups", rec.Detail, StringComparison.Ordinal); // the MS Learn link
    }

    [Fact]
    public void EditionAudit_EnterprisePre2019_NoTde_PrimaryReplica_ConfidenceMedium_AndReviewFinding()
    {
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion: 13, cpuCount: 8, Array.Empty<string>(), monthlyCost: 1000m,
            agReplicaRole: "Primary", isHadrEnabled: true);

        var rec = Assert.Single(recs);
        Assert.Equal("High", rec.Severity);
        Assert.Equal("Medium", rec.Confidence);              // downgraded from High by the AG signal
        Assert.Equal("Enterprise Edition — review Availability Group requirements before downgrading", rec.Finding);
        Assert.Equal(400m, rec.EstMonthlySavings);
        Assert.Contains("Basic Availability Groups", rec.Detail, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(16)] // 2019+
    [InlineData(13)] // pre-2019
    public void EditionAudit_SecondaryReplica_EmitsSecondaryNote_NoSavings_RegardlessOfVersion(int majorVersion)
    {
        // A secondary replica short-circuits every version/TDE branch: one informational note, no savings.
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion, cpuCount: 16, new[] { "Sales" }, monthlyCost: 1000m,
            agReplicaRole: "Secondary", isHadrEnabled: true);

        var rec = Assert.Single(recs);
        Assert.Equal("Licensing", rec.Category);
        Assert.Equal("Low", rec.Severity);
        Assert.Equal("High", rec.Confidence);
        Assert.Equal("Enterprise Edition — Availability Group secondary replica", rec.Finding);
        Assert.Null(rec.EstMonthlySavings);                  // decision belongs to the AG on the primary
        Assert.Contains("secondary replica", rec.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("same SQL Server edition", rec.Detail, StringComparison.Ordinal);
        // The secondary branch never carries the primary Basic-AG caveat.
        Assert.DoesNotContain("primary replica of an Always On", rec.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void EditionAudit_PrimaryRole_ButHadrDisabled_NoCaveat_ConfidenceStaysStandalone()
    {
        // is_hadr_enabled is load-bearing: a "Primary" role with the Always On master switch off does not
        // trigger the AG caveat/confidence downgrade (defensive — the two collected columns should agree).
        var recs = ViewerDataService.BuildEditionAuditRecommendations(
            "Enterprise Edition (64-bit)", majorVersion: 16, cpuCount: 16, Array.Empty<string>(), monthlyCost: 1000m,
            agReplicaRole: "Primary", isHadrEnabled: false);

        var rec = Assert.Single(recs);
        Assert.Equal("Medium", rec.Confidence);              // standalone confidence, not the AG-downgraded Low
        Assert.DoesNotContain("Basic Availability Groups", rec.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void EditionAudit_NullOrBlankAgRole_TreatedAsStandalone()
    {
        // A missing collected role (older store / non-AG platform) falls back to Standalone — no caveat, no throw.
        foreach (var role in new[] { "", "   " })
        {
            var recs = ViewerDataService.BuildEditionAuditRecommendations(
                "Enterprise Edition (64-bit)", majorVersion: 16, cpuCount: 16, Array.Empty<string>(), monthlyCost: 1000m,
                agReplicaRole: role, isHadrEnabled: false);

            var rec = Assert.Single(recs);
            Assert.Equal("Medium", rec.Confidence);
            Assert.DoesNotContain("Basic Availability Groups", rec.Detail, StringComparison.Ordinal);
        }
    }

    // ── Check 1 data: TDE detection from collected database_config.is_encrypted ──

    [Fact]
    public void SelectTdeDatabaseNames_PicksEncryptedOnlineUserDbs_ExcludesSystemOfflineAndUnencrypted()
    {
        var rows = new List<DatabaseConfigRow>
        {
            new() { DatabaseName = "Sales",  IsEncrypted = true,  StateDesc = "ONLINE" },   // TDE user db  -> in
            new() { DatabaseName = "Reports", IsEncrypted = false, StateDesc = "ONLINE" },   // not encrypted -> out
            new() { DatabaseName = "Archive", IsEncrypted = true,  StateDesc = "OFFLINE" },  // offline       -> out
            new() { DatabaseName = "tempdb",  IsEncrypted = true,  StateDesc = "ONLINE" },   // system db     -> out
            new() { DatabaseName = "Payroll", IsEncrypted = true,  StateDesc = "ONLINE" },   // TDE user db   -> in
        };

        var tde = ViewerDataService.SelectTdeDatabaseNames(rows);

        Assert.Equal(new[] { "Payroll", "Sales" }, tde); // ordered by name
    }

    // ── Check 7: dev/test workload name match (reproduced from the collected database list) ──

    [Fact]
    public void MatchDevTestDatabases_MatchesPatternsCaseInsensitive_ExcludesSystemDbs()
    {
        var names = new[]
        {
            "AdventureWorks_DEV", "SalesTest", "app_staging", "MyQaDb", "master", "tempdb", "Production",
        };

        var matches = ViewerDataService.MatchDevTestDatabases(names);

        Assert.Equal(new[] { "AdventureWorks_DEV", "SalesTest", "app_staging", "MyQaDb" }, matches);
        Assert.DoesNotContain("Production", matches);
        Assert.DoesNotContain("master", matches);
    }

    // ── Check 5: compression candidates (reproduced from collected index_object_stats) ──

    [Fact]
    public void BuildCompressionRecommendation_FlagsUncompressedRowstoreOver1Gb_WithStorageFraming()
    {
        var indexes = new[]
        {
            Index("Sales", "dbo", "Orders", "NONE", 30720m),          // 30 GB uncompressed -> candidate
            Index("Sales", "dbo", "OrderLines", "NONE", 30720m),      // 30 GB uncompressed -> candidate
            Index("Sales", "dbo", "Small", "NONE", 512m),             // < 1 GB             -> out
            Index("Sales", "dbo", "Compressed", "PAGE", 40960m),      // already compressed -> out
            Index("Sales", "dbo", "Cci", "COLUMNSTORE", 40960m),      // columnstore        -> out
        };

        var rec = ViewerDataService.BuildCompressionRecommendation(indexes);

        Assert.NotNull(rec);
        Assert.Equal("Storage", rec!.Category);
        Assert.Equal("High", rec.Severity); // 60 GB total > 50
        Assert.Equal("High", rec.Confidence);
        Assert.Equal("2 uncompressed object(s) >= 1GB (60.0GB total)", rec.Finding);
        Assert.Contains("dbo.Orders", rec.Detail, StringComparison.Ordinal);
        Assert.Null(rec.EstMonthlySavings); // compression candidates carry no dollar estimate (mirrors Lite)
    }

    [Theory]
    [InlineData(2048.0, "Low")]     // 2 GB total
    [InlineData(20480.0, "Medium")] // 20 GB total
    [InlineData(61440.0, "High")]   // 60 GB total
    public void BuildCompressionRecommendation_SeverityBandsOnTotalGb(double reservedMb, string expectedSeverity)
    {
        var rec = ViewerDataService.BuildCompressionRecommendation(new[]
        {
            Index("Db", "dbo", "T", "NONE", (decimal)reservedMb),
        });

        Assert.NotNull(rec);
        Assert.Equal(expectedSeverity, rec!.Severity);
    }

    [Fact]
    public void BuildCompressionRecommendation_ReturnsNull_WhenNothingUncompressedAndLarge()
    {
        var rec = ViewerDataService.BuildCompressionRecommendation(new[]
        {
            Index("Db", "dbo", "Small", "NONE", 100m),
            Index("Db", "dbo", "Compressed", "PAGE", 99999m),
        });

        Assert.Null(rec);
    }

    // ── RecommendationRow projection ──

    [Fact]
    public void RecommendationRow_SeveritySortAndSavingsDisplay()
    {
        Assert.Equal(1, new RecommendationRow { Severity = "High" }.SeveritySort);
        Assert.Equal(2, new RecommendationRow { Severity = "Medium" }.SeveritySort);
        Assert.Equal(3, new RecommendationRow { Severity = "Low" }.SeveritySort);
        Assert.Equal(4, new RecommendationRow { Severity = "Whatever" }.SeveritySort);

        Assert.Equal("$1,234", new RecommendationRow { EstMonthlySavings = 1234m }.EstMonthlySavingsDisplay);
        Assert.Equal("", new RecommendationRow { EstMonthlySavings = null }.EstMonthlySavingsDisplay);
    }

    // ── Collected time-series reads: SQL contract (representative case; not the ported arithmetic) ──

    [Fact]
    public void EditionFactsSql_ReadsServerPropertiesBaseTable_WithCpuCount()
    {
        var sql = ViewerDataService.RecommendationsEditionFactsSql;
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("edition", sql, StringComparison.Ordinal);
        Assert.Contains("product_version", sql, StringComparison.Ordinal);
        Assert.Contains("cpu_count", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EditionFactsSql_SelectsCollectedAgRoleAndHadrFlag()
    {
        // Read-path pin: the AG-aware edition branches need the collected ag_replica_role + is_hadr_enabled, so the
        // edition-facts projection must SELECT them (ServerPropertiesCollector writes both; the base table exposes
        // them — the FinOps Server Inventory read already reads the same two columns).
        var sql = ViewerDataService.RecommendationsEditionFactsSql;
        Assert.Contains("ag_replica_role", sql, StringComparison.Ordinal);
        Assert.Contains("is_hadr_enabled", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerPropertiesCollector_WritesAgRoleAndHadrColumns()
    {
        // The collected columns the AG branches depend on exist in the generated server_properties DDL — proving the
        // "Darling collects no AG state" premise false (ServerPropertiesCollector resolves ag_replica_role live from
        // sys.dm_hadr_availability_replica_states and writes is_hadr_enabled from SERVERPROPERTY).
        var serverProps = PgSchemaGenerator.CreateTable(ServerPropertiesCollector.Instance);
        Assert.Contains("ag_replica_role", serverProps, StringComparison.Ordinal);
        Assert.Contains("is_hadr_enabled", serverProps, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryP95Sql_PercentileOverMemoryStats()
    {
        var sql = ViewerDataService.RecommendationsMemoryP95Sql;
        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_server_memory_mb)", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MaintenanceWindowSql_RunningJobsThatRanLongAtLeastThreeTimes()
    {
        var sql = ViewerDataService.RecommendationsMaintenanceWindowSql;
        Assert.Contains("FROM v_running_jobs", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(CASE WHEN is_running_long THEN 1 ELSE 0 END)", sql, StringComparison.Ordinal);
        Assert.Contains(">= 3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StorageTierSql_PerDatabaseFileIoStall()
    {
        var sql = ViewerDataService.RecommendationsStorageTierSql;
        Assert.Contains("FROM v_file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_stall_read_ms)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_stall_write_ms)", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_reads) > 1000", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReservedCapacitySql_CpuMeanAndStddevStability()
    {
        var sql = ViewerDataService.RecommendationsReservedCapacitySql;
        Assert.Contains("FROM v_cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("STDDEV(sqlserver_cpu_utilization)", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING COUNT(*) >= 24", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(ViewerDataService.RecommendationsEditionFactsSql))]
    [InlineData(nameof(ViewerDataService.RecommendationsEngineEditionSql))]
    [InlineData(nameof(ViewerDataService.RecommendationsMemoryP95Sql))]
    [InlineData(nameof(ViewerDataService.RecommendationsCpuP95Sql))]
    [InlineData(nameof(ViewerDataService.RecommendationsMaintenanceWindowSql))]
    [InlineData(nameof(ViewerDataService.RecommendationsStorageTierSql))]
    [InlineData(nameof(ViewerDataService.RecommendationsReservedCapacitySql))]
    public void RecommendationReads_PgDialect_PositionalParams_NoTSqlNamedParams(string sqlName)
    {
        var sql = (string)typeof(ViewerDataService).GetField(sqlName)!.GetValue(null)!;
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    // ── Column parity: every column the reproduced checks reference exists in the generated collector DDL ──

    [Fact]
    public void RecommendationChecks_ReadColumnsThatExistInTheGeneratedTables()
    {
        var serverProps = PgSchemaGenerator.CreateTable(ServerPropertiesCollector.Instance);
        Assert.Equal("server_properties", ServerPropertiesCollector.Instance.TargetTable);
        foreach (var col in new[] { "edition", "product_version", "cpu_count", "engine_edition" })
            Assert.Contains(col, serverProps, StringComparison.Ordinal);

        var databaseConfig = PgSchemaGenerator.CreateTable(DatabaseConfigCollector.Instance);
        Assert.Equal("database_config", DatabaseConfigCollector.Instance.TargetTable);
        foreach (var col in new[] { "database_name", "is_encrypted", "state_desc" })
            Assert.Contains(col, databaseConfig, StringComparison.Ordinal);

        var indexStats = PgSchemaGenerator.CreateTable(IndexObjectStatsCollector.Instance);
        foreach (var col in new[] { "data_compression_desc", "reserved_mb", "schema_name", "table_name" })
            Assert.Contains(col, indexStats, StringComparison.Ordinal);

        var memory = PgSchemaGenerator.CreateTable(MemoryStatsCollector.Instance);
        Assert.Contains("total_server_memory_mb", memory, StringComparison.Ordinal);

        var cpu = PgSchemaGenerator.CreateTable(CpuUtilizationCollector.Instance);
        Assert.Contains("sqlserver_cpu_utilization", cpu, StringComparison.Ordinal);

        var runningJobs = PgSchemaGenerator.CreateTable(RunningJobsCollector.Instance);
        foreach (var col in new[] { "job_name", "current_duration_seconds", "avg_duration_seconds", "is_running_long" })
            Assert.Contains(col, runningJobs, StringComparison.Ordinal);

        var fileIo = PgSchemaGenerator.CreateTable(FileIoStatsCollector.Instance);
        foreach (var col in new[] { "delta_reads", "delta_stall_read_ms", "delta_writes", "delta_stall_write_ms" })
            Assert.Contains(col, fileIo, StringComparison.Ordinal);
    }

    // ── Right-sizing advice needs a measurement, and a server whose hardware is its own ──

    [Theory]
    [InlineData("", false)]                                   // no CPU sample in the window: the no-verdict value
    [InlineData(ProvisioningVerdict.OverProvisioned, true)]
    [InlineData(ProvisioningVerdict.RightSized, true)]
    [InlineData(ProvisioningVerdict.UnderProvisioned, true)]
    public void UtilizationRow_HasCpuSample_FollowsTheEmptyNoVerdictStatus(string status, bool expected)
    {
        Assert.Equal(expected, new UtilizationEfficiencyRow { ProvisioningStatus = status }.HasCpuSample);
    }

    [Fact]
    public void UtilizationRow_NeverRead_HasNoCpuSample()
    {
        // A row that nothing populated has no measurement, so the rules read it as "nothing to advise on".
        Assert.False(new UtilizationEfficiencyRow().HasCpuSample);
    }

    [Fact]
    public void EngineEditionSql_ReadsTheNewestCollectedServerPropertiesRow()
    {
        var sql = ViewerDataService.RecommendationsEngineEditionSql;
        Assert.Contains("engine_edition", sql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The three right-sizing rules run inside <c>GetRecommendationsAsync</c> over the Postgres store, which this
    /// suite does not stand up, so the gates are pinned on the rule source: the CPU rule and the VM rule read
    /// <c>HasCpuSample</c>, and the memory rule and the VM rule stand down on Azure SQL Database (edition 5).
    /// </summary>
    [Fact]
    public void CpuAndVmRightSizing_StandDownWithNoCpuSample()
    {
        var body = RightSizingRulesSource();

        Assert.Matches(
            new Regex(@"util\s*!=\s*null\s*&&\s*util\s*\.\s*HasCpuSample\s*&&\s*util\s*\.\s*P95CpuPct\s*<\s*30"),
            body);
        Assert.Matches(
            new Regex(@"vmUtil\s*!=\s*null\s*&&\s*vmUtil\s*\.\s*HasCpuSample\s*&&"),
            body);
    }

    [Fact]
    public void MemoryAndVmRightSizing_StandDownOnAzureSqlDatabase()
    {
        var body = RightSizingRulesSource();
        const string notAzureSqlDatabase =
            @"await\s+GetRecommendationEngineEditionAsync\(\s*serverId\s*,\s*cancellationToken\s*\)\s*!=\s*CollectorEngineCapability\s*\.\s*AzureSqlDatabaseEngineEdition";

        Assert.Matches(new Regex(@"util\s*\.\s*PhysicalMemoryMb\s*>\s*8192\s*&&\s*" + notAzureSqlDatabase), body);
        Assert.Matches(new Regex(@"vmUtil\s*\.\s*HasCpuSample\s*&&\s*" + notAzureSqlDatabase), body);
        Assert.Equal(5, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
    }

    private static string RightSizingRulesSource()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs"));
        var signature = source.IndexOf("Task<List<RecommendationRow>> GetRecommendationsAsync(", StringComparison.Ordinal);
        Assert.True(signature >= 0, "GetRecommendationsAsync is gone, so this pin would read nothing.");
        return CSharpSourceWalker.BraceBalanced(source, source.IndexOf('{', signature));
    }

    private static IndexCleanupIndexInput Index(string db, string schema, string table, string compression, decimal reservedMb) =>
        new()
        {
            DatabaseName = db,
            SchemaName = schema,
            TableName = table,
            DataCompressionDesc = compression,
            ReservedMb = reservedMb,
        };
}
