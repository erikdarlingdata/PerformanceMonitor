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
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the exact output of the FinOps recommendation builders that the viewer exposes as statics: every field of every
/// row (category, severity, confidence, finding, detail and the savings figure). The text is formatted in the current
/// culture, so each test runs in en-US and restores the previous culture. Money is compared as its invariant string so a
/// change in decimal scale fails too. A change that moves these builders must leave every expectation below untouched.
/// </summary>
public sealed class FinOpsRecommendationsCharacterizationTests
{
    private const string SecondaryDetail =
        "This instance is currently a secondary replica in an Availability Group. " +
        "Every replica in an AG must run the same SQL Server edition, so edition and " +
        "licensing decisions apply to the whole group and should be evaluated on the " +
        "primary replica. A secondary used only for failover may also be covered by " +
        "Software Assurance rather than separately licensed.";

    private const string Modern2019Detail =
        "Starting with SQL Server 2019, most previously Enterprise-only features " +
        "(including TDE, compression, partitioning, and columnstore) are available " +
        "in Standard Edition. Review whether remaining Enterprise-only features " +
        "(such as Always On availability groups with multiple secondaries) are in use " +
        "before considering a downgrade to Standard Edition.";

    private const string PreTdeDetail =
        "No databases use Transparent Data Encryption (TDE), the only feature " +
        "still restricted to Enterprise Edition since SQL Server 2016 SP1. " +
        "Review whether Standard Edition would meet workload requirements for potential license savings.";

    private const string AgCaveat =
        " Note: this instance is the primary replica of an Always On Availability Group. Standard Edition " +
        "supports only Basic Availability Groups, which are limited to two replicas, a single database per " +
        "group, and provide no readable secondary or backups on the secondary " +
        "(see https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/basic-availability-groups-always-on-availability-groups#limitations). " +
        "Factor this into any downgrade decision.";

    private const string CoreMathDetail =
        "Based on list pricing differential of ~$5,000/core/year between Enterprise and Standard. " +
        "Actual savings depend on your licensing agreement. See Enterprise feature audit for downgrade blockers.";

    private const string Enterprise = "Enterprise Edition (64-bit)";

    private static void InEnUs(Action body)
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("en-US");
            body();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    private static void AssertRow(RecommendationRow row, string category, string severity, string confidence,
        string finding, string detail, string? savings)
    {
        Assert.Equal(category, row.Category);
        Assert.Equal(severity, row.Severity);
        Assert.Equal(confidence, row.Confidence);
        Assert.Equal(finding, row.Finding);
        Assert.Equal(detail, row.Detail);
        Assert.Equal(savings, row.EstMonthlySavings?.ToString(CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(0)]
    public void EditionAudit_EveryBranch_FullRows(int cost)
    {
        InEnUs(() =>
        {
            var monthly = (decimal)cost;
            var forty = cost > 0 ? "400.00" : null;
            var none = Array.Empty<string>();

            var secondary = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 16, 16, none, monthly, "Secondary", true);
            AssertRow(Assert.Single(secondary), "Licensing", "Low", "High",
                "Enterprise Edition \u2014 Availability Group secondary replica", SecondaryDetail, null);

            var modern = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 15, 16, none, monthly, "Standalone", false);
            AssertRow(Assert.Single(modern), "Licensing", "High", "Medium",
                "Enterprise Edition may not be required", Modern2019Detail, forty);

            var modernPrimary = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 16, 16, none, monthly, "Primary", true);
            AssertRow(Assert.Single(modernPrimary), "Licensing", "High", "Low",
                "Enterprise Edition may not be required", Modern2019Detail + AgCaveat, forty);

            var preStandalone = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 13, 16, none, monthly, "Standalone", false);
            AssertRow(Assert.Single(preStandalone), "Licensing", "High", "High",
                "Enterprise Edition with no Enterprise-only features detected", PreTdeDetail, forty);

            var prePrimary = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 13, 16, none, monthly, "Primary", true);
            AssertRow(Assert.Single(prePrimary), "Licensing", "High", "Medium",
                "Enterprise Edition \u2014 review Availability Group requirements before downgrading", PreTdeDetail + AgCaveat, forty);

            var preTde = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 13, 16, new[] { "app_tde_a", "app_tde_b" }, monthly, "Standalone", false);
            Assert.Equal(2, preTde.Count);
            AssertRow(preTde[0], "Licensing", "Low", "High", "TDE in use \u2014 Enterprise Edition downgrade blocker",
                "The following databases use Transparent Data Encryption: app_tde_a, app_tde_b. TDE must be removed before downgrading to Standard Edition.", null);
            AssertRow(preTde[1], "Licensing", "Low", "Low", "Enterprise to Standard would save ~$6,667/mo at list pricing (16 cores)",
                CoreMathDetail, "6666.6666666666666666666666667");
        });
    }

    [Fact]
    public void EditionAudit_Tde21Databases_TruncatesAt20_AndCoreMath()
    {
        InEnUs(() =>
        {
            var names = Enumerable.Range(1, 21).Select(i => $"tde_db_{i:D2}").ToList();
            var recs = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 13, 16, names, 1000m, "Standalone", false);

            Assert.Equal(2, recs.Count);
            AssertRow(recs[0], "Licensing", "Low", "High", "TDE in use \u2014 Enterprise Edition downgrade blocker",
                "The following databases use Transparent Data Encryption: " + string.Join(", ", names.Take(20)) +
                " and 1 more. TDE must be removed before downgrading to Standard Edition.", null);
            AssertRow(recs[1], "Licensing", "Low", "Low", "Enterprise to Standard would save ~$6,667/mo at list pricing (16 cores)",
                CoreMathDetail, "6666.6666666666666666666666667");
        });
    }

    [Fact]
    public void EditionAudit_CpuCountZero_OmitsCoreMathRow()
    {
        InEnUs(() =>
        {
            var recs = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 13, 0, new[] { "app_tde_a" }, 1000m, "Standalone", false);

            AssertRow(Assert.Single(recs), "Licensing", "Low", "High", "TDE in use \u2014 Enterprise Edition downgrade blocker",
                "The following databases use Transparent Data Encryption: app_tde_a. TDE must be removed before downgrading to Standard Edition.", null);
        });
    }

    [Fact]
    public void EditionAudit_UsesTheCpuCountArgument()
    {
        InEnUs(() =>
        {
            var eight = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 13, 8, new[] { "app_tde_a" }, 0m, "Standalone", false);
            var thirtyTwo = ViewerDataService.BuildEditionAuditRecommendations(Enterprise, 13, 32, new[] { "app_tde_a" }, 0m, "Standalone", false);

            Assert.Equal("Enterprise to Standard would save ~$3,333/mo at list pricing (8 cores)", eight[1].Finding);
            Assert.Equal("3333.3333333333333333333333333", eight[1].EstMonthlySavings?.ToString(CultureInfo.InvariantCulture));
            Assert.Equal("Enterprise to Standard would save ~$13,333/mo at list pricing (32 cores)", thirtyTwo[1].Finding);
            Assert.Equal("13333.333333333333333333333333", thirtyTwo[1].EstMonthlySavings?.ToString(CultureInfo.InvariantCulture));
        });
    }

    private static IndexCleanupIndexInput Index(string table, decimal? reservedMb, string compression = "NONE") => new()
    {
        SchemaName = "dbo",
        TableName = table,
        DataCompressionDesc = compression,
        ReservedMb = reservedMb,
    };

    [Fact]
    public void Compression_SevenCandidates_Top5ByReserved_AndMore2_N1Gb()
    {
        InEnUs(() =>
        {
            var rec = ViewerDataService.BuildCompressionRecommendation(new[]
            {
                Index("t_small_a", 1024m),
                Index("t_big_b", 5120m),
                Index("t_page_c", 9999m, "PAGE"),
                Index("t_mid_d", 3072m),
                Index("t_tiny_e", 1023.9m),
                Index("t_null_f", null),
                Index("t_tie_g", 1024m),
                Index("t_top_h", 4096m),
                Index("t_low_i", 2048m),
                Index("t_half_j", 1536m),
            });

            Assert.NotNull(rec);
            AssertRow(rec!, "Storage", "Medium", "High",
                "7 uncompressed object(s) >= 1GB (17.5GB total)",
                "Large uncompressed tables/indexes: dbo.t_big_b (5.0GB); dbo.t_top_h (4.0GB); dbo.t_mid_d (3.0GB); dbo.t_low_i (2.0GB); dbo.t_half_j (1.5GB) and 2 more" +
                ". Consider PAGE or ROW compression to reduce storage and improve I/O.", null);
        });
    }

    [Theory]
    [InlineData(10240, "Low", "10.0")]
    [InlineData(10241, "Medium", "10.0")]
    [InlineData(51200, "Medium", "50.0")]
    [InlineData(51201, "High", "50.0")]
    public void Compression_SeverityEdges_10_50Gb(int reservedMb, string severity, string totalGb)
    {
        InEnUs(() =>
        {
            var rec = ViewerDataService.BuildCompressionRecommendation(new[] { Index("t_edge_a", reservedMb) });

            Assert.NotNull(rec);
            Assert.Equal("Storage", rec!.Category);
            Assert.Equal(severity, rec.Severity);
            Assert.Equal("High", rec.Confidence);
            Assert.Equal($"1 uncompressed object(s) >= 1GB ({totalGb}GB total)", rec.Finding);
            Assert.Equal($"Large uncompressed tables/indexes: dbo.t_edge_a ({totalGb}GB). Consider PAGE or ROW compression to reduce storage and improve I/O.", rec.Detail);
            Assert.Null(rec.EstMonthlySavings);
        });
    }

    private static UtilizationEfficiencyRow Util(decimal p95, int cpuCount, int engineEdition, decimal avg = 5.5m, int max = 40) => new()
    {
        ProvisioningStatus = ProvisioningVerdict.OverProvisioned,
        P95CpuPct = p95,
        AvgCpuPct = avg,
        MaxCpuPct = max,
        CpuCount = cpuCount,
        EngineEdition = engineEdition,
    };

    [Fact]
    public void CpuRightSizing_TargetCoresSavingsAndNouns()
    {
        InEnUs(() =>
        {
            /* 16 cores at P95 10%: (int)(16 * 10/70) = 2, floored to 4; savings 1000 * (1 - 4/16) * 0.60. */
            var cores = ViewerDataService.BuildCpuRightSizingRecommendation(Util(10m, 16, 3), 1000m);
            Assert.NotNull(cores);
            AssertRow(cores!, "Compute", "High", "Medium",
                "CPU over-provisioned (16 cores, P95 = 10.0%)",
                "P95 CPU utilization is 10.0% (avg 5.5%, max 40%) across 16 cores. Consider reducing to ~4 cores.",
                "450.0000");

            var noBudget = ViewerDataService.BuildCpuRightSizingRecommendation(Util(10m, 16, 3), 0m);
            Assert.Null(noBudget!.EstMonthlySavings);

            var vcores = ViewerDataService.BuildCpuRightSizingRecommendation(Util(10m, 16, 5), 1000m);
            AssertRow(vcores!, "Compute", "High", "Medium",
                "CPU over-provisioned (16 vCores, P95 = 10.0%)",
                "P95 CPU utilization is 10.0% (avg 5.5%, max 40%) across 16 vCores. Consider reducing to ~4 vCores.",
                "450.0000");

            /* 32 cores at P95 29%: (int)(32 * 29/70) = 13; Medium severity from 15% up. */
            var medium = ViewerDataService.BuildCpuRightSizingRecommendation(Util(29m, 32, 3), 1000m);
            Assert.Equal("Medium", medium!.Severity);
            Assert.Equal("CPU over-provisioned (32 cores, P95 = 29.0%)", medium.Finding);
            Assert.Contains("Consider reducing to ~13 cores.", medium.Detail, StringComparison.Ordinal);
            Assert.Equal("356.2500000", medium.EstMonthlySavings?.ToString(CultureInfo.InvariantCulture));

            /* Edges: P95 of exactly 30 and a CPU count of exactly 4 say nothing; 14.9 is High and 15 is Medium. */
            Assert.Null(ViewerDataService.BuildCpuRightSizingRecommendation(Util(30m, 16, 3), 1000m));
            Assert.Null(ViewerDataService.BuildCpuRightSizingRecommendation(Util(10m, 4, 3), 1000m));
            Assert.NotNull(ViewerDataService.BuildCpuRightSizingRecommendation(Util(10m, 5, 3), 1000m));
            Assert.Equal("High", ViewerDataService.BuildCpuRightSizingRecommendation(Util(14.9m, 16, 3), 1000m)!.Severity);
            Assert.Equal("Medium", ViewerDataService.BuildCpuRightSizingRecommendation(Util(15m, 16, 3), 1000m)!.Severity);
        });
    }

    [Fact]
    public void DevTest_KeepsInputOrder()
    {
        var matched = ViewerDataService.MatchDevTestDatabases(new[]
        {
            "zeta_DEV_a", "master", "Alpha_QA_b", "tempdb", "prod_c", "staging_d", "msdb", "TestDb_e", "model",
        });

        Assert.Equal(new[] { "zeta_DEV_a", "Alpha_QA_b", "staging_d", "TestDb_e" }, matched);
    }

    [Fact]
    public void SelectTde_OrdersIgnoreCase()
    {
        static DatabaseConfigRow Db(string name, bool encrypted, string state = "ONLINE") =>
            new() { DatabaseName = name, IsEncrypted = encrypted, StateDesc = state };

        var names = ViewerDataService.SelectTdeDatabaseNames(new[]
        {
            Db("b_db", true), Db("A_db", true), Db("C_db", true, "online"), Db("off_db", true, "OFFLINE"),
            Db("master", true), Db("plain_db", false), Db("Z_db", true),
        });

        Assert.Equal(new[] { "A_db", "b_db", "C_db", "Z_db" }, names);
    }

    [Fact]
    public void SeveritySort_AndSavingsDisplay()
    {
        InEnUs(() =>
        {
            Assert.Equal(1, new RecommendationRow { Severity = "High" }.SeveritySort);
            Assert.Equal(2, new RecommendationRow { Severity = "Medium" }.SeveritySort);
            Assert.Equal(3, new RecommendationRow { Severity = "Low" }.SeveritySort);
            Assert.Equal(4, new RecommendationRow { Severity = "" }.SeveritySort);
            Assert.Equal(4, new RecommendationRow { Severity = "high" }.SeveritySort);

            Assert.Equal("$1,234", new RecommendationRow { EstMonthlySavings = 1234m }.EstMonthlySavingsDisplay);
            Assert.Equal("$1,235", new RecommendationRow { EstMonthlySavings = 1234.5m }.EstMonthlySavingsDisplay);
            Assert.Equal("$0", new RecommendationRow { EstMonthlySavings = 0m }.EstMonthlySavingsDisplay);
            Assert.Equal("", new RecommendationRow { EstMonthlySavings = null }.EstMonthlySavingsDisplay);
        });
    }
}
