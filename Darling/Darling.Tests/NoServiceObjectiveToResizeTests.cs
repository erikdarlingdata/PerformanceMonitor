/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An Azure SQL Database logical server's <c>master</c> has no service objective to resize, so FinOps gives it no
/// right-sizing advice and no provisioning verdict.
/// </summary>
public sealed class NoServiceObjectiveToResizeTests
{
    [Theory]
    [InlineData(5, "Azure SQL Database (System)", true)]
    [InlineData(5, "Azure SQL Database (General Purpose)", false)]
    [InlineData(5, "Azure SQL Database (Hyperscale)", false)]
    [InlineData(5, null, false)]
    [InlineData(5, "System", false)]
    [InlineData(2, "Azure SQL Database (System)", false)]
    [InlineData(null, "Azure SQL Database (System)", false)]
    [InlineData(2, "Enterprise Edition (System)", false)]
    public void Predicate_IsTrueOnlyForEdition5WithTheSystemDatabaseEdition(int? engineEdition, string? edition, bool expected) =>
        Assert.Equal(expected, ServerHardwareScope.IsLogicalServerMaster(engineEdition, edition));

    [Fact]
    public void RecommendationRules_StandDownOnTheSharedPredicate()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs"));
        Assert.Contains("ProvisioningVerdict.NotApplicable", source, StringComparison.Ordinal);
    }

    [Fact]
    public void CpuRightSizingRecommendation_StandsDownForNotApplicable_AndAdvisesForOverProvisioned()
    {
        var row = new UtilizationEfficiencyRow
        {
            ProvisioningStatus = ProvisioningVerdict.NotApplicable,
            AvgCpuPct = 2m,
            MaxCpuPct = 10,
            P95CpuPct = 5m,
            CpuCount = 32,
            EngineEdition = 5,
        };
        Assert.Null(ViewerDataService.BuildCpuRightSizingRecommendation(row, 0m));

        row.ProvisioningStatus = ProvisioningVerdict.OverProvisioned;
        Assert.NotNull(ViewerDataService.BuildCpuRightSizingRecommendation(row, 0m));
    }

    [Fact]
    public void ProvisioningTrendRow_ShowsNotApplicableAsTheCardDoes()
    {
        Assert.Equal(ProvisioningVerdict.NotApplicableLabel, new ProvisioningTrendRow { Status = ProvisioningVerdict.NotApplicable }.StatusDisplay);
        Assert.Equal("OVER PROVISIONED", new ProvisioningTrendRow { Status = ProvisioningVerdict.OverProvisioned }.StatusDisplay);
    }

    /// <summary>The constant is the collector's stored edition for master: the Azure prefix, then the database edition
    /// (<c>System</c>) in parentheses, passed through by the ELSE arm with no mapping of its own.</summary>
    [Fact]
    public void LogicalServerMasterEdition_MatchesTheCollectorsFormat()
    {
        var sql = System.Text.RegularExpressions.Regex.Replace(
            RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", "ServerPropertiesCollector.cs"), @"/\*.*?\*/", "", RegexOptions.Singleline);

        Assert.Matches(new Regex(@"THEN\s+N'Azure SQL Database'\s*\+\s*ISNULL\(N' \('"), sql);
        Assert.Matches(new Regex(@"ELSE\s+CONVERT\(nvarchar\(128\),\s*DATABASEPROPERTYEX\(DB_NAME\(\),\s*N'Edition'\)\)\s+END\s*\+\s*N'\)'"), sql);
        Assert.DoesNotContain("WHEN N'System'", sql, StringComparison.Ordinal);
        Assert.Equal("Azure SQL Database" + " (" + "System" + ")", ServerHardwareScope.AzureSqlDatabaseSystemEdition);
    }

    [Theory]
    [InlineData("ViewerDataService.FinOps.Utilization.cs")]
    [InlineData("ViewerDataService.FinOps.Inventory.cs")]
    public void ProvisioningVerdictCallers_PassTheEdition(string file)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file));
        Assert.Matches(new Regex(@"ProvisioningVerdict\s*\.\s*Evaluate\s*\([^;]*edition", RegexOptions.Singleline), source);
    }
}
