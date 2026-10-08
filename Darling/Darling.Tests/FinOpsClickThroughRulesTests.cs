/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// The FinOps rules the click-through found in Lite and brought to Darling: one CPU right-sizing row, no licensing advice for free
/// editions, an idle claim that needs each of the last 7 UTC days watched, no health score without a CPU sample, and a Storage
/// Growth baseline that is the sample nearest its mark. Each rule lives once in the Storage helpers, so the Viewer, the web page and
/// the MCP tools agree.
/// </summary>
public class FinOpsClickThroughRulesTests
{
    [Theory]
    [InlineData("Enterprise Edition: Core-based Licensing (64-bit)", true)]
    [InlineData("Enterprise Developer Edition (64-bit)", false)]
    [InlineData("Enterprise Evaluation Edition (64-bit)", false)]
    [InlineData("Developer Edition (64-bit)", false)]
    [InlineData("Express Edition with Advanced Services (64-bit)", false)]
    [InlineData("Standard Edition (64-bit)", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void LicensingAdvice_AppliesOnlyToPaidEnterprise(string? edition, bool expected)
    {
        Assert.Equal(expected, FinOpsRecommendationFigures.EditionNeedsLicensingAdvice(edition));
        if (!expected)
        {
            Assert.Empty(FinOpsRecommendationFigures.EditionAudit(edition ?? "", 16, 16, Array.Empty<string>(), 5000m, "Standalone", false));
        }
    }

    [Fact]
    public void CpuRightSizing_NeedsAFullDayOfSamples()
    {
        Assert.False(FinOpsRecommendationFigures.CpuSamplesSpanEnough(TimeSpan.FromMinutes(4)));
        Assert.False(FinOpsRecommendationFigures.CpuSamplesSpanEnough(TimeSpan.FromHours(23)));
        Assert.True(FinOpsRecommendationFigures.CpuSamplesSpanEnough(TimeSpan.FromHours(24)));
    }

    [Theory]
    [InlineData(null, 2, true)]   // no compute row: the prescriptive row stands alone
    [InlineData(4, 2, false)]     // compute row keeps more cores: it stays
    [InlineData(4, 4, false)]     // tie: the "CPU over-provisioned" row stays
    [InlineData(4, 8, true)]      // prescriptive row keeps more cores: it replaces the compute row
    public void OneCpuRow_TheOneKeepingMoreCoresWins_AndATieKeepsTheComputeRow(int? compute, int prescriptive, bool prescriptiveWins)
    {
        Assert.Equal(prescriptiveWins, FinOpsRecommendationFigures.PrescriptiveCpuRowWins(compute, prescriptive));
    }

    [Fact]
    public void VmRightSizing_CanLeaveTheCpuRowOut_AndKeepTheMemoryRow()
    {
        var withCpu = FinOpsRecommendationFigures.VmRightSizing(10m, "recent samples", 16, 65536, 8000, 100, "recent samples", 1000m);
        var withoutCpu = FinOpsRecommendationFigures.VmRightSizing(10m, "recent samples", 16, 65536, 8000, 100, "recent samples", 1000m, includeCpuRow: false);
        Assert.Equal(2, withCpu.Count);
        Assert.Single(withoutCpu);
        Assert.StartsWith("Memory:", withoutCpu[0].Finding, StringComparison.Ordinal);
    }

    [Fact]
    public void IdleCoverage_IsTheSevenCompleteUtcDaysBeforeToday_AndSevenDaysOfHistory()
    {
        var now = new DateTime(2026, 10, 7, 15, 30, 0, DateTimeKind.Utc);
        Assert.Equal(new DateTime(2026, 9, 30), DarlingFinOpsOptimizationReader.IdleCoverageStartUtc(now));
        Assert.Equal(new DateTime(2026, 10, 7), DarlingFinOpsOptimizationReader.IdleCoverageEndUtc(now));
        Assert.Equal(7, DarlingFinOpsOptimizationReader.IdleCoverageDays);
        Assert.Contains("COUNT(DISTINCT CAST(collection_time AS DATE))", DarlingFinOpsOptimizationReader.IdleCoverageSql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time)", DarlingFinOpsOptimizationReader.IdleCoverageSql, StringComparison.Ordinal);

        var covered = now.AddDays(-7).AddMinutes(-1);
        Assert.True(DarlingFinOpsOptimizationReader.IdleCoverageHolds(covered, 7, now));
        Assert.False(DarlingFinOpsOptimizationReader.IdleCoverageHolds(now.AddDays(-6.5), 7, now));   // 6.5 days of history, however many UTC dates it touches
        Assert.False(DarlingFinOpsOptimizationReader.IdleCoverageHolds(covered, 6, now));              // a missing complete day
        Assert.False(DarlingFinOpsOptimizationReader.IdleCoverageHolds(null, 7, now));                 // no sample at all
        // Just after 00:00 UTC with no sample today yet: today is not part of the days, so the claim stands.
        var justAfterMidnight = new DateTime(2026, 10, 8, 0, 5, 0, DateTimeKind.Utc);
        Assert.True(DarlingFinOpsOptimizationReader.IdleCoverageHolds(justAfterMidnight.AddDays(-7).AddMinutes(-1), 7, justAfterMidnight));
    }

    [Fact]
    public void Cpu24HourRule_NeedsItsOldestSampleInsideTheWindowToBe23HoursOld()
    {
        var now = new DateTime(2026, 10, 7, 15, 30, 0, DateTimeKind.Utc);
        Assert.False(FinOpsRecommendationFigures.Cpu24HourWindowWatched(null, now));
        // Two days of samples last week and 60 minutes since a restart: the window holds one hour, however long the older span was.
        Assert.False(FinOpsRecommendationFigures.Cpu24HourWindowWatched(now.AddMinutes(-60), now));
        Assert.False(FinOpsRecommendationFigures.Cpu24HourWindowWatched(now.AddHours(-22.9), now));
        Assert.True(FinOpsRecommendationFigures.Cpu24HourWindowWatched(now.AddHours(-23), now));
        Assert.True(FinOpsRecommendationFigures.Cpu24HourWindowWatched(now.AddHours(-23.98), now));
    }

    [Fact]
    public void IdleDatabasesGrid_SaysWhyItIsEmpty_WhenTheDaysWereNotWatched()
    {
        Assert.Equal("No idle databases detected", DarlingFinOpsOptimizationReader.IdleDatabasesEmptyText(true));
        Assert.Contains("cannot be judged yet", DarlingFinOpsOptimizationReader.IdleDatabasesEmptyText(false), StringComparison.Ordinal);
    }

    [Fact]
    public void ServerInventoryIdleCount_IsNull_WithoutCoverage_InBothTheRawAndTheRoutedStatement()
    {
        // The raw statement joins idle_dbs to idle_coverage (an inner join), so an uncovered server has no idle_dbs row and its count is NULL.
        Assert.Contains("JOIN idle_coverage ic ON ic.server_id = s.server_id", DarlingFinOpsInventoryReader.ServerMetricsSql, StringComparison.Ordinal);
        Assert.Contains("HAVING COUNT(DISTINCT CAST(collection_time AS DATE)) >= $4", DarlingFinOpsInventoryReader.ServerMetricsSql, StringComparison.Ordinal);
        // The complete days only (before $5, today at 00:00), and the oldest sample at or before the 7-day cutoff ($2).
        Assert.Contains("collection_time <  $5", DarlingFinOpsInventoryReader.ServerMetricsSql, StringComparison.Ordinal);
        Assert.Contains("o.collection_time <= $2", DarlingFinOpsInventoryReader.ServerMetricsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void HealthScore_IsADash_WithoutACpuSample()
    {
        Assert.Null(FinOpsInventoryFigures.HealthScoreOrNull(null));
        Assert.NotNull(FinOpsInventoryFigures.HealthScoreOrNull(0m));
        Assert.Contains("no CPU sample", FinOpsHealthCalculator.NoScoreNote, StringComparison.Ordinal);
    }

    [Fact]
    public void StorageGrowth_ABlankBaseline_NamesTheMissingSample()
    {
        Assert.Equal("No sample from 7 days ago", DarlingFinOpsStorageGrowthReader.NoBaselineNote(7, null));
        Assert.Equal("No sample from 30 days ago", DarlingFinOpsStorageGrowthReader.NoBaselineNote(30, null));
        Assert.Null(DarlingFinOpsStorageGrowthReader.NoBaselineNote(7, 10m));
        Assert.Equal("No sample from 7 days ago; No sample from 30 days ago", DarlingFinOpsStorageGrowthReader.BaselineNote(null, null));
        Assert.Null(DarlingFinOpsStorageGrowthReader.BaselineNote(1m, 2m));
    }

    [Fact]
    public void StorageGrowth_TheBaselineIsTheSampleNearestItsMark_WithinOneDay()
    {
        var sql = DarlingFinOpsStorageGrowthReader.DatabaseSizeSnapshotNearestSql;
        Assert.Contains("ORDER BY abs(EXTRACT(EPOCH FROM (t - $2::timestamp)))", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time < $3", sql, StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromDays(1), DarlingFinOpsStorageGrowthReader.BaselineTolerance);
    }
}
