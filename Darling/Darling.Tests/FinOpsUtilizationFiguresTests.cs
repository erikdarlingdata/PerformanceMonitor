/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>Each Utilization figure helper equals the inline expression the viewer used to hold.</summary>
public sealed class FinOpsUtilizationFiguresTests
{
    private static UtilizationEfficiencyDto Dto(string status, int edition = 0, decimal avg = 12.5m, int max = 40,
        decimal p95 = 35m, int total = 8000, int bp = 4000, int phys = 8000, long waiters = 0, long timeouts = 0,
        long forced = 0, int maxWorkers = 512, int? curWorkers = 100) =>
        new(avg, max, p95, 100, total, 8000, phys, bp, 1m, waiters, timeouts, forced, 0m, maxWorkers, curWorkers, 8, edition, status);

    [Theory]
    [InlineData(0, 0)]
    [InlineData(-5, 10)]
    [InlineData(8000, 4000)]
    [InlineData(8000, 8000)]
    [InlineData(1000, 1500)]
    public void StolenMemoryPct_EqualsTheOldExpression(int total, int bp)
    {
        var old = total > 0 ? (double)(total - bp) / total * 100.0 : 0;
        Assert.Equal(old, FinOpsUtilizationFigures.StolenMemoryPct(total, bp));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(100, 0)]
    [InlineData(4000, 8000)]
    [InlineData(9000, 8000)]
    public void BufferPoolPct_EqualsTheOldExpression(int bp, int phys)
    {
        var old = phys > 0 ? (double)bp / phys * 100.0 : 0;
        Assert.Equal(old, FinOpsUtilizationFigures.BufferPoolPct(bp, phys));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(0, 50)]
    [InlineData(1000, 250)]
    [InlineData(1000, 0)]
    public void FreeSpacePct_EqualsTheOldExpression(double allocated, double free)
    {
        var a = (decimal)allocated;
        var f = (decimal)free;
        Assert.Equal(a > 0 ? f / a * 100m : 100m, FinOpsUtilizationFigures.FreeSpacePct(a, f));
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("RIGHT_SIZED", true)]
    [InlineData("NOT_APPLICABLE", true)]
    public void HasCpuSample_MatchesTheViewerRow(string status, bool expected)
    {
        var dto = Dto(status);
        Assert.Equal(expected, FinOpsUtilizationFigures.HasCpuSample(dto));
        Assert.Equal(expected, PerformanceMonitor.Darling.Viewer.UtilizationEfficiencyRow.From(dto).HasCpuSample);
    }

    [Fact]
    public void HealthScore_HandComputed_P95Of35_HalfBufferPool_TwentyFree_Is82()
    {
        Assert.Equal(82, FinOpsUtilizationFigures.HealthScore(Dto("RIGHT_SIZED"), 20m));
        Assert.Equal(82, FinOpsUtilizationFigures.HealthScore(true, 35m, 8000, 4000, 20m));
    }

    [Theory]
    [InlineData("RIGHT_SIZED", 0, 20)]
    [InlineData("", 0, 20)]
    [InlineData("", 8000, 20)]
    [InlineData("UNDER_PROVISIONED", 8000, 20)]
    [InlineData("RIGHT_SIZED", 8000, 100)]
    [InlineData("RIGHT_SIZED", 8000, -15)]
    public void HealthScore_EqualsTheOriginalFormula(string status, int phys, int freeWhole)
    {
        var free = (decimal)freeWhole;
        var dto = Dto(status, phys: phys);
        var hasCpu = dto.ProvisioningStatus.Length > 0;
        var expected = FinOpsHealthCalculator.Overall(
            hasCpu ? FinOpsHealthCalculator.CpuScore(dto.P95CpuPct) : null,
            FinOpsHealthCalculator.MemoryScore(phys > 0 ? (decimal)dto.BufferPoolMb / phys : 0m),
            FinOpsHealthCalculator.StorageScore(free));
        Assert.Equal(expected, FinOpsUtilizationFigures.HealthScore(dto, free));
        var row = PerformanceMonitor.Darling.Viewer.UtilizationEfficiencyRow.From(dto);
        row.FreeSpacePct = free;
        Assert.Equal(expected, row.ComputeHealthScore());
    }

    [Fact]
    public void ToDto_RoundTripsEveryField()
    {
        var dto = new UtilizationEfficiencyDto(1.5m, 2, 3.5m, 4L, 5, 6, 7, 8, 9.5m, 10L, 11L, 12L, 13.5m, 14, 15, 16, 17, "STATUS_X");
        Assert.Equal(dto, PerformanceMonitor.Darling.Viewer.UtilizationEfficiencyRow.From(dto).ToDto());
    }

    [Fact]
    public void HealthBand_AgreesWithScoreColor_ForEveryScore()
    {
        var scores = Enumerable.Range(0, 101).Concat(new[] { -1, 101, int.MinValue, int.MaxValue });
        foreach (var score in scores)
        {
            var expected = FinOpsHealthCalculator.ScoreColor(score) switch
            {
                "#27AE60" => "good",
                "#F39C12" => "fair",
                _ => "poor"
            };
            Assert.Equal(expected, FinOpsUtilizationFigures.HealthBand(score));
        }
    }

    [Theory]
    [InlineData("RIGHT_SIZED", 0)]
    [InlineData("RIGHT_SIZED", 5)]
    [InlineData("OVER_PROVISIONED", 0)]
    [InlineData("OVER_PROVISIONED", 5)]
    [InlineData("UNDER_PROVISIONED", 0)]
    [InlineData("NOT_APPLICABLE", 0)]
    [InlineData("", 0)]
    public void Explanation_EqualsTheOldSwitch_InTheCurrentCulture(string status, int edition)
    {
        var dto = Dto(status, edition, p95: 90m, waiters: 2);
        var bpPct = dto.PhysicalMemoryMb > 0 ? (double)dto.BufferPoolMb / dto.PhysicalMemoryMb * 100.0 : 0;
        var azure = ServerHardwareScope.HardwareIsTheHosts(dto.EngineEdition);
        var old = dto.ProvisioningStatus switch
        {
            "RIGHT_SIZED" => ServerHardwareScope.RightSizedExplanation(dto.AvgCpuPct, dto.P95CpuPct, bpPct, azure),
            "OVER_PROVISIONED" => ServerHardwareScope.OverProvisionedExplanation(dto.AvgCpuPct, dto.MaxCpuPct, bpPct, azure),
            "UNDER_PROVISIONED" => ProvisioningVerdict.UnderProvisionedReason(
                dto.P95CpuPct, dto.MaxGrantWaiters, dto.GrantTimeouts, dto.ForcedGrants, dto.MaxWorkersCount, dto.CurrentWorkersCount),
            ProvisioningVerdict.NotApplicable => ProvisioningVerdict.NotApplicableExplanation,
            _ => ""
        };
        Assert.Equal(old, FinOpsUtilizationFigures.Explanation(dto, CultureInfo.CurrentCulture));
    }

    [Fact]
    public void Explanation_UsesTheCultureItIsGiven_AndRestoresTheCaller()
    {
        var dto = Dto("RIGHT_SIZED", avg: 12.5m);
        var before = CultureInfo.CurrentCulture;
        var de = FinOpsUtilizationFigures.Explanation(dto, CultureInfo.GetCultureInfo("de-DE"));
        Assert.Contains("12,5", de, System.StringComparison.Ordinal);
        Assert.Same(before, CultureInfo.CurrentCulture);
        Assert.Contains("12.5", FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture), System.StringComparison.Ordinal);
    }

    [Fact]
    public void Explanation_InvariantIgnoresTheThreadCulture_AndCurrentCultureIsThreaded()
    {
        var dto = Dto("RIGHT_SIZED", avg: 12.5m);
        var before = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            var underEn = FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture);
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
            var underDe = FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture);
            Assert.Equal(underEn, underDe);
            Assert.Contains("12.5", underDe, System.StringComparison.Ordinal);
            var current = FinOpsUtilizationFigures.Explanation(dto, CultureInfo.CurrentCulture);
            Assert.Contains("12,5", current, System.StringComparison.Ordinal);
        }
        finally
        {
            CultureInfo.CurrentCulture = before;
        }
    }
}
