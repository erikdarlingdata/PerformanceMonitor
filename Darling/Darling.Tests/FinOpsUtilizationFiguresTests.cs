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

    /// <summary>The RIGHT_SIZED sentence for each reason a server is RIGHT_SIZED, written out as literals (not a copy of the switch). The
    /// default Dto is a 12.5% average with a 40% peak and a buffer pool at exactly half of physical memory.</summary>
    [Theory]
    [InlineData(0, 40, 12.5, 4000, "CPU averaged 12.5% (p95 35.0%), but one sample reached 40%, so this server is not rated over-provisioned and memory is well-utilized (buffer pool uses 50% of physical RAM). No action needed.")]
    [InlineData(5, 40, 12.5, 4000, "CPU averaged 12.5% (p95 35.0%), but one sample reached 40%, so this database is not rated over-provisioned and memory is well-utilized (buffer pool uses 50% of the database's memory limit). No action needed.")]
    [InlineData(0, 40, 12.5, 3999, "CPU averaged 12.5% (p95 35.0%), but one sample reached 40%, so this server is not rated over-provisioned and the buffer pool uses 50% of physical RAM. No action needed.")]
    [InlineData(0, 70, 30.0, 800, "CPU is moderately loaded (avg 30.0%, p95 35.0%) and the buffer pool uses 10% of physical RAM. No action needed.")]
    [InlineData(0, 70, 30.0, 6000, "CPU is moderately loaded (avg 30.0%, p95 35.0%) and memory is well-utilized (buffer pool uses 75% of physical RAM). No action needed.")]
    public void Explanation_ForRightSized_NamesTheCauseAndTheMemoryShare(int edition, int max, double avg, int bp, string expected)
    {
        var dto = Dto("RIGHT_SIZED", edition, avg: (decimal)avg, max: max, bp: bp);
        Assert.Equal(expected, FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture));
    }

    /// <summary>The idle-average, quiet-peak RIGHT_SIZED server is held there by its measured grant utilization, and the sentence prints it.</summary>
    [Fact]
    public void Explanation_ForRightSized_WithAnIdleQuietServer_PrintsTheMeasuredGrantUtilization()
    {
        var dto = Dto("RIGHT_SIZED", avg: 5m, max: 12, p95: 8m) with { GrantUtilizationPct = 77m };
        Assert.Equal(
            "CPU is lightly loaded (avg 5.0%, p95 8.0%), but its memory grants peaked at 77% of their target (threshold: 50%), so this server is not rated over-provisioned and memory is well-utilized (buffer pool uses 50% of physical RAM). No action needed.",
            FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("OVER_PROVISIONED", 0, "CPU is lightly loaded (avg 12.5%, max 40%) and the buffer pool uses 50% of physical RAM. This server may have more resources than it needs.")]
    [InlineData("OVER_PROVISIONED", 5, "CPU is lightly loaded (avg 12.5%, max 40%) and the buffer pool uses 50% of the database's memory limit. This database may have more resources than it needs.")]
    [InlineData("NOT_APPLICABLE", 0, null)]
    [InlineData("", 0, "")]
    public void Explanation_ForTheOtherStatuses_IsTheirSentence(string status, int edition, string? expected)
    {
        var dto = Dto(status, edition, p95: 90m, waiters: 2);
        var actual = FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture);
        Assert.Equal(expected ?? ProvisioningVerdict.NotApplicableExplanation, actual);
    }

    [Fact]
    public void Explanation_ForUnderProvisioned_SaysWhyItWasFlagged()
    {
        var dto = Dto("UNDER_PROVISIONED", p95: 90m);
        Assert.Contains("90", FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture));
    }

    /// <summary>#6139: six samples is no verdict, and the sentence says how many the window holds and how many it needs.</summary>
    [Fact]
    public void Explanation_ForNotEnoughData_SaysTheSampleCountAndTheMinimum()
    {
        var dto = Dto(ProvisioningVerdict.NotEnoughData) with { CpuSamples = 6 };
        Assert.Equal(
            "The last 24 hours hold 6 CPU samples; a verdict needs at least 12.",
            FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture));
    }

    /// <summary>#6139: 72.5% stolen memory is explained as memory pressure, in the same words the verdict used.</summary>
    [Fact]
    public void Explanation_ForHighStolenMemory_SaysMemoryIsUnderPressure()
    {
        var dto = Dto("UNDER_PROVISIONED", p95: 5m, total: 1000, bp: 275, phys: 1000);
        var text = FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture);
        Assert.StartsWith("Memory is under pressure: stolen memory is 73%", text);
        Assert.DoesNotContain("may have more resources", text);
    }

    /// <summary>#6139: the MCP answer carries the Stolen Mem % note when the figure reads amber or red, and no field when it does not.</summary>
    [Fact]
    public void TheMcpStolenMemoryNote_IsTheViewersTooltipTextAboveAmber_AndAbsentBelow()
    {
        Assert.Equal(ProvisioningVerdict.StolenMemoryNote(72.5), PerformanceMonitor.Darling.Service.Mcp.DarlingMcpFinOpsTools.StolenMemoryNoteOrNull(72.5));
        Assert.Contains("outside the buffer pool", PerformanceMonitor.Darling.Service.Mcp.DarlingMcpFinOpsTools.StolenMemoryNoteOrNull(72.5));
        Assert.Null(PerformanceMonitor.Darling.Service.Mcp.DarlingMcpFinOpsTools.StolenMemoryNoteOrNull(20));
        Assert.Equal("", ProvisioningVerdict.StolenMemoryNote(20));
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
