/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Boundary tests on the memory, VM and reserved-capacity recommendation figures. Each expectation was worked out from the
/// viewer's original inline code (ViewerDataService.FinOps.Recommendations.cs before the move), not from the moved code; the
/// line cited on a test is that original line. The text is formatted in the current culture, so each test runs in en-US.
/// </summary>
public sealed class FinOpsRecommendationFiguresTests
{
    private static UtilizationEfficiencyDto Util(int physicalMemoryMb) =>
        new(5m, 10, 10m, 100L, 1000, 1000, physicalMemoryMb, 500, 0.5m, 0L, 0L, 0L, 0m, 100, null, 8, 3, "OK");

    private static void InEnUs(Action body)
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("en-US");
            body();
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    // ── Memory right-sizing (original :269 sampleCount >= 16, :272-276 ratio, target and GB compare, :282 severity, :288 savings) ──

    [Fact]
    public void Memory_NeedsSixteenSamples()
    {
        InEnUs(() =>
        {
            Assert.Null(FinOpsRecommendationFigures.MemoryRightSizing(Util(16384), 4096, 15, "w", 0m));
            Assert.NotNull(FinOpsRecommendationFigures.MemoryRightSizing(Util(16384), 4096, 16, "w", 0m));
        });
    }

    [Fact]
    public void Memory_RatioBelowHalf_IsARow_AndExactlyHalfIsNot()
    {
        InEnUs(() =>
        {
            // 8191 / 16384 < 0.50 and the 15GB target is under the 16GB RAM; 8192 / 16384 is exactly 0.50.
            Assert.NotNull(FinOpsRecommendationFigures.MemoryRightSizing(Util(16384), 8191, 16, "w", 0m));
            Assert.Null(FinOpsRecommendationFigures.MemoryRightSizing(Util(16384), 8192, 16, "w", 0m));
        });
    }

    [Fact]
    public void Memory_SeverityIsHighBelowThirtyPercent_MediumAtThirty()
    {
        InEnUs(() =>
        {
            Assert.Equal("High", FinOpsRecommendationFigures.MemoryRightSizing(Util(10240), 3071, 16, "w", 0m)!.Severity);
            Assert.Equal("Medium", FinOpsRecommendationFigures.MemoryRightSizing(Util(10240), 3072, 16, "w", 0m)!.Severity);
        });
    }

    [Fact]
    public void Memory_ComparesWholeGigabytes_SoAnEightGigabyteTargetOnNineGigabytesIsNoAdvice()
    {
        InEnUs(() =>
        {
            // target is 8192 MB = 8GB and 9000 / 1024 is 8, so 8 < 8 is false.
            Assert.Null(FinOpsRecommendationFigures.MemoryRightSizing(Util(9000), 4000, 16, "w", 0m));
            // 10240 MB is 10GB, so the same 8GB target is advice.
            Assert.NotNull(FinOpsRecommendationFigures.MemoryRightSizing(Util(10240), 4000, 16, "w", 0m));
        });
    }

    [Fact]
    public void Memory_SavingsAreNullWithoutACost_AndThirtyPercentOfTheFreedShareWithOne()
    {
        InEnUs(() =>
        {
            Assert.Null(FinOpsRecommendationFigures.MemoryRightSizing(Util(16384), 4096, 16, "w", 0m)!.EstMonthlySavings);
            // 1000 * (1 - 8192/16384) * 0.30
            Assert.Equal(150m, FinOpsRecommendationFigures.MemoryRightSizing(Util(16384), 4096, 16, "w", 1000m)!.EstMonthlySavings);
        });
    }

    // ── VM right-sizing (original :435-452 cores, :455-480 memory) ──

    [Fact]
    public void Vm_CpuTargetCores_RoundOnTheWholeCoreCounts()
    {
        InEnUs(() =>
        {
            var low = Assert.Single(FinOpsRecommendationFigures.VmRightSizing(14.99m, "w", 16, 0, 0, 0, "m", 1000m));
            Assert.Contains("from 16 to 4 cores", low.Finding, StringComparison.Ordinal);
            Assert.Equal(375m, low.EstMonthlySavings);   // 1000 * (1 - 4/16) * 0.50

            var mid = Assert.Single(FinOpsRecommendationFigures.VmRightSizing(15m, "w", 16, 0, 0, 0, "m", 1000m));
            Assert.Contains("from 16 to 8 cores", mid.Finding, StringComparison.Ordinal);
            Assert.Equal(250m, mid.EstMonthlySavings);   // 1000 * (1 - 8/16) * 0.50

            Assert.Empty(FinOpsRecommendationFigures.VmRightSizing(30m, "w", 16, 0, 0, 0, "m", 1000m));
            Assert.Empty(FinOpsRecommendationFigures.VmRightSizing(10m, "w", 3, 0, 0, 0, "m", 1000m));
        });
    }

    [Fact]
    public void Vm_MemoryPrescription_UsesItsOwnThresholdsAndThirtyPercentFactor()
    {
        InEnUs(() =>
        {
            var quarter = Assert.Single(FinOpsRecommendationFigures.VmRightSizing(90m, "w", 2, 16384, 4095, 16, "m", 1000m));
            Assert.Contains("from 16GB to 4GB", quarter.Finding, StringComparison.Ordinal);
            Assert.Equal(225m, quarter.EstMonthlySavings);   // 1000 * (1 - 4096/16384) * 0.30

            var half = Assert.Single(FinOpsRecommendationFigures.VmRightSizing(90m, "w", 2, 16384, 4096, 16, "m", 1000m));
            Assert.Contains("from 16GB to 8GB", half.Finding, StringComparison.Ordinal);
            Assert.Equal(150m, half.EstMonthlySavings);      // 1000 * (1 - 8192/16384) * 0.30

            Assert.Single(FinOpsRecommendationFigures.VmRightSizing(90m, "w", 2, 16384, 6553, 16, "m", 0m));
            Assert.Empty(FinOpsRecommendationFigures.VmRightSizing(90m, "w", 2, 16384, 6554, 16, "m", 0m));
            Assert.Empty(FinOpsRecommendationFigures.VmRightSizing(90m, "w", 2, 16384, 4095, 15, "m", 0m));
            Assert.Empty(FinOpsRecommendationFigures.VmRightSizing(90m, "w", 2, 4096, 100, 16, "m", 0m));
        });
    }

    [Fact]
    public void Vm_CanEmitBothRows_CpuFirst_AndNeitherWhenNothingQualifies()
    {
        InEnUs(() =>
        {
            var both = FinOpsRecommendationFigures.VmRightSizing(10m, "w", 16, 16384, 4000, 16, "m", 0m);
            Assert.Equal(2, both.Count);
            Assert.StartsWith("CPU:", both[0].Finding, StringComparison.Ordinal);
            Assert.StartsWith("Memory:", both[1].Finding, StringComparison.Ordinal);
            Assert.Null(both[0].EstMonthlySavings);
            Assert.Empty(FinOpsRecommendationFigures.VmRightSizing(80m, "w", 16, 16384, 16000, 100, "m", 1000m));
        });
    }

    // ── Reserved capacity (original :578 avg and stddev gate, :581 CV < 0.3, :584 confidence at 0.15) ──

    [Fact]
    public void Reserved_AverageMustExceedTwenty_AndTheSpreadMustBePositive()
    {
        InEnUs(() =>
        {
            Assert.Null(FinOpsRecommendationFigures.ReservedCapacity(20m, 1m));
            Assert.NotNull(FinOpsRecommendationFigures.ReservedCapacity(20.01m, 1m));
            Assert.Null(FinOpsRecommendationFigures.ReservedCapacity(50m, 0m));
        });
    }

    [Fact]
    public void Reserved_VariationBelowThreeTenths_IsARow_AndExactlyThreeTenthsIsNot()
    {
        InEnUs(() =>
        {
            Assert.NotNull(FinOpsRecommendationFigures.ReservedCapacity(100m, 29.9m));
            Assert.Null(FinOpsRecommendationFigures.ReservedCapacity(100m, 30m));
        });
    }

    [Fact]
    public void Reserved_ConfidenceIsHighBelowFifteenHundredths_MediumAtIt()
    {
        InEnUs(() =>
        {
            var high = FinOpsRecommendationFigures.ReservedCapacity(100m, 14.9m)!;
            Assert.Equal("High", high.Confidence);
            Assert.Equal("Cloud", high.Category);
            Assert.Equal("Low", high.Severity);
            Assert.Null(high.EstMonthlySavings);
            Assert.Equal("Medium", FinOpsRecommendationFigures.ReservedCapacity(100m, 15m)!.Confidence);
        });
    }
}
