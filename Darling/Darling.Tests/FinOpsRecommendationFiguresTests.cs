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
using System.Globalization;
using PerformanceMonitor.Common;
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

    // ── Dormant (original :302 any idle, :304 GB total, :305 top 5, :308-314 share, :318 severity, :322 "and N more", :325 savings) ──

    private static List<(string DatabaseName, decimal TotalSizeMb)> Idle(int count, decimal eachMb) =>
        Enumerable.Range(1, count).Select(i => ($"db{i}", eachMb)).ToList();

    [Fact]
    public void Dormant_NoIdleDatabases_IsNull()
    {
        Assert.Null(FinOpsRecommendationFigures.Dormant(new List<(string, decimal)>(), 1000m, 1000m));
    }

    [Fact]
    public void Dormant_ShowsFiveNames_AndTheMoreCountFromSix()
    {
        InEnUs(() =>
        {
            var five = FinOpsRecommendationFigures.Dormant(Idle(5, 1024m), 0m, 0m)!;
            Assert.Equal("No query activity in 7 days: db1, db2, db3, db4, db5. Consider archiving or removing these databases.", five.Detail);
            var seven = FinOpsRecommendationFigures.Dormant(Idle(7, 1024m), 0m, 0m)!;
            Assert.Equal("No query activity in 7 days: db1, db2, db3, db4, db5 and 2 more. Consider archiving or removing these databases.", seven.Detail);
        });
    }

    [Fact]
    public void Dormant_FindingCarriesTheGbTotal_AndSeverityIsHighFromThree()
    {
        InEnUs(() =>
        {
            var two = FinOpsRecommendationFigures.Dormant(Idle(2, 1536m), 0m, 0m)!;
            Assert.Equal("2 idle database(s) consuming 3.0GB", two.Finding);
            Assert.Equal("Medium", two.Severity);
            Assert.Equal("High", two.Confidence);
            Assert.Equal("Databases", two.Category);
            Assert.Equal("High", FinOpsRecommendationFigures.Dormant(Idle(3, 1024m), 0m, 0m)!.Severity);
        });
    }

    [Fact]
    public void Dormant_ShareNeedsAnAllocatedTotal_AndACost()
    {
        InEnUs(() =>
        {
            Assert.Null(FinOpsRecommendationFigures.Dormant(Idle(2, 1024m), 0m, 1000m)!.EstMonthlySavings);
            Assert.Null(FinOpsRecommendationFigures.Dormant(Idle(2, 1024m), 8192m, 0m)!.EstMonthlySavings);
            // 2048 idle MB of 8192 allocated, at a monthly cost of 1000.
            Assert.Equal(250m, FinOpsRecommendationFigures.Dormant(Idle(2, 1024m), 8192m, 1000m)!.EstMonthlySavings);
        });
    }

    // ── Dev/test (original :338 any, :345 count, :346 top 10, :347 "and N more") ──

    [Fact]
    public void DevTest_NoDatabases_IsNull()
    {
        Assert.Null(FinOpsRecommendationFigures.DevTest(new List<string>()));
    }

    [Fact]
    public void DevTest_ShowsTenNames_AndTheMoreCountFromEleven()
    {
        InEnUs(() =>
        {
            var names = Enumerable.Range(1, 11).Select(i => $"dev{i}").ToList();
            var row = FinOpsRecommendationFigures.DevTest(names)!;
            Assert.Equal("11 possible dev/test database(s) on production server", row.Finding);
            Assert.Equal("Databases matching dev/test patterns: dev1, dev2, dev3, dev4, dev5, dev6, dev7, dev8, dev9, dev10 and 1 more" +
                         ". If these are non-production workloads, consider moving to a lower-cost tier or separate server.", row.Detail);
            Assert.Equal("Environment", row.Category);
            Assert.Equal("Medium", row.Severity);
            Assert.Equal("Low", row.Confidence);
            Assert.DoesNotContain("more", FinOpsRecommendationFigures.DevTest(names.Take(10).ToList())!.Detail, StringComparison.Ordinal);
        });
    }

    // ── Maintenance (original :376-385 severity at 5, finding, detail) and FormatDuration ──

    [Theory]
    [InlineData(0L, "0s")]
    [InlineData(59L, "59s")]
    [InlineData(60L, "1m 0s")]
    [InlineData(3600L, "1h 0m 0s")]
    public void FormatDuration_AtTheBoundaries(long seconds, string expected)
    {
        Assert.Equal(expected, FinOpsRecommendationFigures.FormatDuration(seconds));
    }

    [Fact]
    public void MaintenanceJob_RanLongThreeTimes_IsLow_AndFourIsLowToo_FiveIsMedium()
    {
        InEnUs(() =>
        {
            var row = FinOpsRecommendationFigures.MaintenanceJob(new MaintenanceJobRun("Nightly Index Job", 3600L, 60L, 59L, 3));
            Assert.Equal("Nightly Index Job ran long 3 times in 7 days", row.Finding);
            Assert.Equal("Average duration: 1h 0m 0s, max: 1m 0s, historical average: 59s. Review whether this job's schedule or operations need tuning.", row.Detail);
            Assert.Equal("Maintenance", row.Category);
            Assert.Equal("Low", row.Severity);
            Assert.Equal("High", row.Confidence);
            Assert.Equal("Low", FinOpsRecommendationFigures.MaintenanceJob(new MaintenanceJobRun("j", 0L, 0L, 0L, 4)).Severity);
            Assert.Equal("Medium", FinOpsRecommendationFigures.MaintenanceJob(new MaintenanceJobRun("j", 0L, 0L, 0L, 5)).Severity);
        });
    }

    // ── Storage tier (original :460-464 averages and the 5 ms / 3 ms limits, :466-472 window totals, :478-489 text, take 10) ──

    private static StorageTierIo Io(string name, long reads, long readStall, long writes, long writeStall,
        DateTime? first = null, DateTime? last = null, long samples = 0L) =>
        new(name, reads, readStall, writes, writeStall, first, last, samples);

    [Fact]
    public void StorageTier_NoRows_IsNull()
    {
        Assert.Null(FinOpsRecommendationFigures.StorageTier(new List<StorageTierIo>()));
    }

    [Fact]
    public void StorageTier_ReadAverageOfExactlyFive_IsOut_JustUnderIsIn()
    {
        InEnUs(() =>
        {
            Assert.Null(FinOpsRecommendationFigures.StorageTier(new[] { Io("a", 100, 500, 100, 0) }));
            var row = FinOpsRecommendationFigures.StorageTier(new[] { Io("a", 1000, 4999, 100, 0) })!;
            Assert.Equal("1 database(s) with low IO latency — standard storage may suffice", row.Finding);
            Assert.Contains("a (read 5.0ms, write 0.0ms)", row.Detail, StringComparison.Ordinal);
            Assert.Equal("Storage", row.Category);
            Assert.Equal("Low", row.Severity);
            Assert.Equal("Medium", row.Confidence);
        });
    }

    [Fact]
    public void StorageTier_WriteAverageOfExactlyThree_IsOut_JustUnderIsIn()
    {
        InEnUs(() =>
        {
            Assert.Null(FinOpsRecommendationFigures.StorageTier(new[] { Io("a", 100, 0, 100, 300) }));
            Assert.NotNull(FinOpsRecommendationFigures.StorageTier(new[] { Io("a", 100, 0, 1000, 2999) }));
        });
    }

    [Fact]
    public void StorageTier_ZeroOperationsCountAsZeroLatency()
    {
        InEnUs(() =>
        {
            Assert.NotNull(FinOpsRecommendationFigures.StorageTier(new[] { Io("idle", 0, 0, 0, 0) }));
        });
    }

    [Fact]
    public void StorageTier_ListsTenDatabases_AndTheMoreCountFromEleven()
    {
        InEnUs(() =>
        {
            var rows = Enumerable.Range(1, 11).Select(i => Io($"d{i}", 100, 0, 100, 0)).ToList();
            var row = FinOpsRecommendationFigures.StorageTier(rows)!;
            Assert.Equal("11 database(s) with low IO latency — standard storage may suffice", row.Finding);
            Assert.Contains("d10 (read 0.0ms, write 0.0ms) and 1 more. Premium", row.Detail, StringComparison.Ordinal);
            Assert.DoesNotContain("d11", row.Detail, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void StorageTier_WindowTotalsCoverQualifyingDatabasesOnly()
    {
        InEnUs(() =>
        {
            var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var rows = new[]
            {
                Io("in1", 100, 0, 100, 0, t0.AddHours(1), t0.AddHours(2), 10L),
                Io("in2", 100, 0, 100, 0, t0.AddHours(3), t0.AddHours(4), 20L),
                // Out on read latency: its early first sample, late last sample and 1000 samples must not count.
                Io("out", 100, 5000, 100, 0, t0.AddDays(-30), t0.AddDays(30), 1000L)
            };
            var row = FinOpsRecommendationFigures.StorageTier(rows)!;
            Assert.Contains("across " + RightSizingWindow.Describe(30L, TimeSpan.FromHours(3)) + ":", row.Detail, StringComparison.Ordinal);
            Assert.DoesNotContain("out (", row.Detail, StringComparison.Ordinal);
        });
    }
}
