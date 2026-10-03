/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Server Inventory figures the desktop grid computes itself: the health score, the Standard-edition license warning
/// and the Azure SQL Database hardware blanks and note. Expected values are worked out by hand.
/// </summary>
public sealed class FinOpsInventoryViewerFiguresTests
{
    [Theory]
    [InlineData(null, 90)]
    [InlineData(35, 84)]
    [InlineData(0, 94)]
    [InlineData(100, 54)]
    public void HealthScore_IsTheWeightedTermsWithMemory80AndStorage100(int? avgCpu, int expected)
    {
        int? cpu = avgCpu is int a ? FinOpsHealthCalculator.CpuScore(a) : null;
        Assert.Equal(expected, FinOpsHealthCalculator.Overall(cpu, 80, FinOpsHealthCalculator.StorageScore(50)));
        Assert.Equal(expected, FinOpsInventoryFigures.HealthScore(avgCpu));
    }

    [Theory]
    [InlineData(59, "poor")]
    [InlineData(60, "fair")]
    [InlineData(79, "fair")]
    [InlineData(80, "good")]
    public void HealthBand_CutsAt60And80(int score, string band)
    {
        Assert.Equal(band, FinOpsUtilizationFigures.HealthBand(score));
    }

    [Theory]
    [InlineData("Standard Edition (64-bit)", 24, 131072L, null)]
    [InlineData("Standard Edition (64-bit)", 25, 131072L, "CPU: 25 cores (Standard limited to 24)")]
    [InlineData("Standard Edition (64-bit)", 24, 131073L, "RAM: 128GB (Standard limited to 128GB)")]
    [InlineData("Standard Edition (64-bit)", 32, 262144L, "CPU: 32 cores (Standard limited to 24); RAM: 256GB (Standard limited to 128GB)")]
    [InlineData("Enterprise Edition (64-bit)", 64, 524288L, null)]
    public void LicenseWarning_FiresOnlyOnStandardOverTheCaps(string edition, int cpus, long memoryMb, string? expected)
    {
        var row = new ServerPropertyRow { Edition = edition, CpuCount = cpus, PhysicalMemoryMb = memoryMb };
        Assert.Equal(expected, row.LicenseWarning);
        Assert.Equal(expected, FinOpsInventoryFigures.LicenseWarning(edition, cpus, memoryMb));
    }

    [Fact]
    public void AzureSqlDatabase_BlanksTheHostHardwareAndSaysWhy()
    {
        var row = new ServerPropertyRow { EngineEdition = 5, CpuCount = 2, PhysicalMemoryMb = 934000, SocketCount = 0, CoresPerSocket = 32 };
        Assert.Equal(2, row.CpuCount);
        Assert.Null(row.PhysicalMemoryMb);
        Assert.Null(row.SocketCount);
        Assert.Null(row.CoresPerSocket);
        Assert.Equal(ServerHardwareScope.InventoryHardwareNote, row.HardwareUnavailableReason);
        Assert.Null(FinOpsInventoryFigures.PhysicalMemoryMb(5, 934000));
        Assert.Null(FinOpsInventoryFigures.SocketCount(5, 0));
        Assert.Null(FinOpsInventoryFigures.CoresPerSocket(5, 32));
        Assert.Equal(row.HardwareUnavailableReason, FinOpsInventoryFigures.HardwareNote(5, null));
    }

    [Fact]
    public void OtherEditions_KeepTheStoredHardwareAndNoNote()
    {
        var row = new ServerPropertyRow { EngineEdition = 3, CpuCount = 8, PhysicalMemoryMb = 65536, SocketCount = 2, CoresPerSocket = 4 };
        Assert.Equal(65536L, row.PhysicalMemoryMb);
        Assert.Equal(2, row.SocketCount);
        Assert.Equal(4, row.CoresPerSocket);
        Assert.Null(row.HardwareUnavailableReason);
        row.HardwareUnavailableReason = "no permission";
        Assert.Equal("no permission", row.HardwareUnavailableReason);
        Assert.Equal(65536L, FinOpsInventoryFigures.PhysicalMemoryMb(3, 65536));
        Assert.Equal("no permission", FinOpsInventoryFigures.HardwareNote(5, "no permission"));
        Assert.Null(FinOpsInventoryFigures.HardwareNote(3, null));
    }
}
