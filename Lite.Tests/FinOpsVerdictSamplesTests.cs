/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Data.Common;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6139: Lite's FinOps verdict needs 12 CPU samples before it says anything, and a high stolen-memory share is memory pressure,
/// not spare memory. The fleet row passes both to the shared rule, so it answers as the Darling fleet row does, and the rows the
/// tab and the grid show use the Viewer's words for the new verdict.
/// </summary>
public sealed class FinOpsVerdictSamplesTests
{
    [Fact]
    public void FleetProvisioningStatusFor_ServerWithSixSamples_GetsNotEnoughData()
    {
        using var few = (DbDataReader)AzureSqlDatabaseMasterPinsTests.FleetRow(0, "Enterprise Edition", cpuSamples: 6);
        using var enough = (DbDataReader)AzureSqlDatabaseMasterPinsTests.FleetRow(0, "Enterprise Edition", cpuSamples: 12);

        Assert.Equal(ProvisioningVerdict.NotEnoughData, LocalDataService.FleetProvisioningStatusFor(few));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, LocalDataService.FleetProvisioningStatusFor(enough));
    }

    [Fact]
    public void FleetProvisioningStatusFor_HighStolenMemory_IsUnderProvisionedNotOver()
    {
        using var stolen = (DbDataReader)AzureSqlDatabaseMasterPinsTests.FleetRow(0, "Enterprise Edition", totalMemoryMb: 1000, bufferPoolMb: 275);
        using var spare = (DbDataReader)AzureSqlDatabaseMasterPinsTests.FleetRow(0, "Enterprise Edition", totalMemoryMb: 1000, bufferPoolMb: 800);

        Assert.Equal(ProvisioningVerdict.UnderProvisioned, LocalDataService.FleetProvisioningStatusFor(stolen));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, LocalDataService.FleetProvisioningStatusFor(spare));
    }

    [Fact]
    public void TheRowsShowNotEnoughDataInTheBadgesWords()
    {
        Assert.Equal("not enough data yet", new ProvisioningTrendRow { Status = ProvisioningVerdict.NotEnoughData }.StatusDisplay);
        Assert.Equal("not enough data yet", new ServerPropertyRow { ProvisioningStatus = ProvisioningVerdict.NotEnoughData }.ProvisioningDisplay);
        Assert.Equal("not enough data yet (6 samples)", ProvisioningVerdict.StatusLabel(ProvisioningVerdict.NotEnoughData, 6));
    }
}
