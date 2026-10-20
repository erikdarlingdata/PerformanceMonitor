/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>#6139: no verdict from too few samples, and high stolen memory is memory pressure, never spare memory.</summary>
public sealed class ProvisioningVerdictEvidenceTests
{
    private static string Idle(long? samples, decimal? stolen = null) => ProvisioningVerdict.Evaluate(
        avgCpuPercent: 2m, maxCpuPercent: 10m, p95CpuPercent: 5m,
        maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0, grantUtilizationPercent: 1m,
        maxWorkers: 512, currentWorkers: 20, cpuSamples: samples, stolenMemoryPercent: stolen);

    [Fact]
    public void SixSamples_AfterALongGap_IsNotAVerdict()
    {
        Assert.Equal(ProvisioningVerdict.NotEnoughData, Idle(6));
        Assert.Equal("not enough data yet (6 samples)", ProvisioningVerdict.NotEnoughDataLabel(6));
    }

    [Fact]
    public void AtTheMinimum_TheIdleServerIsOverProvisioned_AsBefore()
    {
        Assert.Equal(ProvisioningVerdict.NotEnoughData, Idle(ProvisioningVerdict.MinSamples - 1));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, Idle(ProvisioningVerdict.MinSamples));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, Idle(null));
    }

    [Fact]
    public void HighStolenMemory_IsMemoryPressure_NotOverProvisioned_AndTheReasonSaysSo()
    {
        // The web case: 1 GB physical, buffer pool 14% of it, stolen memory 72.5%.
        var verdict = Idle(500, stolen: 72.5m);

        Assert.Equal(ProvisioningVerdict.UnderProvisioned, verdict);
        var reason = ProvisioningVerdict.UnderProvisionedReason(5m, 0, 0, 0, 512, 20, stolenMemoryPercent: 72.5m);
        Assert.Contains("Memory is under pressure", reason);
        Assert.DoesNotContain("more resources than it needs", reason);
    }

    [Fact]
    public void LowStolenMemory_LeavesTheIdleServerOverProvisioned()
    {
        Assert.Equal(ProvisioningVerdict.OverProvisioned, Idle(500, stolen: 20m));
    }

    [Fact]
    public void StolenMemoryNote_ExplainsAnAmberOrRedFigure_AndIsSilentOtherwise()
    {
        Assert.Equal("", ProvisioningVerdict.StolenMemoryNote(40));
        Assert.Contains("88%", ProvisioningVerdict.StolenMemoryNote(88));
    }
}
