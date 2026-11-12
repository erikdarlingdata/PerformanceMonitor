/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>#7705: an Availability Group secondary's load is only its work as a secondary, so FinOps does not right-size it from that load.
/// The idle secondary gets no CPU or memory row, one informational row with no savings figure, and a not-judged verdict; the primary with
/// the same figures is judged as before.</summary>
public sealed class SecondaryReplicaSizingTests
{
    private static List<FinOpsRecommendation> Vm(bool secondary) =>
        FinOpsRecommendationFigures.VmRightSizing(
            p95Cpu24h: 0m, cpuWindow: "the last 24 hours", cpuCount: 32, physMb: 32768, p95MemMb: 2048, memSampleCount: 100,
            memWindow: "the last 7 days", monthlyCost: 1000m, includeCpuRow: true, isAgSecondary: secondary);

    [Fact]
    public void IdleSecondary_GetsOneInformationalRow_AndNoRightSizingRowOrSavings()
    {
        var rows = Vm(secondary: true);

        var row = Assert.Single(rows);
        Assert.Equal(ProvisioningVerdict.NotJudgedSecondaryFinding, row.Finding);
        Assert.Equal(ProvisioningVerdict.NotJudgedSecondaryExplanation, row.Detail);
        Assert.Null(row.EstMonthlySavings);
        Assert.DoesNotContain(rows, r => r.Finding.StartsWith("CPU:") || r.Finding.StartsWith("Memory:"));
    }

    [Fact]
    public void IdlePrimary_WithTheSameFigures_StillGetsTheCpuAndMemoryRows()
    {
        var rows = Vm(secondary: false);

        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.Finding.StartsWith("CPU: reduce from 32 to 8"));
        Assert.Contains(rows, r => r.Finding.StartsWith("Memory: reduce"));
        Assert.All(rows, r => Assert.NotNull(r.EstMonthlySavings));
    }

    [Theory]
    [InlineData("Secondary", ProvisioningVerdict.NotJudgedSecondary)]
    [InlineData(" secondary ", ProvisioningVerdict.NotJudgedSecondary)]
    [InlineData("Primary", ProvisioningVerdict.OverProvisioned)]
    [InlineData("Standalone", ProvisioningVerdict.OverProvisioned)]
    [InlineData(null, ProvisioningVerdict.OverProvisioned)]
    public void Verdict_OfAnIdleServer_FollowsTheReplicaRole(string? role, string expected) =>
        Assert.Equal(expected, ProvisioningVerdict.Evaluate(
            avgCpuPercent: 0m, maxCpuPercent: 0m, p95CpuPercent: 0m,
            maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0, grantUtilizationPercent: 0m,
            maxWorkers: 512, currentWorkers: 20, cpuSamples: 288,
            isAgSecondary: ProvisioningVerdict.IsSecondaryReplicaRole(role)));

    [Fact]
    public void SecondaryVerdict_IsNotJudgedEvenUnderLoad_AndSaysWhy()
    {
        var verdict = ProvisioningVerdict.Evaluate(
            avgCpuPercent: 70m, maxCpuPercent: 99m, p95CpuPercent: 95m,
            maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0, grantUtilizationPercent: 0m,
            maxWorkers: 512, currentWorkers: 20, cpuSamples: 288, isAgSecondary: true);

        Assert.Equal(ProvisioningVerdict.NotJudgedSecondary, verdict);
        Assert.Equal("Not judged (secondary replica)", ProvisioningVerdict.StatusLabel(verdict));
        Assert.Contains("Size it with the primary", ProvisioningVerdict.NotJudgedSecondaryExplanation);
    }
}
