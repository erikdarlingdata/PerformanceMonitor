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

/// <summary>
/// #2246: the FinOps provisioning verdict, which used to report <c>UNDER_PROVISIONED</c> for every server
/// alive.
///
/// <para><b>What it got wrong.</b> The rule tested
/// <c>total_server_memory_mb / target_server_memory_mb &gt; 0.95</c>. Those are the perfmon Total and Target
/// Server Memory counters, and they converge at 1.0 the moment an instance is warmed. Measured on 42
/// production servers: median <b>1.0000</b>, min 0.9997, max 1.0002 — so every server tripped it, and
/// <c>OVER_PROVISIONED</c>, whose arm needs the same ratio below 0.5, was unreachable at any workload.</para>
///
/// <para><b>Why these tests exist rather than a fleet check.</b> The replacement reads real pressure signals
/// — grant waiters, grant timeouts, forced grants — and across the store's entire retained history those are
/// zero: 0 nonzero of 2,938,711 grant rows, and 0 of 1,000,560 <c>Memory Grants Pending</c> samples. That is
/// the correct answer for a fleet with no memory pressure, and it also means the fleet CANNOT demonstrate
/// that the alarm fires. So the positive control is built here instead, one input at a time.</para>
///
/// <para>Every threshold asserted below has the fleet distribution behind it, recorded on
/// <see cref="ProvisioningVerdict"/> itself: CPU p95 max 51.0 against a limit of 85, worker ratio max 0.635
/// against 0.8, grant utilization max 18.8% against 50%.</para>
/// </summary>
public sealed class ProvisioningVerdictTests
{
    /// <summary>A quiet server with no pressure anywhere: the downsizing candidate the report exists to
    /// find, and the verdict the old rule could never reach.</summary>
    [Fact]
    public void AQuietServerWithNoPressure_IsOverProvisioned()
    {
        Assert.Equal(
            ProvisioningVerdict.OverProvisioned,
            ProvisioningVerdict.Evaluate(
                avgCpuPercent: 6m, maxCpuPercent: 22m, p95CpuPercent: 11m,
                maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
                grantUtilizationPercent: 0.5m, maxWorkers: 576, currentWorkers: 142));
    }

    /// <summary>The fleet's own median server, from the measurement that drove this change: avg 6.5, p95
    /// 11.0, grant utilization 0.5%, worker ratio 0.246. It must come out OVER_PROVISIONED — under the old
    /// rule this exact server was reported as starved for memory.</summary>
    [Fact]
    public void TheFleetsMedianServer_IsNotReportedAsStarved()
    {
        var verdict = ProvisioningVerdict.Evaluate(
            avgCpuPercent: 6.5m, maxCpuPercent: 30m, p95CpuPercent: 11.0m,
            maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
            grantUtilizationPercent: 0.5m, maxWorkers: 576, currentWorkers: 142);

        Assert.NotEqual(ProvisioningVerdict.UnderProvisioned, verdict);
        Assert.Equal(ProvisioningVerdict.OverProvisioned, verdict);
    }

    /// <summary>Busy enough not to shrink, not pressured enough to grow.</summary>
    [Fact]
    public void ABusyButHealthyServer_IsRightSized()
    {
        Assert.Equal(
            ProvisioningVerdict.RightSized,
            ProvisioningVerdict.Evaluate(
                avgCpuPercent: 35m, maxCpuPercent: 70m, p95CpuPercent: 60m,
                maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
                grantUtilizationPercent: 12m, maxWorkers: 576, currentWorkers: 200));
    }

    /// <summary>THE POSITIVE CONTROL the fleet cannot supply: each pressure input alone must raise
    /// UNDER_PROVISIONED, driven one at a time so a single over-broad condition cannot mask a dead one.
    /// </summary>
    [Theory]
    [InlineData(1, 0, 0, "a query waited for a workspace-memory grant")]
    [InlineData(0, 1, 0, "a grant request timed out")]
    [InlineData(0, 0, 1, "a grant was forced through below its request")]
    public void AnyMemoryPressureSignal_AloneRaisesUnderProvisioned(
        long waiters, long timeouts, long forced, string because)
    {
        var verdict = ProvisioningVerdict.Evaluate(
            avgCpuPercent: 6m, maxCpuPercent: 22m, p95CpuPercent: 11m,
            maxGrantWaiters: waiters, grantTimeouts: timeouts, forcedGrants: forced,
            grantUtilizationPercent: 0.5m, maxWorkers: 576, currentWorkers: 142);

        /* The CPU numbers here are the quiet ones from the OVER_PROVISIONED case above, so this also pins
           the ORDERING: pressure outranks idleness. Recommending a smaller instance for a server whose
           queries are queueing for memory would be the worst possible answer. */
        Assert.Equal(ProvisioningVerdict.UnderProvisioned, verdict);
        Assert.NotEqual(ProvisioningVerdict.OverProvisioned, verdict);
        Assert.False(string.IsNullOrEmpty(because));
    }

    /// <summary>Sustained CPU still means under-provisioned. Threshold unchanged from the rule this
    /// replaces; the fleet's highest p95 is 51.0, so it stays reachable rather than academic.</summary>
    [Fact]
    public void SustainedHighCpu_IsUnderProvisioned()
    {
        Assert.Equal(
            ProvisioningVerdict.UnderProvisioned,
            ProvisioningVerdict.Evaluate(
                avgCpuPercent: 70m, maxCpuPercent: 99m, p95CpuPercent: 92m,
                maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
                grantUtilizationPercent: 5m, maxWorkers: 576, currentWorkers: 200));
    }

    /// <summary>Worker-thread saturation, the term the Full Dashboard's view has always carried and the app
    /// copies dropped — so a worker-starved server was invisible to both of them.</summary>
    [Fact]
    public void WorkerThreadSaturation_IsUnderProvisioned()
    {
        Assert.Equal(
            ProvisioningVerdict.UnderProvisioned,
            ProvisioningVerdict.Evaluate(
                avgCpuPercent: 6m, maxCpuPercent: 22m, p95CpuPercent: 11m,
                maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
                grantUtilizationPercent: 0.5m, maxWorkers: 100, currentWorkers: 81));
    }

    /// <summary>Unknown is not saturation. A sample that never reported a worker ceiling must not imply
    /// exhaustion — the same rule the collector gates follow for an unclassified target, and without it a
    /// missing column would silently flag every server.</summary>
    [Fact]
    public void AnUnknownWorkerCeiling_IsNotSaturation()
    {
        Assert.Equal(
            ProvisioningVerdict.OverProvisioned,
            ProvisioningVerdict.Evaluate(
                avgCpuPercent: 6m, maxCpuPercent: 22m, p95CpuPercent: 11m,
                maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
                grantUtilizationPercent: 0.5m, maxWorkers: 0, currentWorkers: 9999));
    }

    /// <summary>Quiet on CPU but working its semaphore hard: not a downsizing candidate. This is the term
    /// that stops "idle" from being decided on CPU alone, and the fleet's peak utilization is 18.8%, so it
    /// excludes nothing real today.</summary>
    [Fact]
    public void IdleCpuButHighGrantUtilization_IsNotOverProvisioned()
    {
        var verdict = ProvisioningVerdict.Evaluate(
            avgCpuPercent: 6m, maxCpuPercent: 22m, p95CpuPercent: 11m,
            maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
            grantUtilizationPercent: 65m, maxWorkers: 576, currentWorkers: 142);

        Assert.Equal(ProvisioningVerdict.RightSized, verdict);
    }

    /// <summary>
    /// The REASON must name the condition that actually fired. The UI used to derive this itself as
    /// "p95 &gt; 85 ? CPU : blame the memory ratio", so once the verdict gained grant-pressure and
    /// worker-thread reasons, every one of those would have been explained as a memory ratio that no longer
    /// decides anything — citing a threshold the code does not check. These pin that each cause explains
    /// itself.
    /// </summary>
    [Fact]
    public void TheReasonNamesTheConditionThatFired()
    {
        var cpu = ProvisioningVerdict.UnderProvisionedReason(92m, 0, 0, 0, 576, 200);
        Assert.Contains("CPU p95 is 92.0%", cpu, System.StringComparison.Ordinal);

        /* Grant pressure with QUIET cpu: the old text would have blamed the memory ratio here. */
        var grants = ProvisioningVerdict.UnderProvisionedReason(11m, 3, 1, 2, 576, 142);
        Assert.Contains("workspace memory", grants, System.StringComparison.Ordinal);
        Assert.Contains("3 grant waiter(s)", grants, System.StringComparison.Ordinal);
        Assert.Contains("1 grant timeout(s)", grants, System.StringComparison.Ordinal);
        Assert.Contains("2 forced grant(s)", grants, System.StringComparison.Ordinal);
        /* And it must NOT cite the retired threshold. */
        Assert.DoesNotContain("0.95", grants, System.StringComparison.Ordinal);
        Assert.DoesNotContain("memory ratio", grants, System.StringComparison.Ordinal);

        var workers = ProvisioningVerdict.UnderProvisionedReason(11m, 0, 0, 0, 100, 81);
        Assert.Contains("Worker threads", workers, System.StringComparison.Ordinal);
        Assert.Contains("81 of 100", workers, System.StringComparison.Ordinal);

        /* Asked about inputs that are not under-provisioned, it says so rather than inventing a cause. */
        var none = ProvisioningVerdict.UnderProvisionedReason(11m, 0, 0, 0, 576, 142);
        Assert.Contains("No under-provisioning condition", none, System.StringComparison.Ordinal);
    }

    /// <summary>The boundaries themselves, since every one of them is a published constant that something
    /// downstream will eventually be tuned against. Strict comparisons, so a value sitting exactly ON a
    /// limit does not trip it.</summary>
    [Fact]
    public void TheThresholdsAreStrict_AtTheirExactValues()
    {
        /* p95 exactly 85 is not "> 85". */
        Assert.NotEqual(
            ProvisioningVerdict.UnderProvisioned,
            ProvisioningVerdict.Evaluate(50m, 90m, ProvisioningVerdict.HighCpuP95Percent,
                0, 0, 0, 5m, 576, 200));

        /* avg exactly 15 is not "< 15", so it cannot be over-provisioned. */
        Assert.Equal(
            ProvisioningVerdict.RightSized,
            ProvisioningVerdict.Evaluate(ProvisioningVerdict.IdleAvgCpuPercent, 30m, 20m,
                0, 0, 0, 5m, 576, 142));

        /* worker ratio exactly 0.8 is not "> 0.8". */
        Assert.Equal(
            ProvisioningVerdict.OverProvisioned,
            ProvisioningVerdict.Evaluate(6m, 22m, 11m, 0, 0, 0, 0.5m, maxWorkers: 100, currentWorkers: 80));
    }
}
