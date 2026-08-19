/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The FinOps provisioning verdict for one server — over-provisioned, right-sized, or under-provisioned —
/// as a single shared predicate both apps call.
///
/// <para><b>Why this is shared rather than inlined.</b> The rule this replaces was copy-pasted four times
/// (Darling's point-in-time and trend reads, Lite's two), and every copy carried the same defect: it tested
/// <c>total_server_memory_mb / target_server_memory_mb &gt; 0.95</c>. Those are the perfmon Total and Target
/// Server Memory counters, and they converge at 1.0 the moment an instance is warmed — Target is what SQL
/// Server wants and Total is what it holds. Measured across a 42-server production fleet, that ratio ran
/// median <b>1.0000</b> (min 0.9997, max 1.0002), so the rule reported <b>UNDER_PROVISIONED for every
/// server</b> and <c>OVER_PROVISIONED</c>, whose arm needs the same ratio below 0.5, was unreachable at any
/// workload (#2246). One predicate, compiler-shared, is the same answer the collector gate surface reached
/// when it collapsed two drifting layers into one.</para>
///
/// <para><b>Every threshold below was measured, not chosen.</b> Fleet distributions over 24 hours:</para>
/// <list type="bullet">
/// <item>CPU p95 per server: min 0.0, p25 6.5, median 11.0, p95 48.6, max <b>51.0</b> — so
/// <see cref="HighCpuP95Percent"/> at 85 is comfortably clear of a healthy fleet and still reachable.</item>
/// <item>Worker ratio per server: median 0.246, max <b>0.635</b>; zero servers over 0.8 and zero exhaustion
/// warnings, so <see cref="HighWorkerRatio"/> at 0.8 does not false-fire here.</item>
/// <item>Workspace-grant utilization (granted / target): median <b>0.5%</b>, p95 12.0%, max 18.8% — so
/// <see cref="IdleGrantUtilizationPercent"/> at 50 excludes nothing real today while still refusing to call
/// a server idle that is straining its semaphore.</item>
/// <item>Grant waiters, grant timeouts, forced grants and <c>Memory Grants Pending</c>: <b>zero across
/// 2,938,711 grant rows and 1,000,560 counter rows</b>, the entire retained history. This fleet has no
/// memory pressure, which is exactly why a correct memory term must be SILENT on it.</item>
/// </list>
///
/// <para><b>The honest gap.</b> Because nothing in that history ever recorded grant pressure, there is no
/// positive control from the fleet: the measurements show the memory term stays quiet when it should, and
/// cannot show it fires when it should. That is established by construction in the tests instead, which
/// drive each pressure input directly.</para>
///
/// <para><b>Pressure outranks idleness</b> — <see cref="Evaluate"/> tests under-provisioning first. A server
/// can be quiet on CPU while queries queue for workspace memory, and calling that one over-provisioned would
/// recommend taking away the resource it is short of.</para>
/// </summary>
public static class ProvisioningVerdict
{
    /// <summary>Sustained CPU at or above this p95 is under-provisioned. Fleet max p95 is 51.0.</summary>
    public const decimal HighCpuP95Percent = 85m;

    /// <summary>Below this average CPU, with <see cref="IdleMaxCpuPercent"/> also satisfied, a server is a
    /// downsizing candidate.</summary>
    public const decimal IdleAvgCpuPercent = 15m;

    /// <summary>An idle server must also never have spiked past this. Guards against averaging away a
    /// short daily peak that the smaller instance would not survive.</summary>
    public const decimal IdleMaxCpuPercent = 40m;

    /// <summary>Worker-thread saturation. The Full Dashboard's view has always carried this term and the
    /// app copies dropped it, so a genuinely worker-starved server was invisible to them (#2246).</summary>
    public const double HighWorkerRatio = 0.8;

    /// <summary>A server using more than this share of its workspace-memory grant target is not idle, no
    /// matter how quiet its CPU looks. Fleet max is 18.8%.</summary>
    public const decimal IdleGrantUtilizationPercent = 50m;

    /// <summary>Over-provisioned: a downsizing candidate.</summary>
    public const string OverProvisioned = "OVER_PROVISIONED";

    /// <summary>Neither idle enough to shrink nor under pressure.</summary>
    public const string RightSized = "RIGHT_SIZED";

    /// <summary>Under pressure on CPU, workspace memory, or worker threads.</summary>
    public const string UnderProvisioned = "UNDER_PROVISIONED";

    /// <summary>
    /// The verdict for one server from one window's measurements.
    /// </summary>
    /// <param name="avgCpuPercent">Mean SQL Server CPU over the window.</param>
    /// <param name="maxCpuPercent">Peak SQL Server CPU over the window.</param>
    /// <param name="p95CpuPercent">95th-percentile SQL Server CPU over the window.</param>
    /// <param name="maxGrantWaiters">Peak <c>waiter_count</c> from the resource semaphore. Any waiter at all
    /// means a query could not get workspace memory when it asked.</param>
    /// <param name="grantTimeouts">Grant timeouts accrued over the window (delta, not cumulative).</param>
    /// <param name="forcedGrants">Forced grants accrued over the window (delta, not cumulative).</param>
    /// <param name="grantUtilizationPercent">Peak granted-over-target workspace memory, as a percentage.</param>
    /// <param name="maxWorkers">The instance's worker-thread ceiling; 0 or negative means unknown, which
    /// cannot imply saturation.</param>
    /// <param name="currentWorkers">Workers in use at the latest sample.</param>
    public static string Evaluate(
        decimal avgCpuPercent,
        decimal maxCpuPercent,
        decimal p95CpuPercent,
        long maxGrantWaiters,
        long grantTimeouts,
        long forcedGrants,
        decimal grantUtilizationPercent,
        int maxWorkers,
        int currentWorkers)
    {
        /* Any of these three is a query that asked for workspace memory and did not simply get it. They are
           counts of events, not levels, so there is no threshold to tune — and on a fleet with no memory
           pressure they are all zero, which is the point. */
        var memoryPressure = maxGrantWaiters > 0 || grantTimeouts > 0 || forcedGrants > 0;

        /* maxWorkers <= 0 means the sample never reported a ceiling. Unknown is not saturation: the same
           rule the collector gates follow, where an unclassified target must never be gated off by
           assumption. */
        var workerPressure = maxWorkers > 0
            && currentWorkers / (double)maxWorkers > HighWorkerRatio;

        if (p95CpuPercent > HighCpuP95Percent || memoryPressure || workerPressure)
        {
            return UnderProvisioned;
        }

        if (avgCpuPercent < IdleAvgCpuPercent
            && maxCpuPercent < IdleMaxCpuPercent
            && grantUtilizationPercent < IdleGrantUtilizationPercent)
        {
            return OverProvisioned;
        }

        return RightSized;
    }

    /// <summary>
    /// Why <see cref="Evaluate"/> returned <see cref="UnderProvisioned"/>, in the operator's words.
    ///
    /// <para>Shared for the same reason the verdict is: the UI used to re-derive the cause with
    /// <c>P95CpuPct &gt; 85 ? "CPU..." : "memory ratio is {x} (threshold: 0.95)"</c>, so once the verdict
    /// gained grant-pressure and worker-thread reasons, every one of those would have been explained as a
    /// memory ratio that no longer decides anything — a fabricated cause citing a threshold the code does
    /// not check. Deriving the text beside the decision is what stops that recurring.</para>
    ///
    /// <para>Checked in the same order <see cref="Evaluate"/> checks them, so the reason names the condition
    /// that actually fired first.</para>
    /// </summary>
    public static string UnderProvisionedReason(
        decimal p95CpuPercent,
        long maxGrantWaiters,
        long grantTimeouts,
        long forcedGrants,
        int maxWorkers,
        int currentWorkers)
    {
        if (p95CpuPercent > HighCpuP95Percent)
        {
            return $"CPU p95 is {p95CpuPercent:N1}% (threshold: {HighCpuP95Percent:N0}%). "
                + "This server may need more CPU capacity.";
        }

        if (maxGrantWaiters > 0 || grantTimeouts > 0 || forcedGrants > 0)
        {
            return "Queries could not get the workspace memory they asked for: peak "
                + $"{maxGrantWaiters} grant waiter(s), {grantTimeouts} grant timeout(s), "
                + $"{forcedGrants} forced grant(s). This server may need more memory.";
        }

        if (maxWorkers > 0 && currentWorkers / (double)maxWorkers > HighWorkerRatio)
        {
            return $"Worker threads are near the limit: {currentWorkers} of {maxWorkers} in use "
                + $"(threshold: {HighWorkerRatio:P0}). This server may need more CPU capacity.";
        }

        /* Reachable only if a caller asks for a reason on inputs that are not under-provisioned. Say so
           rather than inventing a cause, which is the failure this method exists to end. */
        return "No under-provisioning condition is currently met.";
    }
}
