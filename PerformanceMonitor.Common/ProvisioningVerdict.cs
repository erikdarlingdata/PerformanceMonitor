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
/// The FinOps provisioning verdict for one server — over-provisioned, right-sized, under-provisioned, or not applicable (a logical server's master) —
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

    /// <summary>The fewest CPU samples a window must hold before it gets any verdict (#6139). Twelve is an hour of the
    /// collector's five-minute cadence: a server that came back from a long gap with six samples is not called anything yet.
    /// The one place the number lives; every verdict site passes its sample count to <see cref="Evaluate"/>.</summary>
    public const long MinSamples = 12;

    /// <summary>Stolen memory (total server memory not in the buffer pool) at or above this share is memory pressure, not spare
    /// memory (#6139): a buffer pool that is a small share of physical RAM because the rest is stolen has nothing to give back.
    /// The one place the number lives. It is also where the Stolen Mem % figure starts to read amber.</summary>
    public const decimal HighStolenMemoryPercent = 60m;

    /// <summary>A chosen value, not a measured one (#6646): the share of the memory basis the buffer pool must hold for a RIGHT_SIZED
    /// sentence to call memory "well-utilized". It decides wording only, never the verdict.</summary>
    public const double WellUtilizedBufferPoolPercent = 50.0;

    /// <summary>Too few CPU samples in the window to say (<see cref="MinSamples"/>).</summary>
    public const string NotEnoughData = "NOT_ENOUGH_DATA";

    /// <summary>What a grid cell shows for <see cref="NotEnoughData"/>, where the row carries no sample count.</summary>
    public const string NotEnoughDataShortLabel = "not enough data yet";

    /// <summary>What the verdict badge shows for <see cref="NotEnoughData"/>: "not enough data yet (6 samples)".</summary>
    public static string NotEnoughDataLabel(long samples) =>
        string.Create(System.Globalization.CultureInfo.CurrentCulture, $"{NotEnoughDataShortLabel} ({samples:N0} {(samples == 1 ? "sample" : "samples")})");

    /// <summary>The words a screen shows for a verdict token: "No Data" for none, "N/A" for a logical server's master, "not enough data yet
    /// (6 samples)" for <see cref="NotEnoughData"/> (the short form when <paramref name="samples"/> is null), else the token with spaces.
    /// One place, so the Viewer, Lite and the web page read alike (#6139).</summary>
    public static string StatusLabel(string? status, long? samples = null) =>
        string.IsNullOrEmpty(status) ? "No Data"
        : status == NotApplicable ? NotApplicableLabel
        : status == NotEnoughData ? (samples is long n ? NotEnoughDataLabel(n) : NotEnoughDataShortLabel)
        : status.Replace("_", " ");

    /// <summary>The sentence shown beside <see cref="NotEnoughData"/>.</summary>
    public static string NotEnoughDataExplanation(long samples) =>
        string.Create(System.Globalization.CultureInfo.CurrentCulture,
            $"The last 24 hours hold {samples:N0} CPU {(samples == 1 ? "sample" : "samples")}; a verdict needs at least {MinSamples:N0}.");

    /// <summary>Stolen memory: (total server memory - buffer pool) / total server memory, in percent; 0 when there is no total.</summary>
    public static decimal StolenMemoryPercent(double totalServerMemoryMb, double bufferPoolMb) =>
        totalServerMemoryMb > 0 ? (decimal)((totalServerMemoryMb - bufferPoolMb) / totalServerMemoryMb * 100.0) : 0m;

    /// <summary>The tooltip for a Stolen Mem % figure that reads amber or red, so a coloured figure says why; empty when it is not.</summary>
    public static string StolenMemoryNote(double stolenPercent) =>
        stolenPercent < (double)HighStolenMemoryPercent
            ? ""
            : string.Create(System.Globalization.CultureInfo.CurrentCulture,
                $"{stolenPercent:N0}% of the memory SQL Server holds is outside the buffer pool (plans, grants, other caches). Amber from {HighStolenMemoryPercent:N0}%, red from 85%.");

    /// <summary>Over-provisioned: a downsizing candidate.</summary>
    public const string OverProvisioned = "OVER_PROVISIONED";

    /// <summary>Neither idle enough to shrink nor under pressure.</summary>
    public const string RightSized = "RIGHT_SIZED";

    /// <summary>Under pressure on CPU, workspace memory, or worker threads.</summary>
    public const string UnderProvisioned = "UNDER_PROVISIONED";

    /// <summary>No service objective to resize: a logical server's <c>master</c> database.</summary>
    public const string NotApplicable = "NOT_APPLICABLE";

    /// <summary>What the verdict badge shows for <see cref="NotApplicable"/>.</summary>
    public const string NotApplicableLabel = "N/A";

    /// <summary>The sentence shown beside <see cref="NotApplicable"/>.</summary>
    public const string NotApplicableExplanation = "A logical server's master database has no service objective to resize.";

    /// <summary>
    /// The verdict for one server from one window's measurements. A logical server's <c>master</c>
    /// (<see cref="ServerHardwareScope.IsLogicalServerMaster"/>) gets <see cref="NotApplicable"/> whatever it measured.
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
    /// <param name="currentWorkers">Workers in use at the latest sample; <c>null</c> where the collector cannot read it (an
    /// Azure SQL Database), which is unknown: it never counts as zero in use and it cannot imply saturation.</param>
    /// <param name="engineEdition">The target's engine edition, when known.</param>
    /// <param name="edition">The target's stored edition, when known.</param>
    public static string Evaluate(
        decimal avgCpuPercent,
        decimal maxCpuPercent,
        decimal p95CpuPercent,
        long maxGrantWaiters,
        long grantTimeouts,
        long forcedGrants,
        decimal grantUtilizationPercent,
        int maxWorkers,
        int? currentWorkers,
        int? engineEdition = null,
        string? edition = null,
        long? cpuSamples = null,
        decimal? stolenMemoryPercent = null)
    {
        if (ServerHardwareScope.IsLogicalServerMaster(engineEdition, edition))
        {
            return NotApplicable;
        }

        /* #6139: a window with too few samples says nothing. Null is a caller that does not count samples (a unit test of the
           other rules); every production site passes its count. */
        if (cpuSamples is long counted && counted < MinSamples)
        {
            return NotEnoughData;
        }

        /* Any of these three is a query that asked for workspace memory and did not simply get it. They are
           counts of events, not levels, so there is no threshold to tune — and on a fleet with no memory
           pressure they are all zero, which is the point. */
        var memoryPressure = maxGrantWaiters > 0 || grantTimeouts > 0 || forcedGrants > 0;

        /* maxWorkers <= 0 means the sample never reported a ceiling. Unknown is not saturation: the same
           rule the collector gates follow, where an unclassified target must never be gated off by
           assumption. */
        var workerPressure = maxWorkers > 0
            && currentWorkers is int inUse
            && inUse / (double)maxWorkers > HighWorkerRatio;

        if (p95CpuPercent > HighCpuP95Percent || memoryPressure || workerPressure || StolenMemoryIsHigh(stolenMemoryPercent, engineEdition))
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

    /// <summary>Stolen memory is at or above <see cref="HighStolenMemoryPercent"/> (#6139). Not on an Azure SQL Database, whose memory
    /// figures describe the database's own limit and not a host's RAM.</summary>
    public static bool StolenMemoryIsHigh(decimal? stolenMemoryPercent, int? engineEdition) =>
        stolenMemoryPercent is decimal stolen && stolen >= HighStolenMemoryPercent && !ServerHardwareScope.HardwareIsTheHosts(engineEdition);

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
        int? currentWorkers,
        decimal? stolenMemoryPercent = null,
        int? engineEdition = null)
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

        if (maxWorkers > 0 && currentWorkers is int inUse && inUse / (double)maxWorkers > HighWorkerRatio)
        {
            return $"Worker threads are near the limit: {inUse} of {maxWorkers} in use "
                + $"(threshold: {HighWorkerRatio:P0}). This server may need more CPU capacity.";
        }

        if (StolenMemoryIsHigh(stolenMemoryPercent, engineEdition))
        {
            return $"Memory is under pressure: stolen memory is {stolenMemoryPercent:N0}% of total server memory (threshold: {HighStolenMemoryPercent:N0}%), "
                + "so the buffer pool's small share of RAM is not spare memory. This server may need more memory.";
        }

        /* Reachable only if a caller asks for a reason on inputs that are not under-provisioned. Say so
           rather than inventing a cause, which is the failure this method exists to end. */
        return "No under-provisioning condition is currently met.";
    }
}
