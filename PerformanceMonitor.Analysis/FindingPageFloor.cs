/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The measured-cost floors a finding must clear to PAGE (#7084). A finding under its floor is still a finding: it is
/// persisted, shown on every read surface and recorded as before. It just does not go to a paging channel.
///
/// <para>Three families paged on a statistical move alone, with no check that the move cost anything: a write-latency
/// anomaly over a baseline built from the slot's own routine (178 pages in 14 days on three stores), a plan-regression
/// chain at 5 to 25 percent CPU, and a PostgreSQL wait-mix change whose top wait was 0.2 to 0.6 ms (18 pages, none
/// real). Each floor reads a number the analysis already held for the window.</para>
///
/// <list type="bullet">
///   <item><description><b>Write latency.</b> <c>ANOMALY_WRITE_LATENCY</c> pages only when the window's average write
///   latency (<c>avg_latency_ms</c> on the root fact) is at or over <see cref="WriteLatencyAvgFloorMs"/>. That is the
///   engine's own write-latency bar (<see cref="AnomalyThresholds.WriteLatencyFloorMs"/>), which until now was applied to
///   the window PEAK only, so a window whose mean sat at 12 ms behind one 40 ms sample cleared it.</description></item>
///   <item><description><b>Plan-regression chain.</b> A story with <c>PLAN_REGRESSION</c> on its path pages only when the
///   server's average CPU in the window (<c>CPU_SQL_PERCENT</c>'s <c>avg_total_cpu</c>) is at or over
///   <see cref="PlanRegressionCpuFloorPercent"/>, or when the PLAN_REGRESSION fact reports queries whose timeouts
///   (Query Store Aborted runs, which a client attention event produces) rose (<c>timeout_offender_count</c> above 0).
///   No CPU fact in the pass reads as 0 percent.</description></item>
///   <item><description><b>PostgreSQL wait profile.</b> <c>ANOMALY_PG_WAIT_PROFILE</c> and
///   <c>ANOMALY_PG_SAMPLED_WAIT_PROFILE</c> page only when the window's mean wait rate (<c>mean_ms_per_sec</c> divided by
///   1000, the average number of backends waiting) is at or over <see cref="WaitProfileAvgWaitersFloor"/>, and at most
///   once per server per story per <see cref="WaitProfileCap"/> (the caller keeps the cap's clock).</description></item>
/// </list>
/// </summary>
public static class FindingPageFloor
{
    /// <summary>Average write latency (ms) a write-latency finding must reach to page. The engine's existing write bar,
    /// not a second number.</summary>
    public const double WriteLatencyAvgFloorMs = AnomalyThresholds.WriteLatencyFloorMs;

    /// <summary>Average server CPU percent a plan-regression chain needs to page when no query timed out.</summary>
    public const double PlanRegressionCpuFloorPercent = 50.0;

    /// <summary>Average waiting backends a PostgreSQL wait-profile finding needs to page. Poison Wait's Warning bar
    /// (<c>PoisonWaitEvaluator.WarningAvgWaiters</c> aliases this constant, so there is one definition).</summary>
    public const double WaitProfileAvgWaitersFloor = 1.0;

    /// <summary>One page per server per story per this long for a PostgreSQL wait-profile finding.</summary>
    public static readonly TimeSpan WaitProfileCap = TimeSpan.FromHours(24);

    /// <summary><see cref="AnalysisFinding.PageEvidence"/> key: the server's average total CPU percent in the window.</summary>
    public const string EvidenceWindowAvgCpu = "window_avg_cpu_percent";

    /// <summary><see cref="AnalysisFinding.PageEvidence"/> key: how many queries' timeouts rose (PLAN_REGRESSION).</summary>
    public const string EvidenceTimeoutOffenders = "timeout_offender_count";

    private const string PlanRegressionKey = "PLAN_REGRESSION";
    private const string CpuKey = "CPU_SQL_PERCENT";
    private const string WriteLatencyKey = "ANOMALY_WRITE_LATENCY";

    /// <summary>
    /// The window figures a chain's page floor needs, read off the pass's facts while they are in hand (a fact that scored 0,
    /// like a 15 percent CPU, is not in the story's working set). Null when the path holds no PLAN_REGRESSION: no other floor
    /// reads this. Called by the inference engine once per story.
    /// </summary>
    public static Dictionary<string, double>? Evidence(IReadOnlyList<string> path, IEnumerable<Fact> facts)
    {
        if (path is null || facts is null || !Contains(path, PlanRegressionKey))
            return null;

        double cpu = 0;
        double timeouts = 0;
        foreach (var fact in facts)
        {
            if (string.Equals(fact.Key, CpuKey, StringComparison.Ordinal))
            {
                cpu = fact.Metadata is { } m && m.TryGetValue("avg_total_cpu", out var total) ? total : fact.Value;
            }
            else if (string.Equals(fact.Key, PlanRegressionKey, StringComparison.Ordinal)
                     && fact.Metadata is { } pm && pm.TryGetValue(EvidenceTimeoutOffenders, out var t))
            {
                timeouts = t;
            }
        }

        return new Dictionary<string, double>(StringComparer.Ordinal)
        {
            [EvidenceWindowAvgCpu] = cpu,
            [EvidenceTimeoutOffenders] = timeouts,
        };
    }

    /// <summary>
    /// Why this finding must not page, or null when it clears its floor (or has no floor). Pure. A figure the finding does not
    /// carry (a finding read back from the store) never holds it: the floor is a write-path rule.
    /// </summary>
    public static string? HoldReason(AnalysisFinding finding)
    {
        if (finding is null)
            throw new ArgumentNullException(nameof(finding));

        if (string.Equals(finding.RootFactKey, WriteLatencyKey, StringComparison.Ordinal)
            && finding.RootFactMetadata is { } wm && wm.TryGetValue("avg_latency_ms", out var avgMs)
            && avgMs < WriteLatencyAvgFloorMs)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"write latency averaged {avgMs:0.#} ms in the window, under the {WriteLatencyAvgFloorMs:0} ms floor to page.");
        }

        if (Contains(finding.PathKeys, PlanRegressionKey) && finding.PageEvidence is { } ev)
        {
            var cpu = ev.TryGetValue(EvidenceWindowAvgCpu, out var c) ? c : 0;
            var timeouts = ev.TryGetValue(EvidenceTimeoutOffenders, out var t) ? t : 0;
            if (cpu < PlanRegressionCpuFloorPercent && timeouts <= 0)
            {
                return string.Create(CultureInfo.InvariantCulture,
                    $"plan regression with server CPU averaging {cpu:0.#}% in the window (floor {PlanRegressionCpuFloorPercent:0}%) and no query timeouts.");
            }
        }

        if (IsCappedWaitProfile(finding)
            && finding.RootFactMetadata is { } pm && pm.TryGetValue("mean_ms_per_sec", out var msPerSec)
            && msPerSec / 1000.0 < WaitProfileAvgWaitersFloor)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"wait profile averaged {msPerSec / 1000.0:0.##} waiting backends in the window, under the {WaitProfileAvgWaitersFloor:0.0} floor to page.");
        }

        return null;
    }

    /// <summary>True for a PostgreSQL wait-profile finding, the one family that also pages at most once per
    /// <see cref="WaitProfileCap"/> per server per story.</summary>
    public static bool IsCappedWaitProfile(AnalysisFinding finding)
        => finding is not null
           && (string.Equals(finding.RootFactKey, PgTargetFactKeys.AnomalyWaitProfile, StringComparison.Ordinal)
               || string.Equals(finding.RootFactKey, PgTargetFactKeys.AnomalySampledWaitProfile, StringComparison.Ordinal));

    private static bool Contains(IReadOnlyList<string>? keys, string key)
    {
        if (keys is null)
            return false;
        for (var i = 0; i < keys.Count; i++)
        {
            if (string.Equals(keys[i], key, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
