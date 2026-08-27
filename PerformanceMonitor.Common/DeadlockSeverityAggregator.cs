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

namespace PerformanceMonitor.Common;

/// <summary>
/// One per-minute bucket of deadlock SEVERITY: the count of deadlock VICTIM processes in the bucket, plus
/// the total / max / average deadlock wait over EVERY process in the bucket's graphs (average is
/// process-weighted, i.e. total divided by process count).
/// </summary>
public sealed record DeadlockSeverityStatsPoint(
    DateTime Time, int VictimCount, long TotalWaitMs, long MaxWaitMs, double AvgWaitMs);

/// <summary>
/// Buckets parsed deadlock graphs into per-minute severity points.
///
/// <para>This lived as an <c>internal</c> method on the Darling viewer's data service, which meant the
/// headless service could not reach it — so #2484's Blocking Stats endpoint had the choice of duplicating
/// the arithmetic or moving it here. A second copy of "what counts as a victim" is the kind of divergence
/// that shows up as two surfaces quietly disagreeing about the same deadlock, so it moved.</para>
///
/// <para>Semantics are the Dashboard analyzer's, unchanged: <c>victim_count</c> counts processes flagged
/// victim, and <c>total_deadlock_wait_time_ms</c> sums EVERY process's wait, not just the victims'. A graph
/// with no parseable process (empty or malformed XML) or a null deadlock time contributes nothing rather
/// than contributing a zero, because an unparseable graph is absent evidence and not evidence of calm.</para>
/// </summary>
public static class DeadlockSeverityAggregator
{
    /// <summary>
    /// Buckets graphs by the minute of their DEADLOCK time — the same truncation the count trend applies
    /// as <c>DATE_TRUNC('minute', deadlock_time)</c>, so a severity chart and a count chart drawn over the
    /// same window line up bucket for bucket. Pure: no store access, so it is testable
    /// without one, which is how the viewer's copy was already pinned.
    /// </summary>
    public static List<DeadlockSeverityStatsPoint> Aggregate(
        IReadOnlyList<(DateTime? DeadlockTime, string? Xml)> graphs)
    {
        var buckets = new Dictionary<DateTime, Accumulator>();

        foreach (var (deadlockTime, xml) in graphs)
        {
            if (deadlockTime is not { } dt)
                continue;

            var model = DeadlockGraphParser.Parse(xml);
            if (model.IsEmpty)
                continue;

            var bucket = TruncateToMinute(dt);
            if (!buckets.TryGetValue(bucket, out var acc))
            {
                acc = new Accumulator();
                buckets[bucket] = acc;
            }

            foreach (var process in model.Processes)
            {
                if (process.IsVictim)
                    acc.VictimCount++;
                acc.TotalWaitMs += process.WaitTimeMs;
                if (process.WaitTimeMs > acc.MaxWaitMs)
                    acc.MaxWaitMs = process.WaitTimeMs;
                acc.ProcessCount++;
            }
        }

        /* The dictionary rollup does not preserve read order, so the sort is re-applied here. */
        return buckets
            .OrderBy(kvp => kvp.Key)
            .Select(kvp => new DeadlockSeverityStatsPoint(
                kvp.Key,
                kvp.Value.VictimCount,
                kvp.Value.TotalWaitMs,
                kvp.Value.MaxWaitMs,
                kvp.Value.ProcessCount > 0 ? (double)kvp.Value.TotalWaitMs / kvp.Value.ProcessCount : 0.0))
            .ToList();
    }

    /* The viewer's implementation verbatim. Arithmetically the same as rebuilding the date from its
       parts, but kept identical so this move cannot change a single bucket boundary. */
    private static DateTime TruncateToMinute(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute), value.Kind);

    /// <summary>Mutable per-bucket accumulator — a reference type so the TryGetValue handle mutates the
    /// stored instance in place. One allocation per populated minute, negligible at deadlock volumes.</summary>
    private sealed class Accumulator
    {
        public int VictimCount;
        public long TotalWaitMs;
        public long MaxWaitMs;
        public int ProcessCount;
    }
}
