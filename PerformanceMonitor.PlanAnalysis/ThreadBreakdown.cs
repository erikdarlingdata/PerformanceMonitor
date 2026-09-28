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

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// One metric's per-thread lines inside a collapsed breakdown group.
/// </summary>
public class ThreadBreakdownMetricGroup
{
    public string Metric { get; set; } = "";
    public string Unit { get; set; } = "";
    public List<(int ThreadId, long Value)> Threads { get; set; } = new();
}

/// <summary>
/// The shape of one section's "Per-thread breakdown" sub-expander: its metric groups (each already
/// filtered down to the threads that belong in it), plus the header text and skew suffix.
/// </summary>
public class ThreadBreakdownResult
{
    public List<ThreadBreakdownMetricGroup> Groups { get; set; } = new();
    public string HeaderText { get; set; } = "";
    public bool IsSkewed { get; set; }
    public string SkewSuffix { get; set; } = "";
}

/// <summary>
/// Builds the pure shape of a plan node's per-thread breakdown: which metrics have per-thread data
/// worth showing, in what order, and whether the header should carry a skew suffix.
///
/// <para>Every actual metric used to emit one indented "Thread N" row per thread inline, right
/// under its own summary row. A DOP 4 hash match produces about thirty of those rows and DOP 8
/// produces hundreds, so the handful of summary numbers people actually open this panel for were
/// buried in a scroll marathon. This helper collapses that per-metric detail into one breakdown per
/// section (Actual Statistics / Actual Timing / Actual I/O), grouped under a small header per
/// metric, so the UI layer only has to render what this returns.</para>
///
/// <para>Ported from PerformanceStudio's Avalonia viewer (dev @ ff7f1d9,
/// <c>PlanViewerControl.Properties.cs</c>'s <c>AddPerThreadBreakdown</c>/<c>ThreadRowSkew</c>).</para>
/// </summary>
public static class ThreadBreakdown
{
    /// <summary>
    /// Builds the breakdown for one section, or null if none of the given metrics have any
    /// per-thread data worth showing (a section only grows a breakdown when there is something in
    /// it) or the node has one thread or fewer.
    /// </summary>
    public static ThreadBreakdownResult? Build(
        PlanNode node,
        params (string Metric, Func<PerThreadRuntimeInfo, long> Value, bool IncludeIdleThreads, string Unit)[] metrics)
    {
        if (node.PerThreadStats.Count <= 1)
            return null;

        var groups = new List<ThreadBreakdownMetricGroup>();

        foreach (var metric in metrics)
        {
            var threads = node.PerThreadStats
                .Where(t => metric.IncludeIdleThreads || metric.Value(t) > 0)
                .ToList();
            if (threads.Count == 0 || threads.All(t => metric.Value(t) == 0))
                continue;

            groups.Add(new ThreadBreakdownMetricGroup
            {
                Metric = metric.Metric,
                Unit = metric.Unit,
                Threads = threads.Select(t => (t.ThreadId, metric.Value(t))).ToList()
            });
        }

        if (groups.Count == 0)
            return null;

        var (isSkewed, maxRows, minRows) = ThreadRowSkew(node);
        var skewSuffix = isSkewed ? $"(skewed: {maxRows:N0} max / {minRows:N0} min)" : "";

        return new ThreadBreakdownResult
        {
            Groups = groups,
            HeaderText = $"Per-thread breakdown ({node.PerThreadStats.Count} threads)",
            IsSkewed = isSkewed,
            SkewSuffix = skewSuffix
        };
    }

    /// <summary>
    /// Per-thread row skew, for the breakdown header.
    ///
    /// <para>The share test mirrors <see cref="PlanAnalyzer"/>'s Rule 8 (Parallel Skew) so this
    /// header never disagrees with the warning the same plan raises: same coordinator-thread
    /// filter, same 1,000-rows-per-worker floor, same 0.80/0.50 share threshold. The idle-thread
    /// test is additional — a thread that returned no rows at all while its siblings did real work
    /// is the shape people read as skew on sight, and Rule 8 stays quiet about it whenever the
    /// busiest thread is still under its share threshold.</para>
    /// </summary>
    public static (bool IsSkewed, long MaxRows, long MinRows) ThreadRowSkew(PlanNode node)
    {
        // Thread 0 is the coordinator and normally moves no rows in a parallel operator.
        var workers = node.PerThreadStats.Where(t => t.ThreadId > 0).ToList();
        if (workers.Count < 2) workers = node.PerThreadStats;
        if (workers.Count < 2) return (false, 0, 0);

        var maxRows = workers.Max(t => t.ActualRows);
        var minRows = workers.Min(t => t.ActualRows);
        var totalRows = workers.Sum(t => t.ActualRows);

        // Below this there are too few rows to distribute for a split to mean anything.
        if (totalRows < workers.Count * 1000L) return (false, maxRows, minRows);

        // At DOP 2 a 60/40 split is normal, so that case needs a higher bar.
        var shareThreshold = workers.Count <= 2 ? 0.80 : 0.50;
        var isSkewed = (double)maxRows / totalRows >= shareThreshold || minRows == 0;
        return (isSkewed, maxRows, minRows);
    }
}
