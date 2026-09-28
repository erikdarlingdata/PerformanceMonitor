/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="ThreadBreakdown"/>: the collapsed per-thread breakdown shape ported from
/// PerformanceStudio's Avalonia viewer (dev @ ff7f1d9), and its skew header, which mirrors
/// PlanAnalyzer's Rule 8 (Parallel Skew) math exactly.
/// </summary>
public sealed class ViewerThreadBreakdownTests
{
    private static PlanNode NodeWithThreads(params (int ThreadId, long Rows)[] threads)
    {
        var node = new PlanNode();
        foreach (var t in threads)
            node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = t.ThreadId, ActualRows = t.Rows });
        return node;
    }

    [Fact]
    public void Build_returns_null_for_a_single_thread()
    {
        var node = NodeWithThreads((0, 100));

        var result = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));

        Assert.Null(result);
    }

    [Fact]
    public void Build_returns_null_when_no_metric_has_data()
    {
        // Two threads, but the one metric requested (Rows Read) is zero on both, and idle
        // threads are excluded for it — the same shape ff7f1d9 uses to skip an empty metric.
        var node = NodeWithThreads((0, 0), (1, 0));

        var result = ThreadBreakdown.Build(node, ("Rows Read", t => t.ActualRowsRead, false, ""));

        Assert.Null(result);
    }

    [Fact]
    public void Build_skips_idle_threads_when_not_included()
    {
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRowsRead = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRowsRead = 500 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRowsRead = 0 });

        var result = ThreadBreakdown.Build(node, ("Rows Read", t => t.ActualRowsRead, false, ""));

        Assert.NotNull(result);
        var group = Assert.Single(result!.Groups);
        Assert.Equal("Rows Read", group.Metric);
        var thread = Assert.Single(group.Threads);
        Assert.Equal(1, thread.ThreadId);
        Assert.Equal(500L, thread.Value);
    }

    [Fact]
    public void Build_includes_idle_threads_when_requested()
    {
        // Rows and Executions list idle threads too: a thread sitting at zero while its
        // siblings work is exactly what someone opens the breakdown to see.
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRows = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 500 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 0 });

        var result = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));

        Assert.NotNull(result);
        var group = Assert.Single(result!.Groups);
        Assert.Equal(3, group.Threads.Count);
    }

    [Fact]
    public void Build_header_carries_thread_count_and_no_skew_suffix_when_balanced()
    {
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRows = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 5000 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 5000 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 3, ActualRows = 5000 });

        var result = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));

        Assert.NotNull(result);
        Assert.Equal("Per-thread breakdown (4 threads)", result!.HeaderText);
        Assert.False(result.IsSkewed);
        Assert.Equal("", result.SkewSuffix);
    }

    [Fact]
    public void Build_carries_a_skew_suffix_at_three_or_more_workers_over_the_fifty_percent_share()
    {
        // 3 workers, total 15,000 rows (>= 3*1000 floor). One worker at 51% share crosses
        // Rule 8's >2-worker threshold (0.50).
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRows = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 7650 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 3675 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 3, ActualRows = 3675 });

        var result = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));

        Assert.NotNull(result);
        Assert.True(result!.IsSkewed);
        Assert.Equal("(skewed: 7,650 max / 3,675 min)", result.SkewSuffix);
    }

    [Fact]
    public void Build_does_not_flag_skew_just_under_the_fifty_percent_share_at_three_workers()
    {
        // Same 3 workers, 15,000 rows, max share held to 49% — right below Rule 8's threshold.
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRows = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 7350 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 3825 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 3, ActualRows = 3825 });

        var result = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));

        Assert.NotNull(result);
        Assert.False(result!.IsSkewed);
    }

    [Fact]
    public void Build_uses_the_higher_eighty_percent_threshold_at_two_workers()
    {
        // At DOP 2 a 60/40 split is normal — Rule 8 raises the bar to 0.80 for <=2 workers.
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRows = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 6000 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 4000 });

        var sixtyForty = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));
        Assert.NotNull(sixtyForty);
        Assert.False(sixtyForty!.IsSkewed);

        node.PerThreadStats[1].ActualRows = 8100;
        node.PerThreadStats[2].ActualRows = 1900;
        var eightyOneNineteen = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));
        Assert.NotNull(eightyOneNineteen);
        Assert.True(eightyOneNineteen!.IsSkewed);
    }

    [Fact]
    public void Build_flags_skew_when_a_worker_is_idle_even_under_the_share_threshold()
    {
        // The idle-thread test Rule 8 does not make: a worker that returned nothing at all
        // while its siblings did real work reads as skew on sight, even if the busiest
        // worker's own share is under threshold.
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRows = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 5000 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 5000 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 3, ActualRows = 0 });

        var result = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));

        Assert.NotNull(result);
        Assert.True(result!.IsSkewed);
    }

    [Fact]
    public void Build_does_not_flag_skew_below_the_one_thousand_rows_per_worker_floor()
    {
        // Below the floor, there are too few rows to distribute for a split to mean anything,
        // even though one worker holds 100% of the (tiny) total.
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualRows = 0 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 900 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 900 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 3, ActualRows = 0 });

        var result = ThreadBreakdown.Build(node, ("Rows", t => t.ActualRows, true, ""));

        Assert.NotNull(result);
        Assert.False(result!.IsSkewed);
    }

    [Fact]
    public void Groups_preserve_metric_order_and_skip_empty_metrics()
    {
        var node = new PlanNode();
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 0, ActualLogicalReads = 0, ActualPhysicalReads = 0, ActualScans = 3 });
        node.PerThreadStats.Add(new PerThreadRuntimeInfo { ThreadId = 1, ActualLogicalReads = 10, ActualPhysicalReads = 0, ActualScans = 0 });

        var result = ThreadBreakdown.Build(
            node,
            ("Logical Reads", t => t.ActualLogicalReads, false, ""),
            ("Physical Reads", t => t.ActualPhysicalReads, false, ""),
            ("Scans", t => t.ActualScans, false, ""),
            ("Read-Ahead Reads", t => t.ActualReadAheads, false, ""));

        Assert.NotNull(result);
        // Physical Reads and Read-Ahead Reads are all-zero across every thread, so they drop
        // out entirely — a section only grows a breakdown when there is something in it.
        Assert.Equal(new[] { "Logical Reads", "Scans" }, result!.Groups.ConvertAll(g => g.Metric));
    }

    [Fact]
    public void Unit_carries_through_to_a_ms_metric()
    {
        var node = NodeWithThreads((0, 0), (1, 0));
        node.PerThreadStats[0].ActualElapsedMs = 12;
        node.PerThreadStats[1].ActualElapsedMs = 34;

        var result = ThreadBreakdown.Build(node, ("Elapsed", t => t.ActualElapsedMs, false, " ms"));

        Assert.NotNull(result);
        Assert.Equal(" ms", result!.Groups[0].Unit);
    }
}
