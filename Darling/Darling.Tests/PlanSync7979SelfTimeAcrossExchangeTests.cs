/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7979: a parallel row-mode operator's own time (<see cref="PlanAnalyzer.GetOperatorOwnElapsedMs"/>, the per-thread
/// path) is its time on each worker thread minus its children's time on the same thread, maxed over threads. Across an
/// exchange that pairing is wrong: an exchange moves rows between threads, so thread t above it is not doing thread t's
/// work below it. When the rows below the exchange land on fewer threads than above it, every consumer thread with no
/// producer work of the same number kept the whole branch's time as its own. <c>serially-parallel.sqlplan</c> shows
/// it: the Sort (node 2) runs 17,111 ms on threads 1 to 8, the Nested Loops below its Repartition Streams runs
/// 17,110 ms on thread 1 and 0 ms on threads 2 to 8, so threads 2 to 8 gave the Sort 17,111 ms of its own. The Sort did
/// about 1 ms of work and was reported as the plan's most expensive operator.
/// </summary>
public sealed class PlanSync7979SelfTimeAcrossExchangeTests
{
    private static PlanNode FixtureNode(string fileName, int nodeId)
    {
        var xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName));
        var plan = ShowPlanParser.Parse(xml);
        return PlanStatements.EnumerateAll(plan)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Nodes(s.RootNode!))
            .Single(n => n.NodeId == nodeId);
    }

    private static IEnumerable<PlanNode> Nodes(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Nodes(child))
                yield return descendant;
    }

    /// <summary>The issue's own case. Per thread the Sort is 17,111 ms. The slowest producer thread below the
    /// Repartition Streams is thread 1 of the Nested Loops, 17,110 ms, and no consumer can finish before the last
    /// producer does. 17,111 - 17,110 = 1 ms. The unfixed code returns 17,111 ms (thread 2: 17,111 - 0).</summary>
    [Fact]
    public void SortAboveASkewedRepartition_OwnsOneMillisecond()
    {
        var sort = FixtureNode("serially-parallel.sqlplan", 2);
        Assert.Equal("Sort", sort.PhysicalOp);
        Assert.True(sort.PerThreadStats.Count > 1, "the Sort must take the per-thread path for this test to mean anything");

        Assert.Equal(1, PlanAnalyzer.GetOperatorOwnElapsedMs(sort));
    }

    /// <summary>The exchange itself pairs threads across itself the same way: its consumer threads are 17,111 ms each,
    /// its producer (the same Nested Loops) is 17,110 ms on thread 1 only. 17,111 - 17,110 = 1 ms. The unfixed code
    /// returns 17,111 ms, the figure #4690 saw on this exchange. This is the value an exchange spill warning quotes as
    /// its operator time.</summary>
    [Fact]
    public void RepartitionOverASkewedBranch_OwnsOneMillisecond()
    {
        var exchange = FixtureNode("serially-parallel.sqlplan", 3);
        Assert.Equal("Parallelism", exchange.PhysicalOp);

        Assert.Equal(1, PlanAnalyzer.GetOperatorOwnElapsedMs(exchange));
    }

    /// <summary>The spilling child through an exchange. In <c>spill_plan.sqlplan</c> the Sort (node 11) spills and
    /// runs 25,746 to 26,232 ms per thread. The Repartition Streams below it (node 12) reports 12,621 to 13,406 ms per
    /// thread, about half of it time it spent waiting on the spilling Sort, not work the scan below did. The scan
    /// (node 13) runs 1,044 to 1,294 ms per thread. Taking the slowest scan thread off every Sort thread gives
    /// 26,232 - 1,294 = 24,938 ms. Taking the exchange's own time off the same thread instead would give the Sort only
    /// 26,232 - 12,621 = 13,611 ms and leave the rest of its spill on the exchange. The unfixed code paired the scan by
    /// thread number: 26,232 - 1,084 = 25,148 ms.</summary>
    [Fact]
    public void SpillingSortAboveARepartition_KeepsItsSpillTime()
    {
        var sort = FixtureNode("spill_plan.sqlplan", 11);
        Assert.Equal("Sort", sort.PhysicalOp);
        Assert.Equal("Parallelism", Assert.Single(sort.Children).PhysicalOp);

        Assert.Equal(24_938, PlanAnalyzer.GetOperatorOwnElapsedMs(sort));
    }

    // ---- built in code: the same shape with and without skew ----------------------------------------------------

    private static PerThreadRuntimeInfo Thread(int id, long elapsedMs) =>
        new() { ThreadId = id, ActualElapsedMs = elapsedMs };

    private static PlanNode SortOverRepartition(PerThreadRuntimeInfo[] sortThreads, PerThreadRuntimeInfo[] producerThreads)
    {
        var producer = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            HasActualStats = true,
            ActualElapsedMs = producerThreads.Max(t => t.ActualElapsedMs),
            PerThreadStats = [.. producerThreads],
        };
        var exchange = new PlanNode
        {
            PhysicalOp = "Parallelism",
            LogicalOp = "Repartition Streams",
            HasActualStats = true,
            ActualElapsedMs = sortThreads.Max(t => t.ActualElapsedMs),
            PerThreadStats = [.. sortThreads],
            Children = [producer],
        };
        return new PlanNode
        {
            PhysicalOp = "Sort",
            HasActualStats = true,
            ActualElapsedMs = sortThreads.Max(t => t.ActualElapsedMs),
            PerThreadStats = [.. sortThreads],
            Children = [exchange],
        };
    }

    /// <summary>The issue's shape in miniature: the Sort is 100 ms on both threads, the producer below the exchange did
    /// all its work on thread 1 (99 ms). Own time = 100 - 99 = 1 ms. The unfixed code returns 100 ms (thread 2:
    /// 100 - 0).</summary>
    [Fact]
    public void SkewedProducer_IsNotPairedByThreadNumber()
    {
        var sort = SortOverRepartition([Thread(0, 0), Thread(1, 100), Thread(2, 100)], [Thread(0, 0), Thread(1, 99), Thread(2, 0)]);

        Assert.Equal(1, PlanAnalyzer.GetOperatorOwnElapsedMs(sort));
    }

    /// <summary>No skew: the producer is 80 ms on both threads, the Sort is 100 and 95 ms. Pairing by thread gives
    /// max(100 - 80, 95 - 80) = 20 ms, and so does taking the slowest producer thread off each Sort thread. The fix
    /// leaves this result alone.</summary>
    [Fact]
    public void EvenProducer_SelfTimeIsUnchanged()
    {
        var sort = SortOverRepartition([Thread(0, 0), Thread(1, 100), Thread(2, 95)], [Thread(0, 0), Thread(1, 80), Thread(2, 80)]);

        Assert.Equal(20, PlanAnalyzer.GetOperatorOwnElapsedMs(sort));
    }

    private static PlanNode Producer(PerThreadRuntimeInfo[] threads) => new()
    {
        PhysicalOp = "Nested Loops",
        HasActualStats = true,
        ActualElapsedMs = threads.Max(t => t.ActualElapsedMs),
        PerThreadStats = [.. threads],
    };

    private static PlanNode Repartition(PerThreadRuntimeInfo[] threads, PlanNode child) => new()
    {
        PhysicalOp = "Parallelism",
        LogicalOp = "Repartition Streams",
        HasActualStats = true,
        ActualElapsedMs = threads.Max(t => t.ActualElapsedMs),
        PerThreadStats = [.. threads],
        Children = [child],
    };

    /// <summary>A pass-through (Compute Scalar, no runtime stats) between two exchanges. The Sort is 100 ms on both
    /// threads. The slowest producer under the second exchange is thread 1 of the Nested Loops, 90 ms, and it comes off
    /// every Sort thread: 100 - 90 = 10 ms. Dropping the every-thread time that the pass-through carries up gave the
    /// Sort 100 ms.</summary>
    [Fact]
    public void ExchangeBehindAPassThrough_TakesItsSlowestProducerOffEveryThread()
    {
        PerThreadRuntimeInfo[] sortThreads = [Thread(0, 0), Thread(1, 100), Thread(2, 100)];
        var inner = Repartition(sortThreads, Producer([Thread(0, 0), Thread(1, 90), Thread(2, 0)]));
        var passThrough = new PlanNode { PhysicalOp = "Compute Scalar", HasActualStats = false, Children = [inner] };
        var sort = new PlanNode
        {
            PhysicalOp = "Sort",
            HasActualStats = true,
            ActualElapsedMs = 100,
            PerThreadStats = [.. sortThreads],
            Children = [Repartition(sortThreads, passThrough)],
        };

        Assert.Equal(10, PlanAnalyzer.GetOperatorOwnElapsedMs(sort));
    }

    /// <summary>A batch-mode zone between two exchanges. The Sort (row mode) is 100 ms on both threads. Below the outer
    /// exchange the batch Hash Match is 30 ms on each thread (a batch operator's time is its own) and, through a row-mode
    /// Compute Scalar with no runtime stats, a second exchange's slowest producer is 50 ms. The slowest thread below
    /// the outer exchange is 30 + 50 = 80 ms, so the Sort owns 100 - 80 = 20 ms. Losing the zone's every-thread time
    /// gave the Sort 70 ms.</summary>
    [Fact]
    public void ExchangeUnderABatchZone_TakesItsSlowestProducerOffEveryThread()
    {
        PerThreadRuntimeInfo[] sortThreads = [Thread(0, 0), Thread(1, 100), Thread(2, 100)];
        PerThreadRuntimeInfo[] batchThreads = [Thread(0, 0), Thread(1, 30), Thread(2, 30)];
        var inner = Repartition(sortThreads, Producer([Thread(0, 0), Thread(1, 50), Thread(2, 0)]));
        var passThrough = new PlanNode
        {
            PhysicalOp = "Compute Scalar",
            ExecutionMode = "Row",
            ActualExecutionMode = "Row",
            HasActualStats = false,
            Children = [inner],
        };
        var hashMatch = new PlanNode
        {
            PhysicalOp = "Hash Match",
            ExecutionMode = "Batch",
            ActualExecutionMode = "Batch",
            HasActualStats = true,
            ActualElapsedMs = 30,
            PerThreadStats = [.. batchThreads],
            Children = [passThrough],
        };
        var sort = new PlanNode
        {
            PhysicalOp = "Sort",
            ExecutionMode = "Row",
            ActualExecutionMode = "Row",
            HasActualStats = true,
            ActualElapsedMs = 100,
            PerThreadStats = [.. sortThreads],
            Children = [Repartition(sortThreads, hashMatch)],
        };

        Assert.Equal(20, PlanAnalyzer.GetOperatorOwnElapsedMs(sort));
    }
}
