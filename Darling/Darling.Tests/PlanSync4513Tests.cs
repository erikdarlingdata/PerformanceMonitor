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
/// #4513 — a parallel operator's own elapsed time (<see cref="PlanAnalyzer.GetOperatorOwnElapsedMs"/>, the
/// per-thread path) had two faults that both inflate an operator until it outranks the operator that actually
/// did the work.
///
/// <para>(a) Thread 0 is the coordinator in a parallel plan: it carries no rows, and its own
/// <c>ActualElapsedMs</c> is the wall clock of the whole parallel branch, not a worker's own time. Including it
/// handed an operator near the top of a parallel branch the branch's entire duration as its own work.</para>
///
/// <para>(b) The per-thread child subtraction only summed a direct child's own reported per-thread value. A
/// batch-mode child reports STANDALONE time (not cumulative), so a grandchild inside the same batch zone kept
/// its time inside the parent's self-time — while the grandchild also reported that same time as its own self
/// time, so the time was counted twice across two rows of the same plan.</para>
///
/// <para>(c) A Compute Scalar child commonly carries no per-thread runtime stats at all (a pass-through), so the
/// old code subtracted zero for it and the parent absorbed the whole subtree beneath it.</para>
///
/// <para>Synthetic <see cref="PlanNode"/> trees rather than full ShowPlan XML fixtures: the property under test
/// is arithmetic over <see cref="PerThreadRuntimeInfo"/> rows, so each tree is built to have exactly the shape
/// the assertion needs and the expected numbers are facts of the test, computed by hand below.</para>
/// </summary>
public sealed class PlanSync4513Tests
{
    private static PerThreadRuntimeInfo Thread(int id, long elapsedMs) =>
        new() { ThreadId = id, ActualElapsedMs = elapsedMs };

    /// <summary>(a) Thread 0 (the coordinator) is excluded from a parallel operator's self-time. With no
    /// children, self-time is just the operator's own per-thread elapsed, maxed over WORKER threads only.
    /// Coordinator elapsed (1000ms, the whole branch's wall clock) must not win the max.
    /// Expected: max(50, 45) = 50ms. The unfixed code includes thread 0 and returns 1000ms.</summary>
    [Fact]
    public void CoordinatorThreadZero_IsExcludedFromSelfTime()
    {
        var node = new PlanNode
        {
            PhysicalOp = "Hash Match",
            HasActualStats = true,
            PerThreadStats = new List<PerThreadRuntimeInfo>
            {
                Thread(0, 1000),
                Thread(1, 50),
                Thread(2, 45),
            },
        };

        var self = PlanAnalyzer.GetOperatorOwnElapsedMs(node);

        Assert.Equal(50, self);
    }

    /// <summary>(b) A batch-mode child's subtree is summed, not just its own standalone value, so a grandchild
    /// in the same batch zone doesn't stay behind inside the parent's self-time (which would double-count it
    /// against the grandchild's own separately-reported self-time).
    ///
    /// Shape: Parent (row mode) -> Child (batch, standalone own time) -> Grandchild (batch, standalone own
    /// time, no children). Per thread, Parent's cumulative elapsed = its own work + Child's own + Grandchild's
    /// own:
    ///   thread 1: 5 (parent's own) + 10 (child's own) + 20 (grandchild's own) = 35
    ///   thread 2: 4 + 8 + 15 = 27
    /// Correct self-time = max(35 - (10+20), 27 - (8+15)) = max(5, 4) = 5ms.
    /// The unfixed code only subtracts Child's own value (10, 8), leaving Grandchild's time inside Parent's
    /// self-time: max(35-10, 27-8) = max(25, 19) = 25ms.</summary>
    [Fact]
    public void BatchModeGrandchild_IsNotLeftInsideParentSelfTime()
    {
        var grandchild = new PlanNode
        {
            PhysicalOp = "Hash Match",
            ActualExecutionMode = "Batch",
            HasActualStats = true,
            ActualElapsedMs = 20,
            PerThreadStats = new List<PerThreadRuntimeInfo>
            {
                Thread(1, 20),
                Thread(2, 15),
            },
        };
        var child = new PlanNode
        {
            PhysicalOp = "Compute Scalar",
            ActualExecutionMode = "Batch",
            HasActualStats = true,
            ActualElapsedMs = 10,
            PerThreadStats = new List<PerThreadRuntimeInfo>
            {
                Thread(1, 10),
                Thread(2, 8),
            },
            Children = new List<PlanNode> { grandchild },
        };
        var parent = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            HasActualStats = true,
            ActualElapsedMs = 35,
            PerThreadStats = new List<PerThreadRuntimeInfo>
            {
                Thread(1, 35),
                Thread(2, 27),
            },
            Children = new List<PlanNode> { child },
        };

        var self = PlanAnalyzer.GetOperatorOwnElapsedMs(parent);

        Assert.Equal(5, self);
    }

    /// <summary>(c) A Compute Scalar child with no per-thread runtime stats at all (a pass-through) is looked
    /// through to its own child, so the real work beneath it still comes off the parent's self-time.
    ///
    /// Shape: Parent (row mode) -> Compute Scalar (no PerThreadStats) -> RealWork (row mode, has stats).
    ///   thread 1: 5 (parent's own) + 40 (real work) = 45
    ///   thread 2: 4 + 35 = 39
    /// Correct self-time = max(45-40, 39-35) = max(5, 4) = 5ms.
    /// The unfixed code finds no per-thread stats on the Compute Scalar child and subtracts nothing:
    /// max(45, 39) = 45ms.</summary>
    [Fact]
    public void ComputeScalarPassThroughChild_IsLookedThrough()
    {
        var realWork = new PlanNode
        {
            PhysicalOp = "Clustered Index Scan",
            HasActualStats = true,
            ActualElapsedMs = 40,
            PerThreadStats = new List<PerThreadRuntimeInfo>
            {
                Thread(1, 40),
                Thread(2, 35),
            },
        };
        var computeScalar = new PlanNode
        {
            PhysicalOp = "Compute Scalar",
            HasActualStats = false,
            PerThreadStats = new List<PerThreadRuntimeInfo>(),
            Children = new List<PlanNode> { realWork },
        };
        var parent = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            HasActualStats = true,
            PerThreadStats = new List<PerThreadRuntimeInfo>
            {
                Thread(1, 45),
                Thread(2, 39),
            },
            Children = new List<PlanNode> { computeScalar },
        };

        var self = PlanAnalyzer.GetOperatorOwnElapsedMs(parent);

        Assert.Equal(5, self);
    }
}
