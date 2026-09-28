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
/// #4515 — a SERIAL operator's own elapsed time (<see cref="PlanAnalyzer.GetOperatorOwnElapsedMs"/>, the
/// serial row-mode path) only subtracted a direct child's own <c>ActualElapsedMs</c>. That misses the same two
/// shapes #4513 fixed on the per-thread path, reusing the same two helpers there:
///
/// <para>(a) A Compute Scalar child commonly carries no runtime stats at all (<c>ActualElapsedMs</c> is 0), so
/// the old code subtracted zero for it and the parent absorbed the whole subtree beneath it as its own
/// self-time.</para>
///
/// <para>(b) A batch-mode child reports STANDALONE time (not cumulative), so a grandchild inside the same batch
/// zone kept its time inside the parent's self-time — double-counted, since the grandchild also reports that
/// same time as its own self-time on its own row of the plan.</para>
///
/// <para>Synthetic <see cref="PlanNode"/> trees rather than full ShowPlan XML fixtures, for the same reason as
/// #4513's tests: the property under test is arithmetic, so each tree is built to have exactly the shape the
/// assertion needs, with expected numbers worked out by hand below.</para>
/// </summary>
public sealed class PlanSync4515Tests
{
    /// <summary>(a) A Compute Scalar child with no runtime stats is looked through to its own child, so the
    /// real work beneath it still comes off the parent's self-time.
    ///
    /// Shape: Parent (row mode, ActualElapsedMs=45) -> Compute Scalar (HasActualStats=false,
    /// ActualElapsedMs=0) -> RealWork (row mode, ActualElapsedMs=40).
    /// Correct self-time = 45 - 40 = 5ms.
    /// The unfixed code subtracts the Compute Scalar's own ActualElapsedMs (0) and returns 45ms.</summary>
    [Fact]
    public void ComputeScalarPassThroughChild_IsLookedThrough()
    {
        var realWork = new PlanNode
        {
            PhysicalOp = "Clustered Index Scan",
            HasActualStats = true,
            ActualElapsedMs = 40,
        };
        var computeScalar = new PlanNode
        {
            PhysicalOp = "Compute Scalar",
            HasActualStats = false,
            ActualElapsedMs = 0,
            Children = new List<PlanNode> { realWork },
        };
        var parent = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            HasActualStats = true,
            ActualElapsedMs = 45,
            Children = new List<PlanNode> { computeScalar },
        };

        var self = PlanAnalyzer.GetOperatorOwnElapsedMs(parent);

        Assert.Equal(5, self);
    }

    /// <summary>(b) A batch-mode child's whole contiguous subtree is summed, not just its own standalone
    /// value, so a grandchild in the same batch zone doesn't stay behind inside the parent's self-time.
    ///
    /// Shape: Parent (row mode, ActualElapsedMs=35) -> Child (batch, ActualElapsedMs=10, standalone own time)
    /// -> Grandchild (batch, ActualElapsedMs=20, standalone own time, no children).
    /// Correct effective child elapsed = 10 (child's own) + 20 (grandchild's own) = 30.
    /// Correct self-time = 35 - 30 = 5ms.
    /// The unfixed code only subtracts the direct child's own value (10), leaving the grandchild's time
    /// inside the parent's self-time: 35 - 10 = 25ms.</summary>
    [Fact]
    public void BatchModeGrandchild_IsSummedIntoSubtree()
    {
        var grandchild = new PlanNode
        {
            PhysicalOp = "Hash Match",
            ActualExecutionMode = "Batch",
            HasActualStats = true,
            ActualElapsedMs = 20,
        };
        var child = new PlanNode
        {
            PhysicalOp = "Compute Scalar",
            ActualExecutionMode = "Batch",
            HasActualStats = true,
            ActualElapsedMs = 10,
            Children = new List<PlanNode> { grandchild },
        };
        var parent = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            HasActualStats = true,
            ActualElapsedMs = 35,
            Children = new List<PlanNode> { child },
        };

        var self = PlanAnalyzer.GetOperatorOwnElapsedMs(parent);

        Assert.Equal(5, self);
    }
}
