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
/// #4515 follow-up: <see cref="PlanAnalyzer.GetOperatorOwnElapsedMs"/>'s Parallelism (exchange) branch read
/// <c>child.Children.Max(c => c.ActualElapsedMs)</c> — the exchange's own direct child's raw elapsed — instead
/// of recursing through <c>GetEffectiveChildElapsedMs</c> the way every other branch does. That means a
/// row-mode parent above an exchange above a batch-mode zone (or a no-stats Compute Scalar) only subtracted
/// the exchange's direct child's OWN elapsed, not that child's effective (look-through) elapsed — the same
/// under-subtraction #4515 fixed for the non-exchange case, just one hop further down.
///
/// <para>Shape: Parent (row mode, ActualElapsedMs=100) -> Parallelism exchange (Children.Count=1) ->
/// BatchChild (batch mode, ActualElapsedMs=30, own time) -> BatchGrandchild (batch mode, ActualElapsedMs=50,
/// own time, no children).</para>
///
/// <para>Correct effective elapsed for the exchange's child is the whole batch zone: 30 (BatchChild's own) +
/// 50 (BatchGrandchild's own) = 80. Correct self-time = 100 - 80 = 20ms.</para>
///
/// <para>The unfixed code takes <c>Max(c => c.ActualElapsedMs)</c> over the exchange's direct children, i.e.
/// just BatchChild's own 30, giving self-time = 100 - 30 = 70ms.</para>
/// </summary>
public sealed class PlanSync4515bTests
{
    [Fact]
    public void ParallelismExchange_LooksThroughToChildsEffectiveElapsed()
    {
        var batchGrandchild = new PlanNode
        {
            PhysicalOp = "Hash Match",
            ActualExecutionMode = "Batch",
            HasActualStats = true,
            ActualElapsedMs = 50,
        };
        var batchChild = new PlanNode
        {
            PhysicalOp = "Hash Match",
            ActualExecutionMode = "Batch",
            HasActualStats = true,
            ActualElapsedMs = 30,
            Children = new List<PlanNode> { batchGrandchild },
        };
        var exchange = new PlanNode
        {
            PhysicalOp = "Parallelism",
            HasActualStats = true,
            ActualElapsedMs = 80,
            Children = new List<PlanNode> { batchChild },
        };
        var parent = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            HasActualStats = true,
            ActualElapsedMs = 100,
            Children = new List<PlanNode> { exchange },
        };

        var self = PlanAnalyzer.GetOperatorOwnElapsedMs(parent);

        Assert.Equal(20, self);
    }
}
