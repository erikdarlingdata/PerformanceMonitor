/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Compares an operator's actual row count to its estimate when the estimate is per execution
/// and the actual is a running total — across every execution, or, in a parallel zone, across
/// every thread. Shared by the plan viewer's node label, edge colour and minimap and by the
/// analyzer's row-estimate rules (5, 16 and 26), so every one of them agrees on what "the
/// estimate" means for a node that runs more than once. Ported from
/// erikdarlingdata/PerformanceStudio's <c>RowEstimateHelper</c> (PerformanceStudio#594, fixed in
/// PerformanceStudio#597); the member names match so the two stay diffable (#4627).
/// </summary>
public static class RowEstimateHelper
{
    /// <summary>
    /// True when <paramref name="node"/> sits anywhere inside the second (inner) input of a
    /// Nested Loops join — walking up through every Nested Loops ancestor the same way
    /// <c>PlanAnalyzer.CollectBareOuterReferences</c> does, so a nested-loops-within-
    /// loops chain still counts as inner. SQL Server reports a real per-thread loop-iteration
    /// count for a node on this side, so its ActualExecutions, summed across threads, is a true
    /// total rather than a thread count.
    /// </summary>
    public static bool IsInnerSideOfNestedLoops(PlanNode node)
    {
        var child = node;
        var ancestor = node.Parent;

        while (ancestor != null)
        {
            if (ancestor.PhysicalOp == "Nested Loops" &&
                ancestor.Children.Count > 1 &&
                ancestor.Children[1] == child)
            {
                return true;
            }

            child = ancestor;
            ancestor = ancestor.Parent;
        }

        return false;
    }

    /// <summary>
    /// EstimateRows is always per execution. ActualRows is the total across every execution — or,
    /// in a parallel zone, across every thread. Multiplying EstimateRows by ActualExecutions only
    /// turns it into a fair comparison when ActualExecutions is itself a real per-execution count,
    /// which <see cref="IsInnerSideOfNestedLoops"/> holds only for a node on the inner side of a
    /// Nested Loops join. Everywhere else, a parallel zone reports one thread record per DOP, and
    /// ActualExecutions summed over those threads just counts threads — multiplying by it there
    /// would inflate the expected total by DOP instead of comparing like with like.
    /// </summary>
    public static double GetExpectedRows(PlanNode node)
    {
        return node.ActualExecutions > 0 && IsInnerSideOfNestedLoops(node)
            ? node.EstimateRows * node.ActualExecutions
            : node.EstimateRows;
    }

    /// <summary>
    /// ActualRows / <see cref="GetExpectedRows"/>, with the same zero-estimate handling used
    /// throughout the viewer: no expected rows and no actual rows either is a match (1.0); no
    /// expected rows but some actual rows is an unbounded miss (double.MaxValue).
    /// </summary>
    public static double GetRowAccuracyRatio(PlanNode node)
    {
        var expectedRows = GetExpectedRows(node);
        return expectedRows > 0
            ? node.ActualRows / expectedRows
            : (node.ActualRows > 0 ? double.MaxValue : 1.0);
    }
}
