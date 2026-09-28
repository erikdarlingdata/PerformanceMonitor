/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The one place that turns a plan operator's actual-vs-estimated row counts into a comparable ratio.
/// <c>ActualRows</c> in SQL Server's showplan XML is a total across every execution of the operator,
/// but <c>EstimateRows</c> is per execution — so anything that compares them directly, without
/// dividing by <c>ActualExecutions</c> first, is thousands of times too high on the inner side of a
/// Nested Loops join that runs thousands of times (#4627). Both the node label and the plan-edge
/// color call this so they can't drift apart again.
/// </summary>
public static class PlanRowAccuracy
{
    /// <summary>
    /// Normalizes a total actual row count down to a per-execution figure, the same basis
    /// <c>EstimateRows</c> is already on. Falls back to the raw total when <paramref name="actualExecutions"/>
    /// is zero or negative (no executions recorded — nothing to divide by).
    /// </summary>
    public static double ActualRowsPerExecution(double actualRows, long actualExecutions)
        => actualExecutions > 0 ? actualRows / actualExecutions : actualRows;

    /// <summary>
    /// The accuracy ratio for one execution: <paramref name="actualRowsPerExecution"/> over
    /// <paramref name="estimateRows"/>. An estimate of zero rows with actual rows to show is the
    /// worst possible underestimate (<see cref="double.MaxValue"/>); zero and zero is treated as an
    /// exact match (ratio of 1).
    /// </summary>
    public static double Ratio(double actualRowsPerExecution, double estimateRows)
        => estimateRows > 0
            ? actualRowsPerExecution / estimateRows
            : (actualRowsPerExecution > 0 ? double.MaxValue : 1.0);
}
