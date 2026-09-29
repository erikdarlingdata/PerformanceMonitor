/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

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

    /// <summary>The most significant digits <see cref="FormatActualOfEstimate"/> will add to make a label agree.</summary>
    private const int MaxSignificantDigits = 6;

    /// <summary>
    /// The plan node's row line: <c>"{actual per execution} of {estimate} ({accuracy}%)"</c>, with the percentage
    /// left off only when the estimate is zero (there is nothing to divide by). Values of 1 and over print
    /// exactly as they always have (<c>N0</c>). A value under 1 used to print as "0" (or "1"), so a Key Lookup
    /// that ran 117 times for 1 row read <c>0 of 0 (89%)</c>, a label that contradicts its own percentage.
    /// A value strictly between 0 and 1 now prints in significant digits (<c>0.0085 of 0.0096 (89%)</c>): two
    /// to begin with, and more when the two printed numbers, divided, would not round to the printed
    /// percentage. Zero prints as "0". Culture follows the caller's (the current culture by default), like the
    /// interpolated label this replaced.
    /// </summary>
    public static string FormatActualOfEstimate(
        double actualRowsPerExecution, double estimateRows, IFormatProvider? provider = null)
    {
        provider ??= CultureInfo.CurrentCulture;
        var showPercent = estimateRows > 0;
        var percent = showPercent
            ? (Ratio(actualRowsPerExecution, estimateRows) * 100).ToString("F0", provider)
            : "";

        var actualIsFraction = IsFraction(actualRowsPerExecution);
        var estimateIsFraction = IsFraction(estimateRows);
        string actualText;
        string estimateText;
        for (var digits = 2; ; digits++)
        {
            actualText = actualIsFraction
                ? actualRowsPerExecution.ToString("G" + digits, provider)
                : actualRowsPerExecution.ToString("N0", provider);
            estimateText = estimateIsFraction
                ? estimateRows.ToString("G" + digits, provider)
                : estimateRows.ToString("N0", provider);

            // Only a label with a fraction can be wrong at N0, and only two fractions can be made to agree by
            // printing more digits (a whole number keeps its N0 text, as before).
            if (!showPercent || !actualIsFraction || !estimateIsFraction || digits >= MaxSignificantDigits)
                break;
            if (double.TryParse(actualText, NumberStyles.Float, provider, out var shownActual)
                && double.TryParse(estimateText, NumberStyles.Float, provider, out var shownEstimate)
                && shownEstimate > 0
                && (shownActual / shownEstimate * 100).ToString("F0", provider) == percent)
                break;
        }

        return showPercent
            ? string.Concat(actualText, " of ", estimateText, " (", percent, "%)")
            : string.Concat(actualText, " of ", estimateText);
    }

    private static bool IsFraction(double value) => value > 0 && value < 1;
}
