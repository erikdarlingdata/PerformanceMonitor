/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// One of the seven colors a plan-edge (elbow connector) can take, keyed by how far a child
/// operator's actual row count diverged from its estimate. <see cref="Neutral"/> is the plain
/// default edge color; the other six are the three-tier overestimate/underestimate ramps.
/// </summary>
public enum PlanEdgeColourKey
{
    /// <summary>Estimated plan, or actual within the neutral band: the plain default edge color.</summary>
    Neutral,

    /// <summary>Underestimated (more actual rows than estimated), first tier past the limit.</summary>
    LightOrange,

    /// <summary>Underestimated, second tier (limit x 10).</summary>
    FluoOrange,

    /// <summary>Underestimated, third tier (limit x 100).</summary>
    FluoRed,

    /// <summary>Overestimated (fewer actual rows than estimated), first tier past the limit.</summary>
    Blue,

    /// <summary>Overestimated, second tier (limit x 10).</summary>
    LightBlue,

    /// <summary>Overestimated, third tier (limit x 100).</summary>
    FluoBlue,
}

/// <summary>
/// Pure helper computing plan-edge color by the actual-vs-expected row-count ratio of the CHILD
/// operator feeding that edge (colors only actual plans; estimated plans keep the neutral default).
/// "Expected" is <see cref="RowEstimateHelper.GetExpectedRows"/>, the same basis the analyzer's
/// row-estimate rules use: the per-execution estimate times ActualExecutions on the inner side of a
/// Nested Loops join, where ActualExecutions is a real loop count, and the plain per-execution
/// estimate everywhere else, where a parallel zone's ActualExecutions only counts threads (#4627).
/// This matches erikdarlingdata/PerformanceStudio's <c>GetLinkColorBrush</c> as fixed in
/// PerformanceStudio#594 / #597: it compares the child's total actual rows against that expected
/// figure, and the tier limits are PS's. Kept here (no WPF dependency) so a unit test can pin every
/// tier boundary without needing <c>PerformanceMonitor.Ui</c>, which is net10.0-windows-only.
/// </summary>
public static class PlanEdgeColour
{
    /// <summary>The floor PerformanceStudio clamps the divergence-limit setting to.</summary>
    public const double MinDivergenceLimit = 2.0;

    /// <summary>PerformanceStudio's default divergence-limit setting (before clamping).</summary>
    public const double DefaultDivergenceLimit = 10.0;

    /// <summary>
    /// Returns the color key for the edge feeding <paramref name="child"/>: the node's own actual-stats
    /// flag, its total <c>ActualRows</c>, and the expected rows <see cref="RowEstimateHelper.GetExpectedRows"/>
    /// derives from its position in the tree. This is the overload every caller in the viewer uses, so
    /// no caller can hand the tiers the wrong "expected" (#4627). <paramref name="divergenceLimit"/> is
    /// the raw setting value; it is clamped to <see cref="MinDivergenceLimit"/> like the numeric overload's.
    /// </summary>
    public static PlanEdgeColourKey ForChild(PlanNode child, double divergenceLimit)
        => ForChild(child.HasActualStats, child.ActualRows, RowEstimateHelper.GetExpectedRows(child), divergenceLimit);

    /// <summary>
    /// The tier logic on plain numbers, so a unit test can pin every boundary without building a tree.
    /// <paramref name="divergenceLimit"/> is the raw setting value; this clamps it to
    /// <see cref="MinDivergenceLimit"/> itself, so callers may pass the setting unclamped.
    /// <paramref name="actualRows"/> is the operator's total across every execution (showplan XML's own
    /// basis) and <paramref name="expectedRows"/> is what that total should be on the same basis, from
    /// <see cref="RowEstimateHelper.GetExpectedRows"/>. Passing a bare per-execution estimate as
    /// <paramref name="expectedRows"/> for a node that ran more than once compares a total against a
    /// single execution, which is the original #4627 bug, so viewer code calls
    /// <see cref="ForChild(PlanNode, double)"/> instead.
    /// </summary>
    public static PlanEdgeColourKey ForChild(bool hasActualStats, double actualRows, double expectedRows, double divergenceLimit)
    {
        if (!hasActualStats)
            return PlanEdgeColourKey.Neutral;

        divergenceLimit = System.Math.Max(MinDivergenceLimit, divergenceLimit);

        var accuracyRatio = RowEstimateHelper.GetRowAccuracyRatio(actualRows, expectedRows);

        // Within the neutral band — keep the default color.
        if (accuracyRatio >= 1.0 / divergenceLimit && accuracyRatio <= divergenceLimit)
            return PlanEdgeColourKey.Neutral;

        // Underestimated bands (accuracyRatio > 1 means more actual rows than estimated).
        if (accuracyRatio > divergenceLimit)
        {
            if (accuracyRatio >= divergenceLimit * 100)
                return PlanEdgeColourKey.FluoRed;
            if (accuracyRatio >= divergenceLimit * 10)
                return PlanEdgeColourKey.FluoOrange;
            return PlanEdgeColourKey.LightOrange;
        }

        // Overestimated bands (accuracyRatio < 1 means fewer actual rows than estimated).
        if (accuracyRatio < 1.0 / (divergenceLimit * 100))
            return PlanEdgeColourKey.FluoBlue;
        if (accuracyRatio < 1.0 / (divergenceLimit * 10))
            return PlanEdgeColourKey.LightBlue;
        return PlanEdgeColourKey.Blue;
    }
}
