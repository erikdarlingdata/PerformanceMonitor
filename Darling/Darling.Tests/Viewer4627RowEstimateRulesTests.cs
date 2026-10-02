/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4627 follow-up (PerformanceStudio#594 / #597): the analyzer's rule 5 (Row Estimate Mismatch), rule 16 (Nested Loops
/// High Executions, outer side) and rule 26 (Row Goal) compared <c>ActualRows / ActualExecutions</c> to the per-execution
/// estimate for every node. In a parallel zone <c>ActualExecutions</c> is the THREAD count, so the actual was understated
/// by DOP. They now go through <see cref="RowEstimateHelper"/>, which multiplies the estimate by executions only on the
/// inner side of a Nested Loops join. Rule 32 is deliberately untouched: PerformanceStudio#597 left it comparing the raw
/// total to the raw estimate. Each plan is built in code, as in PerformanceStudio's <c>AnalyzerRuleGateTests</c>.
/// </summary>
public sealed class Viewer4627RowEstimateRulesTests
{
    private static void Analyze(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan, null, null, CancellationToken.None);
    }

    private static bool Has(PlanNode node, string warningType) =>
        node.Warnings.Any(w => w.WarningType == warningType);

    // ---- rule 5 -----------------------------------------------------------------------------------

    /// <summary>Same shape as eager_index_spool_plan.sqlplan's Node 1: a join at DOP 8 under a Gather Streams parent.
    /// ActualExecutions = 8 is a thread count, so the true ratio is 609 / 2983.02 = 0.2 (a 4.9x overestimate, under the
    /// 10x gate). The old code divided 609 by the 8 threads and read 0.026, "39x overestimated".</summary>
    [Fact]
    public void Rule5_NonInnerSideJoinAtDop8_IsNotOverstatedByThreadCount()
    {
        var gather = new PlanNode { PhysicalOp = "Parallelism", LogicalOp = "Gather Streams" };
        var node = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            LogicalOp = "Inner Join",
            HasActualStats = true,
            EstimateRows = 2983.02,
            ActualRows = 609,
            ActualExecutions = 8,
            Parent = gather
        };
        gather.Children.Add(node);
        Analyze(new PlanStatement { RootNode = gather });

        Assert.False(Has(node, "Row Estimate Mismatch"),
            string.Join(" | ", node.Warnings.Select(w => w.Message)));
    }

    /// <summary>A node on the inner side of a Nested Loops join really runs once per outer row, so a genuine
    /// per-execution mismatch still fires with the "rows x executions" phrasing. Identical on the old and new code.</summary>
    [Fact]
    public void Rule5_InnerSideNonLookupNodeWithARealMismatch_StillFiresWithPerExecutionPhrasing()
    {
        var (nl, inner) = InnerSideOfNestedLoops(lookup: false);
        Analyze(new PlanStatement { RootNode = nl });

        var warning = Assert.Single(inner.Warnings, w => w.WarningType == "Row Estimate Mismatch");
        Assert.Contains("50x underestimated", warning.Message);
        Assert.Contains("50 rows x 1,000 executions", warning.Message);
    }

    /// <summary>A Key Lookup is a point lookup (one row per execution), so its per-execution estimate is misleading:
    /// rule 5 has skipped it in both applications, before and after this change.</summary>
    [Fact]
    public void Rule5_KeyLookup_IsSilent()
    {
        var (nl, inner) = InnerSideOfNestedLoops(lookup: true);
        Analyze(new PlanStatement { RootNode = nl });

        Assert.False(Has(inner, "Row Estimate Mismatch"));
    }

    private static (PlanNode Nl, PlanNode Inner) InnerSideOfNestedLoops(bool lookup)
    {
        var outer = new PlanNode { PhysicalOp = "Clustered Index Scan" };
        var inner = new PlanNode
        {
            PhysicalOp = lookup ? "Key Lookup" : "Index Seek",
            LogicalOp = lookup ? "Key Lookup" : "Index Seek",
            Lookup = lookup,
            HasActualStats = true,
            EstimateRows = 1,
            ActualRows = 50_000,
            ActualExecutions = 1000
        };
        var nl = new PlanNode { PhysicalOp = "Nested Loops", LogicalOp = "Inner Join", Children = { outer, inner } };
        outer.Parent = nl;
        inner.Parent = nl;
        return (nl, inner);
    }

    // ---- rule 16 ----------------------------------------------------------------------------------

    /// <summary>The join's OUTER input sits in a parallel zone where ActualExecutions (8) is a thread count. The old code
    /// divided the real 100,000 rows by it and printed "actual 12,500 (125x underestimate)"; the true figure is
    /// "actual 100,000 (1000x underestimate)".</summary>
    [Fact]
    public void Rule16_OuterChildNotInnerSideInParallelZone_UsesTotalActualRowsNotPerThread()
    {
        var outerChild = new PlanNode
        {
            PhysicalOp = "Clustered Index Scan",
            HasActualStats = true,
            EstimateRows = 100,
            ActualRows = 100_000,
            ActualExecutions = 8
        };
        var innerChild = new PlanNode
        {
            PhysicalOp = "Key Lookup",
            HasActualStats = true,
            ActualExecutions = 200_000
        };
        var nl = new PlanNode { PhysicalOp = "Nested Loops", LogicalOp = "Inner Join", Children = { outerChild, innerChild } };
        outerChild.Parent = nl;
        innerChild.Parent = nl;

        Analyze(new PlanStatement { RootNode = nl });

        var warning = Assert.Single(nl.Warnings, w => w.WarningType == "Nested Loops High Executions");
        Assert.Contains("Outer side: estimated 100 rows, actual 100,000 (1000x underestimate)", warning.Message);
        Assert.DoesNotContain("12,500", warning.Message);
    }

    // ---- rule 26 ----------------------------------------------------------------------------------

    /// <summary>A scan that is not inner-side, in a parallel zone where ActualExecutions (8) is a thread count. The old
    /// code divided 500 by 8 (62.5, under the row-goal estimate of 100) and swallowed the warning, though the row goal
    /// did not hold: 500 actual rows against 100 expected.</summary>
    [Fact]
    public void Rule26_NonInnerSideScanInParallelZone_RowGoalCheckUsesTotalActualRows()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Index Scan",
            LogicalOp = "Index Scan",
            HasActualStats = true,
            EstimateRows = 100,
            EstimateRowsWithoutRowGoal = 1000,
            ActualRows = 500,
            ActualExecutions = 8
        };
        Analyze(new PlanStatement { RootNode = scan });

        var warning = Assert.Single(scan.Warnings, w => w.WarningType == "Row Goal");
        Assert.Contains("estimate reduced from 1,000 to 100", warning.Message);
    }
}
