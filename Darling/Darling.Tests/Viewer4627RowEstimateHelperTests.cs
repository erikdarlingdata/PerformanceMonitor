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
/// #4627 follow-up (PerformanceStudio#594 / #597): EstimateRows is always per execution. ActualRows is the total
/// across every execution — or, in a parallel zone, across every thread. <see cref="RowEstimateHelper"/> decides when
/// ActualExecutions is a real per-execution count (the inner side of a Nested Loops join) versus a parallel zone's
/// thread count (everywhere else). <c>eager_index_spool_plan.sqlplan</c> is PerformanceStudio's fixture for it: DOP 8,
/// every RelOp Parallel="true". The tree-shaped facts are hand-built so each one controls the Parent / Children wiring.
/// </summary>
public sealed class Viewer4627RowEstimateHelperTests
{
    private const string Fixture = "eager_index_spool_plan.sqlplan";

    // ---- IsInnerSideOfNestedLoops -----------------------------------------------------------------

    [Fact]
    public void IsInnerSide_NoNestedLoopsAncestor_IsFalse()
    {
        var node = new PlanNode { PhysicalOp = "Clustered Index Scan" };

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(node));
    }

    /// <summary>The OUTER (first) child runs once, not once per outer row.</summary>
    [Fact]
    public void IsInnerSide_OuterChildOfNestedLoops_IsFalse()
    {
        var (nl, outer, _) = NestedLoops();

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(outer));
        Assert.NotNull(nl);
    }

    /// <summary>The INNER (second) child is the direct case #594 is about.</summary>
    [Fact]
    public void IsInnerSide_InnerChildOfNestedLoops_IsTrue()
    {
        var (_, _, inner) = NestedLoops();

        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(inner));
    }

    /// <summary>Only a Nested Loops join has an inner side: the second child of a Hash Match is not one.</summary>
    [Fact]
    public void IsInnerSide_SecondChildOfANonNestedLoopsJoin_IsFalse()
    {
        var build = new PlanNode { PhysicalOp = "Table Scan" };
        var probe = new PlanNode { PhysicalOp = "Table Scan" };
        var hash = new PlanNode { PhysicalOp = "Hash Match", Children = { build, probe } };
        build.Parent = hash;
        probe.Parent = hash;

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(probe));
    }

    /// <summary>A Nested Loops with one child has no second input at all.</summary>
    [Fact]
    public void IsInnerSide_NestedLoopsWithASingleChild_IsFalse()
    {
        var only = new PlanNode { PhysicalOp = "Index Seek" };
        var nl = new PlanNode { PhysicalOp = "Nested Loops", Children = { only } };
        only.Parent = nl;

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(only));
    }

    /// <summary>A node several levels below the inner child (a spool feeding a scan) is still inner-side: the walk
    /// climbs through every ancestor, not just the immediate parent.</summary>
    [Fact]
    public void IsInnerSide_DescendantOfInnerChild_IsTrue()
    {
        var outer = new PlanNode { PhysicalOp = "Sort" };
        var spool = new PlanNode { PhysicalOp = "Index Spool" };
        var scan = new PlanNode { PhysicalOp = "Clustered Index Scan" };
        var nl = new PlanNode { PhysicalOp = "Nested Loops", Children = { outer, spool } };
        outer.Parent = nl;
        spool.Parent = nl;
        spool.Children.Add(scan);
        scan.Parent = spool;

        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(scan));
    }

    /// <summary>A node under the OUTER side of an inner Nested Loops that is itself the OUTER side of nothing but sits
    /// on an outer join's inner side is inner-side because of the outer join: every Nested Loops ancestor counts.</summary>
    [Fact]
    public void IsInnerSide_NestedLoopsWithinLoops_WalksThroughEveryAncestor()
    {
        var deepInner = new PlanNode { PhysicalOp = "Key Lookup" };
        var innerNlOuter = new PlanNode { PhysicalOp = "Index Seek" };
        var innerNl = new PlanNode { PhysicalOp = "Nested Loops", Children = { innerNlOuter, deepInner } };
        innerNlOuter.Parent = innerNl;
        deepInner.Parent = innerNl;

        var outerNlOuter = new PlanNode { PhysicalOp = "Clustered Index Scan" };
        var outerNl = new PlanNode { PhysicalOp = "Nested Loops", Children = { outerNlOuter, innerNl } };
        outerNlOuter.Parent = outerNl;
        innerNl.Parent = outerNl;

        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(innerNl));
        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(deepInner));
        // The inner join's OUTER child is the first input of the inner join, but the inner join is the second input of
        // the outer one, so it still runs once per outer row of the outer join.
        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(innerNlOuter));
        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(outerNlOuter));
    }

    // ---- GetExpectedRows / GetRowAccuracyRatio ----------------------------------------------------

    [Fact]
    public void GetExpectedRows_NonInnerSide_IsNotMultipliedByExecutions()
    {
        // A parallel zone's ActualExecutions counts threads for a node no loop repeats.
        var node = new PlanNode { PhysicalOp = "Nested Loops", EstimateRows = 2983.02, ActualExecutions = 8 };

        Assert.Equal(2983.02, RowEstimateHelper.GetExpectedRows(node), 6);
    }

    [Fact]
    public void GetExpectedRows_InnerSide_IsMultipliedByExecutions()
    {
        var (_, _, inner) = NestedLoops();
        inner.EstimateRows = 1;
        inner.ActualExecutions = 613;

        Assert.Equal(613, RowEstimateHelper.GetExpectedRows(inner), 6);
    }

    /// <summary>An inner-side node that never executed does not get its estimate multiplied by zero.</summary>
    [Fact]
    public void GetExpectedRows_InnerSideButNeverExecuted_FallsBackToRawEstimate()
    {
        var (_, _, inner) = NestedLoops();
        inner.EstimateRows = 5;
        inner.ActualExecutions = 0;

        Assert.Equal(5, RowEstimateHelper.GetExpectedRows(inner), 6);
    }

    [Fact]
    public void GetRowAccuracyRatio_ZeroExpectedAndZeroActual_IsNeutral()
    {
        var node = new PlanNode { EstimateRows = 0, ActualRows = 0 };

        Assert.Equal(1.0, RowEstimateHelper.GetRowAccuracyRatio(node));
    }

    [Fact]
    public void GetRowAccuracyRatio_ZeroExpectedButSomeActual_IsUnbounded()
    {
        var node = new PlanNode { EstimateRows = 0, ActualRows = 100 };

        Assert.Equal(double.MaxValue, RowEstimateHelper.GetRowAccuracyRatio(node));
    }

    [Fact]
    public void GetRowAccuracyRatio_NonZeroExpected_IsActualOverExpected()
    {
        var (_, _, inner) = NestedLoops();
        inner.EstimateRows = 2;
        inner.ActualExecutions = 50;
        inner.ActualRows = 50;

        // Expected is 2 x 50 = 100 on the inner side, so 50 actual rows is half of it.
        Assert.Equal(0.5, RowEstimateHelper.GetRowAccuracyRatio(inner), 6);
    }

    // ---- eager_index_spool_plan.sqlplan (PerformanceStudio's worked numbers) -----------------------

    /// <summary>Node 1, the outer Nested Loops: not inner-side. 609 actual over an EstimateRows of 2983.02 is a 4.9x
    /// overestimate (20%), not the 39x a thread-summed ActualExecutions of 8 gives when it is divided in.</summary>
    [Fact]
    public void EagerIndexSpoolPlan_Node1_NotInnerSide_RatioIsFourPointNineX()
    {
        var node1 = Node(1);

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(node1));
        Assert.Equal(8, node1.ActualExecutions);
        Assert.Equal(609, node1.ActualRows);
        Assert.Equal(2983.02, RowEstimateHelper.GetExpectedRows(node1), 2);

        var ratio = RowEstimateHelper.GetRowAccuracyRatio(node1);
        Assert.Equal(0.204, ratio, 3);
        Assert.Equal(4.9, 1.0 / ratio, 1);
    }

    /// <summary>Nodes 4 and 5 are inner-side of Node 1: 609 actual over 613 executions against an EstimateRows of 1
    /// is 99%, so the estimate was right.</summary>
    [Theory]
    [InlineData(4)]
    [InlineData(5)]
    public void EagerIndexSpoolPlan_InnerSideNodes_RatioIsNinetyNinePercent(int nodeId)
    {
        var node = Node(nodeId);

        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(node));
        Assert.Equal(613, node.ActualExecutions);
        Assert.Equal(609, node.ActualRows);
        Assert.Equal(613, RowEstimateHelper.GetExpectedRows(node), 6);
        Assert.Equal(0.993, RowEstimateHelper.GetRowAccuracyRatio(node), 3);
    }

    /// <summary>Node 6, the scan under the spool: inner-side, but executed once, so multiplying by ActualExecutions is
    /// a no-op and the expected rows equal the raw estimate.</summary>
    [Fact]
    public void EagerIndexSpoolPlan_Node6_InnerSideButSingleExecution_MatchesRawEstimate()
    {
        var node6 = Node(6);

        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(node6));
        Assert.Equal(1, node6.ActualExecutions);
        Assert.Equal(8_042_005, node6.ActualRows);
        Assert.Equal(8_042_010, RowEstimateHelper.GetExpectedRows(node6), 6);
    }

    // ---- helpers ----------------------------------------------------------------------------------

    private static (PlanNode Nl, PlanNode Outer, PlanNode Inner) NestedLoops()
    {
        var outer = new PlanNode { PhysicalOp = "Sort" };
        var inner = new PlanNode { PhysicalOp = "Index Seek" };
        var nl = new PlanNode { PhysicalOp = "Nested Loops", Children = { outer, inner } };
        outer.Parent = nl;
        inner.Parent = nl;
        return (nl, outer, inner);
    }

    private static PlanNode Node(int nodeId)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", Fixture);
        var plan = ShowPlanParser.Parse(File.ReadAllText(path));
        var root = plan.Batches[0].Statements[0].RootNode!;
        return Flatten(root).First(n => n.NodeId == nodeId);
    }

    private static IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }
}
