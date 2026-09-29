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
/// #4627 part 3: the plan viewer's edge colour (and the minimap's copy of it) now takes the child's expected
/// rows from <see cref="RowEstimateHelper"/> instead of dividing ActualRows by ActualExecutions for every node.
/// EstimateRows is per execution and ActualRows is a running total; ActualExecutions turns the estimate into a
/// fair comparison only on the inner side of a Nested Loops join, where it is a real loop count. In a parallel
/// zone anywhere else it counts threads, so the old division made an accurate operator at DOP 11 or more read
/// as a 1/11 overestimate and draw a Blue edge (and hid a genuine underestimate behind the same factor).
/// PerformanceStudio fixed the same bug in PerformanceStudio#594 / #597.
///
/// <para>Every tree here is wired the way <c>ShowPlanParser</c> wires one: <c>Parent</c> set and the child added
/// to the parent's <c>Children</c>. <c>IsInnerSideOfNestedLoops</c> compares by reference against
/// <c>Children[1]</c>, and <c>PlanNode</c> has no <c>==</c> overload, so a tree built with only one of the two
/// links would quietly read as "not inner side" and pass for the wrong reason.</para>
///
/// <para>The old numeric overload is gone, so each "old code said" note below is a number worked out by hand
/// from the old arithmetic (total ActualRows / ActualExecutions / EstimateRows) and stated in the comment. The
/// regressions these pins guard against were each planted in the code, one at a time, and watched fail before
/// this landed.</para>
/// </summary>
public sealed class Viewer4627EdgeColourTests
{
    private const double Limit = PlanEdgeColour.DefaultDivergenceLimit;

    // ---- a parallel zone that is not on a Nested Loops inner side -----------------------------------

    /// <summary>
    /// An operator under Gather Streams that returned exactly the rows it was estimated to. Its
    /// ActualExecutions is the DOP, because each thread reports one execution. The old code divided by it:
    /// 1/11 = 0.0909 is under the 0.1 floor, so DOP 11 and up drew Blue (1/128 = 0.0078 drew LightBlue and
    /// 1/2000 = 0.0005 drew FluoBlue).
    /// DOP 2, 8 and 10 were inside the band under the old math too, so they pass either way: they mark where
    /// the bug starts, not what fixes it.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(10)]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(2000)]
    public void ParallelZoneNode_ThatMetItsEstimate_IsNeutralAtAnyDop(int dop)
    {
        var scan = ScanUnderGather(dop, estimateRows: 1000, actualRows: 1000);

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(scan));
        Assert.Equal(dop, scan.ActualExecutions);
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(scan, Limit));
    }

    /// <summary>
    /// Dividing by the thread count did not just paint accurate operators blue: it shrank every real miss by
    /// the same factor, so a 20x underestimate at DOP 11 read as 1.8x and drew no colour at all. The colour
    /// follows the miss now. Old code said: row 1 Neutral, row 2 LightOrange, row 3 FluoOrange, row 4
    /// LightBlue.
    /// </summary>
    [Theory]
    [InlineData(11, 1000.0, 20_000L, PlanEdgeColourKey.LightOrange)]
    [InlineData(11, 1000.0, 200_000L, PlanEdgeColourKey.FluoOrange)]
    [InlineData(11, 1000.0, 2_000_000L, PlanEdgeColourKey.FluoRed)]
    [InlineData(11, 1000.0, 50L, PlanEdgeColourKey.Blue)]
    public void ParallelZoneNode_ThatMissedItsEstimate_IsColouredByTheMiss(
        int dop, double estimateRows, long actualRows, PlanEdgeColourKey expectedKey)
    {
        var scan = ScanUnderGather(dop, estimateRows, actualRows);

        Assert.Equal(expectedKey, PlanEdgeColour.ForChild(scan, Limit));
    }

    /// <summary>
    /// PerformanceStudio's own DOP 8 fixture. Node 1 is the outer Nested Loops join, not on an inner side:
    /// 609 actual rows over an EstimateRows of 2983.02 is a 4.9x overestimate, inside the 10x band. Dividing
    /// the 609 by its 8 thread-executions first made it 0.0255 and drew Blue, so the bug shows below DOP 11
    /// as well, for any operator that is already a little over-estimated.
    /// </summary>
    [Fact]
    public void EagerIndexSpoolPlan_Node1_AtDop8_IsNeutralWhereDividingByThreadsDrewBlue()
    {
        var node1 = LoadNodes("eager_index_spool_plan.sqlplan").First(n => n.NodeId == 1);

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(node1));
        Assert.Equal(8, node1.ActualExecutions);
        Assert.Equal(609, node1.ActualRows);
        Assert.Equal(2983.02, node1.EstimateRows, precision: 2);

        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(node1, Limit));

        // The old arithmetic, spelled out through the numeric overload: 609 / 8 threads against the estimate.
        var perThread = (double)node1.ActualRows / node1.ActualExecutions;
        Assert.Equal(PlanEdgeColourKey.Blue, PlanEdgeColour.ForChild(true, perThread, node1.EstimateRows, Limit));
    }

    // ---- the inner side of a Nested Loops join keeps its colour --------------------------------------

    /// <summary>
    /// The Key Lookup in <c>key_lookup_plan.sqlplan</c>: EstimateRows 0.00964372, 117 executions, 1 actual
    /// row, on the inner side of a Nested Loops join. 1 / (0.00964372 x 117) is 0.886, inside the band, and
    /// it was Neutral under the old per-execution math (1/117 over 0.00964372 is the same 0.886). The fix must
    /// not move it. A caller that compared the bare 1 row against the 0.00964 estimate would call it 104x and
    /// draw FluoOrange, which is the original #4627 symptom.
    /// </summary>
    [Fact]
    public void KeyLookupFixture_OnTheNestedLoopsInnerSide_KeepsItsNeutralKey()
    {
        var plan = ShowPlanParser.Parse(File.ReadAllText(FixturePath("key_lookup_plan.sqlplan")));
        var node = plan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Flatten(s.RootNode!))
            .Single(n => n.NodeId == 4);

        Assert.Equal(1L, node.ActualRows);
        Assert.Equal(117L, node.ActualExecutions);
        Assert.Equal(0.00964372, node.EstimateRows, precision: 8);
        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(node));

        Assert.Equal(0.00964372 * 117, RowEstimateHelper.GetExpectedRows(node), precision: 6);
        Assert.Equal(0.886, RowEstimateHelper.GetRowAccuracyRatio(node), precision: 3);
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(node, Limit));
    }

    /// <summary>The same numbers on a hand-built tree, so this pin does not depend on the fixture file.</summary>
    [Fact]
    public void KeyLookupNumbers_OnTheInnerSideOfANestedLoops_KeepTheirNeutralKey()
    {
        var lookup = Op("Clustered Index Seek", estimateRows: 0.00964372, actualRows: 1, actualExecutions: 117);
        var outer = Op("Index Seek", estimateRows: 117, actualRows: 117, actualExecutions: 1);
        Above(Op("Nested Loops", estimateRows: 1, actualRows: 1, actualExecutions: 1), outer, lookup);

        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(lookup));
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(lookup, Limit));
    }

    // ---- Nested Loops inside Nested Loops ------------------------------------------------------------

    /// <summary>EstimateRows 0.5, 1,000 executions, 400 actual rows: expected 500, ratio 0.8, Neutral.</summary>
    private const double NestedEstimate = 0.5;
    private const long NestedExecutions = 1_000;
    private const long NestedActualRows = 400;

    /// <summary>The node is the inner child of an inner join that is itself the inner child of an outer join.</summary>
    [Fact]
    public void NestedLoopsWithinLoops_InnerOfBoth_IsNeutral()
    {
        var node = Op("Index Seek", NestedEstimate, NestedActualRows, NestedExecutions);
        var innerJoin = Above(Op("Nested Loops"), Op("Index Seek"), node);
        Above(Op("Nested Loops"), Op("Clustered Index Scan"), innerJoin);

        AssertNestedKeyIsNeutral(node);
    }

    /// <summary>
    /// The node is the OUTER child of the inner join. Nothing repeats it inside that join, but the join is
    /// the inner input of the outer one, so it still runs once per outer row of the outer join: a walk that
    /// stops at the first Nested Loops ancestor would miss it.
    /// </summary>
    [Fact]
    public void NestedLoopsWithinLoops_OuterChildOfTheInnerJoin_IsNeutral()
    {
        var node = Op("Index Seek", NestedEstimate, NestedActualRows, NestedExecutions);
        var innerJoin = Above(Op("Nested Loops"), node, Op("Index Seek"));
        Above(Op("Nested Loops"), Op("Clustered Index Scan"), innerJoin);

        AssertNestedKeyIsNeutral(node);
    }

    /// <summary>Operators that are not joins sit between the node and each Nested Loops it is inside of.</summary>
    [Fact]
    public void NestedLoopsWithinLoops_BehindNonJoinOperators_IsNeutral()
    {
        var node = Op("Index Seek", NestedEstimate, NestedActualRows, NestedExecutions);
        var scalar = Above(Op("Compute Scalar"), node);
        var innerJoin = Above(Op("Nested Loops"), Op("Index Seek"), scalar);
        var sort = Above(Op("Sort"), innerJoin);
        Above(Op("Nested Loops"), Op("Clustered Index Scan"), sort);

        AssertNestedKeyIsNeutral(node);
    }

    /// <summary>
    /// The contrast: the same three numbers on a node no Nested Loops encloses are judged against the plain
    /// estimate. Its 1,000 "executions" cannot be a loop count there, so 400 rows against an estimate of 0.5
    /// is an 800x underestimate and draws FluoOrange. Position in the tree, not the numbers, decides.
    /// </summary>
    [Fact]
    public void SameNumbers_WithNoEnclosingNestedLoops_AreJudgedAgainstThePlainEstimate()
    {
        var node = Op("Index Seek", NestedEstimate, NestedActualRows, NestedExecutions);
        Above(Op("Parallelism"), node);

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(node));
        Assert.Equal(800.0, RowEstimateHelper.GetRowAccuracyRatio(node), precision: 6);
        Assert.Equal(PlanEdgeColourKey.FluoOrange, PlanEdgeColour.ForChild(node, Limit));
    }

    // ---- the node overload is the numeric overload fed RowEstimateHelper's expected rows ---------------------

    /// <summary>An estimated plan never gets an accuracy colour, whatever its numbers say.</summary>
    [Fact]
    public void NodeWithoutActualStats_IsNeutral()
    {
        var node = Op("Index Seek", estimateRows: 1, actualRows: 1_000_000, actualExecutions: 1);
        node.HasActualStats = false;

        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(node, Limit));
    }

    /// <summary>
    /// For every operator in every origin fixture, at three limits, the node overload returns what the numeric
    /// overload returns when handed the node's own flag, total rows and <c>GetExpectedRows</c>. There is one
    /// way to turn a node into an "expected" figure, and the tiers only ever see that figure.
    /// </summary>
    [Fact]
    public void NodeOverload_IsTheNumericOverloadFedRowEstimateHelperExpectedRows_ForEveryFixtureNode()
    {
        var fixtures = Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans"), "*.sqlplan");
        Assert.True(fixtures.Length >= 5, "the origin plan fixtures did not reach the output directory");

        var checkedNodes = 0;
        foreach (var fixture in fixtures)
        {
            foreach (var node in LoadNodes(Path.GetFileName(fixture)))
            {
                foreach (var limit in new[] { 2.0, Limit, 50.0 })
                {
                    var viaNode = PlanEdgeColour.ForChild(node, limit);
                    var viaNumbers = PlanEdgeColour.ForChild(node.HasActualStats, node.ActualRows, RowEstimateHelper.GetExpectedRows(node), limit);
                    Assert.True(viaNode == viaNumbers,
                        $"{Path.GetFileName(fixture)} node {node.NodeId} at limit {limit}: the node overload gave {viaNode}, " +
                        $"the numeric overload given RowEstimateHelper's expected rows gave {viaNumbers}.");
                    checkedNodes++;
                }
            }
        }

        Assert.True(checkedNodes > 60, $"only {checkedNodes} node/limit pairs were checked, so the sweep is not reading the fixtures.");
    }

    // ---- RowEstimateHelper.GetRowAccuracyRatio(actualRows, expectedRows) -----------------------------

    [Fact]
    public void RowAccuracyRatio_OfNumbers_IsActualOverExpected()
    {
        Assert.Equal(0.5, RowEstimateHelper.GetRowAccuracyRatio(50, 100), precision: 9);
        Assert.Equal(20.0, RowEstimateHelper.GetRowAccuracyRatio(20_000, 1000), precision: 9);
    }

    [Fact]
    public void RowAccuracyRatio_OfNumbers_TreatsZeroExpectedTheWayTheNodeOverloadDoes()
    {
        Assert.Equal(1.0, RowEstimateHelper.GetRowAccuracyRatio(0, 0));
        Assert.Equal(double.MaxValue, RowEstimateHelper.GetRowAccuracyRatio(5, 0));
    }

    /// <summary>The node overload is the numeric one fed <c>GetExpectedRows</c>, so the two cannot disagree.</summary>
    [Fact]
    public void RowAccuracyRatio_OfANode_IsTheNumbersOverloadFedItsExpectedRows()
    {
        var node = Op("Index Seek", NestedEstimate, NestedActualRows, NestedExecutions);
        Above(Op("Nested Loops"), Op("Clustered Index Scan"), node);

        Assert.Equal(
            RowEstimateHelper.GetRowAccuracyRatio(node.ActualRows, RowEstimateHelper.GetExpectedRows(node)),
            RowEstimateHelper.GetRowAccuracyRatio(node));
    }

    // ---- test support ------------------------------------------------------------------------------------

    private static void AssertNestedKeyIsNeutral(PlanNode node)
    {
        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(node));
        Assert.Equal(NestedEstimate * NestedExecutions, RowEstimateHelper.GetExpectedRows(node), precision: 6);
        Assert.Equal(0.8, RowEstimateHelper.GetRowAccuracyRatio(node), precision: 6);
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(node, Limit));
    }

    /// <summary>A scan feeding Gather Streams: the shape of a parallel zone. Returns the scan.</summary>
    private static PlanNode ScanUnderGather(int dop, double estimateRows, long actualRows)
    {
        var scan = Op("Clustered Index Scan", estimateRows, actualRows, actualExecutions: dop);
        Above(Op("Parallelism", estimateRows, actualRows, actualExecutions: 1), scan);
        return scan;
    }

    private static PlanNode Op(string physicalOp, double estimateRows = 0, long actualRows = 0, long actualExecutions = 0) =>
        new()
        {
            PhysicalOp = physicalOp,
            EstimateRows = estimateRows,
            ActualRows = actualRows,
            ActualExecutions = actualExecutions,
            HasActualStats = true,
        };

    /// <summary>Wires each child under <paramref name="parent"/> the way <c>ShowPlanParser</c> does, and returns the parent.</summary>
    private static PlanNode Above(PlanNode parent, params PlanNode[] children)
    {
        foreach (var child in children)
        {
            child.Parent = parent;
            parent.Children.Add(child);
        }

        return parent;
    }

    private static string FixturePath(string fileName) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName);

    private static IEnumerable<PlanNode> LoadNodes(string fixtureFileName)
    {
        var plan = ShowPlanParser.Parse(File.ReadAllText(FixturePath(fixtureFileName)));
        return plan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Flatten(s.RootNode!));
    }

    private static IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }
}
