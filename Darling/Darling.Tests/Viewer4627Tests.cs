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
/// #4627 — on the inner side of a Nested Loops join, <c>PlanEdgeColour.ForChild</c> compared the
/// CHILD's un-normalized total <c>ActualRows</c> (summed across every execution) against its
/// per-execution <c>EstimateRows</c>, while the node label already divided by
/// <c>ActualExecutions</c> first. An operator that ran thousands of times therefore got the worst
/// misestimate edge color (FluoRed) even when its own label showed the estimate was close.
///
/// <para>Repro fixture: <c>udf_plan.sqlplan</c>, the same actual plan the issue's repro table is
/// built from. Nodes 7, 8, 10 and 12 are Nested-Loops-inner-side operators whose per-execution
/// estimate was within the default 10x divergence limit (their node labels were never red); node 9
/// diverged by about 22x, comfortably inside the old bug's inflated ratio but only into the first
/// (LightOrange) tier once normalized. These pin both the ratio <see cref="PlanRowAccuracy"/> now
/// returns — the one place both the node label and <see cref="PlanEdgeColour"/> get it from — and
/// the resulting edge color, so the two can't drift apart again.</para>
/// </summary>
public sealed class Viewer4627Tests
{
    private static string FixturePath(string fileName) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName);

    private static ParsedPlan LoadUdfPlan()
    {
        var xml = File.ReadAllText(FixturePath("udf_plan.sqlplan"));
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }

    /// <summary>NodeId is unique across every statement in this fixture, so one flattened search covers it.</summary>
    private static PlanNode FindNode(ParsedPlan plan, int nodeId) =>
        plan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Flatten(s.RootNode!))
            .Single(n => n.NodeId == nodeId);

    // Expected per-execution ratios, computed independently from the fixture's raw RunTimeCountersPerThread
    // and EstimateRows attributes (ActualRows / ActualExecutions / EstimateRows), not through the code under test.
    [Theory]
    [InlineData(7, 5772L, 5829L, 1.0, 0.990221, PlanEdgeColourKey.Neutral)]
    [InlineData(8, 222698L, 5829L, 5.20093, 7.345836, PlanEdgeColourKey.Neutral)]
    [InlineData(10, 5081L, 5829L, 1.0, 0.871676, PlanEdgeColourKey.Neutral)]
    [InlineData(12, 409585L, 5829L, 112.106, 0.626789, PlanEdgeColourKey.Neutral)]
    [InlineData(9, 781284L, 5829L, 6.09976, 21.973646, PlanEdgeColourKey.LightOrange)]
    public void InnerSideNestedLoopsNode_EdgeRatioMatchesFixtureAndAgreesWithLabel(
        int nodeId, long expectedActualRows, long expectedActualExecutions, double expectedEstimateRows,
        double expectedRatio, PlanEdgeColourKey expectedKey)
    {
        var plan = LoadUdfPlan();
        var node = FindNode(plan, nodeId);

        // The fixture itself still holds the numbers the issue's repro table lists.
        Assert.Equal(expectedActualRows, node.ActualRows);
        Assert.Equal(expectedActualExecutions, node.ActualExecutions);
        Assert.Equal(expectedEstimateRows, node.EstimateRows, precision: 5);
        Assert.True(node.HasActualStats);

        // Pin the ratio PlanRowAccuracy returns — the single source both the label and the edge use.
        var actualRowsPerExecution = PlanRowAccuracy.ActualRowsPerExecution(node.ActualRows, node.ActualExecutions);
        var ratio = PlanRowAccuracy.Ratio(actualRowsPerExecution, node.EstimateRows);
        Assert.Equal(expectedRatio, ratio, precision: 5);

        // The edge color must agree with that ratio, not with the old total-over-per-execution math
        // (which put every one of these at FluoRed — see the "old buggy math" fact below).
        var key = PlanEdgeColour.ForChild(node.HasActualStats, node.ActualRows, node.ActualExecutions, node.EstimateRows, PlanEdgeColour.DefaultDivergenceLimit);
        Assert.Equal(expectedKey, key);
        Assert.NotEqual(PlanEdgeColourKey.FluoRed, key);
    }

    /// <summary>
    /// Documents the bug this closes: dividing total ActualRows by the per-execution EstimateRows,
    /// with no normalization by ActualExecutions, put every one of nodes 7, 8, 9, 10 and 12 at the
    /// worst-misestimate tier — exactly what the issue's screenshot showed.
    /// </summary>
    [Theory]
    [InlineData(7, PlanEdgeColourKey.FluoRed)]
    [InlineData(8, PlanEdgeColourKey.FluoRed)]
    [InlineData(9, PlanEdgeColourKey.FluoRed)]
    [InlineData(10, PlanEdgeColourKey.FluoRed)]
    [InlineData(12, PlanEdgeColourKey.FluoRed)]
    public void OldTotalOverPerExecutionMath_WouldHaveMisclassifiedEveryOne(int nodeId, PlanEdgeColourKey oldBuggyKey)
    {
        var plan = LoadUdfPlan();
        var node = FindNode(plan, nodeId);

        // The pre-fix call shape: ForChild(hasActualStats, TOTAL actualRows, PER-EXECUTION estimateRows,
        // divergenceLimit), with no ActualExecutions normalization — reproduced here by passing 1 execution.
        var oldKey = PlanEdgeColour.ForChild(node.HasActualStats, node.ActualRows, actualExecutions: 1, node.EstimateRows, PlanEdgeColour.DefaultDivergenceLimit);
        Assert.Equal(oldBuggyKey, oldKey);
    }
}
