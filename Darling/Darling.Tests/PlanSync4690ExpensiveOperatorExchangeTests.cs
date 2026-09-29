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
/// #4690 (erikdarlingdata/PerformanceStudio#608) - Rule 35 (Expensive Operator) used to rank exchanges
/// by their self-time. An exchange's elapsed time is mostly spent waiting on the operators that feed it
/// and drain it, so it could be named the expensive operator when the operator next to it was the real
/// problem. <c>serially-parallel.sqlplan</c> shows it plainly: the Sort took 17,111 ms and the
/// Repartition Streams below it, which feeds the Sort, was given the same 17,111 ms as if it had done
/// the same work. The three fixtures are copied verbatim from PerformanceStudio's public repo.
/// </summary>
public sealed class PlanSync4690ExpensiveOperatorExchangeTests
{
    private static ParsedPlan LoadAndAnalyze(string fileName)
    {
        var xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName));
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        BenefitScorer.Score(plan);
        return plan;
    }

    private static IEnumerable<PlanNode> Nodes(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Nodes(child))
                yield return descendant;
    }

    private static List<PlanNode> AllNodes(ParsedPlan plan) =>
        PlanStatements.EnumerateAll(plan)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Nodes(s.RootNode!))
            .ToList();

    private static List<PlanNode> ExchangesIn(ParsedPlan plan) =>
        AllNodes(plan).Where(n => n.PhysicalOp == "Parallelism").ToList();

    [Theory]
    [InlineData("serially-parallel.sqlplan")]
    [InlineData("memory_grant_wait_plan.sqlplan")]
    [InlineData("spill_plan.sqlplan")]
    public void Exchange_IsNeverNamedTheExpensiveOperator(string fixture)
    {
        var plan = LoadAndAnalyze(fixture);

        var exchanges = ExchangesIn(plan);
        Assert.NotEmpty(exchanges);   // so the assertion below can't pass on a plan with no exchange
        Assert.All(exchanges, e =>
            Assert.DoesNotContain(e.Warnings, w => w.WarningType == "Expensive Operator"));
    }

    [Fact]
    public void SeriallyParallelPlan_StillNamesTheSort()
    {
        // The Sort is the operator that did the work, and it keeps its Rule 35 warning. Only the
        // exchange that mirrored its time drops out.
        var plan = LoadAndAnalyze("serially-parallel.sqlplan");

        var warning = Assert.Single(AllNodes(plan).SelectMany(n => n.Warnings), w => w.WarningType == "Expensive Operator");
        Assert.StartsWith("Sort took", warning.Message);
    }

    // ---- built in code: the same timing on an exchange and on an ordinary operator ---------------

    private static PlanNode Analyze(string physicalOp, string logicalOp)
    {
        var node = new PlanNode
        {
            NodeId = 1,
            PhysicalOp = physicalOp,
            LogicalOp = logicalOp,
            HasActualStats = true,
            ActualExecutions = 1,
            ActualRows = 100,
            EstimateRows = 100,
            ActualElapsedMs = 6000
        };
        var stmt = new PlanStatement
        {
            RootNode = node,
            QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = 10000, CpuTimeMs = 10000 }
        };
        PlanAnalyzer.Analyze(new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] });
        return node;
    }

    [Fact]
    public void ControlOperator_With60PercentOfTheStatement_IsFlagged()
    {
        var node = Analyze("Sort", "Sort");

        var warning = Assert.Single(node.Warnings, w => w.WarningType == "Expensive Operator");
        Assert.Equal(35, warning.RuleNumber);
    }

    [Theory]
    [InlineData("Repartition Streams")]
    [InlineData("Gather Streams")]
    [InlineData("Distribute Streams")]
    public void Exchange_With60PercentOfTheStatement_IsNotFlagged(string logicalOp)
    {
        var node = Analyze("Parallelism", logicalOp);

        Assert.DoesNotContain(node.Warnings, w => w.WarningType == "Expensive Operator");
    }
}
