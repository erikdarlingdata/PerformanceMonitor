/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4522 (PerformanceStudio#577) — Rule 5 (Row Estimate Mismatch) checked
/// <c>node.HasActualStats &amp;&amp; node.EstimateRows &gt; 0</c> without also requiring
/// <c>node.ActualExecutions &gt; 0</c>. An operator in a branch that never ran (an outer join's
/// inner side, a conditional CASE branch, an OR branch of a concatenation) returns 0 rows
/// because it never executed, not because the estimate was wrong. The rule now skips operators
/// with zero executions, the same way rules 11, 12 and 29 already do, and the now-unreachable
/// <c>ActualExecutions &gt; 0 ? ActualExecutions : 1</c> fallback is removed.
///
/// <para>The repro XML is a synthetic actual plan (<c>dbo.t</c>) with two Sort operators under a
/// Concatenation: one ran zero times (a branch that never executed), one ran once and returned
/// nothing despite a large estimate.</para>
/// </summary>
public sealed class PlanSync4522Tests
{
    private const string ReproXmlTemplate = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.t" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Concatenation" LogicalOp="Concatenation" EstimateRows="2000" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="0" ActualExecutions="1" ActualEndOfScans="1" ActualExecutionMode="Row"/></RunTimeInformation>
              <Concat>
                <RelOp NodeId="1" PhysicalOp="Sort" LogicalOp="Sort" EstimateRows="1000" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="0" ActualExecutions="{{FIRST_EXECUTIONS}}" ActualEndOfScans="1" ActualExecutionMode="Row"/></RunTimeInformation>
                  <Sort Distinct="0"><OrderBy/><DefinedValues/></Sort>
                </RelOp>
                <RelOp NodeId="2" PhysicalOp="Sort" LogicalOp="Sort" EstimateRows="1000" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="0" ActualExecutions="{{SECOND_EXECUTIONS}}" ActualEndOfScans="1" ActualExecutionMode="Row"/></RunTimeInformation>
                  <Sort Distinct="0"><OrderBy/><DefinedValues/></Sort>
                </RelOp>
              </Concat>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static ParsedPlan ParseAndAnalyze(long firstExecutions, long secondExecutions)
    {
        var xml = ReproXmlTemplate
            .Replace("{{FIRST_EXECUTIONS}}", firstExecutions.ToString())
            .Replace("{{SECOND_EXECUTIONS}}", secondExecutions.ToString());
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static bool HasRowEstimateMismatch(PlanNode node) =>
        node.Warnings.Any(w => w.WarningType == "Row Estimate Mismatch");

    private static PlanNode FindSort(ParsedPlan plan, int nodeId)
    {
        var root = plan.Batches.SelectMany(b => b.Statements).Single().RootNode!;
        return Flatten(root).Single(n => n.NodeId == nodeId);
    }

    private static System.Collections.Generic.IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }

    [Fact]
    public void Rule05_OperatorThatNeverExecuted_IsNotAnEstimateMismatch()
    {
        // Node 1 never executed (ActualExecutions=0) despite an estimate of 1000 rows.
        var plan = ParseAndAnalyze(firstExecutions: 0, secondExecutions: 1);
        var neverRan = FindSort(plan, nodeId: 1);

        Assert.False(HasRowEstimateMismatch(neverRan));
    }

    [Fact]
    public void Rule05_OperatorThatExecutedAndReturnedNothing_StillWarns()
    {
        // Node 2 ran once (ActualExecutions=1) and still returned 0 rows against an estimate
        // of 1000 — a real mismatch, not a branch that never ran.
        var plan = ParseAndAnalyze(firstExecutions: 0, secondExecutions: 1);
        var ranButEmpty = FindSort(plan, nodeId: 2);

        Assert.True(HasRowEstimateMismatch(ranButEmpty));
    }
}
