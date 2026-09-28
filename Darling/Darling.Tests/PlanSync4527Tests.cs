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
/// #4527 (PerformanceStudio#215 / #562) — Rule 35 "Expensive Operator" surfaces a leaf
/// operator that takes a large share of a statement's elapsed time even when no other rule
/// has anything specific to say about it, so a large piece of work never silently disappears
/// just because the tool has no dedicated advice for it. It fires when an operator's own time
/// is at least 20% of the statement's elapsed time, the operator has no other warning already
/// on it, and the statement itself ran at least 1,000ms (below that floor, a share of a
/// few-millisecond statement points at nothing — PS#562). Severity is Critical at 50% or
/// more, Warning otherwise, and the benefit percent is set to the self-time share (the
/// benefit scorer isn't wired to recompute this warning type, so that value is carried as-is
/// from the analyzer, same as PerformanceStudio).
///
/// <para>Each repro plan is a single-statement, single leaf Table Scan (no children, no
/// predicate, no other warning) so the operator's own elapsed time is just its
/// <c>ActualElapsedms</c> with nothing to subtract.</para>
/// </summary>
public sealed class PlanSync4527Tests
{
    // elapsedMs is the statement's QueryTimeStats/ElapsedTime; selfMs is the leaf Table
    // Scan's own ActualElapsedms (single thread, so self-time == ActualElapsedms directly).
    private static string PlanXml(long elapsedMs, long selfMs) => $"""
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT * FROM dbo.T" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <QueryTimeStats ElapsedTime="{elapsedMs}" CpuTime="{elapsedMs}"/>
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1000" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="1000" Batches="0" ActualEndOfScans="1" ActualExecutions="1" ActualElapsedms="{selfMs}" ActualExecutionMode="Row"/></RunTimeInformation>
              <TableScan><DefinedValues/><Object Database="[db]" Schema="[dbo]" Table="[T]"/></TableScan>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    // A 60% operator that already has a warning of its own (Rule 4: UDF Execution fires
    // whenever a node shows nonzero UDF CPU/elapsed time). Rule 35 must not stack on top
    // of it.
    private static string PlanXmlWithExistingWarning(long elapsedMs, long selfMs) => $"""
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT * FROM dbo.T" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <QueryTimeStats ElapsedTime="{elapsedMs}" CpuTime="{elapsedMs}"/>
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1000" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="1000" Batches="0" ActualEndOfScans="1" ActualExecutions="1" ActualElapsedms="{selfMs}" UdfCpuTime="1" UdfElapsedTime="1" ActualExecutionMode="Row"/></RunTimeInformation>
              <TableScan><DefinedValues/><Object Database="[db]" Schema="[dbo]" Table="[T]"/></TableScan>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static ParsedPlan ParseAndAnalyze(string xml)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static PlanNode FindScanNode(ParsedPlan plan)
    {
        var root = plan.Batches.SelectMany(b => b.Statements).Single().RootNode!;
        return root.Children.Single(n => n.PhysicalOp == "Table Scan");
    }

    [Fact]
    public void Rule35_60PercentSelfTimeOn2000msStatement_FiresCritical()
    {
        var plan = ParseAndAnalyze(PlanXml(elapsedMs: 2000, selfMs: 1200));
        var node = FindScanNode(plan);

        var warning = Assert.Single(node.Warnings, w => w.WarningType == "Expensive Operator");
        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
        Assert.Equal(60.0, warning.MaxBenefitPercent);
    }

    [Fact]
    public void Rule35_30PercentSelfTime_FiresWarning()
    {
        var plan = ParseAndAnalyze(PlanXml(elapsedMs: 2000, selfMs: 600));
        var node = FindScanNode(plan);

        var warning = Assert.Single(node.Warnings, w => w.WarningType == "Expensive Operator");
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
        Assert.Equal(30.0, warning.MaxBenefitPercent);
    }

    [Fact]
    public void Rule35_10PercentSelfTime_DoesNotFire()
    {
        var plan = ParseAndAnalyze(PlanXml(elapsedMs: 2000, selfMs: 200));
        var node = FindScanNode(plan);

        Assert.DoesNotContain(node.Warnings, w => w.WarningType == "Expensive Operator");
    }

    [Fact]
    public void Rule35_60PercentSelfTimeOn500msStatement_DoesNotFire_BelowFloor()
    {
        // Same 60% share as the Critical case, but the statement only ran 500ms —
        // below the 1,000ms floor (PS#562), so the share points at nothing.
        var plan = ParseAndAnalyze(PlanXml(elapsedMs: 500, selfMs: 300));
        var node = FindScanNode(plan);

        Assert.DoesNotContain(node.Warnings, w => w.WarningType == "Expensive Operator");
    }

    [Fact]
    public void Rule35_60PercentSelfTimeWithExistingWarning_DoesNotStack()
    {
        var plan = ParseAndAnalyze(PlanXmlWithExistingWarning(elapsedMs: 2000, selfMs: 1200));
        var node = FindScanNode(plan);

        Assert.Contains(node.Warnings, w => w.WarningType == "UDF Execution");
        Assert.DoesNotContain(node.Warnings, w => w.WarningType == "Expensive Operator");
    }
}
