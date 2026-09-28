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
/// #4534 (PerformanceStudio#440/#446) — a finding did not record which operator it came from, so
/// on a large plan there was no way to get from a finding to the thing that produced it.
/// <see cref="PlanWarning.OriginNodeIds"/> carries it. An operator-level finding is stamped with
/// its own node, a statement-level finding stays empty unless the rule itself knows which
/// operators are responsible (the table variable rules, which already walk the tree), and a
/// statement-level finding with no operator origin — High Compile CPU — claims none.
/// </summary>
public sealed class PlanSync4534Tests
{
    /// <summary>
    /// Statement 1 (SELECT): a table variable is only read, by node 1 (a Table Scan against
    /// <c>@tv</c>). Statement 2 (INSERT): the same table variable is the modification target of
    /// node 11 (a Table Insert), which forces the plan single-threaded. Also carries a high
    /// compile CPU statement-level warning with no operator origin.
    /// </summary>
    private const string ReproXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM @tv" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="12000" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Compute Scalar" LogicalOp="Compute Scalar" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <ComputeScalar>
                <DefinedValues/>
                <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <TableScan Storage="RowStore">
                    <Object Database="[Repro]" Table="[@tv]" Storage="RowStore"/>
                  </TableScan>
                </RelOp>
              </ComputeScalar>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        <StmtSimple StatementText="INSERT INTO @tv SELECT a FROM dbo.t" StatementId="2" StatementCompId="2" StatementType="INSERT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="10" PhysicalOp="Table Insert" LogicalOp="Insert" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableInsert>
                <Object Database="[Repro]" Table="[@tv]" Storage="RowStore"/>
                <RelOp NodeId="11" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <TableScan Storage="RowStore">
                    <Object Database="[Repro]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
                  </TableScan>
                </RelOp>
              </TableInsert>
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

    /// <summary>An operator-level finding (Table Variable, referencing) is stamped with the node
    /// it hangs off — node 1, the Table Scan against <c>@tv</c> in the SELECT statement.</summary>
    [Fact]
    public void TableVariableReferenceWarning_OriginIsTheReferencingOperator()
    {
        var plan = ParseAndAnalyze(ReproXml);

        var referencing = plan.Batches.Single().Statements[0].PlanWarnings
            .Single(w => w.WarningType == "Table Variable" && w.Severity == PlanWarningSeverity.Warning);

        Assert.Equal(new[] { 1 }, referencing.OriginNodeIds);
    }

    /// <summary>The modification finding is stamped with node 11, the Table Insert against
    /// <c>@tv</c> in the INSERT statement — not the Table Scan reading from <c>dbo.t</c>.</summary>
    [Fact]
    public void TableVariableModificationWarning_OriginIsTheModifyingOperator()
    {
        var plan = ParseAndAnalyze(ReproXml);

        var modifying = plan.Batches.Single().Statements[1].PlanWarnings
            .Single(w => w.WarningType == "Table Variable" && w.Severity == PlanWarningSeverity.Critical);

        Assert.Equal(new[] { 10 }, modifying.OriginNodeIds);
    }

    /// <summary>High Compile CPU is measured before a single row is read, so no operator is
    /// responsible for it — it must claim none rather than pointing somewhere arbitrary.</summary>
    [Fact]
    public void HighCompileCpuWarning_HasNoOperatorOrigin()
    {
        var plan = ParseAndAnalyze(ReproXml);

        var highCompile = plan.Batches.Single().Statements[0].PlanWarnings
            .Single(w => w.WarningType == "High Compile CPU");

        Assert.Empty(highCompile.OriginNodeIds);
    }

    /// <summary>An operator-level finding with no rule-supplied origin is stamped in one place, at
    /// the end of AnalyzeNode, with the node it is hanging off.</summary>
    [Fact]
    public void OperatorLevelWarning_IsStampedWithItsOwnNode()
    {
        var plan = ParseAndAnalyze(ReproXml);
        var root = plan.Batches.Single().Statements[1].RootNode!;

        foreach (var node in Flatten(root))
            foreach (var warning in node.Warnings)
                Assert.Contains(node.NodeId, warning.OriginNodeIds);
    }

    /// <summary>The MCP plan tools carry origin_node_ids next to source on every warning
    /// (additive, next to #4543's source field).</summary>
    [Fact]
    public void McpFormatter_CarriesOriginNodeIds()
    {
        var plan = ShowPlanParser.Parse(ReproXml);
        PlanAnalyzer.Analyze(plan);

        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(ReproXml, "test-server", "showplan", null);

        Assert.Contains("origin_node_ids", json);
        Assert.Contains("\"origin_node_ids\":[1]", json.Replace(" ", ""));
    }

    /// <summary>Rule 35 "Expensive Operator" (#4550) is one more rule that adds a warning with no
    /// origin of its own, so the stamp loop must fill it in too. It only proves this if rule 35
    /// runs BEFORE the stamp loop — the other order was tried and reverted (see below).</summary>
    [Fact]
    public void ExpensiveOperatorWarning_OriginIsTheOperatorItsOwnNode()
    {
        const string xml = """
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
            <StmtSimple StatementText="SELECT * FROM dbo.T" StatementId="1" StatementCompId="1" StatementType="SELECT">
              <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
                <QueryTimeStats ElapsedTime="2000" CpuTime="2000"/>
                <RelOp NodeId="7" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1000" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="1000" Batches="0" ActualEndOfScans="1" ActualExecutions="1" ActualElapsedms="1200" ActualExecutionMode="Row"/></RunTimeInformation>
                  <TableScan><DefinedValues/><Object Database="[db]" Schema="[dbo]" Table="[T]"/></TableScan>
                </RelOp>
              </QueryPlan>
            </StmtSimple>
            </Statements></Batch></BatchSequence></ShowPlanXML>
            """;

        var plan = ParseAndAnalyze(xml);
        var root = plan.Batches.SelectMany(b => b.Statements).Single().RootNode!;
        var node = root.Children.Single(n => n.PhysicalOp == "Table Scan");

        var warning = Assert.Single(node.Warnings, w => w.WarningType == "Expensive Operator");
        Assert.Equal(new[] { 7 }, warning.OriginNodeIds);
    }

    private static System.Collections.Generic.IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }
}
