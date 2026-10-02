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
/// #4535 step S3b: <see cref="AnalyzerConfig.IsRuleDisabled(int)"/> guards and
/// <see cref="PlanWarning.RuleNumber"/> stamps at each node-level rule construction site for
/// rules 13, 14, 15, 16, 17, 22, 23, 24, 26, 28, 29 and 34 (both branches), plus rule 35's
/// interaction with another rule being disabled. Rules 21, 25 and 31 are excluded (#4537 removes
/// them). Ported from PerformanceStudio dev (85492a1)
/// <c>src/PlanViewer.Core/Services/PlanAnalyzer.Node.cs</c>.
/// </summary>
public sealed class PlanSync4535NodeRulesBTests
{
    // Rule 13: Compute Scalar with a mismatched-types DefinedValues marker.
    private const string Rule13Xml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.t WHERE b = @p" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Compute Scalar" LogicalOp="Compute Scalar" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <ComputeScalar>
                <DefinedValues>
                  <DefinedValue>
                    <ColumnReference Column="Expr1001"/>
                    <ScalarOperator ScalarString="GetRangeWithMismatchedTypes(1,2,3)"/>
                  </DefinedValue>
                </DefinedValues>
                <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <TableScan Storage="RowStore">
                    <Object Database="[Repro]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
                  </TableScan>
                </RelOp>
              </ComputeScalar>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    // Rule 34: two bare-scan candidates — a narrow-output (<=3 cols) statement and a wide-output
    // (>3 cols) statement — so both stamp sites are exercised.
    private static string Rule34Xml(bool wideOutput)
    {
        var outputList = wideOutput
            ? """<OutputList><ColumnReference Column="a"/><ColumnReference Column="b"/><ColumnReference Column="c"/><ColumnReference Column="d"/></OutputList>"""
            : """<OutputList><ColumnReference Column="a"/></OutputList>""";

        return $"""
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.t" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="5">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="1000" EstimateIO="5" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              {outputList}
              <IndexScan Ordered="0" ForcedIndex="0" ForceSeek="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
                <Object Database="[Repro]" Schema="[dbo]" Table="[t]" Index="[ci]" Storage="RowStore"/>
              </IndexScan>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;
    }

    private static ParsedPlan Analyze(string xml, AnalyzerConfig? cfg = null)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan, cfg, null, default);
        return plan;
    }

    private static PlanNode FindNode(ParsedPlan plan, int nodeId) =>
        plan.Batches.SelectMany(b => b.Statements)
            .SelectMany(s => s.RootNode is null ? System.Array.Empty<PlanNode>() : Flatten(s.RootNode))
            .First(n => n.NodeId == nodeId);

    private static System.Collections.Generic.IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var c in node.Children)
            foreach (var d in Flatten(c))
                yield return d;
    }

    [Fact]
    public void Rule13_MismatchedDataTypes_IsStampedWithItsRuleNumber()
    {
        var plan = Analyze(Rule13Xml);
        var warning = FindNode(plan, 0).Warnings.Single(w => w.WarningType == "Data Type Mismatch");
        Assert.Equal(13, warning.RuleNumber);
    }

    [Fact]
    public void Rule13_Disabled_ProducesNoDataTypeMismatchWarning()
    {
        var cfg = new AnalyzerConfig { Rules = new RulesConfig { Disabled = { 13 } } };
        var plan = Analyze(Rule13Xml, cfg);
        Assert.DoesNotContain(FindNode(plan, 0).Warnings, w => w.WarningType == "Data Type Mismatch");
    }

    [Fact]
    public void Rule34_NarrowOutput_IsStampedWithItsRuleNumber()
    {
        var plan = Analyze(Rule34Xml(wideOutput: false));
        var warning = FindNode(plan, 0).Warnings.Single(w => w.WarningType == "Bare Scan");
        Assert.Equal(34, warning.RuleNumber);
    }

    [Fact]
    public void Rule34_WideOutput_IsStampedWithItsRuleNumber()
    {
        var plan = Analyze(Rule34Xml(wideOutput: true));
        var warning = FindNode(plan, 0).Warnings.Single(w => w.WarningType == "Bare Scan");
        Assert.Equal(34, warning.RuleNumber);
    }

    [Fact]
    public void Rule34_Disabled_ProducesNoBareScanWarning_EitherBranch()
    {
        var cfg = new AnalyzerConfig { Rules = new RulesConfig { Disabled = { 34 } } };
        var narrow = Analyze(Rule34Xml(wideOutput: false), cfg);
        var wide = Analyze(Rule34Xml(wideOutput: true), cfg);
        Assert.DoesNotContain(FindNode(narrow, 0).Warnings, w => w.WarningType == "Bare Scan");
        Assert.DoesNotContain(FindNode(wide, 0).Warnings, w => w.WarningType == "Bare Scan");
    }

    /// <summary>
    /// Rule 29 only enhances an existing Implicit Conversion warning's severity/message — it never
    /// adds a warning of its own — so disabling it must leave that warning's severity untouched
    /// rather than remove anything. This exercises the guard through the real seek-plan wording
    /// SQL Server's own warning uses, since #4501 found a prior port checking the wrong prefix.
    /// </summary>
    [Fact]
    public void Rule29_Disabled_LeavesImplicitConversionWarningSeverityUnchanged()
    {
        var plan = ShowPlanParser.Parse(Rule13Xml);
        var node = FindNode(plan, 0);
        node.Warnings.Add(new PlanWarning
        {
            WarningType = "Implicit Conversion",
            Message = "Seek Plan: the query optimizer estimates...",
            Severity = PlanWarningSeverity.Warning
        });

        var cfgDisabled = new AnalyzerConfig { Rules = new RulesConfig { Disabled = { 29 } } };
        PlanAnalyzer.Analyze(plan, cfgDisabled, null, default);
        var warning = node.Warnings.Single(w => w.WarningType == "Implicit Conversion");
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
    }

    [Fact]
    public void Rule29_Enabled_EscalatesImplicitConversionSeekPlanWarningToCritical()
    {
        var plan = ShowPlanParser.Parse(Rule13Xml);
        var node = FindNode(plan, 0);
        node.Warnings.Add(new PlanWarning
        {
            WarningType = "Implicit Conversion",
            Message = "Seek Plan: the query optimizer estimates...",
            Severity = PlanWarningSeverity.Warning
        });

        PlanAnalyzer.Analyze(plan, null, null, default);
        var warning = node.Warnings.Single(w => w.WarningType == "Implicit Conversion");
        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
    }

    /// <summary>
    /// Rule 35 (Expensive Operator) only fires when the node has no other warning. Disabling the
    /// only other rule that would have fired on an expensive node lets rule 35 fire in its place —
    /// ported exactly, with no special handling for the interaction.
    /// </summary>
    [Fact]
    public void Rule35_FiresOnceTheOnlyOtherRuleOnAnExpensiveNodeIsDisabled()
    {
        var plan = ShowPlanParser.Parse(Rule13Xml);
        var stmt = plan.Batches.Single().Statements[0];
        stmt.QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = 10_000 };
        var node = FindNode(plan, 0);
        node.HasActualStats = true;
        node.ActualElapsedMs = 5_000;

        var cfgEnabled = new AnalyzerConfig();
        PlanAnalyzer.Analyze(plan, cfgEnabled, null, default);
        Assert.DoesNotContain(node.Warnings, w => w.WarningType == "Expensive Operator");

        node.Warnings.Clear();
        var cfgDisabled = new AnalyzerConfig { Rules = new RulesConfig { Disabled = { 13 } } };
        PlanAnalyzer.Analyze(plan, cfgDisabled, null, default);
        var warning = Assert.Single(node.Warnings, w => w.WarningType == "Expensive Operator");
        Assert.Equal(35, warning.RuleNumber);
    }
}
