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
/// #4525 (PerformanceStudio#584) — Rule 23 (Table-Valued Function) fired on every
/// <c>LogicalOp="Table-valued function"</c> operator, including the engine's own functions:
/// <c>STRING_SPLIT</c>, <c>OPENJSON</c>, <c>GENERATE_SERIES</c>, and every DMV/DMF. Those run as
/// the same operator, but their <c>Object</c> element names no database and no schema — a
/// function the user wrote always has both. <see cref="PlanNode.SchemaName"/> now carries the
/// schema next to <see cref="PlanNode.DatabaseName"/> (both set from the parsed <c>Object</c>
/// element), and rule 23 skips an operator with neither.
///
/// <para>The repro XML is a synthetic actual plan, one variant with no <c>Object</c> element (as
/// the engine's own functions appear) and one with a <c>dbo</c>/<c>[db]</c> user function.</para>
/// </summary>
public sealed class PlanSync4525Tests
{
    private const string EngineFunctionXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT value FROM STRING_SPLIT('a,b', ',')" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Table-valued function" LogicalOp="Table-valued function" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableValuedFunction><DefinedValues/><Object Table="[STRING_SPLIT]"/></TableValuedFunction>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private const string UserFunctionXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.MyTvf(1)" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Table-valued function" LogicalOp="Table-valued function" EstimateRows="100" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableValuedFunction><DefinedValues/><Object Database="[db]" Schema="[dbo]" Table="[MyTvf]"/></TableValuedFunction>
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

    private static System.Collections.Generic.IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }

    // RootNode is the synthetic statement wrapper (NodeId == -1); the parsed Table-valued
    // function operator is its child.
    private static PlanNode FindTvfNode(ParsedPlan plan)
    {
        var root = plan.Batches.SelectMany(b => b.Statements).Single().RootNode!;
        return Flatten(root).Single(n => n.LogicalOp == "Table-valued function");
    }

    [Fact]
    public void Rule23_EngineFunctionWithNoDatabaseAndNoSchema_DoesNotWarn()
    {
        var plan = ParseAndAnalyze(EngineFunctionXml);
        var node = FindTvfNode(plan);

        Assert.Null(node.DatabaseName);
        Assert.DoesNotContain(node.Warnings, w => w.WarningType == "Table-Valued Function");
    }

    [Fact]
    public void Rule23_UserFunctionWithDatabaseAndSchema_StillWarns()
    {
        var plan = ParseAndAnalyze(UserFunctionXml);
        var node = FindTvfNode(plan);

        Assert.Equal("db", node.DatabaseName);
        Assert.Contains(node.Warnings, w => w.WarningType == "Table-Valued Function");
    }

    // Compile-only on dev: PlanNode.SchemaName is a new member (#4525 / PS#584); the parser
    // now sets it next to DatabaseName from the same <Object> element.
    [Fact]
    public void Parser_SetsSchemaName_NextToDatabaseName()
    {
        var enginePlan = ParseAndAnalyze(EngineFunctionXml);
        var engineNode = FindTvfNode(enginePlan);
        Assert.Null(engineNode.SchemaName);

        var userPlan = ParseAndAnalyze(UserFunctionXml);
        var userNode = FindTvfNode(userPlan);
        Assert.Equal("dbo", userNode.SchemaName);
    }
}
