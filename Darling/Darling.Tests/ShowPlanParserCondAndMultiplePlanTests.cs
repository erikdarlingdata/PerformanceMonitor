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
/// #4468 — <see cref="ShowPlanParser.Parse"/> dropped two kinds of statement that carry their own plan or
/// hashes: a <c>StmtCond</c> (<c>IF EXISTS (...)</c>) whose condition's own <c>QueryPlan</c> lives under
/// <c>Condition</c>, and a <c>StmtSimple</c> with <c>StatementType="MULTIPLE PLAN"</c> that carries
/// <c>QueryHash</c>/<c>QueryPlanHash</c> but no <c>QueryPlan</c> child.
///
/// <para>The <c>Condition</c> element (StmtCondType/Condition per the showplan XSD) holds the condition's own
/// <c>QueryPlan</c> (0 or 1) plus optional <c>UDF</c> sub-plans — never a nested <c>Stmt*</c> element. Before the
/// fix, the old code fed each of <c>Condition</c>'s children into the same recursive statement parser used for
/// <c>Then</c>/<c>Else</c>, so the <c>QueryPlan</c> element itself was handed to <c>ParseStatement</c>, which has
/// no <c>QueryPlan</c> child of its own and fell into the no-plan placeholder path: empty <c>StatementType</c>,
/// null hashes, a bare <c>STATEMENT</c> root, and the missing index and its warning silently dropped.</para>
///
/// <para>Separately, <c>ParseStatement</c> read <c>QueryHash</c>/<c>QueryPlanHash</c> (and the rest of
/// <c>ParseStmtAttributes</c>) only after confirming a <c>QueryPlan</c> child existed, so a plan-less
/// <c>MULTIPLE PLAN</c> statement lost hashes it actually carries in the XML.</para>
///
/// <para>The repro XML is the issue's own fixture, synthetic (<c>dbo.t</c>, <c>dbo.u</c>, <c>[db]</c>).</para>
/// </summary>
public sealed class ShowPlanParserCondAndMultiplePlanTests
{
    private const string ReproXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtCond StatementText="IF EXISTS (SELECT 1 FROM dbo.t WHERE id = @id)" StatementId="1" StatementCompId="1" StatementType="COND WITH QUERY" RetrievedFromCache="true" QueryHash="0x1111111111111111" QueryPlanHash="0x2222222222222222">
          <Condition><QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <MissingIndexes><MissingIndexGroup Impact="90.5"><MissingIndex Database="[db]" Schema="[dbo]" Table="[t]"><ColumnGroup Usage="EQUALITY"><Column Name="[id]" ColumnId="1"/></ColumnGroup></MissingIndex></MissingIndexGroup></MissingIndexes>
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row"><OutputList/><TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore"><DefinedValues/><Object Database="[db]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/></TableScan></RelOp>
          </QueryPlan></Condition>
          <Then><Statements><StmtSimple StatementText="RETURN" StatementId="2" StatementCompId="2" StatementType="RETURN NONE"/></Statements></Then>
        </StmtCond>
        <StmtSimple StatementText="SELECT c FROM dbo.u WHERE k = @k" StatementId="3" StatementCompId="3" StatementType="MULTIPLE PLAN" RetrievedFromCache="true" QueryHash="0x3333333333333333" QueryPlanHash="0x4444444444444444"/>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static ParsedPlan ParseAndAnalyze()
    {
        var plan = ShowPlanParser.Parse(ReproXml);
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    [Fact]
    public void StatementCount_IsThree_OneConditionOneThenOneMultiplePlan()
    {
        // StmtCond's own condition plan (1) + the Then branch's RETURN (1) + the sibling
        // MULTIPLE PLAN statement (1) = 3. The condition never nests a Stmt* per the XSD, so
        // it contributes exactly one statement, not zero (dropped) and not two (double-counted).
        var plan = ParseAndAnalyze();
        var statements = plan.Batches.SelectMany(b => b.Statements).ToList();
        Assert.Equal(3, statements.Count);
    }

    [Fact]
    public void CondWithQuery_KeepsItsOwnHashesAndRealOperatorRoot()
    {
        var plan = ParseAndAnalyze();
        var stmt = plan.Batches.SelectMany(b => b.Statements)
            .Single(s => s.StatementType == "COND WITH QUERY");

        Assert.Equal("0x1111111111111111", stmt.QueryHash);
        Assert.Equal("0x2222222222222222", stmt.QueryPlanHash);

        // The synthetic statement-type wrapper node holds the real Table Scan as its child,
        // not a bare STATEMENT placeholder — the operator tree survived.
        Assert.NotNull(stmt.RootNode);
        var operatorChild = Assert.Single(stmt.RootNode!.Children);
        Assert.Equal("Table Scan", operatorChild.PhysicalOp);
    }

    [Fact]
    public void CondWithQuery_KeepsItsMissingIndexSuggestion()
    {
        var plan = ParseAndAnalyze();
        var stmt = plan.Batches.SelectMany(b => b.Statements)
            .Single(s => s.StatementType == "COND WITH QUERY");

        var mi = Assert.Single(stmt.MissingIndexes);
        Assert.Equal("dbo", mi.Schema);
        Assert.Equal("t", mi.Table);
        Assert.Equal(90.5, mi.Impact);
        Assert.Equal("id", Assert.Single(mi.EqualityColumns));

        // Surfaces through the batch-wide rollup the drill-down collectors and the MCP
        // formatter both read (AllMissingIndexes / PlanAdvisoryAggregator) — not just parsed
        // onto the statement and never surfaced anywhere.
        Assert.Single(plan.AllMissingIndexes);
    }

    [Fact]
    public void ThenBranch_StillHasItsReturnStatement()
    {
        var plan = ParseAndAnalyze();
        var stmt = plan.Batches.SelectMany(b => b.Statements)
            .Single(s => s.StatementType == "RETURN NONE");

        Assert.Equal("RETURN", stmt.StatementText);
    }

    [Fact]
    public void MultiplePlan_KeepsItsHashesAndPlaceholderRoot()
    {
        var plan = ParseAndAnalyze();
        var stmt = plan.Batches.SelectMany(b => b.Statements)
            .Single(s => s.StatementType == "MULTIPLE PLAN");

        Assert.Equal("0x3333333333333333", stmt.QueryHash);
        Assert.Equal("0x4444444444444444", stmt.QueryPlanHash);

        // No QueryPlan child in the XML, so the placeholder root is kept (no operator tree to show).
        Assert.NotNull(stmt.RootNode);
        Assert.Equal("MULTIPLE PLAN", stmt.RootNode!.PhysicalOp);
    }
}
