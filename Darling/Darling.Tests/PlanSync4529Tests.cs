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
/// #4529 (PerformanceStudio#272 / erikdarlingdata/PerformanceStudio@5019883) — three cursor-aware
/// rules PM was missing:
///
/// <list type="bullet">
/// <item>Rule 36 (Dynamic Cursor): warns when <see cref="PlanStatement.CursorActualType"/> is
/// <c>Dynamic</c> — those cursors must tolerate data changes between fetches, which prevents many
/// index uses.</item>
/// <item>Rule 37 (Cursor Missing LOCAL): flags a <c>DECLARE ... CURSOR ... FOR</c> that has no
/// <c>LOCAL</c> between <c>CURSOR</c> and <c>FOR</c>. The regex is ported from
/// erikdarlingdata/PerformanceStudio@e8e5a21, not the original @5019883: that first pattern looked
/// for <c>LOCAL</c> BEFORE <c>CURSOR</c>, a position T-SQL never allows it in, so it fired on every
/// cursor declaration including ones already marked <c>LOCAL</c>. The corrected pattern looks between
/// <c>CURSOR</c> and the introducing <c>FOR</c>, and it scans the masked statement text
/// (<c>MaskCommentsAndLiterals</c>, #4524) so a <c>DECLARE</c> sitting inside a comment doesn't count.</item>
/// <item>Rule 11 (Scan With Predicate) augmentation: when the statement's cursor is Dynamic, the
/// message names the cursor as the likely cause instead of ending "Check that you have appropriate
/// indexes."</item>
/// </list>
/// </summary>
public sealed class PlanSync4529Tests
{
    private static ParsedPlan ParseAndAnalyze(string xml)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static PlanStatement SoleStatement(ParsedPlan plan) =>
        plan.Batches.SelectMany(b => b.Statements).Single();

    private const string SimpleSelectXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="{{TEXT}}" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static string SimpleSelect(string statementText) =>
        SimpleSelectXml.Replace("{{TEXT}}", System.Security.SecurityElement.Escape(statementText));

    // A StmtCursor plan whose sole operator is a rowstore scan with a residual predicate, so it
    // exercises rule 11 (Scan With Predicate) together with the cursor's CursorActualType.
    private const string CursorScanXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtCursor StatementText="DECLARE c CURSOR FOR SELECT a FROM dbo.t WHERE b = 1" StatementId="1" StatementCompId="1">
          <CursorPlan CursorName="c" CursorActualType="{{CURSOR_TYPE}}" CursorRequestedType="{{CURSOR_TYPE}}" CursorConcurrency="Optimistic" ForwardOnly="0">
            <Operation OperationType="Fetch">
              <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
                <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="100" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="10" ActualExecutions="1" ActualRowsRead="1000" ActualExecutionMode="Row"/></RunTimeInformation>
                  <TableScan>
                    <DefinedValues/>
                    <Predicate>
                      <ScalarOperator ScalarString="[dbo].[t].[b]=(1)"/>
                    </Predicate>
                    <Object Database="[db]" Schema="[dbo]" Table="[t]"/>
                  </TableScan>
                </RelOp>
              </QueryPlan>
            </Operation>
          </CursorPlan>
        </StmtCursor>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static string CursorScan(string cursorType) =>
        CursorScanXml.Replace("{{CURSOR_TYPE}}", cursorType);

    // --- Rule 36: Dynamic Cursor ---

    [Fact]
    public void Rule36_DynamicCursor_Warns()
    {
        var plan = ParseAndAnalyze(CursorScan("Dynamic"));
        var stmt = SoleStatement(plan);

        Assert.Contains(stmt.PlanWarnings, w => w.WarningType == "Dynamic Cursor");
    }

    [Fact]
    public void Rule36_StaticCursor_DoesNotWarn()
    {
        var plan = ParseAndAnalyze(CursorScan("Static"));
        var stmt = SoleStatement(plan);

        Assert.DoesNotContain(stmt.PlanWarnings, w => w.WarningType == "Dynamic Cursor");
    }

    // --- Rule 37: Cursor Missing LOCAL ---

    [Fact]
    public void Rule37_CursorDeclarationWithoutLocal_Warns()
    {
        var plan = ParseAndAnalyze(SimpleSelect("DECLARE c CURSOR FOR SELECT 1"));
        var stmt = SoleStatement(plan);

        Assert.Contains(stmt.PlanWarnings, w => w.WarningType == "Cursor Missing LOCAL");
    }

    [Fact]
    public void Rule37_CursorDeclarationWithLocal_DoesNotWarn()
    {
        var plan = ParseAndAnalyze(SimpleSelect("DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT 1"));
        var stmt = SoleStatement(plan);

        Assert.DoesNotContain(stmt.PlanWarnings, w => w.WarningType == "Cursor Missing LOCAL");
    }

    [Fact]
    public void Rule37_CursorDeclarationInsideCommentOnly_DoesNotWarn()
    {
        // The masked text (#4524) blanks comment contents, so a DECLARE that only exists
        // inside a -- comment must not be seen as a real cursor declaration.
        var plan = ParseAndAnalyze(SimpleSelect("-- DECLARE c CURSOR FOR SELECT 1\nSELECT 1"));
        var stmt = SoleStatement(plan);

        Assert.DoesNotContain(stmt.PlanWarnings, w => w.WarningType == "Cursor Missing LOCAL");
    }

    // --- Rule 11 augmentation: dynamic cursor named as the cause ---

    [Fact]
    public void Rule11_ScanWithPredicateOnDynamicCursor_NamesTheCursor()
    {
        var plan = ParseAndAnalyze(CursorScan("Dynamic"));
        var stmt = SoleStatement(plan);
        var scanNode = stmt.RootNode!.Children.Single();

        var warning = Assert.Single(scanNode.Warnings, w => w.WarningType == "Scan With Predicate");
        Assert.Contains("running inside a dynamic cursor", warning.Message);
        Assert.DoesNotContain("Check that you have appropriate indexes.", warning.Message);
    }

    [Fact]
    public void Rule11_ScanWithPredicateOnStaticCursor_KeepsTheIndexAdvice()
    {
        var plan = ParseAndAnalyze(CursorScan("Static"));
        var stmt = SoleStatement(plan);
        var scanNode = stmt.RootNode!.Children.Single();

        var warning = Assert.Single(scanNode.Warnings, w => w.WarningType == "Scan With Predicate");
        Assert.Contains("Check that you have appropriate indexes.", warning.Message);
        Assert.DoesNotContain("dynamic cursor", warning.Message);
    }
}
